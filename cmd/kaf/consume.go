package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/Shopify/sarama"
	"github.com/birdayz/kaf/pkg/avro"
	"github.com/birdayz/kaf/pkg/proto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/hokaccha/go-prettyjson"
	"github.com/mattn/go-colorable"
	"github.com/spf13/cobra"
)

var (
	partitionsWhitelistFlag []int
	offsetFlags             []string
	countFlag               int
	countsFlags             []int
	endFlag                 bool
	raw                     bool
	followFlag              bool
	tailFlag                int
	skipFlag                int
	groupFlag               string
	groupCommitFlag         bool
	tailsFlags              []int
	skipsFlags              []int
	schemaCache             *avro.SchemaCache
	keyfmt                  *prettyjson.Formatter

	protoType    string
	keyProtoType string

	flagPartitions []int32

	limitMessagesFlag int64

	reg *proto.DescriptorRegistry
)

func init() {
	rootCmd.AddCommand(consumeCmd)
	consumeCmd.Flags().StringSliceVarP(&offsetFlags, "offset", "o", []string{},
		`Offset to start consuming. Takes a single or partitioned value.
Possible values: oldest, newest, -X, +X, X. (X is a number)
oldest: Start reading at the oldest offset. This is the default if skip/take are undefined.
newest: Start reading at the newest offset.
X: Start reading at offset X.`)
	consumeCmd.Flags().BoolVar(&raw, "raw", false, "Print raw output of messages, without key or prettified JSON")
	consumeCmd.Flags().BoolVarP(&followFlag, "follow", "f", false, "Shorthand to start consuming with offset HEAD-1 on each partition. Overrides --offset flag")
	consumeCmd.Flags().StringSliceVar(&protoFiles, "proto-include", []string{}, "Path to proto files")
	consumeCmd.Flags().StringSliceVar(&protoExclude, "proto-exclude", []string{}, "Proto exclusions (path prefixes)")
	consumeCmd.Flags().StringVar(&protoType, "proto-type", "", "Fully qualified name of the proto message type. Example: com.test.SampleMessage")
	consumeCmd.Flags().StringVar(&keyProtoType, "key-proto-type", "", "Fully qualified name of the proto key type. Example: com.test.SampleMessage")
	consumeCmd.Flags().Int32SliceVarP(&flagPartitions, "partitions", "p", []int32{}, "Partitions to consume from")
	consumeCmd.Flags().Int64VarP(&limitMessagesFlag, "limit-messages", "l", 0, "Limit messages per partition")
	consumeCmd.Flags().StringVarP(&groupFlag, "group", "g", "", "Consumer Group to use for consume")
	consumeCmd.Flags().BoolVar(&groupCommitFlag, "commit", false, "Commit Group offset after receiving messages. Works only if consuming as Consumer Group")

	consumeCmd.Flags().IntVarP(&tailFlag, "tail", "t", 0, "Reads the last X messages across all partitions. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVarP(&tailsFlags, "tails", "T", []int{}, "Reads the last X messages on each partition. Takes a single or partitioned value.")
	consumeCmd.Flags().IntVarP(&skipFlag, "skip", "s", 0, "Skips the first X messages across all partitions. Overrides --offset flag.")
	consumeCmd.Flags().IntSliceVarP(&skipsFlags, "skips", "S", []int{}, "Skips the first X messages on each partition. Takes a single or partitioned value. ")
	consumeCmd.Flags().IntVarP(&countFlag, "count", "n", 0, "Number of messages to consume before exiting.")
	consumeCmd.Flags().IntSliceVarP(&countsFlags, "counts", "N", []int{}, "Number of messages to consume per partition before stopping. Takes a single or partitioned value.")
	consumeCmd.Flags().IntSliceVarP(&partitionsWhitelistFlag, "partitions", "p", []int{}, "Partitions to read from as a comma-delimited list; if unset, all partitions will be read.")
	consumeCmd.Flags().BoolVarP(&endFlag, "end", "e", false, "Stop reading after reaching the end of the partition(s).")

	keyfmt = prettyjson.NewFormatter()
	keyfmt.Newline = " " // Replace newline with space to avoid condensed output.
	keyfmt.Indent = 0
}

func resolveOffset(
	client sarama.Client, topic string, partition int, flag string,
) (int64, error) {

	switch flag {
	case "oldest", "":
		return sarama.OffsetOldest, nil
	case "newest":
		return sarama.OffsetNewest, nil
	default:

		var err error
		var offset, min, max int64

		isSkip := flag[0] == '+'
		isTail := flag[0] == '-'
		numStr := flag
		if isSkip || isTail {
			numStr = flag[1:]
		}
		offset, err = strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid offset flag %q: %s", flag, err)
		}
		min, err = client.GetOffset(topic, int32(partition), sarama.OffsetOldest)
		if err != nil {
			return 0, err
		}
		max, err = client.GetOffset(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			return 0, err
		}

		logFmt := flag

		if isSkip {
			offset = min + offset
			logFmt = fmt.Sprintf("skip first %s", numStr)
		} else if isTail {
			offset = max - offset
			logFmt = fmt.Sprintf("take last %s", numStr)
		} else {
			logFmt = fmt.Sprintf("use offset %s", numStr)
		}
		if offset > max {
			fmt.Fprintf(os.Stderr, "Attempting to %s on partition %d (%d...%d) gives %d, which is after the max offset. Using the max offset instead.\n", logFmt, partition, min, max, offset)
			offset = max
		}
		if offset < min {
			fmt.Fprintf(os.Stderr, "Attempting to %s on partition %d (%d...%d) gives %d, which is before the min offset. Using the min offset instead.\n", logFmt, partition, min, max, offset)
			offset = min
		}

		return offset, nil
	}
}

const (
	offsetsRetry = 500 * time.Millisecond
)

var consumeCmd = &cobra.Command{
	Use:               "consume TOPIC",
	Short:             "Consume messages",
	Args:              cobra.ExactArgs(1),
	ValidArgsFunction: validTopicArgs,
	PreRun:            setupProtoDescriptorRegistry,
	Run: func(cmd *cobra.Command, args []string) {
		var offset int64
		switch offsetFlag {
		case "oldest":
			offset = sarama.OffsetOldest
		case "newest":
			offset = sarama.OffsetNewest
		default:
			// TODO: normally we would parse this to int64 but it's
			// difficult as we can have multiple partitions. need to
			// find a way to give offsets from CLI with a good
			// syntax.
			offset = sarama.OffsetNewest
		}
		cfg := getConfig()
		cfg.Consumer.Offsets.Initial = offset
		topic := args[0]
		client := getClientFromConfig(cfg)

		if groupFlag != "" {
			withConsumerGroup(cmd.Context(), client, topic, groupFlag)
		} else {
			withoutConsumerGroup(cmd.Context(), client, topic, offset)
		}
	},
}

type g struct{}

func (g *g) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *g) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (g *g) ConsumeClaim(s sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for msg := range claim.Messages() {
		handleMessage(msg, &mu)
		if groupCommitFlag {
			s.MarkMessage(msg, "")
		}
	}
	return nil
}

func withConsumerGroup(ctx context.Context, client sarama.Client, topic, group string) {
	cg, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		errorExit("Failed to create consumer group: %v", err)
	}

	err = cg.Consume(ctx, []string{topic}, &g{})
	if err != nil {
		errorExit("Error on consume: %v", err)
	}
}

func withoutConsumerGroup(ctx context.Context, client sarama.Client, topic string, offset int64) {
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		errorExit("Unable to create consumer from client: %v\n", err)
	}

	var partitions []int32
	if len(flagPartitions) == 0 {
		partitions, err = consumer.Partitions(topic)
		if err != nil {
			errorExit("Unable to get partitions: %v\n", err)
		}
	} else {
		partitions = flagPartitions
	}

	schemaCache = getSchemaCache()
	var partitions []int

	if len(partitionsWhitelistFlag) > 0 {
		// filter the partitions but keep the order the user specified
		foundPartitionMap := make(map[int32]bool)
		for _, partition := range foundPartitions {
			foundPartitionMap[partition] = true
		}
		for _, partition := range partitionsWhitelistFlag {
			if !foundPartitionMap[int32(partition)] {
				errorExit("Requested partition %d was not found.", partition)
			}
			partitions = append(partitions, partition)
		}
	} else {
		for _, partition := range foundPartitions {
			partitions = append(partitions, int(partition))
		}
		// make sure partitions are sorted in a reasonable order if the user provided partitioned arguments
		sort.Ints(partitions)
	}

	opts := createOptions(partitions)

	offsetFlagsMap := opts.offsetFlagsMap
	countFlagsMap := opts.countsMap
	globalLimit := int64(opts.globalCount)
	hasGlobalLimit := globalLimit > 0

	// counter used for writing progress to stderr (needed because of buffered tabwriter)
	var globalProgress int64 = 0 // int64 because it's updated by atomic.AddInt64
	// counter used to track how many messages have been written to std out
	var globalWrittenCount int64 = 0

	wg := sync.WaitGroup{}
	mu := sync.Mutex{} // Synchronizes stderr and stdout.
	for _, partition := range partitions {

		wg.Add(1)

		go func(partition int32, offset int64) {
			req := &sarama.OffsetRequest{
				Version: int16(1),
			}
			req.AddBlock(topic, partition, int64(-1), int32(0))
			ldr, err := client.Leader(topic, partition)
			if err != nil {
				errorExit("Unable to get leader: %v\n", err)
			}

			offsets, err := getAvailableOffsetsRetry(ldr, req, offsetsRetry)
			if err != nil {
				errorExit("Unable to get available offsets: %v\n", err)
			}
			followOffset := offsets.GetBlock(topic, partition).Offset - 1

			if follow && followOffset > 0 {
				offset = followOffset
				fmt.Fprintf(errWriter, "Starting on partition %v with offset %v\n", partition, offset)
			}

			pc, err := consumer.ConsumePartition(topic, partition, offset)
			if err != nil {
				errorExit("Unable to consume partition: %v %v %v %v\n", topic, partition, offset, err)
			}

			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-pc.Messages():
					handleMessage(msg, &mu)
					if limitMessagesFlag > 0 && msg.Offset >= offset+limitMessagesFlag {
						return
						// go func(partition int) {

						// 	offsetFlag := offsetFlagsMap[partition]
						// 	partitionLimit, hasPartitionLimit := countFlagsMap[partition]
						// 	offset, err := resolveOffset(client, topic, partition, offsetFlag)
						// 	if err != nil {
						// 		errorExit("Unable to get offsets for partition %d: %v.", partition, err)
						// 	}

						// 	formattedCount := "∞"
						// 	if hasGlobalLimit {
						// 		formattedCount = fmt.Sprintf("at most %d", globalLimit)
						// 	}
						// 	if hasPartitionLimit {
						// 		formattedCount = fmt.Sprintf("%d", partitionLimit)
						// 	}

						// 	var formattedOffset string
						// 	switch offset {
						// 	case sarama.OffsetNewest:
						// 		formattedOffset = "newest"
						// 	case sarama.OffsetOldest:
						// 		formattedOffset = "oldest"
						// 	default:
						// 		formattedOffset = fmt.Sprintf("%v", offset)
						// 	}

						// 	fmt.Fprintf(os.Stderr, "Starting to read %s messages on partition %v at offset %v\n", formattedCount, partition, formattedOffset)

						// 	pc, err := consumer.ConsumePartition(topic, int32(partition), offset)
						// 	if err != nil {
						// 		if strings.Contains(err.Error(), "outside the range of offsets maintained") {
						// 			fmt.Fprintf(os.Stderr, "Unable to consume partition %d starting at offset %d; will use newest offset; error was: %v\n", partition, offset, err)
						// 			pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
						// 		}
						// 		if err != nil {
						// 			errorExit("Unable to consume partition %d starting at offset %d: %v\n", partition, offset, err)
						// 		}
						// 	}

						// 	partitionCount := 0

						// 	for msg := range pc.Messages() {

						// 		var stderr bytes.Buffer

						// 		// TODO make this nicer
						// 		var dataToDisplay []byte
						// 		if protoType != "" {
						// 			dataToDisplay, err = protoDecode(reg, msg.Value, protoType)
						// 			if err != nil {
						// 				fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary output. Error: %v", err)
						// 			}
						// 		} else {
						// 			dataToDisplay, err = avroDecode(msg.Value)
						// 			if err != nil {
						// 				fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
						// 			}
						// 		}

						// 		if !raw {
						// 			formatted, err := prettyjson.Format(dataToDisplay)
						// 			if err == nil {
						// 				dataToDisplay = formatted
						// 			}

						// 			w := tabwriter.NewWriter(&stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

						// 			if len(msg.Headers) > 0 {
						// 				fmt.Fprintf(w, "Headers:\n")
						// 			}

						// 			for _, hdr := range msg.Headers {
						// 				var hdrValue string
						// 				// Try to detect azure eventhub-specific encoding
						// 				if len(hdr.Value) > 0 {
						// 					switch hdr.Value[0] {
						// 					case 161:
						// 						hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
						// 					case 131:
						// 						hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
						// 					default:
						// 						hdrValue = string(hdr.Value)
						// 					}
						// 				}

						// 				fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

						// 			}

						// 			if msg.Key != nil && len(msg.Key) > 0 {

						// 				key, err := avroDecode(msg.Key)
						// 				if err != nil {
						// 					fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
						// 				}
						// 				fmt.Fprintf(w, "Key:\t%v\n", formatKey(key))
						// 			}

						// 			fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
						// 			partitionCount++
						// 			if hasPartitionLimit {
						// 				fmt.Fprintf(w, "Progress:\t%d/%d\n", partitionCount, partitionLimit)
						// 			}
						// 			if hasGlobalLimit {
						// 				count := atomic.AddInt64(&globalProgress, 1)
						// 				fmt.Fprintf(w, "Progress: \t%d/%d\n", count, globalLimit)
						// 			}
						// 			w.Flush()
					}
				}
			}
		}(partition, offset)
	}
	wg.Wait()
}

func handleMessage(msg *sarama.ConsumerMessage, mu *sync.Mutex) {
	var stderr bytes.Buffer

	var dataToDisplay []byte
	var keyToDisplay []byte
	var err error

	if protoType != "" {
		dataToDisplay, err = protoDecode(reg, msg.Value, protoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		dataToDisplay, err = avroDecode(msg.Value)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if keyProtoType != "" {
		keyToDisplay, err = protoDecode(reg, msg.Key, keyProtoType)
		if err != nil {
			fmt.Fprintf(&stderr, "failed to decode proto key. falling back to binary outputla. Error: %v\n", err)
		}
	} else {
		keyToDisplay, err = avroDecode(msg.Key)
		if err != nil {
			fmt.Fprintf(&stderr, "could not decode Avro data: %v\n", err)
		}
	}

	if !raw {
		if isJSON(dataToDisplay) {
			dataToDisplay = formatValue(dataToDisplay)
		}

		if isJSON(keyToDisplay) {
			keyToDisplay = formatKey(keyToDisplay)
		}

		w := tabwriter.NewWriter(&stderr, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)

		if len(msg.Headers) > 0 {
			fmt.Fprintf(w, "Headers:\n")
		}

		for _, hdr := range msg.Headers {
			var hdrValue string
			// Try to detect azure eventhub-specific encoding
			if len(hdr.Value) > 0 {
				switch hdr.Value[0] {
				case 161:
					hdrValue = string(hdr.Value[2 : 2+hdr.Value[1]])
				case 131:
					hdrValue = strconv.FormatUint(binary.BigEndian.Uint64(hdr.Value[1:9]), 10)
				default:
					hdrValue = string(hdr.Value)
					partitionDone := func() bool {
						mu.Lock()
						defer mu.Unlock()
						stderr.WriteTo(os.Stderr)
						colorable.NewColorableStdout().Write(dataToDisplay)
						fmt.Print("\n")
						if hasGlobalLimit {
							// normal increment because we're under lock
							globalWrittenCount++
							if globalWrittenCount >= globalLimit {
								fmt.Fprintf(os.Stderr, "Reached requested message count of %d.\n", globalLimit)
								os.Exit(0)
							}
						}
						if hasPartitionLimit && partitionCount >= partitionLimit {
							fmt.Fprintf(os.Stderr, "Reached requested message count of %d on partition %d.\n", partitionLimit, partition)
							return true
						}
						if endFlag && msg.Offset+1 == pc.HighWaterMarkOffset() {
							fmt.Fprintf(os.Stderr, "Reached end of partition %d (read %d messages).\n", partition, partitionCount)
							return true
						}
						return false
					}()
					if partitionDone {
						pc.Close()
						break
					}
				}
			}

			fmt.Fprintf(w, "\tKey: %v\tValue: %v\n", string(hdr.Key), hdrValue)

		}

		if msg.Key != nil && len(msg.Key) > 0 {
			fmt.Fprintf(w, "Key:\t%v\n", string(keyToDisplay))
		}
		fmt.Fprintf(w, "Partition:\t%v\nOffset:\t%v\nTimestamp:\t%v\n", msg.Partition, msg.Offset, msg.Timestamp)
		w.Flush()
	}

	mu.Lock()
	stderr.WriteTo(errWriter)
	_, _ = colorableOut.Write(dataToDisplay)
	fmt.Fprintln(outWriter)
	mu.Unlock()

}

// proto to JSON
func protoDecode(reg *proto.DescriptorRegistry, b []byte, _type string) ([]byte, error) {
	dynamicMessage := reg.MessageForType(_type)
	if dynamicMessage == nil {
		return b, nil
	}

	err := dynamicMessage.Unmarshal(b)
	if err != nil {
		return nil, err
	}

	var m jsonpb.Marshaler
	var w bytes.Buffer

	err = m.Marshal(&w, dynamicMessage)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil

}

func avroDecode(b []byte) ([]byte, error) {
	if schemaCache != nil {
		return schemaCache.DecodeMessage(b)
	}
	return b, nil
}

func formatKey(key []byte) []byte {
	if b, err := keyfmt.Format(key); err == nil {
		return b
	}
	return key

}

func formatValue(key []byte) []byte {
	if b, err := prettyjson.Format(key); err == nil {
		return b
	}
	return key
}

func isJSON(data []byte) bool {
	var i interface{}
	if err := json.Unmarshal(data, &i); err == nil {
		return true
	}
	return false
}

type consumeOptions struct {
	offsetFlagsMap map[int]string
	countsMap      map[int]int
	globalCount    int
}

// createOptions validates the flags and normalizes them into
// a single offset flag map the command can use
func createOptions(partitions []int) consumeOptions {

	opts := consumeOptions{
		offsetFlagsMap: map[int]string{},
		globalCount:    countFlag,
		countsMap:      map[int]int{},
	}

	numPartitions := len(partitions)

	// validate mutually exclusive flags
	var exclusive []string
	if tailFlag > 0 {
		exclusive = append(exclusive, "tail")
	}
	if len(tailsFlags) > 0 {
		exclusive = append(exclusive, "tails")
	}
	if skipFlag > 0 {
		exclusive = append(exclusive, "skip")
	}
	if len(skipsFlags) > 0 {
		exclusive = append(exclusive, "skips")
	}
	if followFlag {
		exclusive = append(exclusive, "follow")
	}
	if len(offsetFlags) > 0 {
		exclusive = append(exclusive, "offset")
	}
	if len(exclusive) > 1 {
		errorExit("multiple mutually exclusive offset-related flags provided, please only set one (flags set: %v)\n", exclusive)
	}

	if countFlag > 0 && len(countsFlags) > 0 {
		errorExit("--count and --counts are mutually exclusive, please only set one\n")
	}
	switch len(countsFlags) {
	case 0:
	case 1:
		commonCount := countsFlags[0]
		for _, partition := range partitions {
			opts.countsMap[partition] = commonCount
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.countsMap[partition] = countsFlags[i]
		}
	default:
		errorExit("--counts takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(tailsFlags))
	}

	if followFlag {
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = "-1"
		}
	}

	if tailFlag > 0 {
		tailsFlags = make([]int, numPartitions)
		countdown := tailFlag
		for countdown > 0 {
			for i := 0; i < len(partitions) && countdown > 0; i++ {
				tailsFlags[i]++
				countdown--
			}
		}
	}

	switch len(tailsFlags) {
	case 0:
	case 1:
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("-%d", tailsFlags[0])
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("-%d", tailsFlags[i])
		}
	default:
		errorExit("--tails takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(tailsFlags))
	}

	if skipFlag > 0 {
		skipsFlags = make([]int, numPartitions)
		countdown := skipFlag
		for countdown > 0 {
			for i := 0; i < len(partitions) && countdown > 0; i++ {
				skipsFlags[i]++
				countdown--
			}
		}
	}
	switch len(skipsFlags) {
	case 0:
	case 1:
		for _, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("+%d", skipsFlags[0])
		}
	case numPartitions:
		for i, partition := range partitions {
			opts.offsetFlagsMap[partition] = fmt.Sprintf("+%d", skipsFlags[i])
		}
	default:
		errorExit("--skips takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(skipsFlags))
	}

	if len(opts.offsetFlagsMap) == 0 {
		// only use offsetFlags if skip and tail unset
		switch len(offsetFlags) {
		case 0:
			// no other offsetting flags set, default to "oldest"
			for _, partition := range partitions {
				opts.offsetFlagsMap[partition] = "oldest"
			}
		case 1:
			for _, partition := range partitions {
				opts.offsetFlagsMap[partition] = offsetFlags[0]
			}
		case numPartitions:
			for i, partition := range partitions {
				opts.offsetFlagsMap[partition] = offsetFlags[i]
			}
		default:
			errorExit("--offsets takes 1 value or a number of values equal to the number of partitions (have %d partitions, got %d values)", numPartitions, len(offsetFlags))

		}
	}

	return opts
}

build:
	go build -mod vendor -ldflags "-w -s" ./cmd/kaf
install:
	go install -mod vendor -ldflags "-w -s" ./cmd/kaf
release:
	goreleaser --rm-dist
run-kafka:
	docker-compose up -d
bare: build
	scp ./kaf aws-carbon-bastion:/home/ec2-user/alex
	# rm ./kaf

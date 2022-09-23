.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-hubspot.version=${VERSION}'" -o conduit-connector-hubspot cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run

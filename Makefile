.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-hubspot.version=${VERSION}'" -o conduit-connector-hubspot cmd/connector/main.go
	cp conduit-connector-hubspot ${GOPATH}/src/github.com/conduitio/conduit/connectors/hubspot

test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
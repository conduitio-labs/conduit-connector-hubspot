.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-hubspot.version=${VERSION}'" -o conduit-connector-hubspot cmd/connector/main.go

# if you plan to run integration tests it's recommended to do that with -p=1 specified.
test:
	go test $(GOTEST_FLAGS) ./...

lint:
	golangci-lint run

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
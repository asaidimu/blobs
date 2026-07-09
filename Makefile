.PHONY: all build test

all: build

build:
	go build -v ./...

test:
	go clean -testcache && go test -v ./...

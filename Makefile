.PHONY: all build run test clean fmt fix

BINARY_NAME=fathom

all: build

build:
	go build -o bin/$(BINARY_NAME) cmd/fathom/main.go

run: build
	./bin/$(BINARY_NAME) --config configs/config.yaml

test:
	go test -v ./...

fmt:
	go fmt ./...

fix: fmt
	go vet ./...
	go test ./...

clean:
	rm -f bin/$(BINARY_NAME)

.PHONY: all build run test clean fmt

BINARY_NAME=fathom

all: build

build:
	go build -o $(BINARY_NAME) cmd/fathom/main.go

run: build
	./$(BINARY_NAME) --config configs/config.yaml

test:
	go test -v ./...

fmt:
	go fmt ./...

clean:
	rm -f $(BINARY_NAME)

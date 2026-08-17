# Linux amd64 binary for the sandbox Compose mount.
.PHONY: all test linux help

BIN := bin/baselines

all: test

help:
	@echo "make test   go test ./..."
	@echo "make linux  Linux amd64 binary -> bin/baselines"

test:
	go test ./...

ifeq ($(OS),Windows_NT)
linux:
	cmd /C "if not exist bin mkdir bin"
	cmd /C "set CGO_ENABLED=0&& set GOOS=linux&& set GOARCH=amd64&& go build -o bin/baselines ./cmd/baselines"
else
linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BIN) ./cmd/baselines
endif

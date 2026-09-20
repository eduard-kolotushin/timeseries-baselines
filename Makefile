# Linux amd64 binary for the sandbox Compose mount.
#
# The recipes need a POSIX shell — make picks Git Bash's sh.exe on Windows — so both
# platforms run the same lines. The old `ifeq ($(OS),Windows_NT)` branch used
# `cmd /C "…"`, and that shell rewrites `/C` into a path, so `make linux` was a silent
# no-op on Windows: the sandbox's `make up` then mounted whatever binary was already
# in bin/.
.PHONY: all test linux help

BIN := bin/baselines

all: test

help:
	@echo "make test   go test ./..."
	@echo "make linux  Linux amd64 binary -> bin/baselines"

test:
	go test ./...

linux:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BIN) ./cmd/baselines

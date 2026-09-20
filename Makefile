# Linux amd64 binary for the sandbox Compose mount.
#
# The recipes are POSIX. make runs them through sh.exe when PATH has one (a Git Bash
# session) and through cmd.exe otherwise (PowerShell or cmd, where Git for Windows only
# adds its `cmd` directory and sh.exe stays invisible), and `mkdir -p` is a cmd syntax
# error. Pin Git's shell so every launcher runs the same recipes;
# `make SHELL=/path/to/sh.exe` overrides it (make quotes the path itself).
ifeq ($(OS),Windows_NT)
GIT_SH := $(if $(wildcard C:/Program\ Files/Git/bin/sh.exe),C:/Program Files/Git/bin/sh.exe,$(wildcard $(subst \,/,$(LOCALAPPDATA))/Programs/Git/bin/sh.exe))
ifneq ($(strip $(GIT_SH)),)
SHELL := $(GIT_SH)
endif
endif

.PHONY: all test linux help

BIN := bin/baselines

all: test

help:
	@echo "make test   go test ./..."
	@echo "make linux  Linux amd64 binary -> bin/baselines"

test:
	go test ./...

# The quoted paths keep these lines on the shell: Windows make runs a line directly when
# it has no shell character, and a direct `mkdir -p bin` fails with
# "process_begin: CreateProcess … failed".
linux:
	mkdir -p "bin"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$(BIN)" ./cmd/baselines

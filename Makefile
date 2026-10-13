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

.PHONY: all test linux migrate gate-check deps-report help

BIN := bin/baselines
BIN_MIGRATE := bin/baselines-migrate

# Cold-cache scratch for the company gate: the module cache `make gate-check` fills and the
# inventory `make deps-report` writes. See docs/COMPANY-BUILD.md.
GATE_CACHE ?= .gate-cache

# The module cache is deliberately outside the repo: an in-tree cache is walked by gofmt-style
# recursive tools (which do not honour the go tool's dot-directory rule), while a temp dir keeps
# `gofmt -l .` exactly the check CI runs.
GATE_MODCACHE ?= $(if $(TEMP),$(TEMP),/tmp)/forecast-gate-mod-$(notdir $(CURDIR))

all: test

help:
	@echo "make test     go test ./..."
	@echo "make linux    Linux amd64 binary -> bin/baselines"
	@echo "make migrate  Linux amd64 migration CLI -> bin/baselines-migrate"
	@echo "make gate-check  CI Go steps from a cold module cache behind the configured proxy"
	@echo "make deps-report per-module version inventory -> $(GATE_CACHE)/deps-report.md"

test:
	go test ./...

# The quoted paths keep these lines on the shell: Windows make runs a line directly when
# it has no shell character, and a direct `mkdir -p bin` fails with
# "process_begin: CreateProcess … failed".
linux:
	mkdir -p "bin"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$(BIN)" ./cmd/baselines

# CI/CD entry point: applies the embedded migrations (migrations/) to a database
# before the worker starts, so a pipeline can prepare the schema instead of relying
# on the worker's first store use. Optional: the worker applies the same set.
migrate:
	mkdir -p "bin"
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$(BIN_MIGRATE)" ./cmd/migrate

# Cold-cache replica of the CI Go steps behind whatever module proxy is configured: a build that
# only works from a warm local module cache fails here instead of in the pipeline. `-race` stays in
# CI (this dev box has no gcc). See docs/COMPANY-BUILD.md.
gate-check:
	@go env GOPROXY GOSUMDB GOPRIVATE HTTP_PROXY HTTPS_PROXY NO_PROXY
	rm -rf "$(GATE_MODCACHE)"
	GOMODCACHE="$(GATE_MODCACHE)" go mod download all
	GOMODCACHE="$(GATE_MODCACHE)" sh -c 'test -z "$$(gofmt -l .)"'
	GOMODCACHE="$(GATE_MODCACHE)" go test ./...
	GOMODCACHE="$(GATE_MODCACHE)" sh -c 'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$(GATE_CACHE)/baselines" ./cmd/baselines && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$(GATE_CACHE)/baselines-migrate" ./cmd/migrate'

# Per-module version inventory for a security review (docs/COMPANY-BUILD.md).
deps-report:
	mkdir -p "$(GATE_CACHE)"
	sh tools/deps-report.sh "$(GATE_CACHE)/deps-report.md"

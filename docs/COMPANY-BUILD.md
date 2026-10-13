# Building this repo inside the company gate

The company's package-validation proxy sits between every build and the public module registry. This
document is the reproducible form of that gate: what it has to serve, how it is configured, how to run
the whole build against a **cold** module cache, and what a reviewer needs from the dependency graph.

Nothing here changes the dependency graph. `go.mod` and `go.sum` are the authority, no `replace`
directive exists (and none may be added — see [AGENTS.md](../AGENTS.md)), and the two first-party
modules `github.com/eduard-kolotushin/timeseries` and `github.com/eduard-kolotushin/timeseries-forecast`
are consumed as tagged releases through their public proxy paths.

## 1. What the gate must serve

- `go list -m all` resolves **25** modules in this repo (main module excluded) and every one of them
  must be servable by the proxy: `make gate-check` runs `go mod download all` against a cold cache, so
  a module the gate cannot serve fails there instead of in a later pipeline stage.
- Integrity is `go.sum`, verified against `GOSUMDB`: a mirror that served different bytes for a pinned
  version fails the build rather than silently changing it.
- The two first-party modules are consumed as tags — `timeseries v0.2.0`, `timeseries-forecast v0.5.3`
  — and are servable from their tag refs (the proxy reports `"Ref": "refs/tags/…"` for each).
- Vendoring is deliberately not used and the library sources are not copied into this repo: the worker
  calls the public `timeseries`/`forecast` APIs, which the repo rules require.

## 2. Proxy environment

Dev machine, once:

```sh
go env -w GOPROXY=<company-go-proxy>
go env -w GOSUMDB=<internal sumdb URL or sum.golang.org>
```

Credentials belong in the `GOPROXY` userinfo (`https://user:token@host`) or in `~/.netrc`, never in
this repo. If the company proxy cannot reach `sum.golang.org`, set `GOSUMDB=off` or point it at the
internal sumdb; `go.sum` still verifies every module, so nothing is lost.

If validation is an intercepting HTTP proxy rather than a Go module proxy, leave `GOPROXY` at its
default and set `HTTPS_PROXY`, `HTTP_PROXY`, `NO_PROXY`. `make gate-check` prints the effective settings
and works either way.

## 3. Cold-cache gate-check

```sh
make gate-check
```

It mirrors the CI steps (`.github/workflows/ci.yml`) with `GOMODCACHE` pointed at a temp dir
(`$TEMP/forecast-gate-mod-timeseries-baselines` on Windows, `/tmp/…` elsewhere), which the target
empties first, so the module cache starts empty and the repo tree stays free of it: `go mod download all`, `gofmt -l .`,
`go test ./...`, and the two Linux builds (`./cmd/baselines`, `./cmd/migrate`). CI keeps `-race`; this
target does not, because it has to run on the Windows dev box, which has no gcc.

`go mod download all` also records a hash for every module in the graph, so the first run on a tree
whose `go.sum` only carried the build subset adds those lines. Commit them: they are the same versions
the graph already resolved, and they are what makes `go mod verify` cover the whole graph. A later
`go mod tidy` that prunes them simply means the next `make gate-check` adds them back.

## 4. Dependency inventory

```sh
make deps-report   # -> .gate-cache/deps-report.md
```

One row per module: the version the build resolves, and whether that version is a `tag`, a
`pseudo`-version (the upstream publishes no tagged release at all — here that is
`github.com/jackc/pgservicefile`), or an `incompatible` tag (a release that predates Go modules, such
as `v2.1.0+incompatible`; this graph has none). A `pseudo` row with 0 upstream tagged releases cannot
be pinned to a tag by any consumer; moving one means changing the shared module graph, which is a
separate change with its own verification. Attach the report to the security ticket.

## 5. Acceptance

On a machine inside the gate: `make gate-check` exits 0 here and in the sibling `timeseries-grafana`
repo, and `.gate-cache/deps-report.md` is attached to the ticket. A module the proxy refuses is fixed
in the proxy allow-list, never by editing this repo's graph.

## 6. Not covered

The Kubernetes worker image build stage (`timeseries-k8s/docker/baselines/Dockerfile`) runs `go build`
inside `golang:1.26-bookworm` and does not inherit the host's `go env`; if the gate applies there as
well, that is a separate change (an `ARG GOPROXY`/`ENV GOPROXY` in that Dockerfile, or the Docker
daemon's proxy configuration).

# AGENTS.md

Operating manual for agents working in this repository.

## Project

Standalone Druid → minute-of-week baseline → Kafka worker. Not a Grafana plugin.

- **Module:** `github.com/eduard-kolotushin/timeseries-baselines`
- **Package:** `baselines`
- **Go:** 1.26+
- **Libraries:** tagged `timeseries` and `timeseries-forecast` modules (no `replace`)
- **Sandbox:** sibling `timeseries-grafana-sandbox` runs this as Compose `baseline-worker`
- **Kubernetes:** sibling `timeseries-k8s` builds the worker image from a git pin of this repo

## Read first

1. [docs/INTENTIONS.md](docs/INTENTIONS.md)
2. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Hard constraints

- Depend on `timeseries.Series[float64]` and public `forecast.FitSeasonalBaseline`; do not fork Series or models
- Public ops do not mutate caller series
- Source of truth is Druid SQL, not the metrics Kafka topic
- Stay within v1/v2 unless `docs/INTENTIONS.md` is updated first
- Fit in linear time; O(1) work per horizon step; pre-size series slices
- Ownership is a pure, allocation-free function of `(metric_hash, peer set)`: a wrong peer set degrades to duplicate work or a stranded share, never to a crash or a stalled tick

## v1 in scope

Env-configured ticker, skip short/non-1-minute hashes, one Kafka message at last+N minutes.

## v2 in scope

N workers over one table by rendezvous hashing of `metric_hash` over a peer set (`SHARD_ID` / `SHARD_PEERS` / `SHARD_DNS`); Kubernetes Deployment + headless Service or VM processes; idempotency key per message and duplicate-tolerant ingestion.

## v1/v2 out of scope

Grafana hosting, overlay UI, Prometheus, prediction intervals, consuming metrics Kafka, Docker/Helm packaging (see `timeseries-k8s`), a coordinator/leader election, backfill after a restart.

## Workflow

- Table-driven tests next to the code under test
- Depend on tagged `timeseries` and `timeseries-forecast` modules; do not add a `replace` directive
- `make linux` writes `bin/baselines` for the sandbox container
- Do not copy Series internals; use the public timeseries API only
- GitHub Actions on `main`: `gofmt` and `go test ./...`

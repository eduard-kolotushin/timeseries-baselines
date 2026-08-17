# AGENTS.md

Operating manual for agents working in this repository.

## Project

Standalone Druid → minute-of-week baseline → Kafka worker. Not a Grafana plugin.

- **Module:** `github.com/eduard-kolotushin/timeseries-baselines`
- **Package:** `baselines`
- **Go:** 1.26+
- **Local siblings:** `../timeseries`, `../timeseries-forecast` via `go.mod` replace
- **Sandbox:** sibling `timeseries-grafana-sandbox` runs this as Compose `baseline-worker`

## Read first

1. [docs/INTENTIONS.md](docs/INTENTIONS.md)
2. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Hard constraints

- Depend on `timeseries.Series[float64]` and public `forecast.FitSeasonalBaseline`; do not fork Series or models
- Public ops do not mutate caller series
- Source of truth is Druid SQL, not the metrics Kafka topic
- Stay within v1 unless `docs/INTENTIONS.md` is updated first
- Fit in linear time; O(1) work per horizon step; pre-size series slices

## v1 in scope

Env-configured ticker, skip short/non-1-minute hashes, one Kafka message at last+N minutes.

## v1 out of scope

Grafana hosting, overlay UI, Prometheus, prediction intervals, consuming metrics Kafka.

## Workflow

- Table-driven tests next to the code under test
- `make linux` writes `bin/baselines` for the sandbox container
- Do not copy Series internals; use the public timeseries API only

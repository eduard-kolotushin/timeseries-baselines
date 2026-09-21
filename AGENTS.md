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
- Stay within v1/v2/v3 unless `docs/INTENTIONS.md` is updated first
- Every Druid request is windowed and bounded (`DRUID_MAX_RANGE`, `DRUID_MAX_RPS`, `DRUID_MAX_INFLIGHT`, `DRUID_TIMEOUT`, `DRUID_RETRIES`) and one reply is capped at 64 MiB; never re-introduce an unbounded `SELECT`, a `COUNT(*)` pre-size probe or an unbounded `io.ReadAll`
- A column the worker cannot read is never a zero: a null or unparseable `metric_value` is `NaN` and keeps its timestamp (the 1-minute check runs before the fit drops NaN), and a row it cannot place in time is dropped with a warning
- The Kafka sink writes with `RequiredAcks: RequireAll`; a literal `kafka.Writer` would default to `RequireNone`, whose `Produce` returns `(nil, nil)`, making a broker-rejected record look published
- Postgres holds snapshots, the retrain queue and the membership heartbeat. The worker creates only schema `baselines`; `forecast.retrain` is created and owned by `timeseries-grafana`, and this process only reads, claims and finishes rows in it. Those rows are keyed `(scope, org_id, key)` and a `baseline` row lives at the fleet-wide `org_id = 0`; the claim carries that org so `Done` addresses the row by the full key, and the owner predicate is `claimed_by = self` with no `IS NULL` escape
- The worker still exposes no HTTP surface
- Fit in linear time; O(1) work per horizon step; pre-size series slices
- Ownership is a pure, allocation-free function of `(metric_hash, peer set)`: a wrong peer set degrades to duplicate work or a stranded share, never to a crash or a stalled tick

## v1 in scope

Env-configured ticker, skip short/non-1-minute hashes, one Kafka message at last+N minutes.

## v2 in scope

N workers over one table by rendezvous hashing of `metric_hash` over a peer set (`SHARD_ID` / `SHARD_PEERS` / `SHARD_DNS`); Kubernetes Deployment + headless Service or VM processes; idempotency key per message and duplicate-tolerant ingestion.

## v3 in scope

Train on a schedule, persist the fit, publish from the snapshot. Bounded Druid access (`DRUID_MAX_RANGE` slicing, rate/inflight caps, retry, timeout, optional static auth header), Postgres snapshot store (`baselines.snapshots`, gzip `forecast.Snapshot`), scheduled retrain with a fleet-wide `FOR UPDATE SKIP LOCKED` claim queue on `forecast.retrain`, Postgres-heartbeat membership (`baselines.workers`, `SHARD_MEMBERSHIP=store`), and a publish timestamp of minute-truncated `now` + `AHEAD_MINUTES`. The v1 span rule reaches every path that decides: a hash is scheduled, trained and published only while its scan span covers `LOOKBACK` (the schedule insert and `emit` are gated on it, and a claim below it is finished as an error rather than fitted), because the retrain path's fit is not the fit that carries the check.

## v1/v2/v3 out of scope

Grafana hosting, overlay UI, Prometheus, prediction intervals, consuming metrics Kafka, Docker/Helm packaging (see `timeseries-k8s`), a coordinator/leader election for ownership, backfill after a restart, an HTTP endpoint or health probe, more than one shared Postgres.

## Workflow

- Table-driven tests next to the code under test
- Depend on tagged `timeseries` and `timeseries-forecast` modules; do not add a `replace` directive
- `make linux` writes `bin/baselines` for the sandbox container
- Do not copy Series internals; use the public timeseries API only
- GitHub Actions on `main`: `gofmt` and `go test -race ./...` against a `postgres:17` service (`BASELINE_TEST_PG`)

# Project intentions

## Goal

Standalone process that publishes minute-of-week seasonal baselines from a Druid table to Kafka. Grafana does not host this ticker. Fit math stays in `timeseries-forecast`.

Implement the loop efficiently: one O(n) fit per ready hash per tick, O(1) work per horizon step, pre-sized series slices.

One process owns the whole table, or N processes share it by hash (v2). The same binary runs on a VM or as a Kubernetes workload.

## Locked choices

| Decision | Choice |
| --- | --- |
| Repo | sibling `timeseries-baselines` |
| Module | `github.com/eduard-kolotushin/timeseries-baselines` |
| Package | `baselines` |
| Go | 1.26+ |
| Input series | public `timeseries.Series[float64]` from tagged modules (no `replace`) |
| Model | `FitSeasonalBaseline` minute-of-week |
| Source | Druid SQL (not the metrics Kafka topic) |
| Output | one Kafka message per ready metric per tick, at last timestamp + N minutes |
| Config | environment variables (and flags), not Grafana jsonData |
| Scaling | N workers over the same table; rendezvous hashing on `metric_hash` over a peer set (`SHARD_ID` / `SHARD_MEMBERSHIP` = `auto` / `peers` / `dns` / `store`, over `SHARD_PEERS` / `SHARD_DNS` / the `baselines.workers` heartbeat) |
| Sandbox | sibling `timeseries-grafana-sandbox` |
| Kubernetes | sibling `timeseries-k8s` (worker image + Helm; no Dockerfile in this repo) |

## v1 must-have

- Env/flags: Druid broker/datasource, Kafka brokers/baseline topic, lookback, aheadMinutes N, interval, calendar
- Distinct `metric_hash` from Druid; skip unless `max(__time)-min(__time) >= lookback`
- Fit last lookback window with minute-of-week seasonal baseline; skip non-1-minute series
- Publish `{"metric_hash","metric_ts","baseline_value"}` to the baseline Kafka topic (`metric_ts` Unix ms = last + N minutes)

## v1 non-goals

Do not add these without first updating this document:

- Overlay visualization or Grafana plugin hosting
- Helm / container images (those live in `timeseries-k8s`)
- Folding this ticker into `timeseries-forecast`
- Consuming the metrics Kafka topic (Druid is the source of truth)
- Prometheus
- Prediction intervals or alerting
- A forked Series type

## v2 must-have

Run N workers over one Druid table, on VMs and in Kubernetes, without doubling published values.

- Ownership is rendezvous ("highest random weight") hashing of `metric_hash` over the peer set: every worker computes the same owner without a coordinator, and one membership change moves only the hashes that changed owner
- Peer set: `SHARD_PEERS` (explicit identities) or `SHARD_DNS` (A records of a headless Service / round-robin name), else this worker alone. `SHARD_ID` is the worker identity (default: first non-loopback IP). Setting both `SHARD_PEERS` and `SHARD_DNS` is an error
- A worker always includes itself in its own peer set; two workers with the same view publish disjoint sets that cover every hash
- A failed peer lookup keeps the last good set, and with no last good set the worker runs unsharded: duplicate work is preferable to a stalled tick
- Each message carries an idempotency key (`metric_hash|metric_ts`) on a keyed partition so a repeated point can be compacted away, and ingestion must collapse duplicate `(metric_hash, metric_ts)` records instead of summing them (reference: Druid `doubleMax` with minute rollup, not `doubleSum`)
- One `Hashes` scan per worker per tick is accepted: sharding divides the per-hash `Series` load and fit CPU, not the eligibility scan
- The replica count is the only scaling knob in Kubernetes; the VM path passes `SHARD_PEERS`

## v2 non-goals

- A coordinator or leader election for hash ownership (v3 adds a Postgres claim queue for retrains only; ownership stays rendezvous hashing)
- Rebalancing the hash space while a tick is in flight
- Backfilling skipped minutes after a worker restart or a membership change
- Reading the worker's own Kafka output or Druid `baselines` table to dedupe
- Sharding the Druid eligibility scan (a per-hash span summary table is a separate design)
- An HTTP endpoint, metrics port, or health probe

## v3 must-have

Train on a schedule, persist the fit, publish from the snapshot, and bound every Druid request so the production Abyss datasource limit is never tripped.

- **Bounded Druid access.** `DRUID_MAX_RANGE` slices one window into consecutive half-open windows of at most that span (`0` = do not slice the caller's window further; `Series` and `Hashes` always take an explicit `[from, to)`, so even then a request is bounded by that window and never by all history), so a load becomes `ceil(window / DRUID_MAX_RANGE)` queries instead of one unbounded scan. `Series(ctx, hash, from, to)` takes an explicit window and no longer issues the `COUNT(*)` pre-size query: the caller owns the range and the slice count sizes the buffers. `Hashes(ctx, from, to)` slices the scan the same way over `SCAN_RANGE` (default `max(2*LOOKBACK, 24h)`), and the scan result is cached for `HASH_SCAN_TTL` (default 5m). Half-open windows (`__time >= lo AND __time < hi`) plus `timeseries.Concat` make duplication impossible across a sliced load
- **Rate, inflight, and retry caps in front of every Druid request.** `DRUID_MAX_RPS` (default 4, `0` = off) and `DRUID_MAX_INFLIGHT` (default 4) bound the request stream; `TRAIN_CONCURRENCY` (default 2) bounds whole retrains. `DRUID_TIMEOUT` (default 60s) sets the client timeout and `DRUID_RETRIES` (default 2, 250ms → 2s backoff) retries transport errors and 5xx only, never 4xx. Optional static auth via `DRUID_AUTH_HEADER` / `DRUID_AUTH_VALUE`
- **Postgres snapshot store** (`baselines.snapshots` via pgx, gzip JSON of `forecast.Snapshot`): train in one process, publish from the snapshot in every process, on every tick and across restarts, without reading Druid. DSN is `BASELINE_STORE_URL|HOST|PORT|DATABASE|USER|PASSWORD|SSLMODE`, falling back field by field to `FORECAST_STORE_*` (the plugin's store). No host: persist off, and the worker keeps the v1/v2 behaviour of fitting the owned hashes every tick. The worker owns only the `baselines` schema; it never creates the plugin's `forecast.retrain`
- **Scheduled retrain with a durable claim queue.** Every owned hash with no `forecast.retrain` row is scheduled (`scope='baseline'`, `DEFAULT_RETRAIN_CRON`, `timezone='UTC'`, `next_run_at=now()`). Due rows are then claimed fleet-wide with `FOR UPDATE SKIP LOCKED` plus a lease (`RETRAIN_RETRY`, default 5m) and retrained by whichever worker claimed them — a claim is **not** restricted to the hash's owner, so a retrain never waits on a specific process. A failed retrain writes `last_status='error: …'` and `next_run_at=now()+RETRAIN_RETRY`; the tick never stalls on it. Publishing stays rendezvous-sharded
- **Postgres heartbeat membership** (`baselines.workers`). `SHARD_MEMBERSHIP=store` makes the peer set the worker rows seen within `WORKER_TTL` (default `max(30s, 2*INTERVAL)`, and always longer than `INTERVAL`), so a VMs fleet needs one shared Postgres and neither DNS nor a static peer list. `auto` prefers `SHARD_PEERS`, then `SHARD_DNS`, then the store, then this worker alone; `peers` / `dns` / `store` force one source. A **failed** lookup or heartbeat keeps the last good peer set, as in v2; an empty `store` answer is a valid one and collapses to this worker alone, so a departed peer's share is taken over instead of staying stranded
- **Publish from the snapshot at a wall-clock horizon.** The published timestamp is `now` truncated to the minute + `AHEAD_MINUTES`, not the last scanned data point. With a stored snapshot and an unreachable Druid the worker therefore keeps publishing one point per hash per minute: publishing no longer needs Druid at all
- Config: `DRUID_MAX_RANGE`, `DRUID_TIMEOUT`, `DRUID_RETRIES`, `DRUID_MAX_RPS`, `DRUID_MAX_INFLIGHT`, `DRUID_AUTH_HEADER`, `DRUID_AUTH_VALUE`, `HASH_SCAN_TTL`, `SCAN_RANGE`, `TRAIN_CONCURRENCY`, `SNAPSHOT_CACHE_TTL`, `WORKER_TTL`, `SHARD_MEMBERSHIP`, `DEFAULT_RETRAIN_CRON`, `RETRAIN_RETRY`, `LOG_LEVEL`, plus `BASELINE_STORE_*`, each parsed and range-checked in `ConfigFromEnv` / `Validate`
- The worker still exposes **no** HTTP surface. The schedule REST API and the Configuration-page UI live in `timeseries-grafana`; this process only reads, claims, and finishes rows

## v3 non-goals

- A coordinator, leader election, or a rebalancing protocol: ownership is still rendezvous hashing and the claim queue only distributes retrains
- Backfilling skipped minutes after a worker restart, a failed retrain, or a membership change
- Reading the worker's own Kafka output or the Druid `baselines` table to dedupe
- Sharding the Druid eligibility scan (it is windowed and cached in v3, not divided)
- An HTTP endpoint, metrics port, or health probe
- Sharding or replicating `baselines.snapshots` / `forecast.retrain`; one shared Postgres is assumed
- A per-model retrain schedule per hash beyond the single `forecast.retrain` row a worker inserts

## Quality bar

- Do not mutate caller series (libraries already return new series)
- Table-driven tests for config, Druid windows / limits / retry, publisher ticks (scan → retrain → emit), snapshot cache reuse, retrain scheduling, and shard membership (partition, coverage, hash spread, peer-source modes)
- One O(n) fit per hash per retrain; O(1) per horizon step; pre-size series slices to the window length
- Ownership hashing must not allocate per hash: no joined `hash|peer` string
- Dependencies stay minimal: `github.com/robfig/cron/v3` for the 5-field schedule and `github.com/jackc/pgx/v5` for the store, on top of `timeseries`, `timeseries-forecast`, and `kafka-go`
- GitHub Actions on `main` runs `gofmt` and `go test ./...`

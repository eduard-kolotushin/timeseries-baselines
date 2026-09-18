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
| Scaling | N workers over the same table; rendezvous hashing on `metric_hash` over a peer set (`SHARD_ID` / `SHARD_PEERS` / `SHARD_DNS`) |
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

- A coordinator, leader election, or shared lock (etcd, ZooKeeper, Postgres)
- Rebalancing the hash space while a tick is in flight
- Backfilling skipped minutes after a worker restart or a membership change
- Reading the worker's own Kafka output or Druid `baselines` table to dedupe
- Sharding the Druid eligibility scan (a per-hash span summary table is a separate design)
- An HTTP endpoint, metrics port, or health probe

## Quality bar

- Do not mutate caller series (libraries already return new series)
- Table-driven tests for config, Druid SQL client, publisher ticks, and shard ownership (partition, coverage, hash spread, peer-set modes)
- One O(n) fit per hash per tick; O(1) per horizon step; pre-size series slices
- Ownership hashing must not allocate per hash: no joined `hash|peer` string
- GitHub Actions on `main` runs `gofmt` and `go test ./...`

# Architecture

## Layout

Single package `baselines` plus `cmd/baselines`:

| Path | Responsibility |
| --- | --- |
| `config.go` | Env/flag config and validation |
| `shard.go` | Rendezvous ownership over the peer set; peer discovery (`SHARD_PEERS` / `SHARD_DNS`) |
| `druid.go` | Druid SQL `metric_hash` scan and series load |
| `kafka.go` | Kafka writer for baseline messages |
| `publisher.go` | Tick: own the hash, skip short/non-1m hashes, fit, publish last+N |
| `cmd/baselines` | Process entry: load config, run until SIGINT/SIGTERM |

## Data flow

Each tick:

1. Resolve the peer set (see Sharding) and drop the hashes another worker owns.
2. Druid SQL `GROUP BY metric_hash` for `min(__time)`, `max(__time)`.
3. Skip hashes whose span is shorter than `lookback` (default 336h).
4. Load the last lookback window; skip unless inferred step is 1 minute.
5. `FitSeasonalBaseline(..., SeasonMinuteOfWeek, calendar)` then `Forecast(N)`.
6. Publish only the last point to the **baseline** Kafka topic:

```json
{"metric_hash":"...","metric_ts":<unix_ms>,"baseline_value":<float>}
```

`metric_ts` is last observed timestamp + N minutes. The metrics Kafka topic is not read; Druid is the source of truth. The Kafka key is `metric_hash|metric_ts`, so one point has one key: repeats land on the same partition and can be compacted away. Duplicate `(metric_hash, metric_ts)` pairs are also skipped in memory per process, but a restart or a membership change republishes: **consumers must treat the topic as upsert** and collapse duplicates instead of summing them (see Scaling and ingestion).

## Sharding

N workers share one table. Each tick every worker resolves the same peer set and asks who owns each hash:

- Ownership is rendezvous ("highest random weight") hashing: `owner(hash) = argmax(avalanche(fnv1a(hash + "|" + peer)))` over the peers. Any worker computes it from the peer set alone — no coordinator, no lock, no shared state.
- Adding or removing one peer moves about `1/N` of the hashes (measured: 204 of 1024 hashes move when a fifth peer joins), so a scale event only re-fits that share.
- The peer set is `SHARD_PEERS` (explicit identities, for VMs) or the `SHARD_DNS` A records (headless Service in Kubernetes, Compose service name for a scaled Compose service), else the worker alone. Set both and the process refuses to start.
- `SHARD_ID` is the worker identity and defaults to the first non-loopback IP, which is what `SHARD_DNS` returns in a container or pod. Kubernetes sets it from `status.podIP` so the identity matches the DNS records exactly. Two workers on one host share that default and would be the same peer, so co-located processes need explicit `SHARD_ID` values. The same holds across networks: two VMs in different VPCs can both report `10.0.0.5`, which merges them into one peer whose share both of them publish, so a multi-VM fleet names every worker explicitly.
- A worker always adds itself to its peer set, so it never idles out of the table. Two workers that see the same peers publish **disjoint** sets that cover every hash.
- A failed lookup keeps the last good set. With no last good set yet, the worker runs unsharded (owns everything) and logs a warning: duplicate work beats a stalled tick, and the next successful lookup restores sharding.

What sharding does and does not divide:

| Per tick per worker | Effect of N workers |
| --- | --- |
| `Hashes` eligibility scan (`GROUP BY metric_hash`) | unchanged, so N workers scan the table N times |
| `Series` load, fit, publish, for owned hashes | divided by about N |

The scan is the price of eligibility (`min(__time)` over all history). Pushing the shard predicate into the SQL (`fnv_hash(metric_hash) % N`) would shrink the response but not the scan, so it is not used: ownership stays in Go, portable and testable.

Two invariants follow from where the checks sit. Eligibility is tested *after* ownership (`tick` asks `Owns` first and `publishHash` applies the lookback), so `LOOKBACK` must be identical fleet-wide — an owner with a shorter lookback drops a hash that no other worker will pick up. And a static peer list must be edited *before* a process is stopped: a listed name with no process strands its share, whereas adding a name only duplicates points until the views converge.

Membership changes are not transactional. A handover costs at most one tick for the hashes that moved: the new owner starts publishing its own `last + N`, and the old owner may publish the same point once more while its view is stale. There is no backfill; the dashboard shows a lead series, not a per-minute ledger.

## Scaling and ingestion

Duplicates are possible (restart, handover, a peer that is listed but not running), so the storage side must be idempotent for `(metric_hash, metric_ts)`:

- Reference Druid Kafka supervisor: `metricsSpec` `doubleMax` (`doubleSum` double-counts a repeated point), `queryGranularity` `minute`, `rollup` `true`, dimensions `metric_hash`, timestamp `metric_ts` from millis.
- Dashboard queries then read that point with `MAX(baseline_value)`.
- Optional: `cleanup.policy=compact` on the baseline topic. Compaction is asynchronous and Druid reads with `useEarliestOffset`, so `doubleMax` is the correctness mechanism and compaction only trims the log.

Because the Kafka key routes by hash, keep a single producer implementation for this topic (a producer with a different partitioner would put equal keys on different partitions).

## Deployments

Both environments run the same binary and only differ in how the peer set arrives.

Kubernetes (`timeseries-k8s`): a worker Deployment plus a headless Service. `SHARD_ID` comes from `status.podIP` (Downward API) and `SHARD_DNS` points at the headless Service, so `kubectl scale deployment/<release>-baselines --replicas=N` is the whole operation: pods join the endpoint list, the others pick up the new view on their next tick. No replica count needs to agree with anything.

VM: N processes, each with `SHARD_ID` set to its own address and `SHARD_PEERS` listing **every** running worker, e.g. `systemd` template units `baselines@0..N-1` with `SHARD_ID=10.0.0.11`, `SHARD_PEERS=10.0.0.11,10.0.0.12`. Each worker unions only *itself* into its view, so a list that does not name exactly the running set gives workers different views, and the two deviations differ (measured on 2048 hashes with the ownership code):

| `SHARD_PEERS` vs running workers | Outcome |
| --- | --- |
| Names peers that are not running | their share is stranded: 1253 of 2048 hashes unpublished, and 196 published by two workers |
| Omits a running worker | 1047 of 2048 unpublished, 158 published twice |
| A subset of the running workers (every listed peer is up, but not every worker is listed) | no strands, 691 of 2048 published twice — duplicates only |

Only the last shape is harmless-by-ingestion; the first two lose lead points. If the VMs have a shared round-robin name or DNS, use `SHARD_DNS` instead so membership follows liveness.

Peer views can also disagree *while* converging: with `SHARD_DNS` the endpoint list lags a restart or a scale by one tick, and a worker that starts before its own endpoint exists sees only itself and publishes everything for that tick. Both are transient duplicates, never silent loss.

A membership change is not transactional, and a stale view can persist if two workers keep disagreeing (for example a `SHARD_PEERS` list that names a peer which comes back with a different identity): a hash can stay owned by a worker that is not running for longer than one tick. Confirm coverage after a scale event rather than assuming one tick.

Interfaces:

```go
type metricReader interface {
	Hashes(ctx context.Context) ([]metricSpan, error)
	Series(ctx context.Context, hash string, from time.Time) ([]seriesPoint, error)
}

type baselineSink interface {
	Publish(ctx context.Context, msg BaselineMessage) error
	Close() error
}
```


## Horizon clock

Same as `timeseries-forecast`: last timestamp + `k * step` for `k = 1..h`. This worker uses `k = N`.

## Config (env)

| Variable | Default | Meaning |
| --- | --- | --- |
| `DRUID_BROKER` | (required) | Absolute broker URL |
| `DRUID_DATASOURCE` | `metrics` | `[A-Za-z0-9_]+` table name |
| `KAFKA_BROKERS` | (required) | Comma-separated brokers |
| `KAFKA_TOPIC` | `baselines` | Must not be the metrics topic |
| `LOOKBACK` | `336h` | Eligibility span and fit window |
| `AHEAD_MINUTES` | `1` | Last observed + N minutes |
| `INTERVAL` | `1m` | Scan period |
| `CALENDAR` | empty | Empty or `ru` |
| `SHARD_ID` | first non-loopback IP | This worker's identity in the peer set |
| `SHARD_PEERS` | empty | Comma-separated peer identities (VMs) |
| `SHARD_DNS` | empty | Name whose A records are the peer set (headless Service, round-robin) |

Empty `SHARD_PEERS` and `SHARD_DNS` mean one worker owns every hash. Setting both is an error.

## Performance

- One O(n) `FitSeasonalBaseline` per ready hash per tick
- `Forecast(N)` is O(N); only the last point is published
- Pre-size times/values slices to the Druid row count
- Do not keep the training series after fit
- Ownership is `len(peers)` allocation-free hashes per hash per tick, and the whole check is skipped when a worker is alone

## Modules

`go.mod` requires tagged `github.com/eduard-kolotushin/timeseries` and `github.com/eduard-kolotushin/timeseries-forecast`. Do not add a `replace` directive.

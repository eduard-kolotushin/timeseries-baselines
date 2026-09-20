# timeseries-baselines

Standalone worker that reads a Druid metrics table, fits a minute-of-week seasonal baseline from [`timeseries-forecast`](https://github.com/eduard-kolotushin/timeseries-forecast), and publishes one lead point per ready `metric_hash` to Kafka.

One process owns every hash, or N processes share the table by rendezvous hashing (see [Scaling](#scaling)).

This is **not** a Grafana plugin. Grafana overlays live in [`timeseries-grafana`](../timeseries-grafana). Local Compose lives in [`timeseries-grafana-sandbox`](../timeseries-grafana-sandbox). The cluster worker image and Helm chart live in [`timeseries-k8s`](../timeseries-k8s).

See [docs/INTENTIONS.md](docs/INTENTIONS.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Runbook for the sharded deployment on VM and Kubernetes: [docs/POC.md](docs/POC.md) (English) / [docs/POC.ru.md](docs/POC.ru.md) (Русский).

Depends on tagged `timeseries` and `timeseries-forecast` modules.

## Build

```bash
make test    # go test ./...
make linux   # Linux amd64 binary -> bin/baselines (sandbox mount)
```

The sandbox Compose service `baseline-worker` mounts that binary and sets `DRUID_BROKER`, `KAFKA_BROKERS`, and related env. Running the process is what enables the ticker.

## Scaling

Workers share the table by rendezvous hashing of `metric_hash`, so two workers with the same peer set publish disjoint, complete sets. No coordinator and no lock; adding or removing one worker moves about `1/N` of the hashes.

| Environment | Peer set | Scale with |
| --- | --- | --- |
| Kubernetes | `SHARD_DNS` = headless Service (A records), `SHARD_ID` = `status.podIP` | `kubectl scale deployment/<release>-baselines --replicas=N` |
| Compose | `SHARD_MEMBERSHIP=store` over `overlay-postgres` | `docker compose up -d --scale baseline-worker=N` |
| VM | `SHARD_MEMBERSHIP=store` over one shared Postgres, else `SHARD_PEERS` listing every worker with `SHARD_ID` = this worker | add/remove processes; the store path needs no list edit |

`SHARD_MEMBERSHIP` (`auto` by default) picks the source: `auto` takes `SHARD_PEERS`, then `SHARD_DNS`, then the store heartbeat, then this worker alone; `peers` / `dns` / `store` force one source. With `store`, each worker upserts `baselines.workers` every tick and the peer set is the ids seen within `WORKER_TTL` (default `max(30s, 2*INTERVAL)`, and always longer than `INTERVAL`), so a **VM fleet needs one shared Postgres and no DNS or peer list at all**, and a stopped worker ages out on its own — its share moves to the survivors rather than staying stranded.

`SHARD_ID` must be unique per running worker. It defaults to the first non-loopback IP, which is unique for containers, pods, and separate VMs — but two processes on **one host** would both be that same peer: they would publish the same share twice and strand the other peers' hashes. Give co-located workers explicit `SHARD_ID` values or use `SHARD_PEERS`.

Empty peer settings mean one worker owns every hash. Setting both `SHARD_PEERS` and `SHARD_DNS` is an error. A failed lookup keeps the last good peer set, and a static list must match the running workers exactly: a listed peer that is not running strands its share.

Ingestion must be duplicate-tolerant — the reference Druid supervisor uses `doubleMax` with minute rollup, not `doubleSum` — because a handover, a restart, or a stale peer list can publish the same `(metric_hash, metric_ts)` twice. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Snapshots and scheduled retrain

With `BASELINE_STORE_*` (or `FORECAST_STORE_*`) pointing at Postgres, the worker stops fitting every tick:

- A retrain loads the last `LOOKBACK`, fits, and writes a gzip `forecast.Snapshot` to `baselines.snapshots`.
- Every tick publishes from the cached snapshot at minute-truncated `now` + `AHEAD_MINUTES`, so **publishing costs zero Druid requests** and survives a Druid outage.
- Retrains are scheduled per hash in `forecast.retrain` (cron, default `0 3 * * *`) and claimed fleet-wide with `FOR UPDATE SKIP LOCKED`, so any worker retrains any hash and a failed retrain is due again after `RETRAIN_RETRY` (default 5m).
- The Druid scan is windowed too, and cached for `HASH_SCAN_TTL` (default 5m).

Without a store the worker keeps the v1/v2 loop: fit and publish every tick, one Druid request per owned hash per tick. Both modes bound every request with `DRUID_MAX_RANGE` / `DRUID_MAX_RPS` / `DRUID_MAX_INFLIGHT` / `DRUID_TIMEOUT` / `DRUID_RETRIES`; the full env table is in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Agents

Contributors and coding agents: start with [AGENTS.md](AGENTS.md).

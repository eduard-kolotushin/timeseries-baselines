# Architecture

## Layout

Single package `baselines` plus `cmd/baselines`:

| Path | Responsibility |
| --- | --- |
| `config.go` | Env/flag config and validation |
| `limits.go` | Stdlib request semaphore (`DRUID_MAX_INFLIGHT`) and token-bucket rate limiter (`DRUID_MAX_RPS`) |
| `shard.go` | Rendezvous ownership over the peer set; `ShardID` |
| `membership.go` | Peer source (`SHARD_MEMBERSHIP` = `auto` / `peers` / `dns` / `store`), last-good-set fallback |
| `druid.go` | Druid SQL over HTTP: windowed `Hashes` / `Series`, half-open slicing, auth, timeout, retry, limits |
| `store.go` | Store interfaces (`snapshotStore`, `retrainQueue`, `membership`) and DTOs |
| `store_postgres.go` | pgx implementation: `baselines.snapshots` (gzip JSON), `baselines.workers` heartbeat, claim/finish of `forecast.retrain` |
| `storedsn.go` | `BASELINE_STORE_*` → `FORECAST_STORE_*` DSN resolution |
| `schedule.go` | `nextRun(cron, tz, now)` over `github.com/robfig/cron/v3` |
| `kafka.go` | Kafka writer for baseline messages |
| `publisher.go` | Tick: membership → scan → retrain claims → publish from the snapshot cache |
| `cmd/baselines` | Process entry: load config, run until SIGINT/SIGTERM |

## Data flow

Each tick (immediate, then every `INTERVAL`):

1. Read the clock once; every timestamp this tick stamps comes from it.
2. `Heartbeat` this worker into `baselines.workers` (best effort: an error keeps the previous peer set).
3. Resolve the peer set (see Membership) and drop the hashes another worker owns.
4. `Hashes(now - SCAN_RANGE, now)` — one Druid scan, sliced under `DRUID_MAX_RANGE` and cached for `HASH_SCAN_TTL`.
5. Schedule: **one** `INSERT … SELECT … FROM unnest($1::text[]) ON CONFLICT (scope, org_id, key) DO NOTHING` inserts a row for every owned hash that has none (`scope='baseline'`, `org_id=0`, `DEFAULT_RETRAIN_CRON`, `timezone='UTC'`, due now). An existing row keeps its cron, timezone and `enabled`: the operator owns the schedule and a restart must not reset it.
6. Retrain: `Claim(self, RETRAIN_RETRY, TRAIN_CONCURRENCY)` takes due `forecast.retrain` rows (**any** worker may retrain **any** hash, not only the ones it owns), then per claim: `Series(hash, now-LOOKBACK, now)` → `FitSeasonalBaseline` → `SnapshotOf` → `Put` into `baselines.snapshots` → `Done(self, claim.OrgID, key, next=nextRun(cron, tz, now), "ok")`. A failure writes `last_status='error: …'` and `next_run_at=now()+RETRAIN_RETRY`. `Done` only touches a row this worker still claims (`claimed_by = self`, addressed by the full `(scope, org_id, key)`); a lease that expired mid-retrain appears as zero rows affected, logs `claim lost` at debug, and is not an error.
7. Publish: for each owned hash whose snapshot is fresh (`SNAPSHOT_CACHE_TTL`, one `Fresh` query for all owned keys per tick, `Restore` only what moved), `ForecastRange(ts-1m, ts)` with `ts = now.Truncate(1m) + AHEAD_MINUTES`, and emit when `ts` is newer than the in-memory `published` mark and the value is not NaN. The one-minute lookback is deliberate: a grid that is not aligned to the wall-clock minute has no point exactly at `ts`, and asking for the exact point would return `ErrEmptyRange` on every tick; on an aligned grid the last point *is* `ts`.

`metric_ts` is the wall-clock horizon, not the last observed timestamp, so a Druid outage or a stalled retrain cannot stop publishing as long as a snapshot exists. The clock is read once per tick, before the scan: a tick is one minute of wall clock however long its rate-limited scan and retrain take, so a slow tick still publishes the minute it started in instead of skipping it and colliding with the next tick (see Horizon clock). The metrics Kafka topic is not read; Druid is the source of truth for training. The Kafka key is `metric_hash|metric_ts`, so one point has one key: repeats land on the same partition and can be compacted away. Duplicate `(metric_hash, metric_ts)` pairs are also skipped in memory per process, but a restart or a membership change republishes: **consumers must treat the topic as upsert** and collapse duplicates instead of summing them (see Scaling and ingestion).

With no store configured (`BASELINE_STORE_HOST` and `FORECAST_STORE_HOST` both empty) the worker keeps the v1/v2 loop exactly: fit each ready owned hash every tick and publish `last + AHEAD_MINUTES`. No schedule rows are read or written.

## Druid access

Every request is windowed, sliced, rate-limited, retried, and logged.

```go
func (d *druidStore) windows(from, to time.Time) [][2]time.Time  // maxRange <= 0 → the whole range
func (d *druidStore) Hashes(ctx context.Context, from, to time.Time) ([]metricSpan, error)
func (d *druidStore) Series(ctx context.Context, hash string, from, to time.Time) (timeseries.Series[float64], error)
```

- `DRUID_MAX_RANGE` (default `0`) is the maximum span one SQL request may cover, on top of the window the caller already chose. It is not what bounds a request: `Series` and `Hashes` always take an explicit `[from, to)` — `LOOKBACK` for a train, `SCAN_RANGE` for a scan — so even at `0` (one request for that window) no query is unbounded. `DRUID_MAX_RANGE` only slices that window further, which is what the production datasource's per-request cap needs. Windows are consecutive and half-open, so they partition `[from, to)` exactly: `__time >= MILLIS_TO_TIMESTAMP(lo) AND __time < MILLIS_TO_TIMESTAMP(hi)`.
- `Series` runs **exactly one query per window** and stitches them with `timeseries.Concat`, which returns `ErrDuplicateTime` / `ErrUnsorted` if the datasource ever returns an overlapping or out-of-order window. That loud failure is preferred over silently duplicated points, and it is why the pre-v3 whole-series `from`-only query and its `COUNT(*)` pre-size probe are gone.
- Buffers are pre-sized to `int(hi.Sub(lo)/time.Minute) + 1` per window instead of to a `COUNT(*)`.
- `Hashes` merges the per-window rows per hash (`min` of the `min`s, `max` of the `max`es). With a bounded `SCAN_RANGE` the reported `Min` is clamped to the window start and the window is half-open, so even a hash reporting every minute looks at most `SCAN_RANGE - 1m` long. `Validate` therefore requires `SCAN_RANGE >= LOOKBACK + 2*INTERVAL`: at `SCAN_RANGE == LOOKBACK` every hash would look shorter than `LOOKBACK` and the worker would publish nothing; `SCAN_RANGE=0` resolves to `max(2*LOOKBACK, 24h)`, which keeps every live series eligible under the default `LOOKBACK=336h`.
- `DRUID_MAX_RPS` (default 4, `0` = off) is a stdlib token bucket in front of every request; `DRUID_MAX_INFLIGHT` (default 4) is a semaphore. Both live in `limits.go` with no dependency.
- `DRUID_RETRIES` (default 2, 250ms → 2s backoff) retries transport errors and 5xx only. A 4xx (bad SQL, auth) is returned immediately: retrying it cannot help.
- `DRUID_TIMEOUT` (default 60s) is the HTTP client timeout; `DRUID_AUTH_HEADER` / `DRUID_AUTH_VALUE` add one static header when set.
- `LOG_LEVEL=debug` logs one `"druid request"` line per call with `op` (`series` / `hashes`), `from`, and `to`, which is how the request volume below is measured.

### Request volume

| Per tick, before v3 | Per tick, after v3 |
| --- | --- |
| 1 × unscoped `GROUP BY metric_hash` regardless of `SCAN_RANGE` | 1 × sliced, windowed `Hashes`, reused for `HASH_SCAN_TTL` (default 5m) |
| 1 × `COUNT(*)` + 1 × full-`LOOKBACK` `SELECT` per owned hash, **every tick** | 0 in steady state |
| → `2H + 1` requests per minute for `H` owned hashes | → `ceil(SCAN_RANGE / DRUID_MAX_RANGE)` per `HASH_SCAN_TTL`, plus the same count per retrain of a due hash (default: one retrain per hash per `DEFAULT_RETRAIN_CRON`) |

With `LOOKBACK=336h` and `DRUID_MAX_RANGE=24h`, one retrain of one hash costs 14 `series` requests, so the per-hash hourly cost is `14 × 60 / R` for a retrain every `R` minutes, against a flat 120 before (2 per minute). The default `DEFAULT_RETRAIN_CRON=0 3 * * *` gives 14 requests per day per hash instead of 2880 — the order-of-magnitude win. A deliberately frequent cron does **not** reduce the request *count*: the sandbox runs `*/5 * * * *` so the retrain is observable, which costs 168 per hour per hash versus 120 before. What is unconditional is the durability property the count is bought for: publishing itself is 0 requests, so the tick no longer depends on Druid at all, and `DRUID_MAX_RPS` / `DRUID_MAX_INFLIGHT` bound the burst each retrain spends.

## Sharding

N workers share one table. Each tick every worker resolves the same peer set and asks who owns each hash:

- Ownership is rendezvous ("highest random weight") hashing: `owner(hash) = argmax(avalanche(fnv1a(hash + "|" + peer)))` over the peers. Any worker computes it from the peer set alone — no coordinator, no lock, no shared state.
- Adding or removing one peer moves about `1/N` of the hashes (measured: 204 of 1024 hashes move when a fifth peer joins), so a scale event only re-fits that share.
- The peer set comes from `SHARD_MEMBERSHIP` (see Peer sources below), else the worker alone. `SHARD_PEERS` and `SHARD_DNS` together is still an error.
- `SHARD_ID` is the worker identity and defaults to the first non-loopback IP, which is what `SHARD_DNS` returns in a container or pod. Kubernetes sets it from `status.podIP` so the identity matches the DNS records exactly. Two workers on one host share that default and would be the same peer, so co-located processes need explicit `SHARD_ID` values. The same holds across networks: two VMs in different VPCs can both report `10.0.0.5`, which merges them into one peer whose share both of them publish, so a multi-VM fleet names every worker explicitly or uses the store heartbeat.
- A worker always adds itself to its peer set, so it never idles out of the table. Two workers that see the same peers publish **disjoint** sets that cover every hash.
- A failed lookup or heartbeat keeps the last good set. With no last good set yet, the worker runs unsharded (owns everything) and logs a warning: duplicate work beats a stalled tick, and the next successful lookup restores sharding.

### Peer sources

`SHARD_MEMBERSHIP` (`auto` by default) selects where the peer set comes from. `auto` is the v2 behaviour plus the store: it takes the first source that is configured, so adding `BASELINE_STORE_*` to a fleet that already had DNS does not change that fleet.

| Mode | Peer set | When |
| --- | --- | --- |
| `auto` (default) | `SHARD_PEERS`, else `SHARD_DNS`, else the `baselines.workers` heartbeat, else this worker alone | Any deployment; picks the most explicit source present |
| `peers` | `SHARD_PEERS` only | VMs with a list that matches the running set exactly |
| `dns` | The `SHARD_DNS` A records only | Kubernetes headless Service, or a scaled Compose service |
| `store` | Rows in `baselines.workers` seen within `WORKER_TTL` | VMs that share one Postgres; joins and leaves need no list edit and no DNS |

`store` is what replaces static peer lists on VMs: every tick a worker upserts `(id, last_seen, owned, peers)`, and the peer set is every `id` whose `last_seen` is newer than `now() - WORKER_TTL`. A worker that is stopped stops heartbeating, is dropped from everyone's set within `WORKER_TTL`, and its share moves to the survivors on the next tick. The worker still unions itself in, so a fleet of one, or one that has just started, always owns the whole table rather than idling.

`WORKER_TTL` defaults to `max(30s, 2*INTERVAL)` and must stay longer than `INTERVAL`, which `Validate` enforces. The heartbeat is written at the end of a tick and the peer set is read at the start of the next one, so a TTL at or below `INTERVAL` would expire a healthy worker's own row before it is read; with a two-interval default a departed worker is still gone within two ticks. An **empty** peer-set answer is a valid one and must not fall back to the previous set — that would keep a departed worker in the view and strand the share it owns — so only a real query failure keeps the last good peers.


What sharding does and does not divide:

| Per tick per worker | Effect of N workers |
| --- | --- |
| `Hashes` windowed eligibility scan | N scans per `HASH_SCAN_TTL`, each already bounded by `SCAN_RANGE` / `DRUID_MAX_RANGE`, so the cost is `N × ceil(SCAN_RANGE / DRUID_MAX_RANGE)` requests per 5 minutes instead of `N` per tick |
| Publishing for owned hashes (snapshot `Restore` + `ForecastRange`) | divided by about N; costs no Druid request |
| Retrain claims | fleet-wide, bounded by `TRAIN_CONCURRENCY` per worker: a due hash is retrained once by whichever worker claims it, not once per owner |

The scan is the price of eligibility (`min(__time)` over the scan window). Pushing the shard predicate into the SQL (`fnv_hash(metric_hash) % N`) would shrink the response but not the scan, so it is not used: ownership stays in Go, portable and testable.

Two invariants follow from where the checks sit. Eligibility is tested *after* ownership (`tick` asks `Owns` first and the retrain/publish paths apply the lookback), so `LOOKBACK` must be identical fleet-wide — an owner with a shorter lookback drops a hash that no other worker will pick up. And a static peer list must be edited *before* a process is stopped: a listed name with no process strands its share, whereas adding a name only duplicates points until the views converge. The `store` peer source removes both hazards for VMs.

Membership changes are not transactional. A handover costs at most one tick for the hashes that moved: the new owner starts publishing its own wall-clock point, and the old owner may publish the same point once more while its view is stale. There is no backfill; the dashboard shows a lead series, not a per-minute ledger.

## Snapshot store

Fitted state is persisted, so publishing never needs the training series again.

```go
type snapshotStore interface {
	Fresh(ctx context.Context, keys []string) (map[string]time.Time, error)
	Get(ctx context.Context, key string) (forecast.Snapshot, bool, error)
	Put(ctx context.Context, key string, rec snapshotRecord, snap forecast.Snapshot) error
}
```

Lifecycle: retrain loads `Series(hash, now-LOOKBACK, now)` → `FitSeasonalBaseline` → `SnapshotOf` → `Put`. Publish reads `Fresh(ownedKeys)` once per tick (at most one `SELECT metric_hash, updated_at ... WHERE metric_hash = ANY($1)`) and `Restore`s only the keys that are new or whose `updated_at` moved, keeping a `map[string]struct{ updatedAt time.Time; fitted forecast.Fitted }` cache. A minute-of-week baseline is ~60k floats restored, so restoring unconditionally on the publish path would dominate the tick; `SNAPSHOT_CACHE_TTL` (default 60s) also bounds how often the freshness probe runs.

The cache is pruned on every tick, before the TTL can short-circuit anything: a key this worker no longer owns (the hash moved to another peer on a scale event) is dropped along with its ~0.5 MB fit, so a handover does not pin the old owner's memory for the life of the process. The snapshot itself stays in `baselines.snapshots` and the new owner restores it on its next tick.

Worker-owned DDL (created on first successful use; a pre-provisioned table is accepted):

```sql
CREATE SCHEMA IF NOT EXISTS baselines;
CREATE TABLE IF NOT EXISTS baselines.snapshots (
  metric_hash TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  season TEXT NOT NULL,
  calendar TEXT NOT NULL DEFAULT '',
  lookback_ms BIGINT NOT NULL,
  trained_at TIMESTAMPTZ NOT NULL,
  snapshot BYTEA NOT NULL,          -- gzip(JSON forecast.Snapshot)
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS baselines.workers (
  id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  owned INTEGER NOT NULL DEFAULT 0,
  peers INTEGER NOT NULL DEFAULT 0
);
```

The snapshot is stored gzipped because the JSON form of a minute-of-week baseline is two arrays of 30,240 floats (`means` and `ses`) — about 0.55 MB of text — and gzip takes roughly 32× off that: the sandbox row measures 562,558 bytes of JSON against 17,449 bytes stored (`SELECT metric_hash, length(snapshot) FROM baselines.snapshots`).

DSN resolution reuses the plugin's precedence, first non-empty per field:

1. `BASELINE_STORE_URL`, else `BASELINE_STORE_HOST` / `PORT` / `DATABASE` / `USER` / `PASSWORD` / `SSLMODE`
2. Falling back field by field to `FORECAST_STORE_URL` / `FORECAST_STORE_*` (what the Grafana plugin already has)
3. Defaults: port `5432`, database `overlay`, user `overlay`, sslmode `disable`

No host and no URL means persist off: `store == nil`, the schedule and heartbeat are skipped, and the tick fits every ready owned hash as in v2 (this keeps Phases 1 and 6 landable without Postgres).

## Retrain schedule

`forecast.retrain` is created and owned by `timeseries-grafana` (Phase 4 of the plan); the worker only inserts `scope='baseline'` rows, claims them, and finishes them. One row per hash, `PRIMARY KEY (scope, org_id, key)` with the worker's rows at the fleet-wide `org_id = 0` (it has no notion of a Grafana org, and the plugin's panel rows are per-org, which is why the org is part of the key rather than a column).

| Column | Worker use |
| --- | --- |
| `scope`, `org_id`, `key` | `'baseline'`, `0`, `metric_hash` |
| `cron`, `timezone` | `DEFAULT_RETRAIN_CRON` (default `0 3 * * *`), `UTC` on insert; never reset by a later tick |
| `enabled`, `spec` | Left alone. The plugin claims only `scope='panel'` rows with a spec; the worker claims only `scope='baseline'` rows |
| `next_run_at` | Due when `<= now()`. Set to `nextRun(cron, tz, now)` on success and to `now + RETRAIN_RETRY` on failure |
| `claimed_by`, `claimed_until` | Lease: a row is claimable while `claimed_until IS NULL OR claimed_until < now()` |
| `last_status` | `ok`, or `error: <message>` |

The worker schedules its owned set with one statement per tick, not one per hash:

```sql
INSERT INTO forecast.retrain (scope, key, cron, timezone, enabled, next_run_at)
SELECT 'baseline', k, $2, $3, true, now() FROM unnest($1::text[]) AS k
ON CONFLICT (scope, org_id, key) DO NOTHING
```

`DO NOTHING` is what protects an operator's edit: once a row exists its `cron`, `timezone` and `enabled` are never touched again, however the fleet is restarted or rescaled. It also means an admin may delete a row: a hash that still reports comes back on the next tick with the default cron, while a hash that stopped reporting stays gone — which is how a retired metric's row stops being retrained (and re-queried) forever.

```sql
WITH due AS (
  SELECT scope, org_id, key FROM forecast.retrain
  WHERE scope = 'baseline' AND enabled AND next_run_at IS NOT NULL AND next_run_at <= now()
    AND (claimed_until IS NULL OR claimed_until < now())
  ORDER BY next_run_at LIMIT $2 FOR UPDATE SKIP LOCKED
)
UPDATE forecast.retrain r SET claimed_by = $1, claimed_until = now() + $3::interval
FROM due WHERE r.scope = due.scope AND r.org_id = due.org_id AND r.key = due.key
RETURNING r.org_id, r.key, r.cron, r.timezone
```

`FOR UPDATE SKIP LOCKED` is what makes the claim fleet-wide and safe: two workers ticking at the same second both get rows, but never the same row, and neither blocks. The lease means a worker that dies mid-retrain releases the claim after `RETRAIN_RETRY` (default 5m) rather than stranding it. Because a claim is not tied to ownership, a hash owned by a stopped worker is still retrained on time, and the retrain cost is bounded by `TRAIN_CONCURRENCY` (default 2) per worker regardless of how many rows are due. The claim carries the row's `org_id`, which is how `Done` addresses the row by its full key.

Releasing a claim is owner-guarded, because a retrain that outlived its lease has already been re-claimed by another worker:

```sql
UPDATE forecast.retrain
SET next_run_at = $4, last_run_at = now(), last_status = $5,
    claimed_by = NULL, claimed_until = NULL
WHERE scope = 'baseline' AND org_id = $3 AND key = $1 AND claimed_by = $2
```

Zero rows affected means the claim was lost: the finish is dropped (debug log, no error) rather than overwriting the newer owner's `next_run_at`, `last_status` and claim. The owner predicate is the whole guard — there is deliberately no `claimed_by IS NULL` escape, which would let a stale worker write its own outcome over a row the newer owner had already finished and released.

`nextRun` is `cron.ParseStandard(cron)` + `time.LoadLocation(tz)` + `Schedule.Next(now.In(loc)).UTC()`. `ParseStandard` is the 5-field form plus `@daily` / `@hourly` / `@every 1h` descriptors, and `Validate` refuses a `DEFAULT_RETRAIN_CRON` that does not parse at startup.

## Scaling and ingestion

Duplicates are possible (restart, handover, a peer that is listed but not running), so the storage side must be idempotent for `(metric_hash, metric_ts)`:

- Reference Druid Kafka supervisor: `metricsSpec` `doubleMax` (`doubleSum` double-counts a repeated point), `queryGranularity` `minute`, `rollup` `true`, dimensions `metric_hash`, timestamp `metric_ts` from millis.
- Dashboard queries then read that point with `MAX(baseline_value)`.
- Optional: `cleanup.policy=compact` on the baseline topic. Compaction is asynchronous and Druid reads with `useEarliestOffset`, so `doubleMax` is the correctness mechanism and compaction only trims the log.

Because the Kafka key routes by hash, keep a single producer implementation for this topic (a producer with a different partitioner would put equal keys on different partitions).

## Deployments

Both environments run the same binary and only differ in how the peer set arrives.

Kubernetes (`timeseries-k8s`): a worker Deployment plus a headless Service. `SHARD_ID` comes from `status.podIP` (Downward API) and `SHARD_DNS` points at the headless Service, so `kubectl scale deployment/<release>-baselines --replicas=N` is the whole operation: pods join the endpoint list, the others pick up the new view on their next tick. No replica count needs to agree with anything.

VM: `SHARD_MEMBERSHIP=store` plus the shared `BASELINE_STORE_*` DSN is now the recommended shape — join and leave need no list edit and no DNS, and a stopped worker ages out after `WORKER_TTL`. The static path is still supported: N processes, each with `SHARD_ID` set to its own address and `SHARD_PEERS` listing **every** running worker, e.g. `systemd` template units `baselines@0..N-1` with `SHARD_ID=10.0.0.11`, `SHARD_PEERS=10.0.0.11,10.0.0.12`. Each worker unions only *itself* into its view, so a list that does not name exactly the running set gives workers different views, and the deviations differ (measured on 2048 hashes with the ownership code):

| `SHARD_PEERS` vs running workers | Outcome (measured on 2048 hashes with the ownership code) |
| --- | --- |
| Names peers that are not running (`w0,w1,ghost` with `w0,w1` running) | that peer's share is stranded: **687** of 2048 hashes unpublished, 0 published twice |
| Omits a running worker (`w0,w1` with `w0,w1,w2` running) | no strands, **674** of 2048 published twice — duplicates only |
| Leaves one worker alone in its own view (`w1` running a list of just itself) | no strands, **985** of 2048 published twice — duplicates only |

Ownership only ever moves *onto* a peer that a view contains, so a strand needs a peer present in **every** view that is not running; any other disagreement produces a second owner instead, and ingestion collapses that. Adding a name is therefore always recoverable, while a listed name with no process behind it loses lead points until the list is corrected. If the VMs have a shared round-robin name or DNS, use `SHARD_DNS` instead so membership follows liveness.

Peer views can also disagree *while* converging: with `SHARD_DNS` the endpoint list lags a restart or a scale by one tick, and a worker that starts before its own endpoint exists sees only itself and publishes everything for that tick. Both are transient duplicates, never silent loss.

A membership change is not transactional, and a stale view can persist if two workers keep disagreeing (for example a `SHARD_PEERS` list that names a peer which comes back with a different identity): a hash can stay owned by a worker that is not running for longer than one tick. Confirm coverage after a scale event rather than assuming one tick.

Interfaces:

```go
type metricReader interface {
	Hashes(ctx context.Context, from, to time.Time) ([]metricSpan, error)
	Series(ctx context.Context, hash string, from, to time.Time) (timeseries.Series[float64], error)
}

type baselineSink interface {
	Publish(ctx context.Context, msg BaselineMessage) error
	Close() error
}

type snapshotStore interface {
	Fresh(ctx context.Context, keys []string) (map[string]time.Time, error)
	Get(ctx context.Context, key string) (forecast.Snapshot, bool, error)
	Put(ctx context.Context, key string, rec snapshotRecord, snap forecast.Snapshot) error
}

type retrainQueue interface {
	Schedule(ctx context.Context, keys []string, cron, tz string) error
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error)
	Done(ctx context.Context, owner string, orgID int64, key string, next time.Time, status string) error
}

type membership interface {
	Heartbeat(ctx context.Context, id string, owned, peers int) error
	Peers(ctx context.Context, ttl time.Duration) ([]string, error)
}
```

The three store interfaces exist as test seams first: `newPublisher` takes them as arguments, so `publisher_test.go` can prove the tick's shape (zero `Series` calls in steady state, one `Series` per claim, `Done`'s status and next run) without a database.


## Horizon clock

Same as `timeseries-forecast`: last timestamp + `k * step` for `k = 1..h`. This worker publishes `now` truncated to the minute + `AHEAD_MINUTES` from the stored snapshot, not the last observed timestamp; with no store it keeps the v2 form, last observed + `AHEAD_MINUTES`.

`now` is the tick's start, read once before the scan and passed to every step that stamps a time (the scan window, the retrain's `nextRun`/retry, the publish horizon). A tick is one minute of wall clock even when its bounded scan and its retrain take longer than that: reading the clock again after the fit stamped the *next* minute, and the following tick then published the same horizon and skipped one, so a `*/5` retrain lost one minute out of every five in the published series.

## Config (env)

| Variable | Default | Meaning |
| --- | --- | --- |
| `DRUID_BROKER` | (required) | Absolute broker URL |
| `DRUID_DATASOURCE` | `metrics` | `[A-Za-z0-9_]+` table name |
| `DRUID_MAX_RANGE` | `0` | Maximum span one Druid request may cover, on top of the `LOOKBACK`/`SCAN_RANGE` window the caller already set; `0` = do not slice it further (still one request for one window, never an unbounded query) |
| `DRUID_TIMEOUT` | `60s` | HTTP client timeout per request |
| `DRUID_RETRIES` | `2` | Extra attempts after a transport error or 5xx (250ms → 2s backoff); never retries 4xx |
| `DRUID_MAX_RPS` | `4` | Requests per second to Druid; `0` = off |
| `DRUID_MAX_INFLIGHT` | `4` | Concurrent Druid requests |
| `DRUID_AUTH_HEADER` | empty | Optional static auth header name |
| `DRUID_AUTH_VALUE` | empty | Value for `DRUID_AUTH_HEADER` |
| `HASH_SCAN_TTL` | `5m` | How long a `Hashes` scan is reused |
| `SCAN_RANGE` | `0` → `max(2*LOOKBACK, 24h)` | Window of the eligibility scan; when set it must be at least `LOOKBACK + 2*INTERVAL`, because the half-open scan clamps `Min` to the window start and excludes `now` |
| `TRAIN_CONCURRENCY` | `2` | Whole retrains one worker runs at once |
| `SNAPSHOT_CACHE_TTL` | `60s` | How often owned snapshots are re-probed for freshness |
| `WORKER_TTL` | `max(30s, 2*INTERVAL)` | A `baselines.workers` row is a peer while `last_seen` is newer than this; must stay longer than `INTERVAL` |
| `SHARD_MEMBERSHIP` | `auto` | `auto` / `peers` / `dns` / `store` |
| `DEFAULT_RETRAIN_CRON` | `0 3 * * *` | Cron for a newly scheduled `forecast.retrain` row |
| `RETRAIN_RETRY` | `5m` | Lease length and the retry delay after a failed retrain |
| `LOG_LEVEL` | `info` | `info` or `debug` |
| `BASELINE_STORE_URL`, `BASELINE_STORE_HOST`, `BASELINE_STORE_PORT`, `BASELINE_STORE_DATABASE`, `BASELINE_STORE_USER`, `BASELINE_STORE_PASSWORD`, `BASELINE_STORE_SSLMODE` | empty | Snapshot/schedule/membership Postgres; falls back per field to `FORECAST_STORE_*`. No host and no URL: persist off |
| `KAFKA_BROKERS` | (required) | Comma-separated brokers |
| `KAFKA_TOPIC` | `baselines` | Must not be the metrics topic |
| `LOOKBACK` | `336h` | Eligibility span and fit window |
| `AHEAD_MINUTES` | `1` | Published timestamp is `now` truncated to the minute + N minutes |
| `INTERVAL` | `1m` | Tick period |
| `CALENDAR` | empty | Empty or `ru` |
| `SHARD_ID` | first non-loopback IP | This worker's identity in the peer set |
| `SHARD_PEERS` | empty | Comma-separated peer identities (VMs) |
| `SHARD_DNS` | empty | Name whose A records are the peer set (headless Service, round-robin) |

Empty `SHARD_PEERS`, `SHARD_DNS`, and store mean one worker owns every hash. Setting `SHARD_PEERS` and `SHARD_DNS` together is an error.

## Performance

- One O(n) `FitSeasonalBaseline` per retrain, not per tick: publishing is a `Restore` plus one `ForecastRange(ts-1m, ts)` per owned hash
- `Series` buffers are pre-sized per window from the window length, not from a `COUNT(*)`
- Do not keep the training series after fit. `timeseries.Concat` is not incremental: it allocates a new `a.Len()+b.Len()` buffer and copies the accumulator plus the new window into it, so stitching `W` windows copies `O(W²)` points in total — 14 windows of 24h over a 336h lookback copy 91 window-loads, and the final copy holds the whole series. The window count (`ceil(window / DRUID_MAX_RANGE)`) is therefore the cost driver; the fit itself stays linear
- Ownership is `len(peers)` allocation-free hashes per hash per tick, and the whole check is skipped when a worker is alone
- Pruning the fit cache reuses one owned-key set per publisher, so it allocates nothing per tick
- `Fresh` is one query per tick for all owned keys; `Restore` runs only for keys whose `updated_at` moved; the schedule is one statement per tick for the whole owned set

## Modules

`go.mod` requires tagged `github.com/eduard-kolotushin/timeseries` and `github.com/eduard-kolotushin/timeseries-forecast`, plus `github.com/robfig/cron/v3` and `github.com/jackc/pgx/v5`. Do not add a `replace` directive.

# Architecture

## Layout

Single package `baselines` plus `cmd/baselines`:

| Path | Responsibility |
| --- | --- |
| `config.go` | Env/flag config and validation |
| `druid.go` | Druid SQL `metric_hash` scan and series load |
| `kafka.go` | Kafka writer for baseline messages |
| `publisher.go` | Tick: skip short/non-1m hashes, fit, publish last+N |
| `cmd/baselines` | Process entry: load config, run until SIGINT/SIGTERM |

## Data flow

Each tick:

1. Druid SQL `GROUP BY metric_hash` for `min(__time)`, `max(__time)`.
2. Skip hashes whose span is shorter than `lookback` (default 336h).
3. Load the last lookback window; skip unless inferred step is 1 minute.
4. `FitSeasonalBaseline(..., SeasonMinuteOfWeek, calendar)` then `Forecast(N)`.
5. Publish only the last point to the **baseline** Kafka topic:

```json
{"metric_hash":"...","metric_ts":<unix_ms>,"baseline_value":<float>}
```

`metric_ts` is last observed timestamp + N minutes. The metrics Kafka topic is not read; Druid is the source of truth. Duplicate `(hash, metric_ts)` pairs are skipped in memory (process restart may republish; consumers treat as upsert).

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

## Performance

- One O(n) `FitSeasonalBaseline` per ready hash per tick
- `Forecast(N)` is O(N); only the last point is published
- Pre-size times/values slices to the Druid row count
- Do not keep the training series after fit

## Local modules

```
replace github.com/eduard-kolotushin/timeseries => ../timeseries
replace github.com/eduard-kolotushin/timeseries-forecast => ../timeseries-forecast
```

# Project intentions

## Goal

Standalone process that publishes minute-of-week seasonal baselines from a Druid table to Kafka. Grafana does not host this ticker. Fit math stays in `timeseries-forecast`.

Implement the loop efficiently: one O(n) fit per ready hash per tick, O(1) work per horizon step, pre-sized series slices.

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
| Sandbox | sibling `timeseries-grafana-sandbox` |

## v1 must-have

- Env/flags: Druid broker/datasource, Kafka brokers/baseline topic, lookback, aheadMinutes N, interval, calendar
- Distinct `metric_hash` from Druid; skip unless `max(__time)-min(__time) >= lookback`
- Fit last lookback window with minute-of-week seasonal baseline; skip non-1-minute series
- Publish `{"metric_hash","metric_ts","baseline_value"}` to the baseline Kafka topic (`metric_ts` Unix ms = last + N minutes)

## v1 non-goals

Do not add these without first updating this document:

- Overlay visualization or Grafana plugin hosting
- Folding this ticker into `timeseries-forecast`
- Consuming the metrics Kafka topic (Druid is the source of truth)
- Prometheus
- Prediction intervals or alerting
- A forked Series type

## Quality bar

- Do not mutate caller series (libraries already return new series)
- Table-driven tests for config, Druid SQL client, and publisher ticks
- One O(n) fit per hash per tick; O(1) per horizon step; pre-size series slices
- GitHub Actions on `main` runs `gofmt` and `go test ./...`

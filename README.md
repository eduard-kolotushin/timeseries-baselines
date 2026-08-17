# timeseries-baselines

Standalone worker that reads a Druid metrics table, fits a minute-of-week seasonal baseline from [`timeseries-forecast`](https://github.com/eduard-kolotushin/timeseries-forecast), and publishes one lead point per ready `metric_hash` to Kafka.

This is **not** a Grafana plugin. Grafana overlays live in [`timeseries-grafana`](../timeseries-grafana). Local Compose lives in [`timeseries-grafana-sandbox`](../timeseries-grafana-sandbox).

See [docs/INTENTIONS.md](docs/INTENTIONS.md) and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Local siblings

```
replace github.com/eduard-kolotushin/timeseries => ../timeseries
replace github.com/eduard-kolotushin/timeseries-forecast => ../timeseries-forecast
```

## Build

```bash
make test    # go test ./...
make linux   # Linux amd64 binary -> bin/baselines (sandbox mount)
```

The sandbox Compose service `baseline-worker` mounts that binary and sets `DRUID_BROKER`, `KAFKA_BROKERS`, and related env. Running the process is what enables the ticker.

## Agents

Contributors and coding agents: start with [AGENTS.md](AGENTS.md).

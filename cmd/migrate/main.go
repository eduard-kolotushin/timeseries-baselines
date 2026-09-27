// Command baselines-migrate applies this worker's PostgreSQL migrations without
// starting the worker, so a CI/CD pipeline can prepare the database before the
// process starts. It reads the same store settings as the worker
// (BASELINE_STORE_*, falling back per field to FORECAST_STORE_*) and the same
// embedded migration files, which the worker also applies at its first store use:
// running it is optional, and running it a second time is a no-op.
//
//	baselines-migrate [--dsn postgres://…] [--dry-run] [--timeout 60s]
package main

import (
	"os"

	"github.com/eduard-kolotushin/timeseries-baselines"
)

func main() {
	os.Exit(baselines.MigrateMain(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

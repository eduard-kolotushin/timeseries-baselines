package baselines

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MigrateMain applies this worker's migrations without starting the worker, so a
// CI/CD pipeline can prepare the database before the process starts. It reads the
// same store env as ConfigFromEnv (BASELINE_STORE_*, falling back field by field
// to FORECAST_STORE_*), takes --dsn to override them, and returns the process exit
// code: 0 when the schema is up to date (including a ledger written by a newer
// deployment), 1 on any error. It takes its environment and streams as arguments
// so a test can drive it without a subprocess.
//
//	baselines-migrate [--dsn postgres://…] [--dry-run] [--timeout 60s]
func MigrateMain(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("baselines-migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var (
		dsn     = flags.String("dsn", "", "store DSN; overrides BASELINE_STORE_URL / BASELINE_STORE_HOST")
		dryRun  = flags.Bool("dry-run", false, "report the pending migrations without applying them")
		timeout = flags.Duration("timeout", 60*time.Second, "deadline for the whole run")
	)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 1
	}
	resolved := strings.TrimSpace(*dsn)
	if resolved == "" {
		resolved = storeDSN(getenv)
	}
	if resolved == "" {
		fmt.Fprintln(stderr, "baselines-migrate: no store DSN: pass --dsn or set BASELINE_STORE_URL / BASELINE_STORE_HOST")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	pool, err := pgxpool.New(ctx, resolved)
	if err != nil {
		fmt.Fprintf(stderr, "baselines-migrate: %v\n", err)
		return 1
	}
	defer pool.Close()

	res, err := applyMigrations(ctx, pool, schemaMigrations, *dryRun)
	if err != nil {
		fmt.Fprintf(stderr, "baselines-migrate: %v\n", err)
		return 1
	}
	names := make(map[string]string, len(schemaMigrations))
	for _, m := range schemaMigrations {
		names[m.Version] = m.Version + "_" + m.Name
	}
	if len(res.Unknown) > 0 {
		fmt.Fprintf(stderr, "warning: baselines.schema_migrations has versions this binary does not embed: %s (applied by a newer deployment)\n", strings.Join(res.Unknown, " "))
	}
	switch {
	case len(res.Applied) > 0:
		for _, version := range res.Applied {
			fmt.Fprintf(stdout, "applied %s\n", names[version])
		}
	case len(res.Pending) > 0:
		for _, version := range res.Pending {
			fmt.Fprintf(stdout, "pending %s\n", names[version])
		}
	default:
		fmt.Fprintf(stdout, "nothing to apply (%d known, %d applied)\n", len(schemaMigrations), res.Already)
	}
	return 0
}

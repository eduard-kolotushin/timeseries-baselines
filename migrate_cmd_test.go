package baselines

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// unreachable is a DSN that parses and then fails to connect, so a run that gets
// past flag and DSN resolution fails with a connect error instead.
const unreachable = "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1"

func migrateFor(getenv func(string) string, args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := MigrateMain(args, getenv, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func noEnv(string) string { return "" }

func TestMigrateMainNoDSN(t *testing.T) {
	code, stdout, stderr := migrateFor(noEnv)
	if code != 1 {
		t.Fatalf("exit %d, want 1 (stderr %q)", code, stderr)
	}
	if !strings.Contains(stderr, "--dsn") || !strings.Contains(stderr, "BASELINE_STORE_URL") {
		t.Fatalf("stderr %q must name both ways to pass a DSN", stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout %q, want nothing", stdout)
	}
}

// TestMigrateMainResolvesFromEnv: the CLI reads the worker's own env names and the
// plugin's as the fallback, so a deployment that already configures the store does
// not repeat itself. The reachable-DSN case is covered by
// TestMigrateMainAppliesAndReports; this one only has to prove the DSN was found,
// which shows up as a connect error instead of "no store DSN".
func TestMigrateMainResolvesFromEnv(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{name: "BASELINE_STORE_URL", env: map[string]string{"BASELINE_STORE_URL": unreachable}},
		{name: "BASELINE_STORE_HOST and PORT", env: map[string]string{
			"BASELINE_STORE_HOST": "127.0.0.1",
			"BASELINE_STORE_PORT": "1",
		}},
		{name: "FORECAST_STORE_URL", env: map[string]string{"FORECAST_STORE_URL": unreachable}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, _, stderr := migrateFor(func(key string) string { return tt.env[key] }, "--timeout", "5s")
			if code != 1 {
				t.Fatalf("exit %d, want 1 (stderr %q)", code, stderr)
			}
			if strings.Contains(stderr, "no store DSN") {
				t.Fatalf("the env DSN was not read: %q", stderr)
			}
			if !strings.Contains(stderr, "baselines-migrate:") {
				t.Fatalf("stderr %q must name the binary", stderr)
			}
		})
	}
}

func TestMigrateMainBadFlag(t *testing.T) {
	if code, _, _ := migrateFor(noEnv, "--nope"); code != 1 {
		t.Fatalf("an unknown flag must exit 1, got %d", code)
	}
	if code, _, _ := migrateFor(noEnv, "-h"); code != 0 {
		t.Fatalf("-h must exit 0, got %d", code)
	}
}

func TestMigrateMainUnreachableDSN(t *testing.T) {
	code, stdout, stderr := migrateFor(noEnv, "--dsn", unreachable, "--timeout", "5s")
	if code != 1 {
		t.Fatalf("exit %d, want 1 (stderr %q)", code, stderr)
	}
	if stdout != "" {
		t.Fatalf("stdout %q, want nothing", stdout)
	}
	if !strings.Contains(stderr, "baselines-migrate:") {
		t.Fatalf("stderr %q must report the failure", stderr)
	}
}

func TestMigrateMainAppliesAndReports(t *testing.T) {
	ctx := context.Background()
	_, dsn, cleanup := scratchDatabase(t, "migrate_cli")
	defer cleanup()

	code, stdout, stderr := migrateFor(noEnv, "--dsn", dsn, "--timeout", "30s")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	for _, want := range []string{"applied 0001_snapshots", "applied 0002_workers"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout %q lacks %q", stdout, want)
		}
	}
	if stderr != "" {
		t.Fatalf("stderr %q, want nothing", stderr)
	}

	code, stdout, _ = migrateFor(noEnv, "--dsn", dsn, "--timeout", "30s")
	want := fmt.Sprintf("nothing to apply (%d known, %d applied)\n", len(schemaMigrations), len(schemaMigrations))
	if code != 0 || stdout != want {
		t.Fatalf("the second run must be a no-op: exit %d stdout %q want %q", code, stdout, want)
	}

	// A version this binary does not embed is a warning, not a failure.
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, `INSERT INTO baselines.schema_migrations (version, name) VALUES ('9999', 'future')`); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr = migrateFor(noEnv, "--dsn", dsn, "--timeout", "30s")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	if !strings.Contains(stderr, "9999") || !strings.Contains(stderr, "warning") {
		t.Fatalf("stderr %q must warn about the newer ledger version", stderr)
	}
	if !strings.Contains(stdout, "nothing to apply") {
		t.Fatalf("stdout %q", stdout)
	}
	if _, err := admin.Exec(ctx, `DELETE FROM baselines.schema_migrations WHERE version = '9999'`); err != nil {
		t.Fatal(err)
	}

	// A dry run on a database nothing has touched reports the work and leaves no
	// schema behind.
	_, dryDSN, dryCleanup := scratchDatabase(t, "migrate_cli_dry")
	defer dryCleanup()
	code, stdout, stderr = migrateFor(noEnv, "--dsn", dryDSN, "--timeout", "30s", "--dry-run")
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	for _, want := range []string{"pending 0001_snapshots", "pending 0002_workers"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("stdout %q lacks %q", stdout, want)
		}
	}
	dry, err := pgxpool.New(ctx, dryDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer dry.Close()
	var exists bool
	if err := dry.QueryRow(ctx, `SELECT to_regnamespace('baselines') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("a dry run created the schema")
	}
}

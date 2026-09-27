package baselines

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestParseMigrationName(t *testing.T) {
	tests := []struct {
		file    string
		version string
		name    string
		wantErr bool
	}{
		{file: "0001_snapshots.sql", version: "0001", name: "snapshots"},
		{file: "0002_workers.sql", version: "0002", name: "workers"},
		{file: "0010_a_b.sql", version: "0010", name: "a_b"},
		{file: "nope.sql", wantErr: true},
		{file: "0001.sql", wantErr: true},
		{file: "1_a.sql", wantErr: true},
		{file: "000a_a.sql", wantErr: true},
		{file: "0001_.sql", wantErr: true},
		{file: "0001_a.SQL", wantErr: true},
		{file: "0001_a", wantErr: true},
	}
	for _, tt := range tests {
		version, name, err := parseMigrationName(tt.file)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("%s: want an error, got %q/%q", tt.file, version, name)
			}
			continue
		}
		if err != nil || version != tt.version || name != tt.name {
			t.Fatalf("%s: got %q/%q err=%v want %q/%q", tt.file, version, name, err, tt.version, tt.name)
		}
	}
}

func TestAllMigrations(t *testing.T) {
	ms := allMigrations()
	if len(ms) < 2 {
		t.Fatalf("want at least the snapshots and workers migrations, got %d", len(ms))
	}
	seen := map[string]bool{}
	for i, m := range ms {
		if m.Version == "" || m.Name == "" || strings.TrimSpace(m.SQL) == "" {
			t.Fatalf("migration %d is incomplete: %+v", i, m)
		}
		if seen[m.Version] {
			t.Fatalf("two migrations share version %s", m.Version)
		}
		seen[m.Version] = true
		if i > 0 && ms[i-1].Version >= m.Version {
			t.Fatalf("versions are not ascending: %s then %s", ms[i-1].Version, m.Version)
		}
	}
	for _, name := range []string{"snapshots", "workers"} {
		if migrationNamed(t, name).SQL == "" {
			t.Fatalf("no %s migration", name)
		}
	}
}

func migrationNamed(t *testing.T, name string) migration {
	t.Helper()
	for _, m := range allMigrations() {
		if m.Name == name {
			return m
		}
	}
	t.Fatalf("no %s migration in the embedded set", name)
	return migration{}
}

// TestMigrationFilesCarryTheUuidKey was deleted with the retention pass: it asserted
// literal substrings of the embedded .sql files and a byte offset between two
// statements. TestMigrateBaselinesFromScratch and TestMigrateAdoptsTheV3Schema
// assert the same contract against a real database (uuid primary key, UNIQUE
// natural key, the legacy table adopted in place), which is what the files have to
// do rather than what they spell.

// stubPool stands in for Postgres so the apply loop's decisions are pinned without
// a database: what a dry run writes, what a real run writes, and what happens when
// another migrator committed the version while this one waited for the lock.
type stubPool struct {
	// ledger is what the aggregate read answers: one version per line.
	ledger string
	// appliedInTx is what the per-version probe inside the transaction answers.
	appliedInTx bool
	statements  []string
}

func (p *stubPool) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	p.statements = append(p.statements, sql)
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (p *stubPool) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	p.statements = append(p.statements, sql)
	return stubRow{p: p}
}

func (p *stubPool) Begin(context.Context) (pgx.Tx, error) { return stubTx{p: p}, nil }

type stubTx struct {
	pgx.Tx
	p *stubPool
}

func (t stubTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.p.Exec(ctx, sql, args...)
}

func (t stubTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.p.QueryRow(ctx, sql, args...)
}

func (stubTx) Commit(context.Context) error   { return nil }
func (stubTx) Rollback(context.Context) error { return nil }

type stubRow struct{ p *stubPool }

func (r stubRow) Scan(dest ...any) error {
	switch d := dest[0].(type) {
	case *string:
		*d = r.p.ledger
		return nil
	case *int:
		if !r.p.appliedInTx {
			return pgx.ErrNoRows
		}
		*d = 1
		return nil
	}
	return errors.New("stubRow does not answer this statement")
}

// TestApplyMigrationsDryRunReadsOnly: a dry run reports the pending versions and
// writes nothing at all — not the migrations, not the ledger, not even the schema.
func TestApplyMigrationsDryRunReadsOnly(t *testing.T) {
	pool := &stubPool{}
	res, err := applyMigrations(context.Background(), pool, allMigrations(), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("a dry run applied %v", res.Applied)
	}
	if len(res.Pending) != len(allMigrations()) {
		t.Fatalf("pending %v, want every version", res.Pending)
	}
	for _, sql := range pool.statements {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
			t.Fatalf("a dry run wrote: %s", sql)
		}
	}
}

// TestApplyMigrationsWritesLedgerAfterEachFile: one transaction per version, and
// the ledger row only after that version's SQL.
func TestApplyMigrationsWritesLedgerAfterEachFile(t *testing.T) {
	pool := &stubPool{}
	res, err := applyMigrations(context.Background(), pool, allMigrations(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != len(allMigrations()) {
		t.Fatalf("applied %v, want every version", res.Applied)
	}
	var order []string
	for _, sql := range pool.statements {
		switch {
		case sql == migrationLedgerDDL:
			// The idempotent ledger bootstrap runs first in every transaction.
		case strings.HasPrefix(sql, "INSERT INTO baselines.schema_migrations"):
			order = append(order, "ledger-row")
		default:
			for _, m := range allMigrations() {
				if sql == m.SQL {
					order = append(order, m.Version)
				}
			}
		}
	}
	var want []string
	for _, m := range allMigrations() {
		want = append(want, m.Version, "ledger-row")
	}
	if !equalMigrationColumns(order, want) {
		t.Fatalf("statement order %v, want %v", order, want)
	}
}

// TestApplyMigrationsSkipsAVersionAnotherMigratorCommitted: the re-read inside the
// transaction is what makes concurrent migrators safe, so a version committed
// while this one waited for the lock must not be applied twice.
func TestApplyMigrationsSkipsAVersionAnotherMigratorCommitted(t *testing.T) {
	pool := &stubPool{appliedInTx: true}
	res, err := applyMigrations(context.Background(), pool, allMigrations(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("applied %v, want nothing", res.Applied)
	}
	for _, sql := range pool.statements {
		if strings.Contains(sql, "CREATE TABLE IF NOT EXISTS baselines.snapshots") {
			t.Fatal("a migration was applied although another migrator had committed it")
		}
	}
}

func TestMigrateBaselinesFromScratch(t *testing.T) {
	ctx := context.Background()
	pool := scratchPool(t, "migrate_from_scratch")

	res, err := applyMigrations(ctx, pool, allMigrations(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Applied) != len(allMigrations()) || len(res.Pending) != 0 || len(res.Unknown) != 0 {
		t.Fatalf("first run: applied %v pending %v unknown %v", res.Applied, res.Pending, res.Unknown)
	}
	again, err := applyMigrations(ctx, pool, allMigrations(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(again.Applied) != 0 || len(again.Pending) != 0 {
		t.Fatalf("a second run applied %v pending %v, want nothing", again.Applied, again.Pending)
	}

	var versions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM baselines.schema_migrations`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != len(allMigrations()) {
		t.Fatalf("ledger has %d rows, want %d", versions, len(allMigrations()))
	}
	assertMigrationPrimaryKey(t, ctx, pool, "baselines.schema_migrations", "id")
	assertMigrationPrimaryKey(t, ctx, pool, "baselines.snapshots", "id")
	assertMigrationUnique(t, ctx, pool, "baselines.snapshots", "metric_hash")
	assertMigrationPrimaryKey(t, ctx, pool, "baselines.workers", "id")
	assertMigrationUnique(t, ctx, pool, "baselines.workers", "worker_id")
}

// TestMigrateAdoptsTheV3Schema starts from the shape the previous release created:
// the natural key as the primary key and no uuid column (baselines.workers even
// named its identity column id). Everything the worker does to those tables has to
// keep working.
func TestMigrateAdoptsTheV3Schema(t *testing.T) {
	ctx := context.Background()
	pool := scratchPool(t, "migrate_legacy")
	if _, err := pool.Exec(ctx, `
CREATE SCHEMA baselines;
CREATE TABLE baselines.snapshots (
  metric_hash TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  season TEXT NOT NULL,
  calendar TEXT NOT NULL DEFAULT '',
  lookback_ms BIGINT NOT NULL,
  trained_at TIMESTAMPTZ NOT NULL,
  snapshot BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE baselines.workers (
  id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  owned INTEGER NOT NULL DEFAULT 0,
  peers INTEGER NOT NULL DEFAULT 0
);
INSERT INTO baselines.snapshots (metric_hash, model, season, lookback_ms, trained_at, snapshot)
VALUES ('keep-me', 'baseline', 'minute-week', 336, now(), 'x'::bytea);
INSERT INTO baselines.workers (id, owned, peers) VALUES ('10.0.0.5', 7, 2);
`); err != nil {
		t.Fatal(err)
	}
	if _, err := applyMigrations(ctx, pool, allMigrations(), false); err != nil {
		t.Fatal(err)
	}

	// The rows survived and the old identity moved into worker_id.
	var (
		snapID     string
		metricHash string
		workerID   string
		oldID      string
		owned      int
		peers      int
	)
	if err := pool.QueryRow(ctx, `SELECT id::text, metric_hash FROM baselines.snapshots`).Scan(&snapID, &metricHash); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT id::text, worker_id, owned, peers FROM baselines.workers`).Scan(&workerID, &oldID, &owned, &peers); err != nil {
		t.Fatal(err)
	}
	if metricHash != "keep-me" || snapID == "" {
		t.Fatalf("snapshot row changed: id=%q metric_hash=%q", snapID, metricHash)
	}
	if oldID != "10.0.0.5" || owned != 7 || peers != 2 {
		t.Fatalf("worker row changed: worker_id=%q owned=%d peers=%d", oldID, owned, peers)
	}
	if workerID == "" || workerID == oldID {
		t.Fatalf("worker id %q, want a fresh uuid", workerID)
	}
	// The uuid column is a uuid, not the text key the old table had: had the rename
	// been skipped, ADD COLUMN IF NOT EXISTS would have found the text column and
	// left it as the primary key.
	var idType string
	if err := pool.QueryRow(ctx, `
SELECT data_type FROM information_schema.columns
WHERE table_schema = 'baselines' AND table_name = 'workers' AND column_name = 'id'`).Scan(&idType); err != nil {
		t.Fatal(err)
	}
	if idType != "uuid" {
		t.Fatalf("baselines.workers.id is %s, want the uuid surrogate key", idType)
	}

	assertMigrationPrimaryKey(t, ctx, pool, "baselines.snapshots", "id")
	assertMigrationUnique(t, ctx, pool, "baselines.snapshots", "metric_hash")
	assertMigrationPrimaryKey(t, ctx, pool, "baselines.workers", "id")
	assertMigrationUnique(t, ctx, pool, "baselines.workers", "worker_id")

	// Every upsert the worker runs still resolves through the natural key.
	if _, err := pool.Exec(ctx, `
INSERT INTO baselines.snapshots (metric_hash, model, season, lookback_ms, trained_at, snapshot)
VALUES ('keep-me', 'm', 's', 1, now(), 'y'::bytea)
ON CONFLICT (metric_hash) DO UPDATE SET model = EXCLUDED.model`); err != nil {
		t.Fatalf("the snapshot upsert no longer resolves: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO baselines.workers (worker_id, owned, peers) VALUES ('10.0.0.5', 9, 2)
ON CONFLICT (worker_id) DO UPDATE SET owned = EXCLUDED.owned`); err != nil {
		t.Fatalf("the heartbeat upsert no longer resolves: %v", err)
	}
	// …and the natural key is still unique, so ON CONFLICT has something to lock.
	if _, err := pool.Exec(ctx, `
INSERT INTO baselines.snapshots (metric_hash, model, season, lookback_ms, trained_at, snapshot)
VALUES ('keep-me', 'm', 's', 1, now(), 'y'::bytea)`); err == nil {
		t.Fatal("the snapshot natural key is not unique any more")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO baselines.workers (worker_id) VALUES ('10.0.0.5')`); err == nil {
		t.Fatal("the worker natural key is not unique any more")
	}
}

func TestMigrateConcurrent(t *testing.T) {
	ctx := context.Background()
	pool := scratchPool(t, "migrate_concurrent")
	second, err := pgxpool.New(ctx, pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(second.Close)

	var wg sync.WaitGroup
	results := make([]migrationResult, 2)
	errs := make([]error, 2)
	for i, p := range []*pgxpool.Pool{pool, second} {
		wg.Add(1)
		go func(i int, p *pgxpool.Pool) {
			defer wg.Done()
			results[i], errs[i] = applyMigrations(ctx, p, allMigrations(), false)
		}(i, p)
	}
	wg.Wait()
	applied := 0
	for i, err := range errs {
		if err != nil {
			t.Fatalf("migrator %d: %v", i, err)
		}
		applied += len(results[i].Applied)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM baselines.schema_migrations`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != len(allMigrations()) {
		t.Fatalf("ledger has %d rows, want %d", rows, len(allMigrations()))
	}
	if applied != len(allMigrations()) {
		t.Fatalf("the two migrators applied %d versions in total, want %d", applied, len(allMigrations()))
	}
}

func TestMigrateUnknownVersion(t *testing.T) {
	ctx := context.Background()
	pool := scratchPool(t, "migrate_unknown")
	if _, err := applyMigrations(ctx, pool, allMigrations(), false); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO baselines.schema_migrations (version, name) VALUES ('9999', 'future')`); err != nil {
		t.Fatal(err)
	}
	res, err := applyMigrations(ctx, pool, allMigrations(), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Unknown) != 1 || res.Unknown[0] != "9999" {
		t.Fatalf("unknown %v, want [9999]", res.Unknown)
	}
	if len(res.Applied) != 0 {
		t.Fatalf("applied %v, want nothing", res.Applied)
	}
}

// TestApplyMigrationsRollsBackAFailingMigration: one transaction per file means a
// failure leaves neither the DDL nor a ledger row behind.
func TestApplyMigrationsRollsBackAFailingMigration(t *testing.T) {
	ctx := context.Background()
	pool := scratchPool(t, "migrate_failing")
	ms := []migration{
		{Version: "0001", Name: "ok", SQL: `CREATE TABLE baselines.rollback_probe (id UUID PRIMARY KEY DEFAULT gen_random_uuid())`},
		{Version: "0002", Name: "boom", SQL: `CREATE TABLE baselines.rollback_probe_2 (id UUID PRIMARY KEY); SELECT 1/0`},
	}
	_, err := applyMigrations(ctx, pool, ms, false)
	if err == nil {
		t.Fatal("want the failing migration to fail the run")
	}
	if !strings.Contains(err.Error(), "0002_boom") {
		t.Fatalf("the error does not name the file: %v", err)
	}
	var versions string
	if err := pool.QueryRow(ctx, `SELECT coalesce(string_agg(version, ','), '') FROM baselines.schema_migrations`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if versions != "0001" {
		t.Fatalf("ledger %q, want 0001 only", versions)
	}
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('baselines.rollback_probe_2') IS NOT NULL`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("the failing migration's DDL was committed")
	}
}

// scratchDSN points dsn at another database, for a test that needs its own rather
// than the one the application uses. The DSN must be a URL, which is what
// BASELINE_STORE_URL and storeDSN produce; the rewrite is done on the URL text
// because pgx.ConnConfig.ConnString returns the string that was parsed rather than
// the current fields.
func scratchDSN(dsn, database string) (string, error) {
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return "", fmt.Errorf("baseline store: %w", err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("baseline store: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("baseline store: %q is not a URL DSN", dsn)
	}
	u.Path = "/" + database
	return u.String(), nil
}

// scratchDatabase drops and recreates a database for one test and returns the
// admin DSN, the scratch DSN and a cleanup, so the pg-gated migration tests cannot
// race store_test.go's tests on the shared service database.
func scratchDatabase(t *testing.T, name string) (adminDSN, dsn string, cleanup func()) {
	t.Helper()
	configured := os.Getenv("BASELINE_TEST_PG")
	if configured == "" {
		t.Skip("BASELINE_TEST_PG not set")
	}
	ctx := context.Background()
	var err error
	adminDSN, err = scratchDSN(configured, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	dsn, err = scratchDSN(configured, name)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Fatalf("drop %s: %v", name, err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return adminDSN, dsn, func() {
		if admin, err := pgxpool.New(context.Background(), adminDSN); err == nil {
			defer admin.Close()
			if _, err := admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
				t.Errorf("drop %s: %v", name, err)
			}
		}
	}
}

// scratchPool is scratchDatabase as a pool for the tests that drive the engine
// directly.
func scratchPool(t *testing.T, name string) *pgxpool.Pool {
	t.Helper()
	_, dsn, cleanup := scratchDatabase(t, name)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanup()
	})
	return pool
}

// migrationConstraintColumns returns the column names of every constraint of one
// type on a table, keyed by constraint name.
func migrationConstraintColumns(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table, contype string) map[string][]string {
	t.Helper()
	rows, err := pool.Query(ctx, `
SELECT c.conname, array_agg(a.attname ORDER BY x.ord)
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord) ON true
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = x.attnum
WHERE n.nspname = split_part($1, '.', 1) AND t.relname = split_part($1, '.', 2) AND c.contype::text = $2
GROUP BY c.conname`, table, contype)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[string][]string{}
	for rows.Next() {
		var name string
		var columns []string
		if err := rows.Scan(&name, &columns); err != nil {
			t.Fatal(err)
		}
		out[name] = columns
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertMigrationPrimaryKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, columns ...string) {
	t.Helper()
	got := migrationConstraintColumns(t, ctx, pool, table, "p")
	if len(got) != 1 {
		t.Fatalf("%s: want exactly one primary key, got %v", table, got)
	}
	for _, cols := range got {
		if !equalMigrationColumns(cols, columns) {
			t.Fatalf("%s: primary key is %v, want %v", table, cols, columns)
		}
	}
}

func assertMigrationUnique(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string, columns ...string) {
	t.Helper()
	got := migrationConstraintColumns(t, ctx, pool, table, "u")
	for _, cols := range got {
		if equalMigrationColumns(cols, columns) {
			return
		}
	}
	t.Fatalf("%s: no UNIQUE (%s); unique constraints: %v", table, strings.Join(columns, ", "), got)
}

func equalMigrationColumns(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

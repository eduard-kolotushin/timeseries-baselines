// Package baselines owns the PostgreSQL schema this worker creates: the
// versioned migration files under migrations/, the engine that applies them, and
// the store DSN resolution in storedsn.go.
//
// The migration files are the only schema authority for schema baselines.
// cmd/migrate applies them out-of-process so a CI/CD pipeline can prepare a
// database before the worker starts; a deployment that never runs that step still
// converges, because the worker applies the same set at its first store use.
//
// forecast.retrain is NOT created here: that table belongs to the plugin
// (timeseries-grafana), which owns its API; this process only reads, claims and
// finishes rows in it.
package baselines

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationAdvisoryLockKey serialises concurrent migrators — a CI/CD job racing
// a worker that also auto-applies. 0x626173656c696e65 is "baseline" in ASCII.
const migrationAdvisoryLockKey int64 = 0x626173656c696e65

// migrationLedgerDDL is the one table the engine creates outside the migration
// files, because it records them. It is idempotent and runs inside the advisory
// lock, so two migrators starting against an empty database cannot race each
// other into a duplicate-table error.
const migrationLedgerDDL = `
CREATE SCHEMA IF NOT EXISTS baselines;
CREATE TABLE IF NOT EXISTS baselines.schema_migrations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  version TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// migrationLedgerVersionsSQL answers with one aggregated row: an empty ledger
// still has a row to scan, and a missing table is the only way this read comes
// back empty.
const migrationLedgerVersionsSQL = `SELECT coalesce(string_agg(version, E'\n'), '') FROM baselines.schema_migrations`

const (
	migrationAdvisoryLockSQL   = `SELECT pg_advisory_xact_lock($1)`
	migrationAlreadyAppliedSQL = `SELECT 1 FROM baselines.schema_migrations WHERE version = $1`
	migrationInsertLedgerSQL   = `INSERT INTO baselines.schema_migrations (version, name) VALUES ($1, $2)`
	migrationNameHint          = "want NNNN_name.sql"
	migrationNameInfix         = "_"
	migrationFileExt           = ".sql"
	migrationDir               = "migrations"
)

// migration is one versioned, idempotent SQL file: 0001_snapshots.sql is Version
// "0001", Name "snapshots".
type migration struct {
	Version string
	Name    string
	SQL     string
}

// migrationResult is what one applyMigrations call did.
type migrationResult struct {
	Applied []string // versions applied by this call, in order
	Pending []string // versions still to apply; a dry run fills this in and writes nothing
	Unknown []string // versions in the ledger that this binary does not embed
	// Already is how many of ms the ledger already listed when this call read it.
	Already int
}

// migrationPool is the slice of *pgxpool.Pool the engine needs.
type migrationPool interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// allMigrations returns the embedded migrations, sorted by version. A malformed
// file name panics: the names are build inputs, not user input, and a skipped
// migration is worse than a process that will not start.
func allMigrations() []migration {
	entries, err := fs.ReadDir(migrationFS, migrationDir)
	if err != nil {
		panic("baselines: embedded migrations unreadable: " + err.Error())
	}
	ms := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		version, name, err := parseMigrationName(entry.Name())
		if err != nil {
			panic("baselines: " + err.Error())
		}
		raw, err := migrationFS.ReadFile(migrationDir + "/" + entry.Name())
		if err != nil {
			panic("baselines: " + err.Error())
		}
		ms = append(ms, migration{Version: version, Name: name, SQL: strings.TrimSpace(string(raw))})
	}
	sort.Slice(ms, func(i, j int) bool { return ms[i].Version < ms[j].Version })
	return ms
}

// parseMigrationName splits 0001_snapshots.sql into its version and name.
func parseMigrationName(file string) (string, string, error) {
	base, ok := strings.CutSuffix(file, migrationFileExt)
	if !ok {
		return "", "", fmt.Errorf("bad migration filename %q (%s)", file, migrationNameHint)
	}
	version, name, ok := strings.Cut(base, migrationNameInfix)
	if !ok || len(version) != 4 || name == "" || !digitsOnly(version) {
		return "", "", fmt.Errorf("bad migration filename %q (%s)", file, migrationNameHint)
	}
	return version, name, nil
}

func digitsOnly(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// applyMigrations applies every migration of ms that baselines.schema_migrations
// does not list, in version order. Each migration runs in its own transaction that
// begins by taking pg_advisory_xact_lock(migrationAdvisoryLockKey) and re-reading
// the ledger, so the migrator a pipeline runs and a worker that auto-applies
// serialise instead of racing; a failing migration leaves behind neither its DDL
// nor its ledger row.
//
// A dry run reads the ledger and reports Pending without writing anything,
// including the ledger itself.
func applyMigrations(ctx context.Context, pool migrationPool, ms []migration, dryRun bool) (migrationResult, error) {
	applied, err := ledgerVersions(ctx, pool)
	if err != nil {
		return migrationResult{}, err
	}
	sorted := append([]migration(nil), ms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Version < sorted[j].Version })
	known := make(map[string]bool, len(sorted))
	for _, m := range sorted {
		known[m.Version] = true
	}
	res := migrationResult{}
	for version := range applied {
		if !known[version] {
			res.Unknown = append(res.Unknown, version)
		}
	}
	sort.Strings(res.Unknown)
	var pending []migration
	for _, m := range sorted {
		if applied[m.Version] {
			res.Already++
			continue
		}
		pending = append(pending, m)
	}
	if dryRun {
		for _, m := range pending {
			res.Pending = append(res.Pending, m.Version)
		}
		return res, nil
	}
	for _, m := range pending {
		wrote, err := applyOneMigration(ctx, pool, m)
		if err != nil {
			return res, err
		}
		if wrote {
			res.Applied = append(res.Applied, m.Version)
		}
	}
	return res, nil
}

// ledgerVersions reads the applied set. A missing table is not an error: it means
// nothing has been applied yet.
func ledgerVersions(ctx context.Context, pool migrationPool) (map[string]bool, error) {
	var raw string
	err := pool.QueryRow(ctx, migrationLedgerVersionsSQL).Scan(&raw)
	if err != nil {
		if isUndefinedTable(err) || errors.Is(err, pgx.ErrNoRows) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("baseline store: %w", err)
	}
	applied := map[string]bool{}
	for _, version := range strings.Split(raw, "\n") {
		if version = strings.TrimSpace(version); version != "" {
			applied[version] = true
		}
	}
	return applied, nil
}

// applyOneMigration applies one migration inside one transaction, or reports that
// another migrator committed it while this call waited for the advisory lock.
func applyOneMigration(ctx context.Context, pool migrationPool, m migration) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("baseline store: %w", err)
	}
	// Rollback after a commit is a no-op, so every error path can share it.
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, migrationAdvisoryLockSQL, migrationAdvisoryLockKey); err != nil {
		return false, fmt.Errorf("baseline store: %w", err)
	}
	if _, err := tx.Exec(ctx, migrationLedgerDDL); err != nil {
		return false, fmt.Errorf("baseline store: %w", err)
	}
	var seen int
	switch err := tx.QueryRow(ctx, migrationAlreadyAppliedSQL, m.Version).Scan(&seen); {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return false, fmt.Errorf("baseline store: %w", err)
	default:
		return false, nil
	}
	// The migration SQL and the ledger DDL are passed without arguments, which
	// keeps pgx on the simple protocol: a file holds several statements.
	if _, err := tx.Exec(ctx, m.SQL); err != nil {
		return false, migrationError(m, err)
	}
	if _, err := tx.Exec(ctx, migrationInsertLedgerSQL, m.Version, m.Name); err != nil {
		return false, migrationError(m, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, migrationError(m, err)
	}
	return true, nil
}

func migrationError(m migration, err error) error {
	return fmt.Errorf("migration %s_%s: %w", m.Version, m.Name, err)
}

func isUndefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}

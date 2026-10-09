package baselines

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	forecast "github.com/eduard-kolotushin/timeseries-forecast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaMigrations is the embedded migration set, read once at startup: the
// worker applies it at its first store use (see migrations.go), exactly as
// cmd/migrate does out-of-process.
var schemaMigrations = allMigrations()

// ensureRetryAfter throttles reconnect attempts while Postgres is unreachable so
// one outage does not turn every tick into a dial storm.
const ensureRetryAfter = 5 * time.Second

type postgresStore struct {
	pool *pgxpool.Pool

	mu          sync.Mutex
	ready       bool
	lastErr     error
	lastAttempt time.Time
	now         func() time.Time
}

// openPostgresStore parses the DSN and builds the pool. pgxpool connects
// lazily, so a database that is down at startup does not disable persistence for
// the life of the process: the first call pings and provisions.
func openPostgresStore(ctx context.Context, dsn string) (*postgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &postgresStore{pool: pool, now: time.Now}, nil
}

func (s *postgresStore) Close() {
	if s != nil && s.pool != nil {
		s.pool.Close()
	}
}

// ensure pings Postgres and creates the worker schema on first successful use.
// It is retried on later calls (after ensureRetryAfter) rather than failing the
// store permanently when the database is briefly unavailable.
func (s *postgresStore) ensure(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ready {
		return nil
	}
	now := s.now()
	if s.lastErr != nil && now.Sub(s.lastAttempt) < ensureRetryAfter {
		return s.lastErr
	}
	s.lastAttempt = now
	if err := s.pool.Ping(ctx); err != nil {
		s.lastErr = fmt.Errorf("baseline store: %w", err)
		return s.lastErr
	}
	res, err := applyMigrations(ctx, s.pool, schemaMigrations, false)
	if err != nil {
		// A locked-down runtime user may lack CREATE. Accept that when every table
		// this binary reads is already provisioned — all of them, because the schema
		// now arrives as independent files: probing one would accept a database where
		// 0002 never applied, and every Heartbeat/Peers would then fail with 42703
		// against the legacy shape while the store reported itself ready. The error
		// already names the store (or the migration), so it is not wrapped again.
		if probeErr := s.probeSchema(ctx); probeErr != nil {
			s.lastErr = err
			return s.lastErr
		}
		// The probe accepted an already-provisioned schema, but the migration
		// itself failed: log it, so an operator can tell "the migration ran" from
		// "the migration never applied" when only the readiness probe succeeded.
		slog.Warn("baseline schema not applied", "err", err.Error())
	} else {
		if len(res.Applied) > 0 {
			slog.Info("baseline schema migrated", "applied", res.Applied)
		}
		if len(res.Unknown) > 0 {
			slog.Warn("baseline schema is newer than this binary", "versions", res.Unknown)
		}
	}
	s.ready = true
	s.lastErr = nil
	return nil
}

// Put stores one model, gzipped: a minute-of-week baseline is two arrays of
// 30,240 floats, which is ~0.55 MB of JSON text and compresses to ~17 KB (the
// sandbox measures 562,558 bytes of JSON against 17,449 stored), so the column
// stays out of the way of the row cache.
func (s *postgresStore) Put(ctx context.Context, key string, rec snapshotRecord, snap forecast.Snapshot) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(raw); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
INSERT INTO baselines.snapshots (metric_hash, model, season, calendar, lookback_ms, trained_at, snapshot, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, now())
ON CONFLICT (metric_hash) DO UPDATE SET
  model = EXCLUDED.model,
  season = EXCLUDED.season,
  calendar = EXCLUDED.calendar,
  lookback_ms = EXCLUDED.lookback_ms,
  trained_at = EXCLUDED.trained_at,
  snapshot = EXCLUDED.snapshot,
  updated_at = now()
`, key, rec.Model, rec.Season, rec.Calendar, rec.Lookback.Milliseconds(), rec.TrainedAt, buf.Bytes())
	return err
}

func (s *postgresStore) Get(ctx context.Context, key string) (forecast.Snapshot, bool, error) {
	if err := s.ensure(ctx); err != nil {
		return forecast.Snapshot{}, false, err
	}
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT snapshot FROM baselines.snapshots WHERE metric_hash = $1`, key).Scan(&raw)
	if err == pgx.ErrNoRows {
		return forecast.Snapshot{}, false, nil
	}
	if err != nil {
		return forecast.Snapshot{}, false, err
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return forecast.Snapshot{}, false, fmt.Errorf("snapshot %s: %w", key, err)
	}
	defer zr.Close()
	plain, err := io.ReadAll(zr)
	if err != nil {
		return forecast.Snapshot{}, false, fmt.Errorf("snapshot %s: %w", key, err)
	}
	var snap forecast.Snapshot
	if err := json.Unmarshal(plain, &snap); err != nil {
		return forecast.Snapshot{}, false, fmt.Errorf("snapshot %s: %w", key, err)
	}
	return snap, true, nil
}

// Fresh reports updated_at for the keys that have a snapshot, in one query: the
// caller restores only the ones whose timestamp moved.
func (s *postgresStore) Fresh(ctx context.Context, keys []string) (map[string]time.Time, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT metric_hash, updated_at FROM baselines.snapshots WHERE metric_hash = ANY($1)`, keys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]time.Time, len(keys))
	for rows.Next() {
		var (
			key string
			at  time.Time
		)
		if err := rows.Scan(&key, &at); err != nil {
			return nil, err
		}
		out[key] = at
	}
	return out, rows.Err()
}

// SweepSnapshots collects the snapshots nothing has refreshed within ttl whose
// baseline schedule row is equally idle, and returns how many were deleted. It
// is the worker's own garbage collection: baselines.snapshots is written only
// here, so a metric that stopped reporting leaves a model that no retrain and no
// other writer moves updated_at for, and its forecast.retrain row has no recent
// last_run_at either (the owner-guarded Done writes one on every finish,
// including a failed one, so a transient Druid or Grafana outage is never
// mistaken for a dead metric). The row it looks up is the fleet-wide baseline row
// (org_id = 0, the only org this key space uses), so a row another component
// happened to write under a different org cannot pin a snapshot forever. A row an
// admin deleted leaves no row at all, which is also idle. forecast.retrain is only
// read here: the table is created and owned by the plugin.
func (s *postgresStore) SweepSnapshots(ctx context.Context, ttl time.Duration) (int64, error) {
	if err := s.ensure(ctx); err != nil {
		return 0, err
	}
	tag, err := s.pool.Exec(ctx, `
DELETE FROM baselines.snapshots s
WHERE s.updated_at < now() - $1::interval
  AND NOT EXISTS (
    SELECT 1 FROM forecast.retrain r
    WHERE r.scope = 'baseline' AND r.org_id = 0 AND r.key = s.metric_hash
      AND r.last_run_at > now() - $1::interval)
`, ttl)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// probeSchema reads one row from each table this worker needs, naming the columns the
// migrations define, so a database whose files only partly applied is not mistaken for
// a provisioned one: a legacy workers table (no uuid id, no worker_id) fails here while
// the store is still reporting the migration error, instead of latching ready and
// failing every Heartbeat/Peers with 42703. LIMIT 1 keeps it free on an empty table.
func (s *postgresStore) probeSchema(ctx context.Context) error {
	for _, q := range []string{
		`SELECT id, metric_hash FROM baselines.snapshots LIMIT 1`,
		`SELECT id, worker_id FROM baselines.workers LIMIT 1`,
	} {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (s *postgresStore) Heartbeat(ctx context.Context, id string, owned, peers int) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO baselines.workers (worker_id, last_seen, owned, peers)
VALUES ($1, now(), $2, $3)
ON CONFLICT (worker_id) DO UPDATE SET
  last_seen = now(),
  owned = EXCLUDED.owned,
  peers = EXCLUDED.peers
`, id, owned, peers)
	return err
}

func (s *postgresStore) Peers(ctx context.Context, ttl time.Duration) ([]string, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `SELECT worker_id FROM baselines.workers WHERE last_seen > now() - $1::interval ORDER BY worker_id`, ttl)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Schedule inserts the baseline rows for keys unless they are already there, in
// one statement: an owned set of H hashes would otherwise cost H round trips per
// tick for rows that all exist after the first one. An existing row keeps its
// cron and timezone: the operator owns the schedule, and a worker restart must
// not reset it. The forecast.retrain table is created by the plugin, so this
// fails until the plugin has run against the same database.
func (s *postgresStore) Schedule(ctx context.Context, keys []string, cron, tz string) error {
	if len(keys) == 0 {
		return nil
	}
	if err := s.ensure(ctx); err != nil {
		return err
	}
	return s.insertSchedules(ctx, "forecast.retrain", keys, cron, tz)
}

// insertSchedules runs the one-statement insert against table. The table name is
// a parameter so the stale-key error below can be reproduced against an
// old-shape copy of the table in tests; every production call passes
// forecast.retrain.
func (s *postgresStore) insertSchedules(ctx context.Context, table string, keys []string, cron, tz string) error {
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`
INSERT INTO %s (scope, key, cron, timezone, enabled, next_run_at)
SELECT 'baseline', k, $2, $3, true, now() FROM unnest($1::text[]) AS k
ON CONFLICT (scope, org_id, key) DO NOTHING
`, table), keys, cron, tz)
	return staleScheduleKeyError(err)
}

// staleScheduleKeyError names the one condition a table left on the old
// (scope, key) primary key produces for this insert: with org_id absent it is
// Postgres 42703 (the conflict target names a column that is not there), and with
// org_id present but not in the key it is 42P10 (no constraint matches the ON
// CONFLICT specification). Either way every tick would report one generic
// constraint error and no schedule would ever be written, so the remedy the
// operator can apply is named once instead. The plugin migrates the table in
// place when it starts; this process only reads, claims and finishes rows, so it
// never runs the DDL itself.
func staleScheduleKeyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == "42P10" || pgErr.Code == "42703") {
		return fmt.Errorf("forecast.retrain is keyed (scope, key), not (scope, org_id, key): run the plugin once against this database to migrate it: %w", err)
	}
	return err
}

// Claim takes up to limit due baseline rows and leases them to owner. SKIP
// LOCKED lets every worker run this statement concurrently without a
// coordinator, and claimed_until is how a claim comes back after a crash.
//
// A superseded row is never claimed: the plugin marks a schedule that a newer key
// replaced with superseded_at, and both claims skip it so neither side retrains a
// row that is on its way out.
func (s *postgresStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
WITH due AS (
  SELECT scope, org_id, key FROM forecast.retrain
  WHERE scope = 'baseline'
    AND enabled
    AND next_run_at IS NOT NULL
    AND next_run_at <= now()
    AND superseded_at IS NULL
    AND (claimed_until IS NULL OR claimed_until < now())
  ORDER BY next_run_at
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE forecast.retrain r
SET claimed_by = $2, claimed_until = now() + $3::interval
FROM due
WHERE r.scope = due.scope AND r.org_id = due.org_id AND r.key = due.key
RETURNING r.org_id, r.key, r.cron, r.timezone, r.attempts
`, limit, owner, lease)
	if err != nil {
		return nil, missingAttemptsError(err)
	}
	defer rows.Close()
	out := make([]retrainClaim, 0, limit)
	for rows.Next() {
		var c retrainClaim
		if err := rows.Scan(&c.OrgID, &c.Key, &c.Cron, &c.Timezone, &c.Attempts); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Done releases the claim and sets when the row is next due. A failure passes
// now+retryDelay(RETRAIN_RETRY, RETRAIN_RETRY_MAX, attempts) as next, so a broken
// hash is retried — spaced further out on every further failure — instead of being
// lost or hammered.
//
// attempts is the row's new consecutive-failure count: 0 after a success, the
// claim's previous count plus one after a failure, and the unchanged count for a
// claim released at shutdown, because a deployment is not the row's failure. It is
// written in the same statement as the outcome that produced it.
//
// Only the owner of the claim may finish it: a retrain that outlives its lease has
// already been handed to another worker by the time it returns, and a stale
// finisher would overwrite that worker's next run and status — including after that
// worker has released the claim itself, which is why the owner predicate is the
// whole guard and a NULL claim is not a licence to write. Zero rows affected
// therefore means the claim was lost, which is not an error.
func (s *postgresStore) Done(ctx context.Context, owner string, orgID int64, key string, next time.Time, status string, attempts int) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain
SET next_run_at = $4, last_run_at = now(), last_status = $5, attempts = $6,
    claimed_by = NULL, claimed_until = NULL
WHERE scope = 'baseline' AND org_id = $3 AND key = $1 AND claimed_by = $2
`, key, owner, orgID, next, status, attempts)
	if err != nil {
		return missingAttemptsError(err)
	}
	if tag.RowsAffected() == 0 {
		slog.Debug("claim lost", "metric_hash", key, "owner", owner)
	}
	return nil
}

// Extend pushes this worker's lease on a held row out to now+lease and reports
// whether it still owns it. The tick calls it immediately before a row's fit, so a
// fit that takes longer than the lease it was claimed under cannot be re-claimed and
// re-trained by a survivor at the same time — the duplicate scan is exactly the load
// the fleet cannot afford.
//
// Like Done it is owner-guarded: only the claim holder may move the lease. Zero rows
// updated means another worker owns the row now, which the caller treats as "skip
// it", not as an error.
func (s *postgresStore) Extend(ctx context.Context, owner string, orgID int64, key string, lease time.Duration) (bool, error) {
	if err := s.ensure(ctx); err != nil {
		return false, err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain
SET claimed_until = now() + $4::interval
WHERE scope = 'baseline' AND org_id = $3 AND key = $1 AND claimed_by = $2
`, key, owner, orgID, lease)
	if err != nil {
		return false, missingAttemptsError(err)
	}
	return tag.RowsAffected() > 0, nil
}

// missingAttemptsError names the one condition a forecast.retrain table from before
// the plugin's attempts migration produces for a claim, an extend or a finish:
// Postgres 42703 on the attempts column. Every tick would otherwise report one
// generic undefined-column error and no row would ever be claimed, so the remedy the
// operator can apply is named once instead. The column is the plugin's table's, so
// this process never adds it.
//
// The match is on the message, not only on PgError.ColumnName: PostgreSQL leaves
// `columnname` empty when the statement qualifies the column (`column r.attempts
// does not exist` measured against postgres:17), so a matcher that trusted the field
// alone would never fire — which is exactly the raw 42703 this function exists to
// replace.
func missingAttemptsError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "42703" && strings.Contains(pgErr.Message, "attempts") {
		return fmt.Errorf("forecast.retrain has no attempts column: apply the timeseries-grafana migration (gpx_forecast_migrate) before starting this worker: %w", err)
	}
	return err
}

package baselines

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	forecast "github.com/eduard-kolotushin/timeseries-forecast"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ensureSQL is the worker's own schema. forecast.retrain is NOT created here:
// that table belongs to the plugin, which owns its API; this process only reads,
// claims and finishes rows in it.
const ensureSQL = `
CREATE SCHEMA IF NOT EXISTS baselines;
CREATE TABLE IF NOT EXISTS baselines.snapshots (
  metric_hash TEXT PRIMARY KEY,
  model TEXT NOT NULL,
  season TEXT NOT NULL,
  calendar TEXT NOT NULL DEFAULT '',
  lookback_ms BIGINT NOT NULL,
  trained_at TIMESTAMPTZ NOT NULL,
  snapshot BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS baselines.workers (
  id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  owned INTEGER NOT NULL DEFAULT 0,
  peers INTEGER NOT NULL DEFAULT 0
);
`

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
	if _, err := s.pool.Exec(ctx, ensureSQL); err != nil {
		// A locked-down runtime user may lack CREATE. Accept that when the tables
		// are already provisioned.
		if _, probe := s.pool.Exec(ctx, `SELECT 1 FROM baselines.snapshots LIMIT 1`); probe != nil {
			s.lastErr = fmt.Errorf("baseline store: %w", err)
			return s.lastErr
		}
	}
	s.ready = true
	s.lastErr = nil
	return nil
}

// Put stores one model, gzipped: a minute-of-week baseline is 2×30,240 floats,
// which is 0.3–1.2 MB of JSON and about ten times smaller compressed.
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

func (s *postgresStore) Heartbeat(ctx context.Context, id string, owned, peers int) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO baselines.workers (id, last_seen, owned, peers)
VALUES ($1, now(), $2, $3)
ON CONFLICT (id) DO UPDATE SET
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
	rows, err := s.pool.Query(ctx, `SELECT id FROM baselines.workers WHERE last_seen > now() - $1::interval ORDER BY id`, ttl)
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

// Schedule inserts the baseline row for key unless it is already there. An
// existing row keeps its cron and timezone: the operator owns the schedule, and
// a worker restart must not reset it. The forecast.retrain table is created by
// the plugin, so this fails until the plugin has run against the same database.
func (s *postgresStore) Schedule(ctx context.Context, key, cron, tz string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO forecast.retrain (scope, key, cron, timezone, enabled, next_run_at)
VALUES ('baseline', $1, $2, $3, true, now())
ON CONFLICT (scope, key) DO NOTHING
`, key, cron, tz)
	return err
}

// Claim takes up to limit due baseline rows and leases them to owner. SKIP
// LOCKED lets every worker run this statement concurrently without a
// coordinator, and claimed_until is how a claim comes back after a crash.
func (s *postgresStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error) {
	if err := s.ensure(ctx); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
WITH due AS (
  SELECT scope, key FROM forecast.retrain
  WHERE scope = 'baseline'
    AND enabled
    AND next_run_at IS NOT NULL
    AND next_run_at <= now()
    AND (claimed_until IS NULL OR claimed_until < now())
  ORDER BY next_run_at
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE forecast.retrain r
SET claimed_by = $2, claimed_until = now() + $3::interval
FROM due
WHERE r.scope = due.scope AND r.key = due.key
RETURNING r.key, r.cron, r.timezone
`, limit, owner, lease)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]retrainClaim, 0, limit)
	for rows.Next() {
		var c retrainClaim
		if err := rows.Scan(&c.Key, &c.Cron, &c.Timezone); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Done releases the claim and sets when the row is next due. A failure passes
// now+RETRAIN_RETRY as next, so a broken hash is retried instead of being lost.
func (s *postgresStore) Done(ctx context.Context, key string, next time.Time, status string) error {
	if err := s.ensure(ctx); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain
SET next_run_at = $2, last_run_at = now(), last_status = $3,
    claimed_by = NULL, claimed_until = NULL
WHERE scope = 'baseline' AND key = $1
`, key, next, status)
	return err
}

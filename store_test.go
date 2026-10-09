package baselines

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	forecast "github.com/eduard-kolotushin/timeseries-forecast"
	"github.com/jackc/pgx/v5/pgconn"
)

// TestPostgresStoreLazyConnect: an unreachable database must not turn the store
// into a permanent error; calls fail per call and retry after the backoff.
func TestPostgresStoreLazyConnect(t *testing.T) {
	ctx := context.Background()
	s, err := openPostgresStore(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=1")
	if err != nil {
		t.Fatalf("open must only parse the DSN: %v", err)
	}
	t.Cleanup(s.Close)
	clock := time.Unix(1_000_000, 0)
	s.now = func() time.Time { return clock }

	if _, _, err := s.Get(ctx, "ready"); err == nil {
		t.Fatal("expected a connect error")
	}
	first := s.lastAttempt
	// Within the backoff window the cached error is returned without redialing.
	if _, err := s.Fresh(ctx, []string{"ready"}); err == nil || s.lastAttempt != first {
		t.Fatalf("expected the cached error without a redial, err=%v attempt moved=%v", err, s.lastAttempt != first)
	}
	clock = clock.Add(ensureRetryAfter)
	if _, err := s.Fresh(ctx, []string{"ready"}); err == nil || s.lastAttempt == first {
		t.Fatalf("expected a fresh attempt after the backoff, err=%v", err)
	}
	if s.ready {
		t.Fatal("store must not be marked ready")
	}
}

// testDSN is the Postgres the store tests run against, or "" to skip them.
func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("BASELINE_TEST_PG")
	if dsn == "" {
		t.Skip("BASELINE_TEST_PG not set")
	}
	return dsn
}

func openTestStore(t *testing.T) (*postgresStore, context.Context) {
	t.Helper()
	ctx := context.Background()
	s, err := openPostgresStore(ctx, testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, ctx
}

// TestProbeSchemaChecksEveryTable: the readiness fallback runs when the migration could
// not be applied, and the schema arrives as independent files — so accepting one table
// would let a database whose 0002 never applied count as provisioned, after which every
// Heartbeat/Peers fails with 42703 (a legacy workers table has no worker_id) while the
// store reports itself ready.
func TestProbeSchemaChecksEveryTable(t *testing.T) {
	s, ctx := openTestStore(t)
	const worker = "baselines-probe-shape"
	if err := s.Heartbeat(ctx, worker, 0, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM baselines.workers WHERE worker_id = $1`, worker)
	})
	if err := s.probeSchema(ctx); err != nil {
		t.Fatalf("the migrated schema must pass the shape probe: %v", err)
	}

	// Renaming the identity column away is what a table from before 0002 looks like:
	// the probe must refuse it, so ensure keeps reporting the migration error instead
	// of latching ready.
	if _, err := s.pool.Exec(ctx, `ALTER TABLE baselines.workers RENAME COLUMN worker_id TO legacy_id`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `ALTER TABLE baselines.workers RENAME COLUMN legacy_id TO worker_id`)
	})
	if err := s.probeSchema(ctx); err == nil {
		t.Fatal("the shape probe accepted a workers table without worker_id")
	}
}

func TestPostgresSnapshotStore(t *testing.T) {
	s, ctx := openTestStore(t)
	const key = "baselines-store-test"
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM baselines.snapshots WHERE metric_hash = $1`, key)
	})

	end := time.Now().UTC().Truncate(time.Minute).Add(-time.Minute)
	fitted := fitPoints(t, minutePoints(end, 180))
	snap, err := forecast.SnapshotOf(fitted)
	if err != nil {
		t.Fatal(err)
	}
	trainedAt := time.Now().UTC().Truncate(time.Second)
	rec := snapshotRecord{
		Model:     modelBaseline,
		Season:    seasonMinuteWeek,
		Calendar:  "",
		Lookback:  336 * time.Hour,
		TrainedAt: trainedAt,
	}

	// The first call is also what creates the worker schema.
	if err := s.Put(ctx, key, rec, snap); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"baselines.snapshots", "baselines.workers"} {
		var found *string
		if err := s.pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, table).Scan(&found); err != nil {
			t.Fatal(err)
		}
		if found == nil {
			t.Fatalf("ensure did not create %s", table)
		}
	}

	got, ok, err := s.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	restored, err := forecast.Restore(got)
	if err != nil {
		t.Fatal(err)
	}
	// A gzip round trip must survive as a model, not just as bytes: the
	// restored fit has to forecast what the fitted one did.
	at := end.Add(time.Hour)
	want, err := fitted.ForecastRange(at, at)
	if err != nil {
		t.Fatal(err)
	}
	have, err := restored.ForecastRange(at, at)
	if err != nil {
		t.Fatal(err)
	}
	wantPt, _ := want.At(0)
	havePt, _ := have.At(0)
	if wantPt.Value != havePt.Value {
		t.Fatalf("restored forecast %v, want %v", havePt.Value, wantPt.Value)
	}

	// Stored compressed: a minute-of-week fit is ~60k floats, and the column
	// exists to keep that out of the row cache.
	var raw []byte
	if err := s.pool.QueryRow(ctx, `SELECT snapshot FROM baselines.snapshots WHERE metric_hash = $1`, key).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(raw, []byte{0x1f, 0x8b}) {
		t.Fatalf("stored snapshot does not start with the gzip magic: % x", raw[:min(4, len(raw))])
	}
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(plain, []byte(`"minute-week"`)) {
		t.Fatalf("gunzipped snapshot is not the model spec: %s", plain[:min(80, len(plain))])
	}

	// Fresh reports the keys that exist and nothing else.
	fresh, err := s.Fresh(ctx, []string{key, "baselines-store-test-missing"})
	if err != nil {
		t.Fatal(err)
	}
	at2, ok := fresh[key]
	if !ok || at2.IsZero() {
		t.Fatalf("fresh(%s) = %v ok=%v, want its updated_at", key, at2, ok)
	}
	if _, ok := fresh["baselines-store-test-missing"]; ok {
		t.Fatalf("fresh reported a key with no snapshot: %v", fresh)
	}

	// An upsert moves updated_at, which is what the publisher watches.
	rec.TrainedAt = trainedAt.Add(time.Minute)
	if err := s.Put(ctx, key, rec, snap); err != nil {
		t.Fatal(err)
	}
	fresh, err = s.Fresh(ctx, []string{key})
	if err != nil {
		t.Fatal(err)
	}
	if !fresh[key].After(at2) {
		t.Fatalf("updated_at %s did not move from %s", fresh[key], at2)
	}
}

func TestPostgresMembership(t *testing.T) {
	s, ctx := openTestStore(t)
	const (
		first  = "baselines-store-test-a"
		second = "baselines-store-test-b"
	)
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM baselines.workers WHERE worker_id IN ($1, $2)`, first, second)
	})

	if err := s.Heartbeat(ctx, first, 7, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, second, 9, 2); err != nil {
		t.Fatal(err)
	}
	peers, err := s.Peers(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(peers))
	for _, id := range peers {
		seen[id] = true
	}
	if !seen[first] || !seen[second] {
		t.Fatalf("peers %v, want both heartbeating workers", peers)
	}
	if len(peers) < 2 {
		t.Fatalf("peers %v, want both workers", peers)
	}

	// A heartbeat does not resurrect a worker: the TTL is what retires one that
	// stopped, and a stale row must not hold its hashes for good.
	if _, err := s.pool.Exec(ctx, `UPDATE baselines.workers SET last_seen = now() - interval '2 minutes' WHERE worker_id = $1`, second); err != nil {
		t.Fatal(err)
	}
	peers, err = s.Peers(ctx, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range peers {
		if id == second {
			t.Fatalf("peers %v still lists a worker silent for 2 minutes at a 30s TTL", peers)
		}
	}

	// The heartbeat also records what the worker is carrying.
	var owned, count int
	if err := s.pool.QueryRow(ctx, `SELECT owned, peers FROM baselines.workers WHERE worker_id = $1`, first).Scan(&owned, &count); err != nil {
		t.Fatal(err)
	}
	if owned != 7 || count != 2 {
		t.Fatalf("heartbeat stored owned=%d peers=%d, want 7 and 2", owned, count)
	}
}

// pluginRetrainDDL is forecast.retrain as timeseries-grafana creates it
// (pkg/store/migrations/0002_retrain.sql plus 0003_retrain_attempts.sql), plus the
// superseded_at column the plugin adds for a schedule a newer key replaced. It lives
// here as a test fixture: the table is created and owned by the plugin — this process
// only reads, claims and finishes rows in it — so the worker's insert/claim/finish
// SQL would run in no test at all when the database has never seen the plugin (CI
// provisions a bare postgres:17).
const pluginRetrainDDL = `
CREATE SCHEMA IF NOT EXISTS forecast;
CREATE TABLE IF NOT EXISTS forecast.retrain (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  scope TEXT NOT NULL,
  key TEXT NOT NULL,
  org_id BIGINT NOT NULL DEFAULT 0,
  cron TEXT NOT NULL,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  enabled BOOLEAN NOT NULL DEFAULT true,
  spec JSONB,
  next_run_at TIMESTAMPTZ,
  last_run_at TIMESTAMPTZ,
  last_status TEXT,
  claimed_by TEXT,
  claimed_until TIMESTAMPTZ,
  attempts INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,
  CONSTRAINT retrain_scope_org_key_unique UNIQUE (scope, org_id, key)
);
-- A table created before superseded_at joined the row is topped up, which is what
-- the plugin's own migration does; CREATE TABLE IF NOT EXISTS alone would leave a
-- legacy table without the column the claim filters on.
ALTER TABLE forecast.retrain ADD COLUMN IF NOT EXISTS superseded_at TIMESTAMPTZ;
-- Same for the retry counter the claim returns and the finish writes.
ALTER TABLE forecast.retrain ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
`

// ensurePluginRetrainTable provides forecast.retrain in the plugin's shape.
func ensurePluginRetrainTable(t *testing.T, s *postgresStore) {
	t.Helper()
	if _, err := s.pool.Exec(context.Background(), pluginRetrainDDL); err != nil {
		t.Fatalf("provide forecast.retrain: %v", err)
	}
}

// TestPostgresRetrainQueue covers the worker's side of forecast.retrain: the
// batched insert, the SKIP LOCKED claim with its lease, the owner-guarded finish,
// and the two conditions that keep a row out of the claim (a live lease, a
// superseded schedule).
func TestPostgresRetrainQueue(t *testing.T) {
	s, ctx := openTestStore(t)
	ensurePluginRetrainTable(t, s)
	const (
		key    = "baselines-store-test"
		second = "baselines-store-test-2"
	)
	cleanup := func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key IN ($1, $2)`, key, second)
	}
	cleanup()
	t.Cleanup(cleanup)

	// One statement schedules the whole owned set: the second insert must add the
	// new key without resetting the row that is already there.
	if err := s.Schedule(ctx, []string{key}, "*/5 * * * *", "UTC"); err != nil {
		t.Fatal(err)
	}
	if err := s.Schedule(ctx, []string{key, second}, "0 3 * * *", "Europe/Moscow"); err != nil {
		t.Fatal(err)
	}
	var (
		cron, tz       string
		enabled        bool
		nextRunDue     bool
		nextRunNotNull bool
	)
	if err := s.pool.QueryRow(ctx, `
SELECT cron, timezone, enabled, next_run_at <= now(), next_run_at IS NOT NULL
FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key).Scan(&cron, &tz, &enabled, &nextRunDue, &nextRunNotNull); err != nil {
		t.Fatal(err)
	}
	if cron != "*/5 * * * *" || tz != "UTC" {
		t.Fatalf("schedule stored cron=%q tz=%q, want the first insert", cron, tz)
	}
	if !enabled || !nextRunNotNull || !nextRunDue {
		t.Fatalf("new row enabled=%v next_run_at set=%v due=%v, want an enabled, immediately due schedule", enabled, nextRunNotNull, nextRunDue)
	}
	if err := s.pool.QueryRow(ctx, `
SELECT cron, timezone FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, second).Scan(&cron, &tz); err != nil {
		t.Fatal(err)
	}
	if cron != "0 3 * * *" || tz != "Europe/Moscow" {
		t.Fatalf("the batched schedule wrote cron=%q tz=%q for the new key, want the statement's values", cron, tz)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, second); err != nil {
		t.Fatal(err)
	}

	// Move the row to the front of the queue so a small claim cannot miss it.
	if _, err := s.pool.Exec(ctx, `UPDATE forecast.retrain SET next_run_at = now() - interval '10 years' WHERE scope = 'baseline' AND key = $1`, key); err != nil {
		t.Fatal(err)
	}
	claims, err := s.Claim(ctx, "baselines-store-test", time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Key != key {
		t.Fatalf("claimed %+v, want the test row", claims)
	}
	if claims[0].Cron != "*/5 * * * *" || claims[0].Timezone != "UTC" {
		t.Fatalf("claimed %+v, want the stored schedule", claims[0])
	}
	// The claim carries the row's org, which Done addresses the row by: the worker's
	// own rows are the fleet-wide org 0.
	orgID := claims[0].OrgID
	if orgID != 0 {
		t.Fatalf("claim org = %d, want the fleet-wide 0 the worker writes", orgID)
	}
	var claimedBy *string
	if err := s.pool.QueryRow(ctx, `SELECT claimed_by FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key).Scan(&claimedBy); err != nil {
		t.Fatal(err)
	}
	if claimedBy == nil || *claimedBy != "baselines-store-test" {
		t.Fatalf("claimed_by %v, want the claiming owner", claimedBy)
	}

	// A live lease hides the row from the rest of the fleet.
	if claims, err = s.Claim(ctx, "other", time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	for _, c := range claims {
		if c.Key == key {
			t.Fatalf("a leased row was claimed again: %+v", c)
		}
	}

	// A finisher that no longer holds the claim must leave the row alone: its
	// lease can expire mid-retrain and the row go to another worker, whose claim
	// and next_run_at a stale finish would otherwise overwrite.
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	if err := s.Done(ctx, "someone-else", orgID, key, next, "error: stale", 2); err != nil {
		t.Fatal(err)
	}
	var (
		holder    *string
		dueAt     time.Time
		lastRun   *time.Time
		staleStat *string
	)
	if err := s.pool.QueryRow(ctx, `
SELECT claimed_by, next_run_at, last_run_at, last_status
FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key).Scan(&holder, &dueAt, &lastRun, &staleStat); err != nil {
		t.Fatal(err)
	}
	if holder == nil || *holder != "baselines-store-test" {
		t.Fatalf("a non-holder released the claim: claimed_by %v", holder)
	}
	if dueAt.After(time.Now().UTC()) {
		t.Fatalf("a non-holder moved next_run_at to %s, want the claimed row's due time", dueAt)
	}
	if staleStat != nil || lastRun != nil {
		t.Fatalf("a non-holder wrote last_status=%v last_run_at=%v, want the row untouched", staleStat, lastRun)
	}

	if err := s.Done(ctx, "baselines-store-test", orgID, key, next, "ok", 0); err != nil {
		t.Fatal(err)
	}
	var (
		status   *string
		storedAt time.Time
	)
	if err := s.pool.QueryRow(ctx, `
SELECT last_status, next_run_at, claimed_by, claimed_until
FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key).Scan(&status, &storedAt, &claimedBy, new(*string)); err != nil {
		t.Fatal(err)
	}
	if status == nil || *status != "ok" {
		t.Fatalf("last_status %v, want ok", status)
	}
	if !storedAt.Equal(next) {
		t.Fatalf("next_run_at %s, want %s", storedAt, next)
	}
	if claimedBy != nil {
		t.Fatalf("claimed_by %v after finishing, want the claim released", claimedBy)
	}

	// A released claim is not an invitation. The owner predicate is the whole guard:
	// once the holder has finished, an owner that lost its lease must still not write
	// its own stale outcome over this row.
	staleNext := next.Add(time.Hour)
	if err := s.Done(ctx, "baselines-store-test", orgID, key, staleNext, "error: stale", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `
SELECT last_status, next_run_at FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key).Scan(&status, &storedAt); err != nil {
		t.Fatal(err)
	}
	if status == nil || *status != "ok" || !storedAt.Equal(next) {
		t.Fatalf("a released claim was written by a stale owner: status=%v next=%s", status, storedAt)
	}

	// A superseded row is never claimed, however due it is: the plugin marks a
	// schedule a newer key replaced, and neither claim may retrain it.
	if _, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain SET next_run_at = now() - interval '10 years', superseded_at = now()
WHERE scope = 'baseline' AND key = $1`, key); err != nil {
		t.Fatal(err)
	}
	if claims, err = s.Claim(ctx, "other", time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	for _, c := range claims {
		if c.Key == key {
			t.Fatalf("a superseded row was claimed: %+v", c)
		}
	}
}

// TestPostgresScheduleStaleKey: a table still keyed (scope, key) makes every
// tick's insert fail with a raw Postgres error the operator has no way to read as
// "the plugin has not migrated this table yet": with org_id absent it is 42703 (the
// conflict target names a column that is not there) and with org_id present but out
// of the key it is 42P10 (no constraint matches the ON CONFLICT specification).
// Both must arrive as the one named condition, and nothing else may be misreported
// as it.
func TestPostgresScheduleStaleKey(t *testing.T) {
	s, ctx := openTestStore(t)
	const table = "baselines.retrain_stale_key_test"
	t.Cleanup(func() { _, _ = s.pool.Exec(context.Background(), `DROP TABLE IF EXISTS `+table) })
	if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS `+table); err != nil {
		t.Fatal(err)
	}
	// The pre-org shape: no org_id column at all, so the conflict target cannot
	// resolve and the insert fails with 42703.
	if _, err := s.pool.Exec(ctx, `CREATE TABLE `+table+` (
  scope TEXT NOT NULL, key TEXT NOT NULL, cron TEXT NOT NULL, timezone TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true, next_run_at TIMESTAMPTZ,
  PRIMARY KEY (scope, key)
)`); err != nil {
		t.Fatal(err)
	}
	err := s.insertSchedules(ctx, table, []string{"stale-key"}, "*/5 * * * *", "UTC")
	if err == nil {
		t.Fatal("an insert against a table keyed (scope, key) must fail")
	}
	if !strings.Contains(err.Error(), "(scope, org_id, key)") || !strings.Contains(err.Error(), "run the plugin once") {
		t.Fatalf("stale-key error must name the key shape and the fix: %v", err)
	}

	// Any other failure keeps its own message: an unrelated error must not be
	// reported as a migration problem.
	absent := s.insertSchedules(ctx, table+"_absent", []string{"x"}, "*/5 * * * *", "UTC")
	if absent == nil {
		t.Fatal("an insert into an absent table must fail")
	}
	if strings.Contains(absent.Error(), "run the plugin once") {
		t.Fatalf("an absent table was reported as a stale key: %v", absent)
	}
}

// TestPostgresSweepSnapshots: the worker collects the snapshots nothing
// refreshed within SNAPSHOT_TTL whose baseline schedule row is equally idle. A
// busy row (a recent last_run_at, which the owner-guarded Done writes on every
// finish, success or failure) pins its snapshot even when updated_at is old,
// because the metric is still being retrained: a transient outage must not look
// like a dead metric. The sweep reads forecast.retrain only; this test
// provisions that table in the plugin's shape, exactly as the DSN path assumes.
func TestPostgresSweepSnapshots(t *testing.T) {
	s, ctx := openTestStore(t)
	ensurePluginRetrainTable(t, s)
	const (
		staleIdle     = "baselines-sweep-stale-idle"
		staleBusy     = "baselines-sweep-stale-busy"
		fresh         = "baselines-sweep-fresh"
		staleOtherOrg = "baselines-sweep-stale-otherorg"
	)
	keys := []string{staleIdle, staleBusy, fresh, staleOtherOrg}
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM baselines.snapshots WHERE metric_hash = ANY($1)`, keys)
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key = ANY($1)`, keys)
	})

	const stale = 8 * 24 * time.Hour
	seedSnapshot := func(key string, age time.Duration) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
INSERT INTO baselines.snapshots (metric_hash, model, season, calendar, lookback_ms, trained_at, snapshot, updated_at)
VALUES ($1, 'baseline', 'minute-week', '', 1209600000, now() - ($2 * interval '1 second'), $3, now() - ($2 * interval '1 second'))
ON CONFLICT (metric_hash) DO UPDATE SET updated_at = EXCLUDED.updated_at
`, key, int64(age.Seconds()), []byte{0x1f, 0x8b}); err != nil {
			t.Fatal(err)
		}
	}
	seedRow := func(key string, age time.Duration) {
		t.Helper()
		if _, err := s.pool.Exec(ctx, `
INSERT INTO forecast.retrain (scope, org_id, key, cron, timezone, enabled, next_run_at, last_run_at)
VALUES ('baseline', 0, $1, '0 3 * * *', 'UTC', true, now() + interval '1 hour', now() - ($2 * interval '1 second'))
ON CONFLICT (scope, org_id, key) DO UPDATE SET last_run_at = EXCLUDED.last_run_at, next_run_at = EXCLUDED.next_run_at, superseded_at = NULL
`, key, int64(age.Seconds())); err != nil {
			t.Fatal(err)
		}
	}

	// Stale snapshot, idle row: nothing has refreshed either, so it is collected.
	seedSnapshot(staleIdle, stale)
	seedRow(staleIdle, stale)
	// Stale snapshot, busy row: a recent finish (the retrain is running) keeps it.
	seedSnapshot(staleBusy, stale)
	seedRow(staleBusy, 0)
	// Fresh snapshot, idle row: the metric is only refreshed by non-retrain
	// writers, which still counts as fresh.
	seedSnapshot(fresh, 0)
	seedRow(fresh, stale)
	// Stale snapshot, and a same-key `baseline` row under another org with a recent
	// finish: baseline rows are the fleet-wide org 0, so the correlation is scoped to
	// it and this row cannot pin the snapshot.
	seedSnapshot(staleOtherOrg, stale)
	if _, err := s.pool.Exec(ctx, `
INSERT INTO forecast.retrain (scope, org_id, key, cron, timezone, enabled, next_run_at, last_run_at)
VALUES ('baseline', 7, $1, '0 3 * * *', 'UTC', true, now() + interval '1 hour', now())
ON CONFLICT (scope, org_id, key) DO UPDATE SET last_run_at = EXCLUDED.last_run_at, superseded_at = NULL
`, staleOtherOrg); err != nil {
		t.Fatal(err)
	}

	n, err := s.SweepSnapshots(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 {
		t.Fatalf("swept %d rows, want at least the one stale, idle snapshot", n)
	}
	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM baselines.snapshots WHERE metric_hash = ANY($1)`, keys).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("kept %d of the four seeded snapshots, want 2", count)
	}
	for _, tc := range []struct {
		key  string
		want bool
	}{
		{staleIdle, false},
		{staleBusy, true},
		{fresh, true},
		{staleOtherOrg, false},
	} {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM baselines.snapshots WHERE metric_hash = $1)`, tc.key).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != tc.want {
			t.Fatalf("snapshot %s exists=%v, want %v", tc.key, exists, tc.want)
		}
	}
}

// TestPostgresClaimReclaimsAnExpiredLease is the fleet case the reliability contract
// exists for: a worker claims a due row, dies mid-retrain and never finishes it. A
// claim never touches next_run_at, so once claimed_until passes the row is due again
// and a survivor takes it — the baseline the dead worker held is retrained, not
// stranded, and the hashes it never reached were due all along. The stale worker can
// then neither extend the claim nor write its outcome over the survivor's.
func TestPostgresClaimReclaimsAnExpiredLease(t *testing.T) {
	s, ctx := openTestStore(t)
	ensurePluginRetrainTable(t, s)

	key := "baselines-lease-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	t.Cleanup(func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key)
	})
	if err := s.Schedule(ctx, []string{key}, "*/5 * * * *", "UTC"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain SET next_run_at = now() - interval '10 years'
WHERE scope = 'baseline' AND key = $1`, key); err != nil {
		t.Fatal(err)
	}
	// The claim is fleet-wide, so a database that holds other rows (the sandbox's own
	// hashes, say) could hand them back too. The row this test seeded is backdated ten
	// years, so it is first in the ORDER BY and limit 1 keeps the test off rows it did
	// not create — and off their leases.
	claim := func(owner string) *retrainClaim {
		claims, err := s.Claim(ctx, owner, time.Minute, 1)
		if err != nil {
			t.Fatal(err)
		}
		for i := range claims {
			if claims[i].Key == key {
				return &claims[i]
			}
		}
		return nil
	}
	if claim("dead-worker") == nil {
		t.Fatal("the due row was not claimed")
	}
	// A live lease still holds the fleet out, so the reclaim is not early.
	if c := claim("survivor"); c != nil {
		t.Fatalf("a live lease was claimed twice: %+v", c)
	}
	// The crash leaves claimed_by and claimed_until exactly as they were; only the
	// lease has to expire. The UPDATE stands in for the clock passing.
	if _, err := s.pool.Exec(ctx, `
UPDATE forecast.retrain SET claimed_until = now() - interval '1 second'
WHERE scope = 'baseline' AND key = $1`, key); err != nil {
		t.Fatal(err)
	}
	reclaimed := claim("survivor")
	if reclaimed == nil {
		t.Fatal("an expired lease was not re-claimed: the dead worker stranded the row")
	}
	if reclaimed.Attempts != 0 {
		t.Fatalf("attempts=%d, want the untouched 0", reclaimed.Attempts)
	}
	// The dead worker is no longer an owner: it may neither extend the claim nor write
	// its outcome — next run, status or attempt count — over the survivor's.
	if ok, err := s.Extend(ctx, "dead-worker", 0, key, time.Hour); err != nil || ok {
		t.Fatalf("a stale owner extended the claim: ok=%v err=%v", ok, err)
	}
	staleNext := time.Now().UTC().Add(time.Hour)
	if err := s.Done(ctx, "dead-worker", 0, key, staleNext, "error: dead", 7); err != nil {
		t.Fatal(err)
	}
	var (
		status   *string
		dueAt    time.Time
		attempts int
	)
	if err := s.pool.QueryRow(ctx, `
SELECT last_status, next_run_at, attempts FROM forecast.retrain
WHERE scope = 'baseline' AND key = $1`, key).Scan(&status, &dueAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != nil || attempts != 0 || dueAt.After(time.Now().UTC()) {
		t.Fatalf("a stale owner wrote its outcome: status=%v attempts=%d next=%s", status, attempts, dueAt)
	}
	// The survivor still owns the row: it extends, and its own finish lands with the
	// attempt count the retry backoff is derived from.
	if ok, err := s.Extend(ctx, "survivor", 0, key, time.Hour); err != nil || !ok {
		t.Fatalf("the owner could not extend its claim: ok=%v err=%v", ok, err)
	}
	next := time.Now().UTC().Add(5 * time.Minute)
	if err := s.Done(ctx, "survivor", 0, key, next, "error: druid is down (attempt 1)", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.pool.QueryRow(ctx, `
SELECT last_status, next_run_at, attempts FROM forecast.retrain
WHERE scope = 'baseline' AND key = $1`, key).Scan(&status, &dueAt, &attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || status == nil || !strings.Contains(*status, "attempt 1") {
		t.Fatalf("the owner's finish did not land: status=%v attempts=%d", status, attempts)
	}
	if d := dueAt.Sub(next); d < -time.Millisecond || d > time.Millisecond {
		t.Fatalf("next_run_at %s, want about %s", dueAt, next)
	}
}

// TestMissingAttemptsError: the attempts column belongs to the plugin's table, so a
// database that has not had the plugin's 0003 migration applied must be named, not
// surface a bare undefined-column error on every tick. The live shape is the one that
// matters: PostgreSQL 17 answers `column r.attempts does not exist` with an *empty*
// PgError.ColumnName when the statement qualifies the column (measured against the
// Compose database), so a matcher keyed on that field alone never fires. Every other
// error passes through untouched, because only this one has a remedy worth naming.
func TestMissingAttemptsError(t *testing.T) {
	t.Parallel()
	qualified := &pgconn.PgError{Code: "42703", ColumnName: "", Message: "column r.attempts does not exist"}
	if got := missingAttemptsError(qualified); !strings.Contains(got.Error(), "gpx_forecast_migrate") {
		t.Fatalf("err=%v, want the qualified-column shape to name the remedy", got)
	}
	quoted := &pgconn.PgError{Code: "42703", ColumnName: "attempts", Message: `column "attempts" does not exist`}
	if got := missingAttemptsError(quoted); !strings.Contains(got.Error(), "gpx_forecast_migrate") {
		t.Fatalf("err=%v, want the unqualified shape to name the remedy", got)
	}
	other := &pgconn.PgError{Code: "42703", ColumnName: "superseded_at", Message: `column "superseded_at" does not exist`}
	if got := missingAttemptsError(other); got != error(other) {
		t.Fatalf("err=%v, want it passed through", got)
	}
	if got := missingAttemptsError(context.Canceled); got != context.Canceled {
		t.Fatalf("err=%v, want it passed through", got)
	}
}

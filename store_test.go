package baselines

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"os"
	"testing"
	"time"

	forecast "github.com/eduard-kolotushin/timeseries-forecast"
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
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM baselines.workers WHERE id IN ($1, $2)`, first, second)
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
	if _, err := s.pool.Exec(ctx, `UPDATE baselines.workers SET last_seen = now() - interval '2 minutes' WHERE id = $1`, second); err != nil {
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
	if err := s.pool.QueryRow(ctx, `SELECT owned, peers FROM baselines.workers WHERE id = $1`, first).Scan(&owned, &count); err != nil {
		t.Fatal(err)
	}
	if owned != 7 || count != 2 {
		t.Fatalf("heartbeat stored owned=%d peers=%d, want 7 and 2", owned, count)
	}
}

// TestPostgresRetrainQueue covers the worker's side of forecast.retrain. The
// table is created by the plugin, so this skips until the plugin has run against
// the same database.
func TestPostgresRetrainQueue(t *testing.T) {
	s, ctx := openTestStore(t)
	var table *string
	if err := s.pool.QueryRow(ctx, `SELECT to_regclass('forecast.retrain')::text`).Scan(&table); err != nil {
		t.Fatal(err)
	}
	if table == nil {
		t.Skip("forecast.retrain does not exist: the plugin has not run against this database")
	}
	const key = "baselines-store-test"
	cleanup := func() {
		_, _ = s.pool.Exec(context.Background(), `DELETE FROM forecast.retrain WHERE scope = 'baseline' AND key = $1`, key)
	}
	cleanup()
	t.Cleanup(cleanup)

	if err := s.Schedule(ctx, key, "*/5 * * * *", "UTC"); err != nil {
		t.Fatal(err)
	}
	// A worker restart must not reset a schedule the operator has changed.
	if err := s.Schedule(ctx, key, "0 3 * * *", "Europe/Moscow"); err != nil {
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

	next := time.Now().UTC().Add(time.Hour).Truncate(time.Minute)
	if err := s.Done(ctx, key, next, "ok"); err != nil {
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
}

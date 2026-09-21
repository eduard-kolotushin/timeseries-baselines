package baselines

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eduard-kolotushin/timeseries"
	forecast "github.com/eduard-kolotushin/timeseries-forecast"
)

// fakeReader is the scripted Druid side.
type fakeReader struct {
	spans   []metricSpan
	points  map[string][]timeseries.Point[float64]
	hashErr error
	seriErr error

	// clock and seriesAdvance let a test make a fit outlast the minute it started
	// in: every Series call moves the publisher's test clock forward, which is how
	// the real sliced load (14 requests at DRUID_MAX_RPS) behaves.
	clock         *time.Time
	seriesAdvance time.Duration

	mu sync.Mutex
	// noSeries makes Series panic: a path that must not touch Druid is proven by
	// the failure, not by counting calls.
	noSeries bool
	calls    []seriesCall
}

type seriesCall struct {
	hash     string
	from, to time.Time
}

func (f *fakeReader) Hashes(context.Context, time.Time, time.Time) ([]metricSpan, error) {
	if f.hashErr != nil {
		return nil, f.hashErr
	}
	return f.spans, nil
}

func (f *fakeReader) Series(_ context.Context, hash string, from, to time.Time) (timeseries.Series[float64], error) {
	f.mu.Lock()
	if f.noSeries {
		f.mu.Unlock()
		panic("Series called: this path must not query Druid")
	}
	f.calls = append(f.calls, seriesCall{hash: hash, from: from, to: to})
	if f.clock != nil && f.seriesAdvance > 0 {
		*f.clock = f.clock.Add(f.seriesAdvance)
	}
	f.mu.Unlock()
	if f.seriErr != nil {
		return timeseries.Series[float64]{}, f.seriErr
	}
	var (
		times  []time.Time
		values []float64
	)
	for _, p := range f.points[hash] {
		if p.Time.Before(from) || !p.Time.Before(to) {
			continue
		}
		times = append(times, p.Time)
		values = append(values, p.Value)
	}
	return timeseries.New(times, values)
}

func (f *fakeReader) seriesCalls() []seriesCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seriesCall(nil), f.calls...)
}

type fakeSink struct {
	msgs []BaselineMessage
}

func (f *fakeSink) Publish(_ context.Context, msg BaselineMessage) error {
	f.msgs = append(f.msgs, msg)
	return nil
}

func (f *fakeSink) Close() error { return nil }

// fakeBackend is the scripted Postgres side: snapshots, retrain queue and
// membership in one struct, like the real store.
type fakeBackend struct {
	peers   []string
	peerErr error
	due     []retrainClaim
	putErr  error
	hbErr   error

	mu        sync.Mutex
	snaps     map[string]forecast.Snapshot
	updated   map[string]time.Time
	puts      []putCall
	dones     []doneCall
	schedules []scheduleCall
	claims    []claimCall
	beats     []heartbeatCall
	freshRead int
	getRead   int
}

type putCall struct {
	key string
	rec snapshotRecord
}

type doneCall struct {
	owner  string
	orgID  int64
	key    string
	next   time.Time
	status string
}

type scheduleCall struct {
	keys []string
	cron string
	tz   string
}

type claimCall struct {
	owner string
	lease time.Duration
	limit int
}

type heartbeatCall struct {
	id           string
	owned, peers int
}

func (f *fakeBackend) Close() {}

// seedFit stores a real snapshot for key, the same shape a retrain would store.
func (f *fakeBackend) seedFit(t *testing.T, key string, fitted forecast.Fitted, updatedAt time.Time) {
	t.Helper()
	snap, err := forecast.SnapshotOf(fitted)
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snaps == nil {
		f.snaps = make(map[string]forecast.Snapshot)
		f.updated = make(map[string]time.Time)
	}
	f.snaps[key] = snap
	f.updated[key] = updatedAt
}

func (f *fakeBackend) Fresh(_ context.Context, keys []string) (map[string]time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.freshRead++
	out := make(map[string]time.Time, len(keys))
	for _, key := range keys {
		if at, ok := f.updated[key]; ok {
			out[key] = at
		}
	}
	return out, nil
}

func (f *fakeBackend) Get(_ context.Context, key string) (forecast.Snapshot, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getRead++
	snap, ok := f.snaps[key]
	return snap, ok, nil
}

func (f *fakeBackend) Put(_ context.Context, key string, rec snapshotRecord, snap forecast.Snapshot) error {
	if f.putErr != nil {
		return f.putErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.snaps == nil {
		f.snaps = make(map[string]forecast.Snapshot)
		f.updated = make(map[string]time.Time)
	}
	f.snaps[key] = snap
	f.updated[key] = rec.TrainedAt
	f.puts = append(f.puts, putCall{key: key, rec: rec})
	return nil
}

func (f *fakeBackend) Schedule(_ context.Context, keys []string, cron, tz string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules = append(f.schedules, scheduleCall{keys: append([]string(nil), keys...), cron: cron, tz: tz})
	return nil
}

func (f *fakeBackend) Claim(_ context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, claimCall{owner: owner, lease: lease, limit: limit})
	return f.due, nil
}

func (f *fakeBackend) Done(_ context.Context, owner string, orgID int64, key string, next time.Time, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dones = append(f.dones, doneCall{owner: owner, orgID: orgID, key: key, next: next, status: status})
	return nil
}

func (f *fakeBackend) Heartbeat(_ context.Context, id string, owned, peers int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.beats = append(f.beats, heartbeatCall{id: id, owned: owned, peers: peers})
	return f.hbErr
}

func (f *fakeBackend) Peers(context.Context, time.Duration) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.peerErr != nil {
		return nil, f.peerErr
	}
	return f.peers, nil
}

func (f *fakeBackend) putCalls() []putCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]putCall(nil), f.puts...)
}

func (f *fakeBackend) doneCalls() []doneCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]doneCall(nil), f.dones...)
}

func (f *fakeBackend) scheduleCalls() []scheduleCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]scheduleCall(nil), f.schedules...)
}

func (f *fakeBackend) claimCalls() []claimCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]claimCall(nil), f.claims...)
}

func (f *fakeBackend) heartbeats() []heartbeatCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]heartbeatCall(nil), f.beats...)
}

// readCounts is how many freshness queries and snapshot reads the store served.
func (f *fakeBackend) readCounts() (fresh, gets int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.freshRead, f.getRead
}

// minutePoints is n consecutive 1-minute points ending at end (inclusive).
func minutePoints(end time.Time, n int) []timeseries.Point[float64] {
	out := make([]timeseries.Point[float64], n)
	for i := range n {
		out[i] = timeseries.Point[float64]{
			Time:  end.Add(time.Duration(i-n+1) * time.Minute),
			Value: float64(10 + i%20),
		}
	}
	return out
}

// fitPoints trains the model the worker stores, so a seeded snapshot forecasts
// exactly like a real one.
func fitPoints(t *testing.T, points []timeseries.Point[float64]) forecast.Fitted {
	t.Helper()
	fitted, err := forecast.FitSeasonalBaseline(timeseries.MustFromPoints(points), forecast.SeasonMinuteOfWeek, nil)
	if err != nil {
		t.Fatal(err)
	}
	return fitted
}

// readerWithHashes builds a reader whose hashes each carry n minutes of points,
// so each hash's scan span is (n-1) minutes. Every test about training or
// publishing must pick n so that span covers the config's LOOKBACK: below it the
// tick skips the hash as ineligible, which is the point of the eligibility tests
// and the reason a 120-point fixture cannot stand in for a trainable hash.
func readerWithHashes(end time.Time, n int, hashes ...string) *fakeReader {
	r := &fakeReader{points: make(map[string][]timeseries.Point[float64], len(hashes))}
	for _, hash := range hashes {
		pts := minutePoints(end, n)
		r.spans = append(r.spans, metricSpan{Hash: hash, Min: pts[0].Time, Max: pts[len(pts)-1].Time})
		r.points[hash] = pts
	}
	return r
}

// horizon is the publish timestamp: the truncated minute plus the ahead minutes.
func horizon(now time.Time, ahead int) int64 {
	return now.UTC().Truncate(time.Minute).Add(time.Duration(ahead) * time.Minute).UnixMilli()
}

func TestPublisherTick(t *testing.T) {
	t.Parallel()
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	ready := minutePoints(end, 180)
	short := minutePoints(end, 30)
	hourly := []timeseries.Point[float64]{
		{Time: end.Add(-2 * time.Hour), Value: 1},
		{Time: end.Add(-time.Hour), Value: 2},
		{Time: end, Value: 3},
	}

	for _, tc := range []struct {
		name  string
		ahead int
		store *fakeReader
		want  int
		hash  string
		ts    time.Time
	}{
		{
			name:  "skip short span",
			ahead: 1,
			store: &fakeReader{
				spans:  []metricSpan{{Hash: "short", Min: short[0].Time, Max: short[len(short)-1].Time}},
				points: map[string][]timeseries.Point[float64]{"short": short},
			},
			want: 0,
		},
		{
			name:  "skip hourly step",
			ahead: 1,
			store: &fakeReader{
				spans:  []metricSpan{{Hash: "hour", Min: hourly[0].Time, Max: hourly[len(hourly)-1].Time}},
				points: map[string][]timeseries.Point[float64]{"hour": hourly},
			},
			want: 0,
		},
		{
			name:  "one message at last+1m",
			ahead: 1,
			store: &fakeReader{
				spans:  []metricSpan{{Hash: "ready", Min: ready[0].Time, Max: ready[len(ready)-1].Time}},
				points: map[string][]timeseries.Point[float64]{"ready": ready},
			},
			want: 1,
			hash: "ready",
			ts:   end.Add(time.Minute),
		},
		{
			name:  "one message at last+N minutes",
			ahead: 3,
			store: &fakeReader{
				spans:  []metricSpan{{Hash: "ready", Min: ready[0].Time, Max: ready[len(ready)-1].Time}},
				points: map[string][]timeseries.Point[float64]{"ready": ready},
			},
			want: 1,
			hash: "ready",
			ts:   end.Add(3 * time.Minute),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			p := newPublisher(Config{
				Lookback:     2 * time.Hour,
				AheadMinutes: tc.ahead,
				Interval:     time.Minute,
			}, tc.store, sink, nil, nil)
			p.tick(context.Background())
			if len(sink.msgs) != tc.want {
				t.Fatalf("got %d msgs %#v want %d", len(sink.msgs), sink.msgs, tc.want)
			}
			if tc.want == 0 {
				return
			}
			got := sink.msgs[0]
			if got.MetricHash != tc.hash {
				t.Fatalf("hash %s want %s", got.MetricHash, tc.hash)
			}
			if got.MetricTS != tc.ts.UnixMilli() {
				t.Fatalf("metric_ts %d want %d", got.MetricTS, tc.ts.UnixMilli())
			}
		})
	}
}

func TestPublisherSkipsDuplicateTimestamp(t *testing.T) {
	t.Parallel()
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	ready := minutePoints(end, 180)
	reader := &fakeReader{
		spans:  []metricSpan{{Hash: "ready", Min: ready[0].Time, Max: ready[len(ready)-1].Time}},
		points: map[string][]timeseries.Point[float64]{"ready": ready},
	}
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		Interval:     time.Minute,
	}, reader, sink, nil, nil)
	p.tick(context.Background())
	p.tick(context.Background())
	if len(sink.msgs) != 1 {
		t.Fatalf("got %d msgs, want 1 (second tick is a duplicate ts)", len(sink.msgs))
	}
}

func TestPublisherPublishesAtTheHorizonFromASnapshot(t *testing.T) {
	t.Parallel()
	// The snapshot was trained 20 minutes ago and the reader panics if asked for
	// a series: the published point can only come from the stored model, and its
	// timestamp must be the truncated minute plus AHEAD_MINUTES rather than the
	// last data point.
	end := time.Now().UTC().Truncate(time.Minute).Add(-20 * time.Minute)
	backend := &fakeBackend{}
	backend.seedFit(t, "ready", fitPoints(t, minutePoints(end, 180)), end)

	reader := &fakeReader{spans: []metricSpan{{Hash: "ready", Min: end.Add(-3 * time.Hour), Max: end}}}
	reader.noSeries = true
	sink := &fakeSink{}
	p := newPublisher(Config{Lookback: 3 * time.Hour, AheadMinutes: 1, ShardID: "w0"}, reader, sink, nil, backend)

	before := horizon(time.Now(), 1)
	p.tick(context.Background())
	after := horizon(time.Now(), 1)

	if len(sink.msgs) != 1 {
		t.Fatalf("got %d msgs %#v, want one from the snapshot", len(sink.msgs), sink.msgs)
	}
	got := sink.msgs[0]
	if got.MetricTS < before || got.MetricTS > after {
		t.Fatalf("published at %d (%s), want the horizon between %d and %d",
			got.MetricTS, time.UnixMilli(got.MetricTS).UTC(), before, after)
	}
	if calls := len(reader.seriesCalls()); calls != 0 {
		t.Fatalf("took %d Druid series requests while a snapshot was fresh, want 0", calls)
	}

	p.tick(context.Background())
	if len(sink.msgs) != 1 {
		t.Fatalf("a second tick in the same minute published again: %#v", sink.msgs)
	}
}

// The store path decides eligibility from the scan. It has to: the fit it runs
// is fitHash, which only rejects an empty window or a non-minute step, so without
// this the tick inserted a schedule row for every scanned hash and published
// whatever snapshot a fit had left — a hash with three days of history got the
// same lead as one with fifteen. The no-store path's equivalent case is
// TestPublisherTick's "skip short span".
func TestPublisherSkipsIneligibleHashesOnTheStorePath(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	points := map[string][]timeseries.Point[float64]{
		"ready": minutePoints(end, 200), // 199m of history, over a 3h LOOKBACK
		"short": minutePoints(end, 30),  // 29m: the fixture's short hash
	}

	for _, tc := range []struct {
		name           string
		hashes         []string // scan order
		wantSchedule   []string
		wantPublish    []string
		wantIneligible int
	}{
		{
			name:           "the eligible hash is scheduled and published, the short one is neither",
			hashes:         []string{"ready", "short"},
			wantSchedule:   []string{"ready"},
			wantPublish:    []string{"ready"},
			wantIneligible: 1,
		},
		{
			name:           "a short hash alone is silent: no row, no publish, no cached fit",
			hashes:         []string{"short"},
			wantSchedule:   nil,
			wantPublish:    nil,
			wantIneligible: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeReader{points: points}
			backend := &fakeBackend{}
			for _, hash := range tc.hashes {
				pts := points[hash]
				reader.spans = append(reader.spans, metricSpan{Hash: hash, Min: pts[0].Time, Max: pts[len(pts)-1].Time})
				// Every hash has a stored snapshot, the short one included: a fit
				// in the store is not an entitlement to publish (in the sandbox
				// the short hash has one, left by the round that trained it).
				backend.seedFit(t, hash, fitPoints(t, pts), end)
			}
			reader.noSeries = true
			sink := &fakeSink{}
			p := newPublisher(Config{
				Lookback:     3 * time.Hour,
				AheadMinutes: 1,
				ShardID:      "w0",
			}, reader, sink, nil, backend)

			res, ok := p.runTick(context.Background())
			if !ok {
				t.Fatal("tick failed")
			}

			var scheduled []string
			for _, call := range backend.scheduleCalls() {
				scheduled = append(scheduled, call.keys...)
			}
			if got, want := strings.Join(scheduled, ","), strings.Join(tc.wantSchedule, ","); got != want {
				t.Fatalf("scheduled %q, want %q", got, want)
			}
			var published []string
			for _, msg := range sink.msgs {
				published = append(published, msg.MetricHash)
			}
			if got, want := strings.Join(published, ","), strings.Join(tc.wantPublish, ","); got != want {
				t.Fatalf("published %q, want %q", got, want)
			}
			if res.ineligible != tc.wantIneligible {
				t.Fatalf("ineligible %d, want %d", res.ineligible, tc.wantIneligible)
			}
			if len(p.fitted) != len(tc.wantPublish) {
				t.Fatalf("cached %d fits, want %d: an ineligible hash must not hold a fit", len(p.fitted), len(tc.wantPublish))
			}
		})
	}
}

// A row can outlive the rule that would not create it now: one written by an
// older binary during a rollout, or one whose history was truncated. It is
// finished with the reason instead of being trained on the fraction of LOOKBACK
// that is left, and costs no Druid request to find that out.
func TestPublisherRefusesToTrainAnIneligibleClaim(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	short := minutePoints(end, 30)
	reader := &fakeReader{
		spans:  []metricSpan{{Hash: "short", Min: short[0].Time, Max: short[len(short)-1].Time}},
		points: map[string][]timeseries.Point[float64]{"short": short},
	}
	backend := &fakeBackend{due: []retrainClaim{{Key: "short", Cron: "*/5 * * * *", Timezone: "UTC"}}}
	retry := 5 * time.Minute
	p := newPublisher(Config{
		Lookback:         3 * time.Hour,
		AheadMinutes:     1,
		ShardID:          "w0",
		TrainConcurrency: 1,
		RetrainRetry:     retry,
	}, reader, &fakeSink{}, nil, backend)

	p.tick(context.Background())

	if calls := len(reader.seriesCalls()); calls != 0 {
		t.Fatalf("took %d Druid series requests for an ineligible row, want 0", calls)
	}
	if len(backend.puts) != 0 {
		t.Fatalf("stored %d snapshots for an ineligible row, want 0", len(backend.puts))
	}
	dones := backend.doneCalls()
	if len(dones) != 1 {
		t.Fatalf("finished %d claims, want 1", len(dones))
	}
	if !strings.HasPrefix(dones[0].status, "error: only ") || !strings.Contains(dones[0].status, "want 3h0m0s") {
		t.Fatalf("finished with status %q, want the history shortfall in it", dones[0].status)
	}
	if next := dones[0].next; !next.After(time.Now().UTC()) || next.After(time.Now().UTC().Add(retry+time.Minute)) {
		t.Fatalf("next run %s is not within RETRAIN_RETRY: a broken row must come back", next)
	}
}

func TestPublisherTrainsADueClaim(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	lookback := 3 * time.Hour
	reader := readerWithHashes(end, 200, "ready")
	backend := &fakeBackend{due: []retrainClaim{{Key: "ready", Cron: "*/5 * * * *", Timezone: "UTC"}}}
	sink := &fakeSink{}
	cfg := Config{
		Lookback:           lookback,
		AheadMinutes:       1,
		ShardID:            "w0",
		TrainConcurrency:   1,
		RetrainRetry:       5 * time.Minute,
		DefaultRetrainCron: "0 4 * * *",
	}
	p := newPublisher(cfg, reader, sink, nil, backend)

	before := time.Now().UTC()
	p.tick(context.Background())
	after := time.Now().UTC()

	calls := reader.seriesCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d series calls %v, want exactly one per claim", len(calls), calls)
	}
	if calls[0].hash != "ready" {
		t.Fatalf("trained %q, want the claimed hash", calls[0].hash)
	}
	if span := calls[0].to.Sub(calls[0].from); span != lookback {
		t.Fatalf("trained over %s, want LOOKBACK of %s", span, lookback)
	}
	if calls[0].to.Before(before) || calls[0].to.After(after) {
		t.Fatalf("trained over [%s, %s], want a window ending now", calls[0].from, calls[0].to)
	}

	if puts := backend.putCalls(); len(puts) != 1 {
		t.Fatalf("got %d puts %v, want one", len(puts), puts)
	} else if puts[0].key != "ready" || puts[0].rec.Model != modelBaseline || puts[0].rec.Season != seasonMinuteWeek {
		t.Fatalf("stored %+v, want a minute-week baseline", puts[0])
	}

	if schedules := backend.scheduleCalls(); len(schedules) != 1 ||
		len(schedules[0].keys) != 1 || schedules[0].keys[0] != "ready" ||
		schedules[0].cron != "0 4 * * *" || schedules[0].tz != "UTC" {
		t.Fatalf("scheduled %v, want the owned hash with DEFAULT_RETRAIN_CRON in UTC", schedules)
	}

	claims := backend.claimCalls()
	if len(claims) != 1 {
		t.Fatalf("got %d claims %v, want one", len(claims), claims)
	}
	if claims[0].owner != "w0" || claims[0].lease != cfg.RetrainRetry || claims[0].limit != cfg.TrainConcurrency {
		t.Fatalf("claimed as %+v, want owner w0 with lease %s and limit %d", claims[0], cfg.RetrainRetry, cfg.TrainConcurrency)
	}

	dones := backend.doneCalls()
	if len(dones) != 1 {
		t.Fatalf("got %d finishes %v, want one", len(dones), dones)
	}
	if dones[0].owner != "w0" {
		t.Fatalf("finished as %q, want the claim's owner w0", dones[0].owner)
	}
	if dones[0].status != "ok" {
		t.Fatalf("finished with status %q, want ok", dones[0].status)
	}
	if !dones[0].next.After(after) || dones[0].next.Sub(after) > 5*time.Minute || dones[0].next.Minute()%5 != 0 {
		t.Fatalf("next run %s is not the next */5 minute after %s", dones[0].next, after)
	}
}

// A tick stamps every time it owes from the clock it read at its start. The
// retrain used to read the clock a second time, so a slow tick trained and
// scheduled from an hour it had not started in.
func TestPublisherStampsEveryStepFromTheTicksClock(t *testing.T) {
	t.Parallel()
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	start := time.Date(2026, 1, 1, 3, 7, 0, 0, time.UTC)
	later := start.Add(time.Hour)
	reader := readerWithHashes(end, 200, "ready")
	backend := &fakeBackend{due: []retrainClaim{{Key: "ready", Cron: "*/5 * * * *", Timezone: "UTC"}}}
	p := newPublisher(Config{
		Lookback:         3 * time.Hour,
		AheadMinutes:     1,
		ShardID:          "w0",
		TrainConcurrency: 1,
		RetrainRetry:     5 * time.Minute,
	}, reader, &fakeSink{}, nil, backend)

	// The first read is the tick's; every later read is the bug this pins.
	var reads int
	p.now = func() time.Time {
		reads++
		if reads == 1 {
			return start
		}
		return later
	}

	p.tick(context.Background())

	if reads != 1 {
		t.Fatalf("read the clock %d times in one tick, want exactly once", reads)
	}
	calls := reader.seriesCalls()
	if len(calls) != 1 {
		t.Fatalf("got %d series calls %v, want one per claim", len(calls), calls)
	}
	if !calls[0].to.Equal(start) {
		t.Fatalf("trained over a window ending %s, want the tick's %s", calls[0].to, start)
	}
	dones := backend.doneCalls()
	if len(dones) != 1 {
		t.Fatalf("got %d finishes %v, want one", len(dones), dones)
	}
	if want := time.Date(2026, 1, 1, 3, 10, 0, 0, time.UTC); !dones[0].next.Equal(want) {
		t.Fatalf("next run %s, want %s: the next */5 slot after the tick's clock", dones[0].next, want)
	}
}

func TestPublisherRetrainFailureIsRescheduled(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	reader := readerWithHashes(end, 200, "ready")
	reader.seriErr = errors.New("druid is down")
	backend := &fakeBackend{due: []retrainClaim{{Key: "ready", Cron: "*/5 * * * *", Timezone: "UTC"}}}
	retry := 5 * time.Minute
	cfg := Config{Lookback: 3 * time.Hour, AheadMinutes: 1, ShardID: "w0", TrainConcurrency: 1, RetrainRetry: retry}
	p := newPublisher(cfg, reader, &fakeSink{}, nil, backend)

	before := time.Now().UTC()
	p.tick(context.Background())
	after := time.Now().UTC()

	if puts := backend.putCalls(); len(puts) != 0 {
		t.Fatalf("stored %v after a failed fit, want nothing", puts)
	}
	dones := backend.doneCalls()
	if len(dones) != 1 {
		t.Fatalf("got %d finishes %v, want one", len(dones), dones)
	}
	if !strings.HasPrefix(dones[0].status, "error: ") || !strings.Contains(dones[0].status, "druid is down") {
		t.Fatalf("finished with status %q, want the failure", dones[0].status)
	}
	if want := before.Add(retry); dones[0].next.Before(want) || dones[0].next.After(after.Add(retry)) {
		t.Fatalf("retry scheduled at %s, want %s + %s", dones[0].next, before, retry)
	}
}

func TestPublisherKeepsPeersWhenTheLookupFails(t *testing.T) {
	t.Parallel()
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	backend := &fakeBackend{peers: []string{"w0", "w1"}}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		WorkerTTL:    30 * time.Second,
	}, readerWithHashes(end, 180, hashes...), &fakeSink{}, nil, backend)

	p.tick(context.Background())
	beats := backend.heartbeats()
	if len(beats) != 1 || beats[0].peers != 2 {
		t.Fatalf("heartbeat %v, want one row reporting both peers", beats)
	}
	owned := beats[0].owned
	if owned == 0 || owned >= len(hashes) {
		t.Fatalf("with two peers w0 owns %d of %d hashes", owned, len(hashes))
	}

	// A failing lookup must keep the last good set: falling back to self-only
	// here would take over the hashes the other worker is still publishing.
	backend.peerErr = errors.New("store is down")
	p.tick(context.Background())
	beats = backend.heartbeats()
	if len(beats) != 2 {
		t.Fatalf("got %d heartbeats, want one per tick", len(beats))
	}
	if last := beats[1]; last.peers != 2 || last.owned != owned {
		t.Fatalf("after a failed lookup the worker reports peers=%d owned=%d, want 2 and %d", last.peers, last.owned, owned)
	}
}

func TestPublisherCountsSkippedHashes(t *testing.T) {
	t.Parallel()
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		ShardPeers:   []string{"w0", "w1"},
	}, readerWithHashes(end, 180, hashes...), sink, nil, nil)

	res, ok := p.runTick(context.Background())
	if !ok {
		t.Fatal("tick reported no scan result")
	}
	if res.owned == 0 || res.skipped == 0 {
		t.Fatalf("two workers: w0 owns %d and skips %d of %d hashes", res.owned, res.skipped, len(hashes))
	}
	if res.published != res.owned || len(sink.msgs) != res.owned {
		t.Fatalf("published %d of %d owned hashes (%d messages)", res.published, res.owned, len(sink.msgs))
	}
}

func TestPublisherWorkersOwnDisjointHashes(t *testing.T) {
	t.Parallel()
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	reader := readerWithHashes(end, 180, hashes...)
	peers := []string{"w0", "w1", "w2"}

	published := make(map[string]string, len(hashes))
	for _, self := range peers {
		sink := &fakeSink{}
		p := newPublisher(Config{
			Lookback:     2 * time.Hour,
			AheadMinutes: 1,
			ShardID:      self,
			ShardPeers:   peers,
		}, reader, sink, nil, nil)
		p.tick(context.Background())
		for _, m := range sink.msgs {
			if prev, ok := published[m.MetricHash]; ok {
				t.Fatalf("hash %s published by both %s and %s", m.MetricHash, prev, self)
			}
			published[m.MetricHash] = self
		}
	}
	if len(published) != len(hashes) {
		t.Fatalf("published %d hashes, want all %d", len(published), len(hashes))
	}
}

func TestPublisherStrandsShareOfAPeerThatIsNotRunning(t *testing.T) {
	t.Parallel()
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)

	// Static peer list with a worker that is not running: the hashes it owns are
	// published by nobody. This is why SHARD_PEERS must match the running
	// workers, and why live discovery (store or DNS) is the safer default.
	sink := &fakeSink{}
	newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		ShardPeers:   []string{"w0", "ghost"},
	}, readerWithHashes(end, 180, hashes...), sink, nil, nil).tick(context.Background())

	if len(sink.msgs) == 0 || len(sink.msgs) >= len(hashes) {
		t.Fatalf("w0 published %d of %d hashes, want its own share only", len(sink.msgs), len(hashes))
	}
}

func TestPublisherTakesOverHashesAfterPeerLeaves(t *testing.T) {
	t.Parallel()
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	reader := readerWithHashes(end, 180, hashes...)

	before := &fakeSink{}
	newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		ShardPeers:   []string{"w0", "w1"},
	}, reader, before, nil, nil).tick(context.Background())
	if len(before.msgs) == 0 || len(before.msgs) >= len(hashes) {
		t.Fatalf("two workers: w0 published %d of %d hashes", len(before.msgs), len(hashes))
	}

	// w1 is gone, so w0 owns the whole table and must publish on the next tick.
	after := &fakeSink{}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		ShardPeers:   []string{"w0"},
	}, reader, after, nil, nil)
	p.tick(context.Background())
	if len(after.msgs) != len(hashes) {
		t.Fatalf("after w1 left, w0 published %d of %d hashes", len(after.msgs), len(hashes))
	}
}

func TestPublisherRunsUnshardedWhenPeerLookupFails(t *testing.T) {
	t.Parallel()
	hashes := corpus(32)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "10.0.0.1",
		ShardDNS:     "baselines-headless",
	}, readerWithHashes(end, 180, hashes...), sink, nil, nil)
	p.peers.resolver = &fakeResolver{err: errors.New("no such host")}

	// A lookup that fails with no last good set must not stall the tick: the
	// worker publishes everything, which costs duplicate work but no lead points.
	p.tick(context.Background())
	if len(sink.msgs) != len(hashes) {
		t.Fatalf("worker published %d of %d hashes after a failed lookup", len(sink.msgs), len(hashes))
	}
}

func TestPublisherKeepsPublishingWhenTheScanFails(t *testing.T) {
	t.Parallel()
	// A Druid outage must not stop publishing: the hash set and the models are
	// already in memory, so the tick keeps running on the last good scan. Without
	// one (a cold start) there is nothing to publish and the tick aborts.
	end := time.Now().UTC().Truncate(time.Minute).Add(-20 * time.Minute)
	backend := &fakeBackend{}
	backend.seedFit(t, "ready", fitPoints(t, minutePoints(end, 180)), end)

	reader := &fakeReader{spans: []metricSpan{{Hash: "ready", Min: end.Add(-3 * time.Hour), Max: end}}}
	reader.noSeries = true
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:         3 * time.Hour,
		AheadMinutes:     1,
		ShardID:          "w0",
		HashScanTTL:      time.Nanosecond,
		SnapshotCacheTTL: time.Nanosecond,
	}, reader, sink, nil, backend)

	reader.hashErr = errors.New("druid is down")
	if res, ok := p.runTick(context.Background()); ok {
		t.Fatalf("a cold tick with no scan and no cache reported %+v, want a failed tick", res)
	}
	if len(sink.msgs) != 0 {
		t.Fatalf("published %d messages without a scan", len(sink.msgs))
	}

	// Recover the scan, then take Druid away again: publishing continues on the
	// stale hash set instead of skipping the tick.
	reader.hashErr = nil
	if _, ok := p.runTick(context.Background()); !ok {
		t.Fatal("tick failed while Druid was healthy")
	}
	reader.hashErr = errors.New("druid is down")
	res, ok := p.runTick(context.Background())
	if !ok {
		t.Fatal("a tick with a stale scan aborted, want it to keep publishing")
	}
	if res.owned != 1 || res.skipped != 0 {
		t.Fatalf("stale tick reports %+v, want the cached hash set", res)
	}
}

func TestPublisherPublishesOnAMisalignedGrid(t *testing.T) {
	t.Parallel()
	// A snapshot whose minute grid is offset from the wall clock has no point
	// exactly at the horizon. The worker must publish the newest point at or
	// before it instead of returning no point on every tick.
	const offset = 37 * time.Second
	end := time.Now().UTC().Truncate(time.Minute).Add(-20 * time.Minute).Add(offset)
	backend := &fakeBackend{}
	backend.seedFit(t, "ready", fitPoints(t, minutePoints(end, 180)), end)

	reader := &fakeReader{spans: []metricSpan{{Hash: "ready", Min: end.Add(-3 * time.Hour), Max: end}}}
	reader.noSeries = true
	sink := &fakeSink{}
	p := newPublisher(Config{Lookback: 3 * time.Hour, AheadMinutes: 1, ShardID: "w0"}, reader, sink, nil, backend)

	gridPoint := func(now time.Time) int64 {
		return now.UTC().Truncate(time.Minute).Add(offset).UnixMilli()
	}
	before := gridPoint(time.Now())
	p.tick(context.Background())
	after := gridPoint(time.Now())

	if len(sink.msgs) != 1 {
		t.Fatalf("got %d msgs %#v, want one from the misaligned snapshot", len(sink.msgs), sink.msgs)
	}
	if ms := sink.msgs[0].MetricTS; ms < before || ms > after {
		t.Fatalf("published at %d (%s), want the last grid point at or before the horizon, between %d and %d",
			ms, time.UnixMilli(ms).UTC(), before, after)
	}
}

func TestPublisherReadsSnapshotsOncePerCacheTTL(t *testing.T) {
	t.Parallel()
	// The publish path must not pay a Restore (~60k floats for a minute-of-week
	// baseline) per tick: within SNAPSHOT_CACHE_TTL the store is asked once,
	// however many ticks run.
	end := time.Now().UTC().Truncate(time.Minute).Add(-20 * time.Minute)
	backend := &fakeBackend{}
	backend.seedFit(t, "ready", fitPoints(t, minutePoints(end, 180)), end)

	reader := &fakeReader{spans: []metricSpan{{Hash: "ready", Min: end.Add(-3 * time.Hour), Max: end}}}
	reader.noSeries = true
	p := newPublisher(Config{Lookback: 3 * time.Hour, AheadMinutes: 1, ShardID: "w0"}, reader, &fakeSink{}, nil, backend)

	p.tick(context.Background())
	p.tick(context.Background())
	p.tick(context.Background())

	if fresh, gets := backend.readCounts(); fresh != 1 || gets != 1 {
		t.Fatalf("three ticks in one cache window made %d freshness queries and %d snapshot reads, want 1 and 1", fresh, gets)
	}
}

func TestPublisherPublishesEveryMinuteWhenRetrainCrossesTheMinute(t *testing.T) {
	t.Parallel()
	// The tick reads its clock once. Before that, the retrain tick stamped its
	// point from the clock it read *after* the fit, so a sliced retrain that ran
	// past the end of its own minute moved the horizon one minute forward; the
	// next tick then published that same horizon and skipped one. With a */5 cron
	// and a 14-request fit the sandbox lost exactly 17:35, 17:40, 17:45, … from
	// the Druid `baselines` table.
	start := time.Date(2026, 1, 1, 17, 24, 59, 0, time.UTC)
	clock := start
	reader := readerWithHashes(start.Truncate(time.Minute), 200, "ready")
	// Every Series request costs wall clock, like the real rate-limited load.
	reader.clock, reader.seriesAdvance = &clock, 3*time.Second

	backend := &fakeBackend{due: []retrainClaim{{Key: "ready", Cron: "*/5 * * * *", Timezone: "UTC"}}}
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:         3 * time.Hour,
		AheadMinutes:     1,
		ShardID:          "w0",
		TrainConcurrency: 1,
		RetrainRetry:     5 * time.Minute,
	}, reader, sink, nil, backend)
	p.now = func() time.Time { return clock }

	for i := range 3 {
		p.tick(context.Background())
		// The claim is finished and the cron is */5, so only the first tick
		// retrains; the next ticks are the plain publish path a fast tick is.
		backend.due = nil
		clock = start.Add(time.Duration(i+1) * time.Minute)
	}

	if len(sink.msgs) != 3 {
		t.Fatalf("three ticks published %d points %v, want one per minute", len(sink.msgs), sink.msgs)
	}
	for i, msg := range sink.msgs {
		want := horizon(start.Add(time.Duration(i)*time.Minute), 1)
		if msg.MetricTS != want {
			t.Fatalf("tick %d published %d (%s), want the horizon %d (%s): the retrain crossed the minute and skipped one",
				i, msg.MetricTS, time.UnixMilli(msg.MetricTS).UTC(), want, time.UnixMilli(want).UTC())
		}
	}
}

func TestPublisherPrunesFitsForHashesItNoLongerOwns(t *testing.T) {
	t.Parallel()
	// A hash that moves to another peer must release its fit here: a minute-of-week
	// fit is ~0.5 MB, and the cache is keyed by hash, so without pruning the old
	// owner holds it for the life of the process.
	start := time.Date(2026, 1, 1, 17, 0, 0, 0, time.UTC)
	view2 := []string{"w0", "w1"}
	var kept, moved string
	for _, hash := range corpus(64) {
		if Owns(hash, "w0", view2) {
			if kept == "" {
				kept = hash
			}
			continue
		}
		if moved == "" {
			moved = hash
		}
	}
	if kept == "" || moved == "" {
		t.Fatal("fixture: want one hash w0 keeps and one it loses when w1 joins")
	}

	backend := &fakeBackend{}
	fitted := fitPoints(t, minutePoints(start, 180))
	for _, hash := range []string{kept, moved} {
		backend.seedFit(t, hash, fitted, start)
	}
	reader := readerWithHashes(start, 200, kept, moved)
	reader.noSeries = true
	sink := &fakeSink{}
	clock := start
	p := newPublisher(Config{
		Lookback:     3 * time.Hour,
		AheadMinutes: 1,
		ShardID:      "w0",
		ShardPeers:   []string{"w0"},
	}, reader, sink, nil, backend)
	p.now = func() time.Time { return clock }

	p.tick(context.Background())
	if len(p.fitted) != 2 {
		t.Fatalf("alone the worker owns both hashes and cached %d fits, want 2", len(p.fitted))
	}

	// w1 joins and takes `moved`.
	p.peers.static = view2
	before := len(sink.msgs)
	clock = start.Add(time.Minute)
	p.tick(context.Background())

	if _, ok := p.fitted[moved]; ok {
		t.Fatalf("the fit for %s survived the hash moving to another peer", moved)
	}
	if _, ok := p.fitted[kept]; !ok {
		t.Fatalf("the fit for %s was dropped while this worker still owns it", kept)
	}
	published := make([]string, 0, len(sink.msgs)-before)
	for _, msg := range sink.msgs[before:] {
		published = append(published, msg.MetricHash)
	}
	if len(published) != 1 || published[0] != kept {
		t.Fatalf("after the handover the worker published %v, want only %s", published, kept)
	}
}

func TestPublisherSchedulesTheOwnedSetInOneStoreCall(t *testing.T) {
	t.Parallel()
	// One INSERT … SELECT for the whole owned set instead of one round trip per
	// hash per tick: a row exists after the first tick, so the other 1439 ticks a
	// day would pay H statements each for nothing.
	hashes := corpus(64)
	end := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	backend := &fakeBackend{}
	p := newPublisher(Config{
		Lookback:           2 * time.Hour,
		AheadMinutes:       1,
		ShardID:            "w0",
		DefaultRetrainCron: "0 4 * * *",
	}, readerWithHashes(end, 180, hashes...), &fakeSink{}, nil, backend)

	p.tick(context.Background())

	schedules := backend.scheduleCalls()
	if len(schedules) != 1 {
		t.Fatalf("a tick over %d owned hashes made %d schedule calls, want one", len(hashes), len(schedules))
	}
	if got := schedules[0]; len(got.keys) != len(hashes) || got.cron != "0 4 * * *" || got.tz != "UTC" {
		t.Fatalf("scheduled %d keys with %q/%q, want all %d owned hashes with DEFAULT_RETRAIN_CRON in UTC",
			len(got.keys), got.cron, got.tz, len(hashes))
	}
	seen := make(map[string]bool, len(schedules[0].keys))
	for _, key := range schedules[0].keys {
		seen[key] = true
	}
	for _, hash := range hashes {
		if !seen[hash] {
			t.Fatalf("the scheduled set is missing the owned hash %s", hash)
		}
	}
}

func TestPublisherKeepsItsShareWhenTheHeartbeatFails(t *testing.T) {
	t.Parallel()
	// A heartbeat that cannot be written is best effort: the peer set was already
	// resolved from the store, so the tick must keep publishing its share rather
	// than degrading to self-only (every hash, all duplicates) or to nothing.
	hashes := corpus(64)
	start := time.Date(2026, 1, 1, 17, 0, 0, 0, time.UTC)
	fitted := fitPoints(t, minutePoints(start, 180))

	backend := &fakeBackend{peers: []string{"w0", "w1"}, hbErr: errors.New("store is down")}
	for _, hash := range hashes {
		backend.seedFit(t, hash, fitted, start)
	}
	reader := &fakeReader{spans: make([]metricSpan, 0, len(hashes))}
	for _, hash := range hashes {
		reader.spans = append(reader.spans, metricSpan{Hash: hash, Min: start.Add(-3 * time.Hour), Max: start})
	}
	reader.noSeries = true
	sink := &fakeSink{}
	clock := start
	p := newPublisher(Config{
		Lookback:        3 * time.Hour,
		AheadMinutes:    1,
		ShardID:         "w0",
		ShardMembership: "store",
		WorkerTTL:       30 * time.Second,
	}, reader, sink, nil, backend)
	p.now = func() time.Time { return clock }

	p.tick(context.Background())
	first := msgHashes(sink.msgs)
	if len(first) == 0 || len(first) == len(hashes) {
		t.Fatalf("with two live peers w0 published %d of %d hashes, want its own share", len(first), len(hashes))
	}
	if len(backend.heartbeats()) != 1 {
		t.Fatalf("got %d heartbeats, want one attempt per tick", len(backend.heartbeats()))
	}

	// The store keeps answering with the same two peers; only the heartbeat fails.
	clock = start.Add(time.Minute)
	before := len(sink.msgs)
	p.tick(context.Background())

	second := msgHashes(sink.msgs[before:])
	if len(second) != len(first) {
		t.Fatalf("after a failed heartbeat the worker published %d hashes, want the same %d", len(second), len(first))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Fatalf("the share changed after a failed heartbeat: %v then %v", first, second)
		}
	}
	if len(backend.heartbeats()) != 2 {
		t.Fatalf("got %d heartbeats, want one attempt per tick", len(backend.heartbeats()))
	}
}

func TestPublisherFailedSnapshotPutIsRescheduled(t *testing.T) {
	t.Parallel()
	// The fit succeeded but the store rejected it: reporting "ok" would push the
	// next attempt a whole cron period away and leave the publish path on a stale
	// snapshot, so the claim must finish as an error and come back after RETRY.
	end := time.Now().UTC().Truncate(time.Minute)
	reader := readerWithHashes(end, 200, "ready")
	backend := &fakeBackend{
		due:    []retrainClaim{{Key: "ready", Cron: "*/5 * * * *", Timezone: "UTC"}},
		putErr: errors.New("store is down"),
	}
	retry := 5 * time.Minute
	p := newPublisher(Config{
		Lookback:         3 * time.Hour,
		AheadMinutes:     1,
		ShardID:          "w0",
		TrainConcurrency: 1,
		RetrainRetry:     retry,
	}, reader, &fakeSink{}, nil, backend)

	before := time.Now().UTC()
	p.tick(context.Background())
	after := time.Now().UTC()

	if puts := backend.putCalls(); len(puts) != 0 {
		t.Fatalf("stored %v although the store rejected it, want nothing", puts)
	}
	dones := backend.doneCalls()
	if len(dones) != 1 {
		t.Fatalf("got %d finishes %v, want one", len(dones), dones)
	}
	if !strings.HasPrefix(dones[0].status, "error: ") || !strings.Contains(dones[0].status, "store is down") {
		t.Fatalf("finished with status %q, want the store failure", dones[0].status)
	}
	if dones[0].owner != "w0" {
		t.Fatalf("finished as %q, want the claim's owner w0", dones[0].owner)
	}
	if want := before.Add(retry); dones[0].next.Before(want) || dones[0].next.After(after.Add(retry)) {
		t.Fatalf("retry scheduled at %s, want %s + %s", dones[0].next, before, retry)
	}
}

// msgHashes lists the hashes a set of published messages covers, in order.
func msgHashes(msgs []BaselineMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		out = append(out, msg.MetricHash)
	}
	return out
}

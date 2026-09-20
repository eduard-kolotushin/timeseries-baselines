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
	peers    []string
	peerErr  error
	due      []retrainClaim
	putErr   error
	freshErr error
	hbErr    error

	mu        sync.Mutex
	snaps     map[string]forecast.Snapshot
	updated   map[string]time.Time
	puts      []putCall
	dones     []doneCall
	schedules []string
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
	key    string
	next   time.Time
	status string
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
	f.freshRead++
	f.mu.Unlock()
	if f.freshErr != nil {
		return nil, f.freshErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
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

func (f *fakeBackend) Schedule(_ context.Context, key, cron, tz string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schedules = append(f.schedules, strings.Join([]string{key, cron, tz}, "|"))
	return nil
}

func (f *fakeBackend) Claim(_ context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, claimCall{owner: owner, lease: lease, limit: limit})
	return f.due, nil
}

func (f *fakeBackend) Done(_ context.Context, key string, next time.Time, status string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dones = append(f.dones, doneCall{key: key, next: next, status: status})
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

func (f *fakeBackend) scheduleCalls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.schedules...)
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

// readerWithHashes is a reader where every hash has a ready 1-minute series
// ending at end.
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

func TestPublisherTrainsADueClaim(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	lookback := 3 * time.Hour
	reader := readerWithHashes(end, 120, "ready")
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

	if schedules := backend.scheduleCalls(); len(schedules) != 1 || schedules[0] != "ready|0 4 * * *|UTC" {
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
	if dones[0].status != "ok" {
		t.Fatalf("finished with status %q, want ok", dones[0].status)
	}
	if !dones[0].next.After(after) || dones[0].next.Sub(after) > 5*time.Minute || dones[0].next.Minute()%5 != 0 {
		t.Fatalf("next run %s is not the next */5 minute after %s", dones[0].next, after)
	}
}

func TestPublisherRetrainFailureIsRescheduled(t *testing.T) {
	t.Parallel()
	end := time.Now().UTC().Truncate(time.Minute)
	reader := readerWithHashes(end, 120, "ready")
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
	if res.owned+res.skipped != len(hashes) {
		t.Fatalf("owned %d + skipped %d, want %d hashes", res.owned, res.skipped, len(hashes))
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

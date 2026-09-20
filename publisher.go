package baselines

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/eduard-kolotushin/timeseries"
	forecast "github.com/eduard-kolotushin/timeseries-forecast"
)

// Publisher scans Druid on a tick, retrains the hashes whose schedule is due and
// writes one baseline point per owned hash from its fitted snapshot.
type Publisher struct {
	cfg       Config
	src       metricReader
	sink      baselineSink
	cal       *forecast.Calendar
	published map[string]int64
	peers     *peerSource
	store     storeBackend
	trainSem  *semaphore

	// One metric_hash scan per HashScanTTL: the hash set moves on the order of
	// hours, so paying a GROUP BY per worker per minute buys nothing.
	scanAt    time.Time
	scanSpans []metricSpan
	scanOK    bool

	// Fitted models restored from the store, refreshed at most once per
	// SnapshotCacheTTL: a minute-of-week fit is ~60k floats, so the publish path
	// must not restore one per tick.
	freshAt time.Time
	fitted  map[string]snapshotFit
	// warned is the last condition logged per hash, so a hash that is scheduled
	// but not yet trained does not log on every tick.
	warned map[string]string

	lastPeers    int
	lastOwned    int
	countsLogged bool
	hbWarned     bool
}

type snapshotFit struct {
	updatedAt time.Time
	fitted    forecast.Fitted
}

// tickResult is one pass, logged as a single line.
type tickResult struct {
	peers     int
	owned     int
	skipped   int
	published int
	retrained int
}

func newPublisher(cfg Config, src metricReader, sink baselineSink, cal *forecast.Calendar, store storeBackend) *Publisher {
	cfg = cfg.normalized()
	return &Publisher{
		cfg:       cfg,
		src:       src,
		sink:      sink,
		cal:       cal,
		store:     store,
		published: make(map[string]int64),
		peers:     newPeerSource(cfg, store),
		trainSem:  newSemaphore(cfg.TrainConcurrency),
		fitted:    make(map[string]snapshotFit),
		warned:    make(map[string]string),
	}
}

// NewPublisher wires Druid, Kafka and (when BASELINE_STORE_* is set) the
// snapshot store from cfg.
func NewPublisher(cfg Config, cal *forecast.Calendar) (*Publisher, error) {
	cfg = cfg.normalized()
	var store storeBackend
	if cfg.StoreDSN != "" {
		s, err := openPostgresStore(context.Background(), cfg.StoreDSN)
		if err != nil {
			return nil, fmt.Errorf("snapshot store: %w", err)
		}
		store = s
	}
	return newPublisher(cfg, newDruidStore(cfg, nil), newKafkaSink(cfg.KafkaBrokers, cfg.KafkaTopic), cal, store), nil
}

// Close stops the Kafka writer and releases the store pool.
func (p *Publisher) Close() error {
	if p == nil {
		return nil
	}
	if p.store != nil {
		p.store.Close()
	}
	if p.sink == nil {
		return nil
	}
	return p.sink.Close()
}

// MembershipMode is the membership source this publisher resolved.
func (p *Publisher) MembershipMode() string { return p.peers.mode }

// Run ticks immediately, then every cfg.Interval until ctx is cancelled.
func (p *Publisher) Run(ctx context.Context) {
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	p.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.tick(ctx)
		}
	}
}

func (p *Publisher) tick(ctx context.Context) {
	if r, ok := p.runTick(ctx); ok {
		p.logTick(r)
	}
}

// runTick is one pass: heartbeat, retrain the due claims, publish the owned
// hashes from their snapshots. ok is false when the scan failed, which leaves
// the tick with nothing to publish and nothing to log but the error.
//
// The heartbeat carries this tick's owned count, which is only known after the
// scan, so it is written once the scan is in — still one row write per tick, and
// a peer always includes itself, so nobody waits for their own row to appear.
func (p *Publisher) runTick(ctx context.Context) (tickResult, bool) {
	if err := ctx.Err(); err != nil {
		return tickResult{}, false
	}
	now := time.Now().UTC()
	peers := p.peers.peers(ctx)
	spans, err := p.scan(ctx, now)
	if err != nil {
		slog.Error("list hashes", "err", err)
		return tickResult{}, false
	}
	keys := ownedKeys(spans, p.peers.self, peers)
	p.heartbeat(ctx, len(keys), len(peers))
	res := tickResult{peers: len(peers), owned: len(keys), skipped: len(spans) - len(keys)}
	res.retrained = p.retrain(ctx, keys)
	res.published = p.emit(ctx, spans, keys, peers)
	return res, true
}

// scan returns the metric_hash spans seen in the last ScanRange. The result is
// cached for HashScanTTL so a fleet of workers does not run one GROUP BY each
// per tick, and a failed refresh keeps the last good set: the models are already
// fitted, so a Druid outage must not stop publishing.
func (p *Publisher) scan(ctx context.Context, now time.Time) ([]metricSpan, error) {
	if p.scanOK && now.Sub(p.scanAt) < p.cfg.HashScanTTL {
		return p.scanSpans, nil
	}
	from := now.Add(-p.cfg.ScanRange)
	spans, err := p.src.Hashes(ctx, from, now)
	if err != nil {
		if p.scanOK {
			slog.Error("list hashes", "err", err, "staleSince", p.scanAt)
			return p.scanSpans, nil
		}
		return nil, err
	}
	p.scanAt, p.scanSpans, p.scanOK = now, spans, true
	slog.Debug("scan", "hashes", len(spans), "requests", len(windows(from, now, p.cfg.DruidMaxRange)), "ttl", p.cfg.HashScanTTL.String())
	return spans, nil
}

// heartbeat publishes this worker's liveness for store-based membership. It is
// best effort: a failed write keeps the previous peer set and must not cost a
// tick, so it warns once per failing streak.
func (p *Publisher) heartbeat(ctx context.Context, owned, peers int) {
	if p.store == nil {
		return
	}
	if err := p.store.Heartbeat(ctx, p.peers.self, owned, peers); err != nil {
		if !p.hbWarned {
			slog.Warn("heartbeat failed", "id", p.peers.self, "err", err)
			p.hbWarned = true
		}
		return
	}
	p.hbWarned = false
}

// retrain schedules every owned hash that has no row yet, then claims due rows
// fleet-wide and trains them. Claims are deliberately not restricted to owned
// hashes: any worker may run any schedule, which is what keeps a retrain alive
// when the hash's rendezvous owner is down.
func (p *Publisher) retrain(ctx context.Context, keys []string) int {
	if p.store == nil {
		return 0
	}
	for _, key := range keys {
		if err := p.store.Schedule(ctx, key, p.cfg.DefaultRetrainCron, "UTC"); err != nil {
			// Almost always systemic (table missing, database down), so one
			// error per tick is enough detail.
			slog.Error("schedule", "metric_hash", key, "err", err)
			break
		}
	}
	claims, err := p.store.Claim(ctx, p.peers.self, p.cfg.RetrainRetry, p.cfg.TrainConcurrency)
	if err != nil {
		slog.Error("claim retrains", "err", err)
		return 0
	}
	if len(claims) == 0 {
		return 0
	}
	now := time.Now().UTC()
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		train int
	)
	for _, claim := range claims {
		// Bounded goroutine pool: TRAIN_CONCURRENCY fits at a time, and the claim
		// limit matches the pool so a tick never claims work it cannot start.
		release, err := p.trainSem.acquire(ctx)
		if err != nil {
			break
		}
		wg.Add(1)
		go func(c retrainClaim) {
			defer wg.Done()
			defer release()
			if p.trainHash(ctx, c, now) {
				mu.Lock()
				train++
				mu.Unlock()
			}
		}(claim)
	}
	wg.Wait()
	return train
}

// trainHash refits one hash and stores the snapshot.
func (p *Publisher) trainHash(ctx context.Context, c retrainClaim, now time.Time) bool {
	err := p.fitHash(ctx, c.Key, now)
	if err == nil {
		var next time.Time
		if next, err = nextRun(c.Cron, c.Timezone, now); err == nil {
			p.finish(ctx, c.Key, next, "ok")
			return true
		}
	}
	slog.Error("retrain", "metric_hash", c.Key, "err", err)
	// A failure is due again after RETRAIN_RETRY rather than at the next cron
	// fire: the row is broken now, and waiting until tomorrow hides it.
	p.finish(ctx, c.Key, now.Add(p.cfg.RetrainRetry), "error: "+err.Error())
	return false
}

func (p *Publisher) finish(ctx context.Context, key string, next time.Time, status string) {
	if err := p.store.Done(ctx, key, next, status); err != nil {
		slog.Error("retrain finish", "metric_hash", key, "err", err)
	}
}

// fitHash trains over the last LOOKBACK and stores the snapshot. It is the only
// place the worker reads a series for training, and the range is the caller's
// because a single window per hash would exceed the datasource's request cap.
func (p *Publisher) fitHash(ctx context.Context, key string, now time.Time) error {
	s, err := p.src.Series(ctx, key, now.Add(-p.cfg.Lookback), now)
	if err != nil {
		return err
	}
	if s.Empty() {
		return fmt.Errorf("no data in the last %s", p.cfg.Lookback)
	}
	if step := lastStep(s); step != time.Minute {
		return fmt.Errorf("step between the last two points is %s, want 1m", step)
	}
	fitted, err := forecast.FitSeasonalBaseline(s, forecast.SeasonMinuteOfWeek, p.cal)
	if err != nil {
		return err
	}
	snap, err := forecast.SnapshotOf(fitted)
	if err != nil {
		return err
	}
	return p.store.Put(ctx, key, snapshotRecord{
		Model:     modelBaseline,
		Season:    seasonMinuteWeek,
		Calendar:  p.cfg.Calendar,
		Lookback:  p.cfg.Lookback,
		TrainedAt: now,
	}, snap)
}

// emit publishes one point per owned hash and reports how many another worker
// owns, so a single tick line describes the whole fleet.
func (p *Publisher) emit(ctx context.Context, spans []metricSpan, keys []string, peers []string) int {
	p.refreshSnapshots(ctx, keys)
	ts := time.Now().UTC().Truncate(time.Minute).Add(time.Duration(p.cfg.AheadMinutes) * time.Minute)
	published := 0
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return published
		}
		if !Owns(span.Hash, p.peers.self, peers) {
			continue
		}
		ok := false
		if p.store == nil {
			ok = p.fitForecast(ctx, span)
		} else {
			ok = p.emitSnapshot(ctx, span.Hash, ts)
		}
		if ok {
			published++
		}
	}
	return published
}

// emitSnapshot forecasts the horizon point from the cached fit. The publish
// timestamp is the truncated minute plus AHEAD_MINUTES, not the last data point:
// a series that stops reporting must not keep a fresh-looking timestamp alive.
func (p *Publisher) emitSnapshot(ctx context.Context, key string, ts time.Time) bool {
	cached, ok := p.fitted[key]
	if !ok {
		p.warnOnce(key, "no snapshot", "no snapshot", "metric_hash", key)
		return false
	}
	// One grid step back as well as the horizon: a grid that is not aligned to
	// the wall-clock minute has no point exactly at ts, and asking for the exact
	// point would return ErrEmptyRange on every tick — silently stopping
	// publishing for that hash. On an aligned grid (every real minute series) the
	// last point is exactly ts, so the horizon is unchanged.
	out, err := cached.fitted.ForecastRange(ts.Add(-time.Minute), ts)
	if errors.Is(err, forecast.ErrEmptyRange) {
		// The grid does not reach back to the horizon, which the next retrain
		// fixes unless the clock itself moved backwards.
		p.warnOnce(key, "behind horizon", "snapshot is behind the publish horizon", "metric_hash", key, "ts", ts)
		return false
	}
	if err != nil {
		slog.Error("metric", "metric_hash", key, "err", err)
		return false
	}
	if out.Empty() {
		return false
	}
	pt, err := out.At(out.Len() - 1)
	if err != nil {
		return false
	}
	if math.IsNaN(pt.Value) {
		return false
	}
	return p.publish(ctx, key, pt.Time, pt.Value)
}

// fitForecast is the path when no store is configured, unchanged from the
// pre-snapshot loop: fit the scan's span and publish the point AHEAD_MINUTES
// after the data. It costs one Druid request per owned hash per tick, which is
// what snapshots exist to avoid.
func (p *Publisher) fitForecast(ctx context.Context, span metricSpan) bool {
	if span.Max.Sub(span.Min) < p.cfg.Lookback {
		return false
	}
	s, err := p.src.Series(ctx, span.Hash, span.Max.Add(-p.cfg.Lookback), time.Now().UTC())
	if err != nil {
		slog.Error("metric", "metric_hash", span.Hash, "err", err)
		return false
	}
	if lastStep(s) != time.Minute {
		return false
	}
	fitted, err := forecast.FitSeasonalBaseline(s, forecast.SeasonMinuteOfWeek, p.cal)
	if err != nil {
		slog.Error("metric", "metric_hash", span.Hash, "err", err)
		return false
	}
	out, err := fitted.Forecast(p.cfg.AheadMinutes)
	if err != nil {
		slog.Error("metric", "metric_hash", span.Hash, "err", err)
		return false
	}
	if out.Empty() {
		return false
	}
	pt, err := out.At(out.Len() - 1)
	if err != nil {
		return false
	}
	if math.IsNaN(pt.Value) {
		return false
	}
	return p.publish(ctx, span.Hash, pt.Time, pt.Value)
}

// publish writes one point unless it is not newer than the last one for that
// hash, which is how a restart or a repeated tick stays idempotent.
func (p *Publisher) publish(ctx context.Context, key string, at time.Time, value float64) bool {
	ms := at.UTC().UnixMilli()
	if prev, ok := p.published[key]; ok && ms <= prev {
		return false
	}
	msg := BaselineMessage{MetricHash: key, MetricTS: ms, BaselineValue: value}
	if err := p.sink.Publish(ctx, msg); err != nil {
		slog.Error("metric", "metric_hash", key, "err", err)
		return false
	}
	p.published[key] = ms
	return true
}

// refreshSnapshots restores the fits behind the owned hashes, one Fresh query at
// most per SnapshotCacheTTL, and only for the snapshots whose updated_at moved.
// A failed freshness query keeps the cached fits: publishing must survive a
// store hiccup, because the models it needs are already in memory.
func (p *Publisher) refreshSnapshots(ctx context.Context, keys []string) {
	if p.store == nil || len(keys) == 0 {
		return
	}
	now := time.Now().UTC()
	if !p.freshAt.IsZero() && now.Sub(p.freshAt) < p.cfg.SnapshotCacheTTL {
		return
	}
	fresh, err := p.store.Fresh(ctx, keys)
	if err != nil {
		slog.Error("snapshot freshness", "err", err)
		return
	}
	p.freshAt = now
	for _, key := range keys {
		updated, ok := fresh[key]
		if !ok {
			delete(p.fitted, key)
			continue
		}
		if cached, ok := p.fitted[key]; ok && cached.updatedAt.Equal(updated) {
			continue
		}
		snap, ok, err := p.store.Get(ctx, key)
		if err != nil {
			slog.Error("snapshot get", "metric_hash", key, "err", err)
			continue
		}
		if !ok {
			delete(p.fitted, key)
			continue
		}
		fitted, err := forecast.Restore(snap)
		if err != nil {
			slog.Error("snapshot restore", "metric_hash", key, "err", err)
			continue
		}
		p.fitted[key] = snapshotFit{updatedAt: updated, fitted: fitted}
		delete(p.warned, key)
	}
}

// warnOnce logs a per-hash condition once per streak; the flag clears when the
// hash gets a snapshot.
func (p *Publisher) warnOnce(key, reason, msg string, args ...any) {
	if p.warned[key] == reason {
		return
	}
	p.warned[key] = reason
	slog.Warn(msg, args...)
}

// logTick logs every tick at debug, and the fleet view again at info whenever
// the peer set or the owned count changes: that is the line that shows a
// takeover happening without turning on debug logging.
func (p *Publisher) logTick(r tickResult) {
	if !p.countsLogged || r.peers != p.lastPeers || r.owned != p.lastOwned {
		slog.Info("membership", "shard", p.peers.self, "mode", p.peers.mode, "peers", r.peers, "owned", r.owned)
		p.lastPeers, p.lastOwned, p.countsLogged = r.peers, r.owned, true
	}
	slog.Debug("tick", "shard", p.peers.self, "peers", r.peers, "owned", r.owned,
		"skipped", r.skipped, "published", r.published, "retrained", r.retrained)
}

// ownedKeys returns the hashes this worker publishes, in span order.
func ownedKeys(spans []metricSpan, self string, peers []string) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		if Owns(span.Hash, self, peers) {
			out = append(out, span.Hash)
		}
	}
	return out
}

// lastStep is the interval between the last two points, or 0 when the series is
// too short to tell. A minute-of-week baseline needs minute spacing; anything
// else would fit the wrong grid.
func lastStep(s timeseries.Series[float64]) time.Duration {
	n := s.Len()
	if n < 2 {
		return 0
	}
	last, err := s.Time(n - 1)
	if err != nil {
		return 0
	}
	prev, err := s.Time(n - 2)
	if err != nil {
		return 0
	}
	return last.Sub(prev)
}

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

	// now is the tick's clock. One tick reads it once, so a slow retrain cannot
	// move the publish horizon past the minute the tick belongs to: a tick is
	// one minute of wall clock, however long its work takes.
	now func() time.Time

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
	// ownedSet is the owned key set of the current tick, reused so pruning the
	// fit cache does not allocate a map per tick.
	ownedSet map[string]struct{}
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

// tickResult is one pass, logged as a single line. owned and skipped are the
// sharding view (what this worker owns, what another peer owns); ineligible is
// the subset of owned hashes the scan says has too little history to train.
type tickResult struct {
	peers      int
	owned      int
	skipped    int
	ineligible int
	published  int
	retrained  int
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
		now:       time.Now,
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
//
// The tick reads the clock once and hands it to every step that stamps a time,
// so a retrain that runs across a minute boundary still publishes the minute the
// tick started in.
func (p *Publisher) runTick(ctx context.Context) (tickResult, bool) {
	if err := ctx.Err(); err != nil {
		return tickResult{}, false
	}
	now := p.now().UTC()
	peers := p.peers.peers(ctx)
	spans, err := p.scan(ctx, now)
	if err != nil {
		slog.Error("list hashes", "err", err)
		return tickResult{}, false
	}
	keys := ownedKeys(spans, p.peers.self, peers)
	p.heartbeat(ctx, len(keys), len(peers))
	ready := readyKeys(spans, p.peers.self, peers, p.cfg.Lookback)
	res := tickResult{
		peers:      len(peers),
		owned:      len(keys),
		skipped:    len(spans) - len(keys),
		ineligible: len(keys) - len(ready),
	}
	res.retrained = p.retrain(ctx, ready)
	res.published = p.emit(ctx, spans, ready, peers, now)
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

// retrain schedules every owned hash that is eligible and has no row yet, then
// claims due rows fleet-wide and trains them. Claims are deliberately not
// restricted to owned hashes: any worker may run any schedule, which is what
// keeps a retrain alive when the hash's rendezvous owner is down. claims are
// still checked against this worker's scan (see trainable), so a row below
// LOOKBACK is finished with a reason instead of trained.
func (p *Publisher) retrain(ctx context.Context, keys []string) int {
	if p.store == nil {
		return 0
	}
	// One statement for the whole owned set: a row per hash exists after the
	// first tick, so the other 1439 ticks a day would pay an INSERT each for
	// nothing.
	if err := p.store.Schedule(ctx, keys, p.cfg.DefaultRetrainCron, "UTC"); err != nil {
		// Almost always systemic (table missing, database down), so one error
		// per tick is enough detail.
		slog.Error("schedule", "hashes", len(keys), "err", err)
	}
	claims, err := p.store.Claim(ctx, p.peers.self, p.cfg.RetrainRetry, p.cfg.TrainConcurrency)
	if err != nil {
		slog.Error("claim retrains", "err", err)
		return 0
	}
	if len(claims) == 0 {
		return 0
	}
	now := p.now().UTC()
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

// trainHash refits one hash and stores the snapshot. The finish carries the
// owner of the claim: a lease that expired while this retrain ran has already
// been re-claimed, and that worker's row must not be overwritten from here.
func (p *Publisher) trainHash(ctx context.Context, c retrainClaim, now time.Time) bool {
	err := p.trainable(c.Key)
	if err == nil {
		err = p.fitHash(ctx, c.Key, now)
	}
	if err == nil {
		var next time.Time
		if next, err = nextRun(c.Cron, c.Timezone, now); err == nil {
			p.finish(ctx, c, next, "ok")
			return true
		}
	}
	slog.Error("retrain", "metric_hash", c.Key, "err", err)
	// A failure is due again after RETRAIN_RETRY rather than at the next cron
	// fire: the row is broken now, and waiting until tomorrow hides it.
	p.finish(ctx, c, now.Add(p.cfg.RetrainRetry), "error: "+err.Error())
	return false
}

// trainable rejects a claim the scan already knows is below LOOKBACK, before a
// Druid request is spent on it. A row can outlive the rule that would not create
// it now — written by an older binary during a rollout, or left by a hash whose
// history was truncated — and it must be recorded as broken rather than trained
// on the fraction of the window that is left. A key this worker's scan does not
// carry is left to fitHash, which reports the missing data itself.
func (p *Publisher) trainable(key string) error {
	for _, span := range p.scanSpans {
		if span.Hash != key {
			continue
		}
		if !eligible(span, p.cfg.Lookback) {
			return fmt.Errorf("only %s of history in the last %s, want %s",
				span.Max.Sub(span.Min), p.cfg.ScanRange, p.cfg.Lookback)
		}
		return nil
	}
	return nil
}

func (p *Publisher) finish(ctx context.Context, c retrainClaim, next time.Time, status string) {
	if err := p.store.Done(ctx, p.peers.self, c.OrgID, c.Key, next, status); err != nil {
		slog.Error("retrain finish", "metric_hash", c.Key, "err", err)
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

// emit publishes one point per owned, eligible hash and returns how many were
// published. keys is the ready set: the snapshot cache is loaded and pruned for
// it, so a hash below LOOKBACK keeps no fit in memory either. now is the tick's
// own start, not the moment the retrain happened to finish: the horizon this
// tick owes is now.Truncate(1m) + AHEAD_MINUTES, and a retrain that runs across
// a minute boundary must publish that minute, not skip it.
func (p *Publisher) emit(ctx context.Context, spans []metricSpan, keys []string, peers []string, now time.Time) int {
	p.refreshSnapshots(ctx, keys)
	ts := now.Truncate(time.Minute).Add(time.Duration(p.cfg.AheadMinutes) * time.Minute)
	published := 0
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return published
		}
		if !Owns(span.Hash, p.peers.self, peers) {
			continue
		}
		// A stored snapshot is not an entitlement to publish: a hash whose
		// history is shorter than LOOKBACK (or shrank to less than it) must not
		// keep a fresh-looking lead alive from a fit it should never have had.
		if !eligible(span, p.cfg.Lookback) {
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
	if !eligible(span, p.cfg.Lookback) {
		return false
	}
	s, err := p.src.Series(ctx, span.Hash, span.Max.Add(-p.cfg.Lookback), p.now().UTC())
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
//
// Every call also drops the fits this worker no longer owns, before the cache
// TTL can short-circuit the query: a hash that moved to another peer would
// otherwise keep ~0.5 MB of fitted state on this process for its whole life.
func (p *Publisher) refreshSnapshots(ctx context.Context, keys []string) {
	if p.store == nil {
		return
	}
	p.pruneFits(keys)
	if len(keys) == 0 {
		return
	}
	now := p.now().UTC()
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

// pruneFits drops the fits and warning state of every hash this worker no longer
// owns, so a hash that moved to another peer releases its fitted state instead of
// pinning it for the life of the process.
func (p *Publisher) pruneFits(keys []string) {
	if len(p.fitted) == 0 {
		return
	}
	if p.ownedSet == nil {
		p.ownedSet = make(map[string]struct{}, len(keys))
	}
	clear(p.ownedSet)
	for _, key := range keys {
		p.ownedSet[key] = struct{}{}
	}
	for key := range p.fitted {
		if _, ok := p.ownedSet[key]; ok {
			continue
		}
		delete(p.fitted, key)
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
		"skipped", r.skipped, "ineligible", r.ineligible, "published", r.published, "retrained", r.retrained)
}

// eligible is the v1 rule that a hash needs a full training window before it is
// trained or published: the scan's span (its earliest sighting in SCAN_RANGE to
// its latest) must cover LOOKBACK. The scan reports the hash's real first point,
// so this is the whole-history test v1 applied, on a window bounded by SCAN_RANGE
// instead of all time.
func eligible(span metricSpan, lookback time.Duration) bool {
	return span.Max.Sub(span.Min) >= lookback
}

// ownedKeys returns the hashes this worker owns, in span order.
func ownedKeys(spans []metricSpan, self string, peers []string) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		if Owns(span.Hash, self, peers) {
			out = append(out, span.Hash)
		}
	}
	return out
}

// readyKeys returns the owned hashes that may be trained and published. The
// store path decides both from this list — the schedule row is inserted for it
// and emit publishes it — because the fit the retrain path runs is not the fit
// that carries the eligibility check: fitForecast's gate covers the no-store
// loop only, so without this the store turned every scanned hash into a trained,
// published series and a hash with days of history was published like any other.
func readyKeys(spans []metricSpan, self string, peers []string, lookback time.Duration) []string {
	out := make([]string, 0, len(spans))
	for _, span := range spans {
		if eligible(span, lookback) && Owns(span.Hash, self, peers) {
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

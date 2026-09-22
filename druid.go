package baselines

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eduard-kolotushin/timeseries"
)

const (
	druidRetryBackoff    = 250 * time.Millisecond
	druidRetryMaxBackoff = 2 * time.Second

	// maxDruidReplyBytes caps one Druid SQL reply. A series window holds at most
	// one row per minute and the scan is aggregate-only, so a legitimate reply is
	// orders of magnitude smaller than this; the cap is what keeps a broken or
	// hostile datasource from pulling the process out of memory.
	maxDruidReplyBytes = 64 << 20

	// maxDruidWindows bounds how many requests one query may be sliced into. A
	// window is at least DRUID_MAX_RANGE long and Validate refuses a positive
	// DRUID_MAX_RANGE below a minute, so a legitimate count is the caller's own
	// window divided by that minimum (336h at 1m is 20160); this bound is the
	// guard for a Config that never went through Validate, where
	// ceil(span/maxRange) could ask for billions of windows — and the slice is
	// allocated before the first request is sent, so it would take the process
	// out of memory without asking Druid anything.
	maxDruidWindows = 1 << 20
)

type metricSpan struct {
	Hash string
	Min  time.Time
	Max  time.Time
}

// metricReader is the Druid access the publisher needs. Both calls take a
// window because the caller, not the store, owns the range: the production
// datasource caps how far one request may reach, so every query is sliced.
type metricReader interface {
	Hashes(ctx context.Context, from, to time.Time) ([]metricSpan, error)
	Series(ctx context.Context, hash string, from, to time.Time) (timeseries.Series[float64], error)
}

type druidStore struct {
	broker     string
	datasource string
	client     *http.Client
	maxRange   time.Duration
	maxReply   int64
	retries    int
	authHeader string
	authValue  string
	sem        *semaphore
	rl         *rateLimiter
}

func newDruidStore(cfg Config, client *http.Client) *druidStore {
	cfg = cfg.normalized()
	if client == nil {
		client = &http.Client{Timeout: cfg.DruidTimeout}
	}
	return &druidStore{
		broker:     strings.TrimRight(cfg.DruidBroker, "/"),
		datasource: cfg.DruidDatasource,
		client:     client,
		maxRange:   cfg.DruidMaxRange,
		maxReply:   maxDruidReplyBytes,
		retries:    cfg.DruidRetries,
		authHeader: cfg.DruidAuthHeader,
		authValue:  cfg.DruidAuthValue,
		sem:        newSemaphore(cfg.DruidMaxInflight),
		rl:         newRateLimiter(cfg.DruidMaxRPS),
	}
}

// windowCount is how many requests windows produces for [from, to): one when
// maxRange <= 0 (the whole range is one request) or the range is empty, else
// ceil(span/maxRange). It is pure arithmetic, so a caller that only wants the
// count — the scan debug line — does not build the slice.
func windowCount(from, to time.Time, maxRange time.Duration) int {
	span := to.Sub(from)
	if maxRange <= 0 || span <= 0 {
		return 1
	}
	// Integer division, not (span+maxRange-1)/maxRange: Sub saturates at the
	// duration limit, where the addition would overflow into a negative count.
	n := int64(span / maxRange)
	if span%maxRange != 0 {
		n++
	}
	return int(n)
}

// windows splits [from, to) into consecutive half-open windows of at most
// maxRange. Half-open bounds are what makes stitching safe: no row can be
// returned twice, so Concat only fails on a datasource that itself duplicates or
// reorders timestamps. maxRange <= 0 keeps one window, which is one request.
//
// A span that would need more than maxDruidWindows requests is refused rather
// than sliced: one query is then at most maxDruidWindows requests, and a
// misconfiguration fails loudly at the first call instead of allocating a window
// per millisecond of history before Druid is asked anything.
func windows(from, to time.Time, maxRange time.Duration) ([][2]time.Time, error) {
	from, to = from.UTC(), to.UTC()
	if maxRange <= 0 || !to.After(from) {
		return [][2]time.Time{{from, to}}, nil
	}
	n := windowCount(from, to, maxRange)
	if n > maxDruidWindows {
		return nil, fmt.Errorf("query span %s at DRUID_MAX_RANGE=%s needs %d windows, above the %d-window bound; raise DRUID_MAX_RANGE",
			to.Sub(from), maxRange, n, maxDruidWindows)
	}
	out := make([][2]time.Time, 0, n)
	for lo := from; lo.Before(to); {
		hi := lo.Add(maxRange)
		if hi.After(to) {
			hi = to
		}
		out = append(out, [2]time.Time{lo, hi})
		lo = hi
	}
	return out, nil
}

// Series returns the metric_hash series over [from, to), one Druid request per
// window. The result is stitched in order, so an overlapping or out-of-order
// window fails loudly (timeseries.ErrDuplicateTime/ErrUnsorted) instead of
// quietly feeding the fit duplicated points.
func (d *druidStore) Series(ctx context.Context, hash string, from, to time.Time) (timeseries.Series[float64], error) {
	esc := strings.ReplaceAll(hash, `'`, `''`)
	ws, err := windows(from, to, d.maxRange)
	if err != nil {
		return timeseries.Series[float64]{}, err
	}
	var out timeseries.Series[float64]
	for _, w := range ws {
		lo, hi := w[0], w[1]
		q := fmt.Sprintf(
			`SELECT __time, metric_value FROM %s WHERE metric_hash = '%s' AND __time >= MILLIS_TO_TIMESTAMP(%d) AND __time < MILLIS_TO_TIMESTAMP(%d) ORDER BY __time`,
			d.datasource, esc, lo.UnixMilli(), hi.UnixMilli(),
		)
		rows, err := d.sql(ctx, "series", q, lo, hi)
		if err != nil {
			return timeseries.Series[float64]{}, err
		}
		// A window of this size holds at most one row per minute.
		n := int(hi.Sub(lo)/time.Minute) + 1
		times := make([]time.Time, 0, n)
		values := make([]float64, 0, n)
		skipped, missing := 0, 0
		for _, row := range rows {
			t, err := parseDruidTime(row["__time"])
			if err != nil {
				// A row we cannot place in time is data we do not have: dropping it
				// keeps the rest of the window usable, but it is not silent.
				skipped++
				continue
			}
			v := asFloat(row["metric_value"])
			if math.IsNaN(v) {
				missing++
			}
			// The timestamp is kept even when the value is missing: the 1-minute
			// grid is checked on this series before the fit drops NaN, so dropping
			// the point instead would make a gapped series look like a bad step.
			times = append(times, t)
			values = append(values, v)
		}
		if skipped > 0 || missing > 0 {
			slog.Warn("druid series rows unusable", "hash", hash, "skipped", skipped, "missing", missing, "window", lo)
		}
		part, err := timeseries.New(times, values)
		if err != nil {
			return timeseries.Series[float64]{}, err
		}
		if out, err = timeseries.Concat(out, part); err != nil {
			return timeseries.Series[float64]{}, fmt.Errorf("metric_hash %s: %w", hash, err)
		}
	}
	return out, nil
}

// Hashes reports the metric_hash spans seen in [from, to), merged across
// windows: Min is the earliest and Max the latest sighting. Because the scan
// window is bounded, Min is clamped to the window start, so a hash whose data
// ended before now-SCAN_RANGE is no longer reported as eligible.
func (d *druidStore) Hashes(ctx context.Context, from, to time.Time) ([]metricSpan, error) {
	ws, err := windows(from, to, d.maxRange)
	if err != nil {
		return nil, err
	}
	out := make([]metricSpan, 0, 64)
	index := make(map[string]int, 64)
	for _, w := range ws {
		lo, hi := w[0], w[1]
		q := fmt.Sprintf(
			`SELECT metric_hash, MIN(__time) AS tmin, MAX(__time) AS tmax FROM %s WHERE __time >= MILLIS_TO_TIMESTAMP(%d) AND __time < MILLIS_TO_TIMESTAMP(%d) GROUP BY 1`,
			d.datasource, lo.UnixMilli(), hi.UnixMilli(),
		)
		rows, err := d.sql(ctx, "hashes", q, lo, hi)
		if err != nil {
			return nil, err
		}
		skipped := 0
		for _, row := range rows {
			hash := asString(row["metric_hash"])
			if hash == "" {
				skipped++
				continue
			}
			minT, err := parseDruidTime(row["tmin"])
			if err != nil {
				skipped++
				continue
			}
			maxT, err := parseDruidTime(row["tmax"])
			if err != nil {
				skipped++
				continue
			}
			i, ok := index[hash]
			if !ok {
				index[hash] = len(out)
				out = append(out, metricSpan{Hash: hash, Min: minT, Max: maxT})
				continue
			}
			if minT.Before(out[i].Min) {
				out[i].Min = minT
			}
			if maxT.After(out[i].Max) {
				out[i].Max = maxT
			}
		}
		if skipped > 0 {
			slog.Warn("druid hash rows unusable", "skipped", skipped, "window", lo)
		}
	}
	return out, nil
}

// sql runs one Druid SQL statement under the concurrency semaphore and the rate
// limiter, retrying transport errors and 5xx only: a 4xx means the query is
// wrong, so retrying it repeats the same failure.
func (d *druidStore) sql(ctx context.Context, op, query string, from, to time.Time) ([]map[string]any, error) {
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, err
	}
	release, err := d.sem.acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	var lastErr error
	for attempt := 0; attempt <= d.retries; attempt++ {
		if attempt > 0 {
			if err := sleepCtx(ctx, retryBackoff(attempt)); err != nil {
				return nil, err
			}
		}
		if err := d.rl.wait(ctx); err != nil {
			return nil, err
		}
		rows, retryable, err := d.do(ctx, op, body, from, to)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, lastErr
}

// do performs one request. The returned flag reports whether a retry could
// plausibly succeed.
func (d *druidStore) do(ctx context.Context, op string, body []byte, from, to time.Time) ([]map[string]any, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.broker+"/druid/v2/sql", bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	if d.authHeader != "" {
		req.Header.Set(d.authHeader, d.authValue)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, true, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, d.maxReply+1))
	if err != nil {
		return nil, true, err
	}
	if int64(len(raw)) > d.maxReply {
		// Not retryable: the same query returns the same oversized body, so a
		// retry only multiplies the read.
		return nil, false, fmt.Errorf("druid sql reply exceeds %d bytes", d.maxReply)
	}
	if resp.StatusCode >= 500 {
		return nil, true, fmt.Errorf("druid sql: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	if resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("druid sql: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, false, fmt.Errorf("druid sql decode: %w", err)
	}
	slog.Debug("druid request", "op", op, "from", from, "to", to)
	return rows, false, nil
}

// retryBackoff is the delay before attempt n (1-based): 250ms doubling to a 2s
// cap, so a datasource restarting under load is not hammered.
func retryBackoff(attempt int) time.Duration {
	d := druidRetryBackoff << (attempt - 1)
	if d <= 0 || d > druidRetryMaxBackoff {
		return druidRetryMaxBackoff
	}
	return d
}

// sleepCtx waits d or ctx.Done, whichever comes first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func parseDruidTime(v any) (time.Time, error) {
	switch x := v.(type) {
	case nil:
		return time.Time{}, fmt.Errorf("empty time")
	case float64:
		return time.UnixMilli(int64(x)).UTC(), nil
	case int64:
		return time.UnixMilli(x).UTC(), nil
	case json.Number:
		ms, err := x.Int64()
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMilli(ms).UTC(), nil
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return time.Time{}, fmt.Errorf("empty time")
		}
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t.UTC(), nil
		}
		if t, err := time.Parse(time.RFC3339, strings.ReplaceAll(s, " ", "T")); err == nil {
			return t.UTC(), nil
		}
		s = strings.ReplaceAll(s, " ", "T")
		if !strings.Contains(s, "Z") && !strings.Contains(s, "+") {
			s += "Z"
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return time.Time{}, err
		}
		return t.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported time %T", v)
	}
}

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprint(x)
	}
}

// asFloat reads a numeric column. A null, an unparseable string or an unknown
// type is math.NaN() — the library's missing-value marker, which the fit drops —
// never 0, which would be a fabricated data point in the middle of a series.
func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			return math.NaN()
		}
		return f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err != nil {
			return math.NaN()
		}
		return f
	default:
		return math.NaN()
	}
}

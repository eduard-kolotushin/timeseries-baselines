package baselines

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// windowRE captures the half-open millisecond bounds of a sliced query, which
// is the whole contract with the datasource: `>= lo AND < hi` is what makes a
// stitched series impossible to duplicate.
var windowRE = regexp.MustCompile(`__time >= MILLIS_TO_TIMESTAMP\((\d+)\) AND __time < MILLIS_TO_TIMESTAMP\((\d+)\)`)

// druidServer is a fake Druid SQL endpoint that records every query it is asked
// to run and replies with what the test's reply function returns.
type druidServer struct {
	*httptest.Server

	mu      sync.Mutex
	queries []string
}

func newDruidServer(t *testing.T, reply func(t *testing.T, attempt int, query string) (any, int)) *druidServer {
	t.Helper()
	ds := &druidServer{}
	ds.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
			return
		}
		ds.mu.Lock()
		ds.queries = append(ds.queries, body.Query)
		attempt := len(ds.queries)
		ds.mu.Unlock()
		rows, status := reply(t, attempt, body.Query)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if rows != nil {
			if err := json.NewEncoder(w).Encode(rows); err != nil {
				t.Errorf("encode rows: %v", err)
			}
		}
	}))
	t.Cleanup(ds.Close)
	return ds
}

func (d *druidServer) sqlQueries() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.queries...)
}

func queryWindow(t *testing.T, query string) (time.Time, time.Time) {
	t.Helper()
	m := windowRE.FindStringSubmatch(query)
	if m == nil {
		t.Fatalf("query has no half-open window: %s", query)
	}
	lo, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	hi, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return time.UnixMilli(lo).UTC(), time.UnixMilli(hi).UTC()
}

// minuteRows answers with one row per minute of [lo, hi). Dense rows are what
// makes an overlapping window visible: it would return the same minute twice and
// timeseries.Concat would refuse to stitch it.
func minuteRows(lo, hi int64) []map[string]any {
	out := make([]map[string]any, 0, (hi-lo)/60000+1)
	for ms := lo; ms < hi; ms += 60000 {
		out = append(out, map[string]any{
			"__time":       time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z"),
			"metric_value": float64(ms/60000%1000) + 0.5,
		})
	}
	return out
}

func TestDruidSeriesWindowsPartitionTheRange(t *testing.T) {
	t.Parallel()
	to := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	from := to.Add(-336 * time.Hour)
	srv := newDruidServer(t, func(t *testing.T, _ int, query string) (any, int) {
		lo, hi := queryWindow(t, query)
		return minuteRows(lo.UnixMilli(), hi.UnixMilli()), http.StatusOK
	})
	store := newDruidStore(Config{
		DruidBroker:     srv.URL,
		DruidDatasource: "metrics",
		DruidMaxRange:   24 * time.Hour,
		DruidRetries:    0,
	}, srv.Client())

	s, err := store.Series(context.Background(), "ready", from, to)
	if err != nil {
		t.Fatal(err)
	}

	queries := srv.sqlQueries()
	if len(queries) != 14 {
		t.Fatalf("336h of lookback at 24h per request took %d requests, want 14", len(queries))
	}
	for i, query := range queries {
		lo, hi := queryWindow(t, query)
		wantLo := from.Add(time.Duration(i) * 24 * time.Hour)
		if !lo.Equal(wantLo) {
			t.Fatalf("window %d starts at %s, want %s", i, lo, wantLo)
		}
		if hi.Sub(lo) > 24*time.Hour {
			t.Fatalf("window %d spans %s, want at most 24h", i, hi.Sub(lo))
		}
		if i == 0 {
			continue
		}
		if _, prevHi := queryWindow(t, queries[i-1]); !prevHi.Equal(lo) {
			t.Fatalf("window %d starts at %s but window %d ends at %s: a gap or an overlap", i, lo, i-1, prevHi)
		}
	}
	if _, lastHi := queryWindow(t, queries[13]); !lastHi.Equal(to) {
		t.Fatalf("the last window ends at %s, want %s", lastHi, to)
	}

	// 14 dense windows of 24h: every minute exactly once, so an overlap would
	// have failed in Concat instead of showing up here.
	if s.Len() != 336*60 {
		t.Fatalf("series has %d points, want %d", s.Len(), 336*60)
	}
	first, err := s.Time(0)
	if err != nil {
		t.Fatal(err)
	}
	last, err := s.Time(s.Len() - 1)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Equal(from) || !last.Equal(to.Add(-time.Minute)) {
		t.Fatalf("series covers [%s, %s], want [%s, %s]", first, last, from, to.Add(-time.Minute))
	}
}

func TestDruidSeriesOneRequestWhenMaxRangeOff(t *testing.T) {
	t.Parallel()
	to := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	from := to.Add(-6 * time.Hour)
	srv := newDruidServer(t, func(t *testing.T, _ int, query string) (any, int) {
		lo, hi := queryWindow(t, query)
		return minuteRows(lo.UnixMilli(), hi.UnixMilli()), http.StatusOK
	})
	store := newDruidStore(Config{
		DruidBroker:     srv.URL,
		DruidDatasource: "metrics",
		DruidMaxRange:   0,
		DruidRetries:    0,
	}, srv.Client())

	s, err := store.Series(context.Background(), "ready", from, to)
	if err != nil {
		t.Fatal(err)
	}
	queries := srv.sqlQueries()
	if len(queries) != 1 {
		t.Fatalf("DRUID_MAX_RANGE=0 took %d requests, want 1", len(queries))
	}
	if lo, hi := queryWindow(t, queries[0]); !lo.Equal(from) || !hi.Equal(to) {
		t.Fatalf("the single window is [%s, %s], want [%s, %s]", lo, hi, from, to)
	}
	if s.Len() != 6*60 {
		t.Fatalf("series has %d points, want %d", s.Len(), 6*60)
	}
}

func TestDruidHashesMergesWindows(t *testing.T) {
	t.Parallel()
	to := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	from := to.Add(-30 * time.Hour)
	boundary := from.Add(24 * time.Hour)
	srv := newDruidServer(t, func(t *testing.T, _ int, query string) (any, int) {
		if !strings.Contains(query, "GROUP BY") {
			t.Fatalf("hashes query is not grouped: %s", query)
		}
		lo, hi := queryWindow(t, query)
		if lo.Before(boundary) {
			// The hash is seen early in the first window only.
			return []map[string]any{{
				"metric_hash": "ready",
				"tmin":        lo.Add(time.Minute).Format(time.RFC3339),
				"tmax":        lo.Add(5 * time.Minute).Format(time.RFC3339),
			}}, http.StatusOK
		}
		return []map[string]any{
			{"metric_hash": "ready", "tmin": lo.Format(time.RFC3339), "tmax": hi.Add(-time.Minute).Format(time.RFC3339)},
			{"metric_hash": "other", "tmin": lo.Format(time.RFC3339), "tmax": lo.Add(time.Minute).Format(time.RFC3339)},
		}, http.StatusOK
	})
	store := newDruidStore(Config{
		DruidBroker:     srv.URL,
		DruidDatasource: "metrics",
		DruidMaxRange:   24 * time.Hour,
		DruidRetries:    0,
	}, srv.Client())

	spans, err := store.Hashes(context.Background(), from, to)
	if err != nil {
		t.Fatal(err)
	}
	if len(srv.sqlQueries()) != 2 {
		t.Fatalf("30h at 24h per request took %d requests, want 2", len(srv.sqlQueries()))
	}
	byHash := make(map[string]metricSpan, len(spans))
	for _, span := range spans {
		byHash[span.Hash] = span
	}
	if len(byHash) != 2 {
		t.Fatalf("spans %+v, want one per hash", spans)
	}
	want := metricSpan{Hash: "ready", Min: from.Add(time.Minute), Max: to.Add(-time.Minute)}
	if got := byHash["ready"]; !got.Min.Equal(want.Min) || !got.Max.Equal(want.Max) {
		t.Fatalf("ready span [%s, %s], want [%s, %s]", got.Min, got.Max, want.Min, want.Max)
	}
	if _, ok := byHash["other"]; !ok {
		t.Fatalf("the hash seen only in the second window is missing: %+v", spans)
	}
}

func TestDruidAuthHeader(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		header     string
		value      string
		wantHeader string
		wantValue  string
	}{
		{"unset", "", "", "", ""},
		{"value without a header name", "", "secret", "", ""},
		{"authorization", "Authorization", "Bearer token", "Authorization", "Bearer token"},
		{"custom header", "X-Api-Key", "key-1", "X-Api-Key", "key-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var (
				mu   sync.Mutex
				sent http.Header
			)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				sent = r.Header.Clone()
				mu.Unlock()
				_, _ = w.Write([]byte("[]"))
			}))
			t.Cleanup(srv.Close)
			store := newDruidStore(Config{
				DruidBroker:     srv.URL,
				DruidDatasource: "metrics",
				DruidAuthHeader: tc.header,
				DruidAuthValue:  tc.value,
				DruidRetries:    0,
			}, srv.Client())

			if _, err := store.Hashes(context.Background(), time.Now().Add(-time.Hour), time.Now()); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			if tc.wantHeader == "" {
				if auth, key := sent.Get("Authorization"), sent.Get("X-Api-Key"); auth != "" || key != "" {
					t.Fatalf("sent Authorization=%q X-Api-Key=%q, want no auth header", auth, key)
				}
				return
			}
			if got := sent.Get(tc.wantHeader); got != tc.wantValue {
				t.Fatalf("%s is %q, want %q", tc.wantHeader, got, tc.wantValue)
			}
		})
	}
}

func TestDruidRetries(t *testing.T) {
	t.Parallel()
	to := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	from := to.Add(-time.Hour)
	for _, tc := range []struct {
		name      string
		retries   int
		status    func(attempt int) int
		wantCalls int
		wantErr   string
	}{
		{
			name:    "5xx is retried once and then answers",
			retries: 2,
			status: func(attempt int) int {
				if attempt == 1 {
					return 500
				}
				return 200
			},
			wantCalls: 2,
		},
		{
			name:      "5xx until the retries run out",
			retries:   2,
			status:    func(int) int { return 503 },
			wantCalls: 3,
			wantErr:   "503",
		},
		{
			name:      "4xx is not retried",
			retries:   2,
			status:    func(int) int { return 400 },
			wantCalls: 1,
			wantErr:   "400",
		},
		{
			name:      "retries off",
			retries:   0,
			status:    func(int) int { return 500 },
			wantCalls: 1,
			wantErr:   "500",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newDruidServer(t, func(t *testing.T, attempt int, query string) (any, int) {
				status := tc.status(attempt)
				if status != http.StatusOK {
					return map[string]any{"error": "boom"}, status
				}
				lo, hi := queryWindow(t, query)
				return minuteRows(lo.UnixMilli(), hi.UnixMilli()), status
			})
			store := newDruidStore(Config{
				DruidBroker:     srv.URL,
				DruidDatasource: "metrics",
				DruidRetries:    tc.retries,
			}, srv.Client())

			s, err := store.Series(context.Background(), "ready", from, to)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				if s.Len() != 60 {
					t.Fatalf("series has %d points, want 60", s.Len())
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("got error %v, want one mentioning %q", err, tc.wantErr)
			}
			if calls := len(srv.sqlQueries()); calls != tc.wantCalls {
				t.Fatalf("took %d requests, want %d", calls, tc.wantCalls)
			}
		})
	}
}

func TestDruidRateLimitSpacesRequests(t *testing.T) {
	t.Parallel()
	// DRUID_MAX_RPS=2 hands out one token per 500ms and banks none, so four
	// requests cannot finish in less than 1.5s.
	srv := newDruidServer(t, func(t *testing.T, _ int, _ string) (any, int) {
		return []map[string]any{}, http.StatusOK
	})
	store := newDruidStore(Config{
		DruidBroker:     srv.URL,
		DruidDatasource: "metrics",
		DruidMaxRPS:     2,
		DruidRetries:    0,
	}, srv.Client())

	start := time.Now()
	for i := 0; i < 4; i++ {
		if _, err := store.Hashes(context.Background(), time.Now().Add(-time.Hour), time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond {
		t.Fatalf("4 requests at DRUID_MAX_RPS=2 took %s, want at least 1.5s", elapsed)
	}
}

func TestWindows(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		from, to time.Time
		maxRange time.Duration
		want     int
	}{
		{name: "off is one window", from: base, to: base.Add(336 * time.Hour), maxRange: 0, want: 1},
		{name: "negative is one window", from: base, to: base.Add(time.Hour), maxRange: -time.Hour, want: 1},
		{name: "exact multiple", from: base, to: base.Add(336 * time.Hour), maxRange: 24 * time.Hour, want: 14},
		{name: "short last window", from: base, to: base.Add(30 * time.Hour), maxRange: 24 * time.Hour, want: 2},
		{name: "empty range", from: base, to: base, maxRange: 24 * time.Hour, want: 1},
		{name: "range shorter than the window", from: base, to: base.Add(time.Minute), maxRange: 24 * time.Hour, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := windows(tc.from, tc.to, tc.maxRange)
			if len(got) != tc.want {
				t.Fatalf("got %d windows %v, want %d", len(got), got, tc.want)
			}
			if got[0][0] != tc.from.UTC() {
				t.Fatalf("first window starts at %s, want %s", got[0][0], tc.from)
			}
			for i, w := range got {
				if tc.maxRange > 0 && w[1].Sub(w[0]) > tc.maxRange {
					t.Fatalf("window %d spans %s, want at most %s", i, w[1].Sub(w[0]), tc.maxRange)
				}
				if i > 0 && !got[i-1][1].Equal(w[0]) {
					t.Fatalf("window %d starts at %s but window %d ends at %s", i, w[0], i-1, got[i-1][1])
				}
			}
			if last := got[len(got)-1][1]; !last.Equal(tc.to.UTC()) {
				t.Fatalf("the last window ends at %s, want %s", last, tc.to)
			}
		})
	}
}

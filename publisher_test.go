package baselines

import (
	"context"
	"testing"
	"time"
)

type fakeStore struct {
	spans  []metricSpan
	series map[string][]seriesPoint
}

func (f *fakeStore) Hashes(context.Context) ([]metricSpan, error) {
	return f.spans, nil
}

func (f *fakeStore) Series(_ context.Context, hash string, from time.Time) ([]seriesPoint, error) {
	src := f.series[hash]
	out := make([]seriesPoint, 0, len(src))
	for _, p := range src {
		if !p.Time.Before(from) {
			out = append(out, p)
		}
	}
	return out, nil
}

type fakeSink struct {
	msgs []BaselineMessage
}

func (f *fakeSink) Publish(_ context.Context, msg BaselineMessage) error {
	f.msgs = append(f.msgs, msg)
	return nil
}

func (f *fakeSink) Close() error { return nil }

func minutePoints(start time.Time, n int, val func(i int) float64) []seriesPoint {
	out := make([]seriesPoint, n)
	for i := range n {
		out[i] = seriesPoint{Time: start.Add(time.Duration(i) * time.Minute), Value: val(i)}
	}
	return out
}

func TestPublisherTick(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lookback := 2 * time.Hour
	ready := minutePoints(start, 180, func(i int) float64 { return float64(10 + i%20) })
	short := minutePoints(start.Add(90*time.Minute), 30, func(i int) float64 { return 1 })
	hourly := []seriesPoint{
		{Time: start, Value: 1},
		{Time: start.Add(time.Hour), Value: 2},
		{Time: start.Add(2 * time.Hour), Value: 3},
	}

	for _, tc := range []struct {
		name  string
		ahead int
		store *fakeStore
		want  int
		hash  string
		ts    time.Time
	}{
		{
			name:  "skip short span",
			ahead: 1,
			store: &fakeStore{
				spans: []metricSpan{{
					Hash: "short",
					Min:  short[0].Time,
					Max:  short[len(short)-1].Time,
				}},
				series: map[string][]seriesPoint{"short": short},
			},
			want: 0,
		},
		{
			name:  "skip hourly step",
			ahead: 1,
			store: &fakeStore{
				spans: []metricSpan{{
					Hash: "hour",
					Min:  hourly[0].Time,
					Max:  hourly[len(hourly)-1].Time,
				}},
				series: map[string][]seriesPoint{"hour": hourly},
			},
			want: 0,
		},
		{
			name:  "one message at last+1m",
			ahead: 1,
			store: &fakeStore{
				spans: []metricSpan{{
					Hash: "ready",
					Min:  ready[0].Time,
					Max:  ready[len(ready)-1].Time,
				}},
				series: map[string][]seriesPoint{"ready": ready},
			},
			want: 1,
			hash: "ready",
			ts:   ready[len(ready)-1].Time.Add(time.Minute),
		},
		{
			name:  "one message at last+N minutes",
			ahead: 3,
			store: &fakeStore{
				spans: []metricSpan{{
					Hash: "ready",
					Min:  ready[0].Time,
					Max:  ready[len(ready)-1].Time,
				}},
				series: map[string][]seriesPoint{"ready": ready},
			},
			want: 1,
			hash: "ready",
			ts:   ready[len(ready)-1].Time.Add(3 * time.Minute),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &fakeSink{}
			p := newPublisher(Config{
				Lookback:     lookback,
				AheadMinutes: tc.ahead,
				Interval:     time.Minute,
			}, tc.store, sink, nil)
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
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ready := minutePoints(start, 180, func(i int) float64 { return float64(i) })
	store := &fakeStore{
		spans: []metricSpan{{
			Hash: "ready",
			Min:  ready[0].Time,
			Max:  ready[len(ready)-1].Time,
		}},
		series: map[string][]seriesPoint{"ready": ready},
	}
	sink := &fakeSink{}
	p := newPublisher(Config{
		Lookback:     2 * time.Hour,
		AheadMinutes: 1,
		Interval:     time.Minute,
	}, store, sink, nil)
	p.tick(context.Background())
	p.tick(context.Background())
	if len(sink.msgs) != 1 {
		t.Fatalf("got %d msgs, want 1 (second tick is a duplicate ts)", len(sink.msgs))
	}
}

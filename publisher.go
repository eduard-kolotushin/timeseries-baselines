package baselines

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/eduard-kolotushin/timeseries"
	forecast "github.com/eduard-kolotushin/timeseries-forecast"
)

// Publisher scans Druid and writes one baseline point per ready hash per tick.
type Publisher struct {
	cfg       Config
	src       metricReader
	sink      baselineSink
	cal       *forecast.Calendar
	published map[string]int64
}

func newPublisher(cfg Config, src metricReader, sink baselineSink, cal *forecast.Calendar) *Publisher {
	return &Publisher{
		cfg:       cfg,
		src:       src,
		sink:      sink,
		cal:       cal,
		published: make(map[string]int64),
	}
}

// NewPublisher wires Druid and Kafka from cfg.
func NewPublisher(cfg Config, cal *forecast.Calendar) *Publisher {
	return newPublisher(cfg, newDruidStore(cfg.DruidBroker, cfg.DruidDatasource, nil), newKafkaSink(cfg.KafkaBrokers, cfg.KafkaTopic), cal)
}

// Close the Kafka writer.
func (p *Publisher) Close() error {
	if p == nil || p.sink == nil {
		return nil
	}
	return p.sink.Close()
}

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
	if err := ctx.Err(); err != nil {
		return
	}
	spans, err := p.src.Hashes(ctx)
	if err != nil {
		slog.Error("list hashes", "err", err)
		return
	}
	for _, span := range spans {
		if err := ctx.Err(); err != nil {
			return
		}
		if err := p.publishHash(ctx, span); err != nil {
			slog.Error("metric", "metric_hash", span.Hash, "err", err)
		}
	}
}

func (p *Publisher) publishHash(ctx context.Context, span metricSpan) error {
	if span.Max.Sub(span.Min) < p.cfg.Lookback {
		return nil
	}
	from := span.Max.Add(-p.cfg.Lookback)
	pts, err := p.src.Series(ctx, span.Hash, from)
	if err != nil {
		return err
	}
	if len(pts) < 2 {
		return nil
	}
	step := pts[len(pts)-1].Time.Sub(pts[len(pts)-2].Time)
	if step != time.Minute {
		return nil
	}
	times := make([]time.Time, len(pts))
	values := make([]float64, len(pts))
	for i, pt := range pts {
		times[i] = pt.Time
		values[i] = pt.Value
	}
	s, err := timeseries.New(times, values)
	if err != nil {
		return err
	}
	fitted, err := forecast.FitSeasonalBaseline(s, forecast.SeasonMinuteOfWeek, p.cal)
	if err != nil {
		return err
	}
	out, err := fitted.Forecast(p.cfg.AheadMinutes)
	if err != nil {
		return err
	}
	if out.Len() == 0 {
		return nil
	}
	i := out.Len() - 1
	ts := out.Times()[i]
	v := out.Values()[i]
	if math.IsNaN(v) {
		return nil
	}
	ms := ts.UTC().UnixMilli()
	if prev, ok := p.published[span.Hash]; ok && ms <= prev {
		return nil
	}
	msg := BaselineMessage{
		MetricHash:    span.Hash,
		MetricTS:      ms,
		BaselineValue: v,
	}
	if err := p.sink.Publish(ctx, msg); err != nil {
		return err
	}
	p.published[span.Hash] = ms
	return nil
}

package baselines

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type metricSpan struct {
	Hash string
	Min  time.Time
	Max  time.Time
}

type seriesPoint struct {
	Time  time.Time
	Value float64
}

type metricReader interface {
	Hashes(ctx context.Context) ([]metricSpan, error)
	Series(ctx context.Context, hash string, from time.Time) ([]seriesPoint, error)
}

type druidStore struct {
	broker     string
	datasource string
	client     *http.Client
}

func newDruidStore(broker, datasource string, client *http.Client) *druidStore {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &druidStore{
		broker:     strings.TrimRight(broker, "/"),
		datasource: datasource,
		client:     client,
	}
}

func (d *druidStore) Hashes(ctx context.Context) ([]metricSpan, error) {
	q := fmt.Sprintf(
		`SELECT metric_hash, MIN(__time) AS tmin, MAX(__time) AS tmax FROM %s GROUP BY 1`,
		d.datasource,
	)
	rows, err := d.sql(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]metricSpan, 0, len(rows))
	for _, row := range rows {
		hash := asString(row["metric_hash"])
		minT, err := parseDruidTime(row["tmin"])
		if err != nil || hash == "" {
			continue
		}
		maxT, err := parseDruidTime(row["tmax"])
		if err != nil {
			continue
		}
		out = append(out, metricSpan{Hash: hash, Min: minT, Max: maxT})
	}
	return out, nil
}

func (d *druidStore) Series(ctx context.Context, hash string, from time.Time) ([]seriesPoint, error) {
	esc := strings.ReplaceAll(hash, `'`, `''`)
	fromMs := from.UTC().UnixMilli()
	countQ := fmt.Sprintf(
		`SELECT COUNT(*) AS c FROM %s WHERE metric_hash = '%s' AND __time >= MILLIS_TO_TIMESTAMP(%d)`,
		d.datasource, esc, fromMs,
	)
	countRows, err := d.sql(ctx, countQ)
	if err != nil {
		return nil, err
	}
	n := 0
	if len(countRows) > 0 {
		n = asInt(countRows[0]["c"], 0)
	}
	if n <= 0 {
		return nil, nil
	}
	q := fmt.Sprintf(
		`SELECT __time, metric_value FROM %s WHERE metric_hash = '%s' AND __time >= MILLIS_TO_TIMESTAMP(%d) ORDER BY __time`,
		d.datasource, esc, fromMs,
	)
	rows, err := d.sql(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]seriesPoint, 0, n)
	for _, row := range rows {
		t, err := parseDruidTime(row["__time"])
		if err != nil {
			continue
		}
		out = append(out, seriesPoint{Time: t, Value: asFloat(row["metric_value"])})
	}
	return out, nil
}

func (d *druidStore) sql(ctx context.Context, query string) ([]map[string]any, error) {
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.broker+"/druid/v2/sql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("druid sql: %s: %s", resp.Status, strings.TrimSpace(string(raw)))
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("druid sql decode: %w", err)
	}
	return rows, nil
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

func asInt(v any, def int) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return def
		}
		return int(i)
	case string:
		i, err := strconv.Atoi(strings.TrimSpace(x))
		if err != nil {
			return def
		}
		return i
	default:
		return def
	}
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

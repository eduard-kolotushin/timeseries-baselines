package baselines

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDruidStoreSQL(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.HandleFunc("/druid/v2/sql", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Query string `json:"query"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(body.Query, "GROUP BY"):
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"metric_hash": "ready", "tmin": "2026-01-01T00:00:00.000Z", "tmax": "2026-01-15T00:00:00.000Z"},
			})
		case strings.Contains(body.Query, "COUNT(*)"):
			_ = json.NewEncoder(w).Encode([]map[string]any{{"c": 2}})
		default:
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"__time": "2026-01-14T23:58:00.000Z", "metric_value": 1.5},
				{"__time": "2026-01-14T23:59:00.000Z", "metric_value": 2.5},
			})
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	store := newDruidStore(srv.URL, "metrics", srv.Client())
	spans, err := store.Hashes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 1 || spans[0].Hash != "ready" {
		t.Fatalf("spans=%v", spans)
	}
	pts, err := store.Series(context.Background(), "ready", time.Date(2026, 1, 14, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(pts) != 2 || pts[1].Value != 2.5 {
		t.Fatalf("series=%v", pts)
	}
}

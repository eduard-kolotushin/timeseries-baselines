package baselines

import (
	"encoding/json"
	"testing"
)

func TestMessageKeyIsUniquePerPoint(t *testing.T) {
	t.Parallel()
	base := BaselineMessage{MetricHash: "ready", MetricTS: 1767236400000}
	for _, tc := range []struct {
		name string
		a    BaselineMessage
		b    BaselineMessage
		same bool
	}{
		{"same point", base, base, true},
		{"different minute", base, BaselineMessage{MetricHash: "ready", MetricTS: 1767236460000}, false},
		{"different hash", base, BaselineMessage{MetricHash: "short", MetricTS: 1767236400000}, false},
		{"separator is not ambiguous", base,
			BaselineMessage{MetricHash: "re|ady", MetricTS: 1767236400000}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, want := string(messageKey(tc.a)), string(messageKey(tc.b))
			if same := got == want; same != tc.same {
				t.Fatalf("keys equal=%v want %v: %q vs %q", same, tc.same, got, want)
			}
		})
	}
}

// The value on the wire is what the Druid supervisor reads; keep it pinned.
func TestBaselineMessageJSON(t *testing.T) {
	t.Parallel()
	got, err := json.Marshal(BaselineMessage{
		MetricHash:    "ready",
		MetricTS:      1767236400000,
		BaselineValue: 19.578512396694215,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"metric_hash":"ready","metric_ts":1767236400000,"baseline_value":19.578512396694215}`
	if string(got) != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

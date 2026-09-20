package baselines

import (
	"context"
	"math"
	"testing"
	"time"
)

// DRUID_MAX_RPS is operator input and nothing rejects an absurd value: above 1e9 the
// period truncates to zero, and time.NewTicker panics on a non-positive duration. The
// limiter has to clamp so a typo costs a slow tick instead of the whole worker.
func TestRateLimiterClampsUnrepresentablePeriod(t *testing.T) {
	tests := []struct {
		name string
		rps  int
		off  bool
	}{
		{"negative is off", -1, true},
		{"zero is off", 0, true},
		{"one per second", 1, false},
		{"configured default", 4, false},
		{"exactly a nanosecond", 1_000_000_000, false},
		{"above a nanosecond", 2_000_000_000, false},
		{"max int", math.MaxInt, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			limiter := newRateLimiter(tc.rps)
			if tc.off != (limiter == nil) {
				t.Fatalf("rps=%d produced limiter=%v, want the limit %s", tc.rps, limiter, map[bool]string{true: "off", false: "on"}[tc.off])
			}
			// A usable limiter hands out a token without the caller cancelling: that is
			// what the clamp is for, and the unclamped period panics before this point.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := limiter.wait(ctx); err != nil {
				t.Fatalf("rps=%d: wait=%v", tc.rps, err)
			}
		})
	}
}

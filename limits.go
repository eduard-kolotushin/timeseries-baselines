package baselines

import (
	"context"
	"time"
)

// semaphore bounds concurrent Druid requests. acquire returns the release for a
// slot, or the context error when the caller gives up waiting. A nil receiver
// means the limit is off.
type semaphore struct{ ch chan struct{} }

func newSemaphore(n int) *semaphore {
	if n < 1 {
		n = 1
	}
	return &semaphore{ch: make(chan struct{}, n)}
}

func (s *semaphore) acquire(ctx context.Context) (func(), error) {
	if s == nil {
		return func() {}, nil
	}
	select {
	case s.ch <- struct{}{}:
		return func() { <-s.ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// rateLimiter spaces outbound Druid requests: one token per 1/DRUID_MAX_RPS, so
// a burst of sliced windows reaches the datasource at the configured rate
// instead of at the speed of the network. Tokens are not banked — the ticker's
// buffered channel is the whole queue — which is what makes the limit hold for
// a scan and a retrain burst that start at the same moment. A nil receiver
// means the limit is off.
type rateLimiter struct{ tick *time.Ticker }

func newRateLimiter(rps int) *rateLimiter {
	if rps <= 0 {
		return nil
	}
	// DRUID_MAX_RPS above 1e9 truncates this period to zero and time.NewTicker
	// panics on a non-positive duration, so a typo in the environment would take
	// the worker down at startup. Below a nanosecond the limiter cannot space
	// anything anyway, so clamp instead.
	period := time.Second / time.Duration(rps)
	if period < time.Nanosecond {
		period = time.Nanosecond
	}
	return &rateLimiter{tick: time.NewTicker(period)}
}

// wait blocks until a token is available or ctx is done.
func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil || l.tick == nil {
		return nil
	}
	select {
	case <-l.tick.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

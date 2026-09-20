package baselines

import (
	"strings"
	"testing"
	"time"
)

func TestNextRun(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		cron string
		tz   string
		now  time.Time
		want time.Time
	}{
		{
			name: "daily descriptor",
			cron: "0 3 * * *",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 16, 3, 0, 0, 0, time.UTC),
		},
		{
			name: "the next five-minute mark",
			cron: "*/5 * * * *",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 15, 12, 5, 0, 0, time.UTC),
		},
		{
			name: "sub-minute now still lands on the next mark",
			cron: "*/5 * * * *",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 4, 59, 0, time.UTC),
			want: time.Date(2026, 1, 15, 12, 5, 0, 0, time.UTC),
		},
		{
			name: "daily in a timezone behind UTC",
			cron: "@daily",
			tz:   "America/New_York",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 16, 5, 0, 0, 0, time.UTC),
		},
		{
			name: "daily in a timezone ahead of UTC",
			cron: "@daily",
			tz:   "Europe/Moscow",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 15, 21, 0, 0, 0, time.UTC),
		},
		{
			name: "cron wall clock follows the timezone",
			cron: "0 3 * * *",
			tz:   "Europe/Moscow",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name: "hourly descriptor",
			cron: "@hourly",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 0, 30, 0, time.UTC),
			want: time.Date(2026, 1, 15, 13, 0, 0, 0, time.UTC),
		},
		{
			name: "every descriptor",
			cron: "@every 1h30m",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2026, 1, 15, 13, 30, 0, 0, time.UTC),
		},
		{
			name: "yearly",
			cron: "0 0 1 1 *",
			tz:   "UTC",
			now:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
			want: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := nextRun(tc.cron, tc.tz, tc.now)
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("nextRun(%q, %q, %s) = %s, want %s", tc.cron, tc.tz, tc.now, got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("next run is in %s, want UTC", got.Location())
			}
		})
	}
}

func TestNextRunErrors(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		cron string
		tz   string
		want string
	}{
		{name: "not a cron", cron: "every day", tz: "UTC", want: "cron"},
		{name: "unknown timezone", cron: "@daily", tz: "Mars/Olympus", want: "timezone"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := nextRun(tc.cron, tc.tz, now)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %q", err, tc.want)
			}
		})
	}
}

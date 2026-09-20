package baselines

import (
	"fmt"
	"time"

	cronv3 "github.com/robfig/cron/v3"
)

// nextRun is the next fire time of a cron expression in tz, in UTC. The worker
// and the plugin both write next_run_at, so they have to agree: 5-field
// descriptors plus @daily/@hourly/@every, evaluated in the row's timezone so a
// DST shift does not drift the schedule.
func nextRun(cron, tz string, now time.Time) (time.Time, error) {
	sched, err := cronv3.ParseStandard(cron)
	if err != nil {
		return time.Time{}, fmt.Errorf("cron %q: %w", cron, err)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, fmt.Errorf("timezone %q: %w", tz, err)
	}
	return sched.Next(now.UTC().In(loc)).UTC(), nil
}

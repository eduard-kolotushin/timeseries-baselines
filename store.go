package baselines

import (
	"context"
	"time"

	forecast "github.com/eduard-kolotushin/timeseries-forecast"
)

// The model spec the worker and the plugin agree on: the worker stores it with
// every snapshot, and the plugin writes it as a baseline row's spec.
const (
	modelBaseline    = "baseline"
	seasonMinuteWeek = "minute-week"
)

// snapshotStore keeps the fitted models. Fresh answers which of keys have moved
// since the caller last looked, so the publish path restores only what changed
// instead of every snapshot on every tick.
type snapshotStore interface {
	Fresh(ctx context.Context, keys []string) (map[string]time.Time, error)
	Get(ctx context.Context, key string) (forecast.Snapshot, bool, error)
	Put(ctx context.Context, key string, rec snapshotRecord, snap forecast.Snapshot) error
}

// retrainQueue distributes scheduled retrains. Claims are fleet-wide and not
// restricted to a worker's own hashes, so a schedule runs even when the hash's
// rendezvous owner is down.
//
// Schedule takes the whole owned set at once: one statement per tick instead of
// one round trip per hash. Done carries the owner of the claim it finishes,
// because a lease that expired mid-retrain can hand the row to another worker,
// whose schedule a stale finish must not overwrite.
type retrainQueue interface {
	Schedule(ctx context.Context, keys []string, cron, tz string) error
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]retrainClaim, error)
	Done(ctx context.Context, owner string, orgID int64, key string, next time.Time, status string) error
}

// membership is the Postgres heartbeat that replaces static peer discovery on
// VMs: the peer set is the ids seen within ttl.
type membership interface {
	Heartbeat(ctx context.Context, id string, owned, peers int) error
	Peers(ctx context.Context, ttl time.Duration) ([]string, error)
}

// storeBackend is the whole Postgres-backed state of a worker: snapshots,
// retrain queue and membership table behind one pool.
type storeBackend interface {
	snapshotStore
	retrainQueue
	membership
	Close()
}

// snapshotRecord is the metadata of one trained model, kept in columns so a
// reader can tell what a snapshot holds without inflating it.
type snapshotRecord struct {
	Model     string
	Season    string
	Calendar  string
	Lookback  time.Duration
	TrainedAt time.Time
}

// retrainClaim is one due schedule row this worker now owns. OrgID is carried so
// Done can address the row by its full key: forecast.retrain is keyed
// (scope, org_id, key), and the worker's own rows are the fleet-wide org 0.
type retrainClaim struct {
	OrgID    int64
	Key      string
	Cron     string
	Timezone string
}

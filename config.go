package baselines

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	cronv3 "github.com/robfig/cron/v3"
)

const (
	defaultLookback         = 336 * time.Hour
	defaultAheadMinutes     = 1
	defaultInterval         = time.Minute
	maxAheadMinutes         = 10080
	defaultDatasource       = "metrics"
	defaultTopic            = "baselines"
	defaultDruidTimeout     = 60 * time.Second
	defaultDruidRetries     = 2
	defaultDruidRPS         = 4
	defaultDruidInflight    = 4
	defaultHashScanTTL      = 5 * time.Minute
	defaultTrainConcurrency = 2
	defaultSnapshotCacheTTL = 60 * time.Second
	defaultWorkerTTL        = 30 * time.Second
	defaultRetrainCron      = "0 3 * * *"
	defaultRetrainRetry     = 5 * time.Minute
)

var datasourceName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// Config is process env for the Druid → Kafka baseline loop.
type Config struct {
	DruidBroker     string
	DruidDatasource string
	KafkaBrokers    []string
	KafkaTopic      string
	Lookback        time.Duration
	AheadMinutes    int
	Interval        time.Duration
	Calendar        string

	// Druid access bounds. The production datasource caps how far a single
	// request may reach, so every scan and train query is sliced into windows of
	// at most DruidMaxRange (0 = one request per query); MaxRPS and MaxInflight
	// cap the request rate and the requests in flight, so a retrain burst cannot
	// overwhelm it either.
	DruidMaxRange    time.Duration
	DruidTimeout     time.Duration
	DruidRetries     int
	DruidMaxRPS      int
	DruidMaxInflight int
	DruidAuthHeader  string
	DruidAuthValue   string

	// HashScanTTL caches the metric_hash scan across ticks. ScanRange bounds it;
	// a bounded window reports Min as the window start, so a hash that stopped
	// reporting before now-ScanRange drops out of the scan (see druidStore.Hashes).
	HashScanTTL time.Duration
	ScanRange   time.Duration

	// TrainConcurrency is the retrain claim limit and the number of fits in
	// flight. SnapshotCacheTTL throttles the per-tick snapshot freshness query;
	// RetrainRetry is both the claim lease and the delay before a failed retrain
	// is due again. DefaultRetrainCron is the cron a hash is scheduled with the
	// first time it is seen.
	TrainConcurrency   int
	SnapshotCacheTTL   time.Duration
	RetrainRetry       time.Duration
	WorkerTTL          time.Duration
	DefaultRetrainCron string

	// StoreDSN is the Postgres snapshot/schedule/membership store. Empty leaves
	// the process stateless, refitting every tick and sharding statically.
	StoreDSN string

	// Sharding. Empty ShardPeers and ShardDNS mean one worker owns every hash.
	// ShardID defaults to the first non-loopback IP (ShardID in shard.go).
	// ShardMembership selects the source: auto resolves to peers, dns, store or
	// self, in that order of specificity (MembershipMode in membership.go).
	ShardID         string
	ShardPeers      []string
	ShardDNS        string
	ShardMembership string

	LogLevel string
}

// ConfigFromEnv reads DRUID_*, KAFKA_*, LOOKBACK, AHEAD_MINUTES, INTERVAL,
// CALENDAR, SHARD_*, LOG_LEVEL and the BASELINE_STORE_* DSN.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		DruidBroker:        strings.TrimSpace(os.Getenv("DRUID_BROKER")),
		DruidDatasource:    strings.TrimSpace(os.Getenv("DRUID_DATASOURCE")),
		KafkaBrokers:       splitList(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:         strings.TrimSpace(os.Getenv("KAFKA_TOPIC")),
		Lookback:           defaultLookback,
		AheadMinutes:       defaultAheadMinutes,
		Interval:           defaultInterval,
		Calendar:           strings.TrimSpace(os.Getenv("CALENDAR")),
		DruidTimeout:       defaultDruidTimeout,
		DruidRetries:       defaultDruidRetries,
		DruidMaxRPS:        defaultDruidRPS,
		DruidMaxInflight:   defaultDruidInflight,
		DruidAuthHeader:    strings.TrimSpace(os.Getenv("DRUID_AUTH_HEADER")),
		DruidAuthValue:     strings.TrimSpace(os.Getenv("DRUID_AUTH_VALUE")),
		HashScanTTL:        defaultHashScanTTL,
		TrainConcurrency:   defaultTrainConcurrency,
		SnapshotCacheTTL:   defaultSnapshotCacheTTL,
		DefaultRetrainCron: defaultRetrainCron,
		RetrainRetry:       defaultRetrainRetry,
		ShardID:            strings.TrimSpace(os.Getenv("SHARD_ID")),
		ShardPeers:         splitList(os.Getenv("SHARD_PEERS")),
		ShardDNS:           strings.TrimSpace(os.Getenv("SHARD_DNS")),
		ShardMembership:    strings.TrimSpace(os.Getenv("SHARD_MEMBERSHIP")),
		LogLevel:           "info",
		StoreDSN:           storeDSN(os.Getenv),
	}
	if cfg.DruidDatasource == "" {
		cfg.DruidDatasource = defaultDatasource
	}
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = defaultTopic
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv("LOG_LEVEL"))); v != "" {
		cfg.LogLevel = v
	}
	if v := strings.TrimSpace(os.Getenv("DEFAULT_RETRAIN_CRON")); v != "" {
		cfg.DefaultRetrainCron = v
	}

	durations := []struct {
		name string
		dst  *time.Duration
	}{
		{"LOOKBACK", &cfg.Lookback},
		{"INTERVAL", &cfg.Interval},
		{"DRUID_MAX_RANGE", &cfg.DruidMaxRange},
		{"DRUID_TIMEOUT", &cfg.DruidTimeout},
		{"HASH_SCAN_TTL", &cfg.HashScanTTL},
		{"SCAN_RANGE", &cfg.ScanRange},
		{"SNAPSHOT_CACHE_TTL", &cfg.SnapshotCacheTTL},
		{"WORKER_TTL", &cfg.WorkerTTL},
		{"RETRAIN_RETRY", &cfg.RetrainRetry},
	}
	for _, d := range durations {
		v, ok, err := envDuration(d.name)
		if err != nil {
			return Config{}, err
		}
		if ok {
			*d.dst = v
		}
	}

	numbers := []struct {
		name string
		dst  *int
	}{
		{"AHEAD_MINUTES", &cfg.AheadMinutes},
		{"DRUID_RETRIES", &cfg.DruidRetries},
		{"DRUID_MAX_RPS", &cfg.DruidMaxRPS},
		{"DRUID_MAX_INFLIGHT", &cfg.DruidMaxInflight},
		{"TRAIN_CONCURRENCY", &cfg.TrainConcurrency},
	}
	for _, n := range numbers {
		v, ok, err := envInt(n.name)
		if err != nil {
			return Config{}, err
		}
		if ok {
			*n.dst = v
		}
	}
	return cfg, nil
}

// Validate checks required fields and ranges. Optional fields left at their zero
// value are checked against the default they will actually run with, so an
// omission is never mistaken for an invalid value.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DruidBroker) == "" {
		return fmt.Errorf("DRUID_BROKER is required")
	}
	u, err := url.Parse(c.DruidBroker)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("DRUID_BROKER must be an absolute URL")
	}
	if !datasourceName.MatchString(c.DruidDatasource) {
		return fmt.Errorf("DRUID_DATASOURCE must be [A-Za-z0-9_]+")
	}
	if len(c.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if c.KafkaTopic == "" {
		return fmt.Errorf("KAFKA_TOPIC is required")
	}
	if c.Lookback < time.Minute {
		return fmt.Errorf("LOOKBACK must be at least 1m")
	}
	if c.AheadMinutes < 1 || c.AheadMinutes > maxAheadMinutes {
		return fmt.Errorf("AHEAD_MINUTES must be in 1..%d", maxAheadMinutes)
	}
	if c.Interval < time.Second {
		return fmt.Errorf("INTERVAL must be at least 1s")
	}
	if c.DruidMaxRange < 0 {
		return fmt.Errorf("DRUID_MAX_RANGE must not be negative")
	}
	if c.DruidRetries < 0 {
		return fmt.Errorf("DRUID_RETRIES must not be negative")
	}
	if c.DruidMaxRPS < 0 {
		return fmt.Errorf("DRUID_MAX_RPS must not be negative")
	}
	if c.ScanRange > 0 && c.ScanRange < c.Lookback {
		// A scan window shorter than LOOKBACK hides hashes that are still
		// trainable, because the bounded scan clamps a hash's Min to the window
		// start and such a hash then looks ineligible.
		return fmt.Errorf("SCAN_RANGE must be at least LOOKBACK")
	}
	if c.TrainConcurrency < 1 {
		return fmt.Errorf("TRAIN_CONCURRENCY must be at least 1")
	}
	ttl := c.WorkerTTL
	if ttl <= 0 {
		ttl = max(defaultWorkerTTL, 2*c.Interval)
	}
	if ttl <= c.Interval {
		// Otherwise a live worker's own heartbeat has already expired when the
		// next tick reads the peer set, and with SHARD_MEMBERSHIP=store the view
		// falls back to self-only (or, worse, keeps a departed peer alive).
		return fmt.Errorf("WORKER_TTL must be longer than INTERVAL")
	}
	if c.DefaultRetrainCron != "" {
		if _, err := cronv3.ParseStandard(c.DefaultRetrainCron); err != nil {
			return fmt.Errorf("DEFAULT_RETRAIN_CRON: %w", err)
		}
	}
	if c.LogLevel != "" && c.LogLevel != "info" && c.LogLevel != "debug" {
		return fmt.Errorf("LOG_LEVEL must be info or debug")
	}
	if len(c.ShardPeers) > 0 && c.ShardDNS != "" {
		return fmt.Errorf("set either SHARD_PEERS or SHARD_DNS, not both")
	}
	switch c.ShardMembership {
	case "", "auto", "peers", "dns":
	case "store":
		if c.StoreDSN == "" {
			return fmt.Errorf("SHARD_MEMBERSHIP=store requires BASELINE_STORE_HOST or BASELINE_STORE_URL")
		}
	default:
		return fmt.Errorf("SHARD_MEMBERSHIP must be auto, peers, dns or store")
	}
	return nil
}

// SlogLevel maps LOG_LEVEL to the process log level; anything but "debug" is info.
func (c Config) SlogLevel() slog.Level {
	if c.LogLevel == "debug" {
		return slog.LevelDebug
	}
	return slog.LevelInfo
}

// normalized fills the fields where an unset value has a default, so a Config
// built in code behaves like one read from the environment. Fields where zero is
// a real setting (DRUID_MAX_RANGE, DRUID_RETRIES, DRUID_MAX_RPS, LOG_LEVEL) are
// left alone.
func (c Config) normalized() Config {
	if c.DruidTimeout <= 0 {
		c.DruidTimeout = defaultDruidTimeout
	}
	if c.DruidMaxInflight <= 0 {
		c.DruidMaxInflight = defaultDruidInflight
	}
	if c.HashScanTTL <= 0 {
		c.HashScanTTL = defaultHashScanTTL
	}
	if c.ScanRange <= 0 {
		// Two lookbacks cover a series that skipped a scrape; 24h is the floor so
		// a short LOOKBACK does not make the scan window the bottleneck.
		c.ScanRange = max(2*c.Lookback, 24*time.Hour)
	}
	if c.TrainConcurrency <= 0 {
		c.TrainConcurrency = defaultTrainConcurrency
	}
	if c.SnapshotCacheTTL <= 0 {
		c.SnapshotCacheTTL = defaultSnapshotCacheTTL
	}
	if c.WorkerTTL <= 0 {
		// A heartbeat is written once per tick and read at the start of the next
		// one, so the TTL must outlast INTERVAL or a worker's own row expires
		// before it is read and the store view collapses to "nobody is live".
		// Two intervals leave room for tick jitter.
		c.WorkerTTL = max(defaultWorkerTTL, 2*c.Interval)
	}
	if c.RetrainRetry <= 0 {
		c.RetrainRetry = defaultRetrainRetry
	}
	if c.DefaultRetrainCron == "" {
		c.DefaultRetrainCron = defaultRetrainCron
	}
	return c
}

// envDuration parses name as a Go duration; ok is false when it is unset.
func envDuration(name string) (time.Duration, bool, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %w", name, err)
	}
	return d, true, nil
}

// envInt parses name as a base-10 integer; ok is false when it is unset.
func envInt(name string) (int, bool, error) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return 0, false, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, false, fmt.Errorf("%s: %w", name, err)
	}
	return n, true, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

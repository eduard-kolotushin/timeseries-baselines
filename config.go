package baselines

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	defaultLookback     = 336 * time.Hour
	defaultAheadMinutes = 1
	defaultInterval     = time.Minute
	maxAheadMinutes     = 10080
	defaultDatasource   = "metrics"
	defaultTopic        = "baselines"
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

	// Sharding. Empty ShardPeers and ShardDNS mean one worker owns every hash.
	// ShardID defaults to the first non-loopback IP (ShardID in shard.go).
	ShardID    string
	ShardPeers []string
	ShardDNS   string
}

// ConfigFromEnv reads DRUID_*, KAFKA_*, LOOKBACK, AHEAD_MINUTES, INTERVAL, CALENDAR, SHARD_*.
func ConfigFromEnv() (Config, error) {
	cfg := Config{
		DruidBroker:     strings.TrimSpace(os.Getenv("DRUID_BROKER")),
		DruidDatasource: strings.TrimSpace(os.Getenv("DRUID_DATASOURCE")),
		KafkaBrokers:    splitList(os.Getenv("KAFKA_BROKERS")),
		KafkaTopic:      strings.TrimSpace(os.Getenv("KAFKA_TOPIC")),
		Lookback:        defaultLookback,
		AheadMinutes:    defaultAheadMinutes,
		Interval:        defaultInterval,
		Calendar:        strings.TrimSpace(os.Getenv("CALENDAR")),
		ShardID:         strings.TrimSpace(os.Getenv("SHARD_ID")),
		ShardPeers:      splitList(os.Getenv("SHARD_PEERS")),
		ShardDNS:        strings.TrimSpace(os.Getenv("SHARD_DNS")),
	}
	if cfg.DruidDatasource == "" {
		cfg.DruidDatasource = defaultDatasource
	}
	if cfg.KafkaTopic == "" {
		cfg.KafkaTopic = defaultTopic
	}
	if v := strings.TrimSpace(os.Getenv("LOOKBACK")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("LOOKBACK: %w", err)
		}
		cfg.Lookback = d
	}
	if v := strings.TrimSpace(os.Getenv("INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("INTERVAL: %w", err)
		}
		cfg.Interval = d
	}
	if v := strings.TrimSpace(os.Getenv("AHEAD_MINUTES")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("AHEAD_MINUTES: %w", err)
		}
		cfg.AheadMinutes = n
	}
	return cfg, nil
}

// Validate checks required fields and ranges.
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
	if len(c.ShardPeers) > 0 && c.ShardDNS != "" {
		return fmt.Errorf("set either SHARD_PEERS or SHARD_DNS, not both")
	}
	return nil
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

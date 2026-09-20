package baselines

import (
	"os"
	"strings"
	"testing"
	"time"
)

// clearStoreEnv makes the store tests independent of the developer's machine.
func clearStoreEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"BASELINE_STORE_URL", "BASELINE_STORE_HOST", "BASELINE_STORE_PORT", "BASELINE_STORE_DATABASE",
		"BASELINE_STORE_USER", "BASELINE_STORE_PASSWORD", "BASELINE_STORE_SSLMODE",
		"FORECAST_STORE_URL", "FORECAST_STORE_HOST", "FORECAST_STORE_PORT", "FORECAST_STORE_DATABASE",
		"FORECAST_STORE_USER", "FORECAST_STORE_PASSWORD", "FORECAST_STORE_SSLMODE",
	} {
		t.Setenv(key, "")
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("DRUID_DATASOURCE", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("LOOKBACK", "")
	t.Setenv("INTERVAL", "")
	t.Setenv("AHEAD_MINUTES", "")
	t.Setenv("CALENDAR", "")
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("SHARD_MEMBERSHIP", "")
	clearStoreEnv(t)
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DruidDatasource != defaultDatasource || cfg.KafkaTopic != defaultTopic {
		t.Fatalf("names: %+v", cfg)
	}
	if cfg.Lookback != defaultLookback || cfg.AheadMinutes != 1 || cfg.Interval != time.Minute {
		t.Fatalf("defaults: %+v", cfg)
	}
	if cfg.DruidTimeout != 60*time.Second || cfg.DruidRetries != 2 || cfg.DruidMaxRPS != 4 || cfg.DruidMaxInflight != 4 {
		t.Fatalf("druid defaults: %+v", cfg)
	}
	if cfg.DruidMaxRange != 0 || cfg.DruidAuthHeader != "" || cfg.DruidAuthValue != "" {
		t.Fatalf("druid bounds default to one unbounded request: %+v", cfg)
	}
	if cfg.HashScanTTL != 5*time.Minute || cfg.TrainConcurrency != 2 || cfg.SnapshotCacheTTL != time.Minute {
		t.Fatalf("retrain defaults: %+v", cfg)
	}
	if cfg.RetrainRetry != 5*time.Minute || cfg.DefaultRetrainCron != "0 3 * * *" {
		t.Fatalf("schedule defaults: %+v", cfg)
	}
	if cfg.LogLevel != "info" || cfg.SlogLevel().String() != "INFO" {
		t.Fatalf("log level: %q %s", cfg.LogLevel, cfg.SlogLevel())
	}
	if cfg.StoreDSN != "" {
		t.Fatalf("store DSN %q, want empty without a host", cfg.StoreDSN)
	}
	if cfg.ScanRange != 0 {
		t.Fatalf("SCAN_RANGE %s, want unset (resolved at use)", cfg.ScanRange)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	// The resolved defaults are what the publisher actually uses.
	got := cfg.normalized()
	if got.ScanRange != 2*defaultLookback {
		t.Fatalf("resolved SCAN_RANGE %s, want 2*LOOKBACK", got.ScanRange)
	}
	short := Config{Lookback: time.Hour}.normalized()
	if short.ScanRange != 24*time.Hour {
		t.Fatalf("resolved SCAN_RANGE %s for a 1h lookback, want the 24h floor", short.ScanRange)
	}
	// WORKER_TTL is left unset by ConfigFromEnv so the resolver can outlast the
	// tick: a heartbeat written at the end of one tick is read at the start of
	// the next, so a 30s TTL under a 1m INTERVAL would expire before it is seen.
	if got.WorkerTTL != 2*defaultInterval {
		t.Fatalf("resolved WORKER_TTL %s, want two ticks (%s)", got.WorkerTTL, 2*defaultInterval)
	}
	if fast := (Config{Interval: 10 * time.Second}).normalized(); fast.WorkerTTL != defaultWorkerTTL {
		t.Fatalf("resolved WORKER_TTL %s for a 10s interval, want the %s floor", fast.WorkerTTL, defaultWorkerTTL)
	}
}

func TestConfigFromEnvValues(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("DRUID_DATASOURCE", "metrics")
	t.Setenv("DRUID_MAX_RANGE", "24h")
	t.Setenv("DRUID_TIMEOUT", "30s")
	t.Setenv("DRUID_RETRIES", "5")
	t.Setenv("DRUID_MAX_RPS", "2")
	t.Setenv("DRUID_MAX_INFLIGHT", "3")
	t.Setenv("DRUID_AUTH_HEADER", "Authorization")
	t.Setenv("DRUID_AUTH_VALUE", "Bearer t")
	t.Setenv("KAFKA_BROKERS", "kafka:9092, kafka:9093")
	t.Setenv("KAFKA_TOPIC", "baselines")
	t.Setenv("LOOKBACK", "48h")
	t.Setenv("AHEAD_MINUTES", "3")
	t.Setenv("INTERVAL", "30s")
	t.Setenv("CALENDAR", "ru")
	t.Setenv("HASH_SCAN_TTL", "30m")
	t.Setenv("SCAN_RANGE", "96h")
	t.Setenv("TRAIN_CONCURRENCY", "4")
	t.Setenv("SNAPSHOT_CACHE_TTL", "10s")
	t.Setenv("WORKER_TTL", "2m")
	t.Setenv("DEFAULT_RETRAIN_CRON", "*/5 * * * *")
	t.Setenv("RETRAIN_RETRY", "90s")
	t.Setenv("SHARD_MEMBERSHIP", "store")
	t.Setenv("LOG_LEVEL", "DEBUG")
	clearStoreEnv(t)
	t.Setenv("BASELINE_STORE_HOST", "overlay-postgres")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DruidBroker != "http://druid-broker:8082" || cfg.DruidDatasource != "metrics" {
		t.Fatalf("core: %+v", cfg)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "kafka:9092" || cfg.KafkaTopic != "baselines" {
		t.Fatalf("kafka: %+v", cfg.KafkaBrokers)
	}
	if cfg.Lookback != 48*time.Hour || cfg.AheadMinutes != 3 || cfg.Interval != 30*time.Second || cfg.Calendar != "ru" {
		t.Fatalf("timing: %+v", cfg)
	}
	if cfg.DruidMaxRange != 24*time.Hour || cfg.DruidTimeout != 30*time.Second || cfg.DruidRetries != 5 {
		t.Fatalf("druid: %+v", cfg)
	}
	if cfg.DruidMaxRPS != 2 || cfg.DruidMaxInflight != 3 || cfg.DruidAuthHeader != "Authorization" || cfg.DruidAuthValue != "Bearer t" {
		t.Fatalf("druid limits: %+v", cfg)
	}
	if cfg.HashScanTTL != 30*time.Minute || cfg.ScanRange != 96*time.Hour {
		t.Fatalf("scan: %+v", cfg)
	}
	if cfg.TrainConcurrency != 4 || cfg.SnapshotCacheTTL != 10*time.Second || cfg.WorkerTTL != 2*time.Minute {
		t.Fatalf("retrain: %+v", cfg)
	}
	if cfg.DefaultRetrainCron != "*/5 * * * *" || cfg.RetrainRetry != 90*time.Second {
		t.Fatalf("schedule: %+v", cfg)
	}
	if cfg.ShardMembership != "store" || cfg.StoreDSN == "" {
		t.Fatalf("store membership: %+v", cfg)
	}
	if cfg.LogLevel != "debug" || cfg.SlogLevel().String() != "DEBUG" {
		t.Fatalf("LOG_LEVEL is case-insensitive: %q", cfg.LogLevel)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFromEnvErrors(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	clearStoreEnv(t)
	for _, tc := range []struct {
		key  string
		val  string
		want string
	}{
		{"DRUID_MAX_RANGE", "soon", "DRUID_MAX_RANGE"},
		{"SCAN_RANGE", "forever", "SCAN_RANGE"},
		{"TRAIN_CONCURRENCY", "many", "TRAIN_CONCURRENCY"},
		{"DRUID_RETRIES", "lots", "DRUID_RETRIES"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			t.Setenv(tc.key, tc.val)
			_, err := ConfigFromEnv()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error mentioning %s", err, tc.want)
			}
		})
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	ok := Config{
		DruidBroker:        "http://druid-broker:8082",
		DruidDatasource:    "metrics",
		KafkaBrokers:       []string{"kafka:9092"},
		KafkaTopic:         "baselines",
		Lookback:           time.Hour,
		AheadMinutes:       1,
		Interval:           time.Minute,
		TrainConcurrency:   2,
		WorkerTTL:          2 * time.Minute,
		DefaultRetrainCron: "0 3 * * *",
		LogLevel:           "info",
	}
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"ok", func(*Config) {}, ""},
		{"debug logging", func(c *Config) { c.LogLevel = "debug" }, ""},
		{"store membership", func(c *Config) {
			c.ShardMembership = "store"
			c.StoreDSN = "postgres://overlay@pg:5432/overlay"
		}, ""},
		{"broker", func(c *Config) { c.DruidBroker = "druid-broker:8082" }, "absolute URL"},
		{"datasource", func(c *Config) { c.DruidDatasource = "metrics-1;drop" }, "DRUID_DATASOURCE"},
		{"kafka", func(c *Config) { c.KafkaBrokers = nil }, "KAFKA_BROKERS"},
		{"topic", func(c *Config) { c.KafkaTopic = "" }, "KAFKA_TOPIC"},
		{"lookback", func(c *Config) { c.Lookback = time.Second }, "LOOKBACK"},
		{"ahead", func(c *Config) { c.AheadMinutes = 0 }, "AHEAD_MINUTES"},
		{"interval", func(c *Config) { c.Interval = time.Millisecond }, "INTERVAL"},
		{"druid max range", func(c *Config) { c.DruidMaxRange = -time.Hour }, "DRUID_MAX_RANGE"},
		{"druid retries", func(c *Config) { c.DruidRetries = -1 }, "DRUID_RETRIES"},
		{"druid rps", func(c *Config) { c.DruidMaxRPS = -2 }, "DRUID_MAX_RPS"},
		{"scan range below lookback", func(c *Config) {
			c.Lookback = 336 * time.Hour
			c.ScanRange = 24 * time.Hour
		}, "SCAN_RANGE"},
		{"scan range equal to lookback", func(c *Config) {
			// The half-open scan clamps a hash's Min to the window start and
			// excludes now, so a window of exactly LOOKBACK makes every hash look
			// shorter than LOOKBACK and the worker publishes nothing.
			c.ScanRange = c.Lookback
		}, "SCAN_RANGE"},
		{"scan range with two intervals of slack", func(c *Config) {
			c.ScanRange = c.Lookback + 2*c.Interval
		}, ""},
		{"train concurrency", func(c *Config) { c.TrainConcurrency = 0 }, "TRAIN_CONCURRENCY"},
		{"worker ttl", func(c *Config) { c.WorkerTTL = 500 * time.Millisecond }, "WORKER_TTL"},
		{"worker ttl not longer than the interval", func(c *Config) { c.WorkerTTL = c.Interval }, "WORKER_TTL"},
		{"retrain cron", func(c *Config) { c.DefaultRetrainCron = "every night" }, "DEFAULT_RETRAIN_CRON"},
		{"retrain cron as a descriptor", func(c *Config) { c.DefaultRetrainCron = "@daily" }, ""},
		{"log level", func(c *Config) { c.LogLevel = "trace" }, "LOG_LEVEL"},
		{"membership mode", func(c *Config) { c.ShardMembership = "etcd" }, "SHARD_MEMBERSHIP"},
		{"store membership without a store", func(c *Config) { c.ShardMembership = "store" }, "SHARD_MEMBERSHIP=store"},
		{"shard peers and dns", func(c *Config) {
			c.ShardPeers = []string{"a"}
			c.ShardDNS = "baselines"
		}, "SHARD_PEERS"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := ok
			tc.mut(&c)
			err := c.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v want substring %q", err, tc.want)
			}
		})
	}
}

func TestConfigFromEnvShards(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	clearStoreEnv(t)
	t.Setenv("SHARD_ID", "10.0.0.7")
	t.Setenv("SHARD_PEERS", "10.0.0.8, 10.0.0.9")
	t.Setenv("SHARD_DNS", "")
	t.Setenv("SHARD_MEMBERSHIP", "")
	cfg, err := ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShardID != "10.0.0.7" {
		t.Fatalf("SHARD_ID: %+v", cfg)
	}
	if len(cfg.ShardPeers) != 2 || cfg.ShardPeers[0] != "10.0.0.8" {
		t.Fatalf("SHARD_PEERS: %+v", cfg.ShardPeers)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("SHARD_PEERS", "")
	t.Setenv("SHARD_DNS", "baselines-headless.timeseries.svc.cluster.local")
	cfg, err = ConfigFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShardDNS == "" || len(cfg.ShardPeers) != 0 {
		t.Fatalf("SHARD_DNS: %+v", cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreDSN(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "no host and no url means no store"},
		{
			name: "url wins over the fields",
			env:  map[string]string{"BASELINE_STORE_URL": "postgres://u:p@h:5433/db", "BASELINE_STORE_HOST": "ignored"},
			want: "postgres://u:p@h:5433/db",
		},
		{
			name: "host with the documented defaults",
			env:  map[string]string{"BASELINE_STORE_HOST": "overlay-postgres"},
			// The password has no default: the sandbox and the chart both set it.
			want: "postgres://overlay@overlay-postgres:5432/overlay?sslmode=disable",
		},
		{
			name: "every field",
			env: map[string]string{
				"BASELINE_STORE_HOST":     "pg",
				"BASELINE_STORE_PORT":     "6432",
				"BASELINE_STORE_DATABASE": "metrics",
				"BASELINE_STORE_USER":     "worker",
				"BASELINE_STORE_PASSWORD": "s3cret",
				"BASELINE_STORE_SSLMODE":  "require",
			},
			want: "postgres://worker:s3cret@pg:6432/metrics?sslmode=require",
		},
		{
			name: "host without a password",
			env:  map[string]string{"BASELINE_STORE_HOST": "pg"},
			want: "postgres://overlay@pg:5432/overlay?sslmode=disable",
		},
		{
			name: "each field falls back to the plugin's name",
			env: map[string]string{
				"FORECAST_STORE_HOST":     "pg",
				"FORECAST_STORE_USER":     "plugin",
				"FORECAST_STORE_PASSWORD": "pw",
			},
			want: "postgres://plugin:pw@pg:5432/overlay?sslmode=disable",
		},
		{
			name: "the worker's field wins per field",
			env: map[string]string{
				"BASELINE_STORE_HOST":     "worker-pg",
				"FORECAST_STORE_HOST":     "plugin-pg",
				"FORECAST_STORE_DATABASE": "plugin-db",
			},
			want: "postgres://overlay@worker-pg:5432/plugin-db?sslmode=disable",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clearStoreEnv(t)
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			if got := storeDSN(os.Getenv); got != tc.want {
				t.Fatalf("storeDSN() = %q, want %q", got, tc.want)
			}
		})
	}
}

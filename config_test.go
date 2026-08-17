package baselines

import (
	"strings"
	"testing"
	"time"
)

func TestConfigFromEnvDefaults(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("KAFKA_BROKERS", "kafka:9092")
	t.Setenv("DRUID_DATASOURCE", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("LOOKBACK", "")
	t.Setenv("INTERVAL", "")
	t.Setenv("AHEAD_MINUTES", "")
	t.Setenv("CALENDAR", "")
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
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigFromEnvValues(t *testing.T) {
	t.Setenv("DRUID_BROKER", "http://druid-broker:8082")
	t.Setenv("DRUID_DATASOURCE", "metrics")
	t.Setenv("KAFKA_BROKERS", "kafka:9092, kafka:9093")
	t.Setenv("KAFKA_TOPIC", "baselines")
	t.Setenv("LOOKBACK", "48h")
	t.Setenv("AHEAD_MINUTES", "3")
	t.Setenv("INTERVAL", "30s")
	t.Setenv("CALENDAR", "ru")
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
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	ok := Config{
		DruidBroker:     "http://druid-broker:8082",
		DruidDatasource: "metrics",
		KafkaBrokers:    []string{"kafka:9092"},
		KafkaTopic:      "baselines",
		Lookback:        time.Hour,
		AheadMinutes:    1,
		Interval:        time.Minute,
	}
	for _, tc := range []struct {
		name string
		mut  func(*Config)
		want string
	}{
		{"ok", func(*Config) {}, ""},
		{"broker", func(c *Config) { c.DruidBroker = "druid-broker:8082" }, "absolute URL"},
		{"datasource", func(c *Config) { c.DruidDatasource = "metrics-1;drop" }, "DRUID_DATASOURCE"},
		{"kafka", func(c *Config) { c.KafkaBrokers = nil }, "KAFKA_BROKERS"},
		{"topic", func(c *Config) { c.KafkaTopic = "" }, "KAFKA_TOPIC"},
		{"lookback", func(c *Config) { c.Lookback = time.Second }, "LOOKBACK"},
		{"ahead", func(c *Config) { c.AheadMinutes = 0 }, "AHEAD_MINUTES"},
		{"interval", func(c *Config) { c.Interval = time.Millisecond }, "INTERVAL"},
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

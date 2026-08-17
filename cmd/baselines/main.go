package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/eduard-kolotushin/timeseries-baselines"
	forecast "github.com/eduard-kolotushin/timeseries-forecast"
)

func main() {
	cfg, err := baselines.ConfigFromEnv()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	cal, err := forecast.CalendarByName(cfg.Calendar)
	if err != nil {
		slog.Error("calendar", "err", err)
		os.Exit(1)
	}
	pub := baselines.NewPublisher(cfg, cal)
	defer pub.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	slog.Info("baseline publisher started",
		"datasource", cfg.DruidDatasource,
		"topic", cfg.KafkaTopic,
		"interval", cfg.Interval.String(),
		"aheadMinutes", cfg.AheadMinutes,
	)
	pub.Run(ctx)
}

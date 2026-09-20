package baselines

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type baselineSink interface {
	Publish(ctx context.Context, msg BaselineMessage) error
	Close() error
}

// BaselineMessage is one lead baseline point for a metric_hash.
type BaselineMessage struct {
	MetricHash    string  `json:"metric_hash"`
	MetricTS      int64   `json:"metric_ts"`
	BaselineValue float64 `json:"baseline_value"`
}

type kafkaSink struct {
	w *kafka.Writer
}

func newKafkaSink(brokers []string, topic string) *kafkaSink {
	return &kafkaSink{
		w: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
			// A literal Writer is not kafka.NewWriter, which is the only place
			// kafka-go turns a 0 into RequireAll. Left at 0, RequiredAcks is
			// RequireNone, and the client's Produce returns (nil, nil) for it: a
			// record the broker rejects would be counted as published.
			RequiredAcks:           kafka.RequireAll,
			AllowAutoTopicCreation: true,
		},
	}
}

// Publish writes one point.
func (s *kafkaSink) Publish(ctx context.Context, msg BaselineMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.w.WriteMessages(ctx, kafka.Message{
		Key:   messageKey(msg),
		Value: b,
	})
}

// messageKey is unique per published point, so a duplicate publish of the same
// (metric_hash, metric_ts) lands on the same partition and a compacted topic
// keeps only the last record for it. One point, one key, across restarts.
func messageKey(msg BaselineMessage) []byte {
	return []byte(msg.MetricHash + "|" + strconv.FormatInt(msg.MetricTS, 10))
}

func (s *kafkaSink) Close() error {
	if s == nil || s.w == nil {
		return nil
	}
	return s.w.Close()
}

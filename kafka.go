package baselines

import (
	"context"
	"encoding/json"

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
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (s *kafkaSink) Publish(ctx context.Context, msg BaselineMessage) error {
	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return s.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.MetricHash),
		Value: b,
	})
}

func (s *kafkaSink) Close() error {
	if s == nil || s.w == nil {
		return nil
	}
	return s.w.Close()
}

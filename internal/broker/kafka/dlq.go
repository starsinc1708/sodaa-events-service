package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type DLQWriter struct {
	w *kafka.Writer
}

func NewDLQWriter(brokers []string, topic string) *DLQWriter {
	return &DLQWriter{w: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		WriteTimeout: 10 * time.Second,
	}}
}

func (d *DLQWriter) Send(ctx context.Context, m kafka.Message, reason string) error {
	return d.w.WriteMessages(ctx, kafka.Message{
		Key:   m.Key,
		Value: m.Value,
		Headers: []kafka.Header{
			{Key: "x-error-reason", Value: []byte(reason)},
		},
	})
}

func (d *DLQWriter) Close() error { return d.w.Close() }

var _ DLQ = (*DLQWriter)(nil)

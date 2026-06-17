package kafka

import (
	"context"
	"encoding/json"
	"events/internal/domain"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{writer: &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    500,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 10 * time.Second,
	}}
}

func (p *Producer) PublishEvent(ctx context.Context, e *domain.Event, topic string) error {
	msg, err := json.Marshal(map[string]any{
		"id":         e.ID,
		"user_id":    e.UserID,
		"event_type": e.EventType,
		"payload":    e.Payload,
		"created_at": e.CreatedAt,
	})
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(e.ID.String()),
		Value: msg,
	}); err != nil {
		return fmt.Errorf("publish event %s: %w", e.ID, err)
	}
	return nil
}

func (p *Producer) Close() error { return p.writer.Close() }

var _ domain.EventPublisher = (*Producer)(nil)

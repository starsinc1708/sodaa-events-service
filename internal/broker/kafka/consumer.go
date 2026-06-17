package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"events/internal/domain"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type DLQ interface {
	Send(ctx context.Context, m kafka.Message, reason string) error
}

type Consumer struct {
	reader      *kafka.Reader
	dlq         DLQ
	stats       domain.StatsWriter
	sink        domain.EventSink
	log         *slog.Logger
	batchSize   int
	fillWindow  time.Duration
	backoffBase time.Duration
	backoffMax  time.Duration
}

func NewConsumer(reader *kafka.Reader, dlq DLQ, stats domain.StatsWriter, sink domain.EventSink, log *slog.Logger) *Consumer {
	return &Consumer{
		reader:      reader,
		dlq:         dlq,
		stats:       stats,
		sink:        sink,
		log:         log,
		batchSize:   500,
		fillWindow:  200 * time.Millisecond,
		backoffBase: time.Second,
		backoffMax:  30 * time.Second,
	}
}

func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		MaxWait:        500 * time.Millisecond,
		QueueCapacity:  500,
		CommitInterval: 0,
		StartOffset:    kafka.FirstOffset,
	})
}

type outcome int

const (
	outcomeOK        outcome = iota
	outcomeTransient
)

type eventMessage struct {
	ID        string          `json:"id"`
	UserID    string          `json:"user_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

func (c *Consumer) Run(ctx context.Context) error {
	c.log.Info("worker started")
	backoff := c.backoffBase
	for {
		batch, err := c.fetchBatch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				c.log.Info("worker stopped")
				return nil
			}
			c.log.Error("fetch failed", "err", err, "backoff", backoff)
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, c.backoffMax)
			continue
		}

		switch c.handleBatch(ctx, batch) {
		case outcomeOK:
			c.commit(ctx, batch...)
			backoff = c.backoffBase
		case outcomeTransient:
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, c.backoffMax)
		}
	}
}

func (c *Consumer) fetchBatch(ctx context.Context) ([]kafka.Message, error) {
	first, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	batch := make([]kafka.Message, 0, c.batchSize)
	batch = append(batch, first)

	fillCtx, cancel := context.WithTimeout(ctx, c.fillWindow)
	defer cancel()
	for len(batch) < c.batchSize {
		m, err := c.reader.FetchMessage(fillCtx)
		if err != nil {
			break
		}
		batch = append(batch, m)
	}
	return batch, nil
}

func (c *Consumer) handleBatch(ctx context.Context, batch []kafka.Message) outcome {
	valid := make([]domain.Event, 0, len(batch))
	var poison []kafka.Message
	var reasons []string

	for i := range batch {
		ev, reason := c.parse(batch[i])
		if reason != "" {
			poison = append(poison, batch[i])
			reasons = append(reasons, reason)
			continue
		}
		valid = append(valid, ev)
	}

	for i := range poison {
		if c.dlq == nil {
			return outcomeTransient
		}
		if err := c.dlq.Send(ctx, poison[i], reasons[i]); err != nil {
			c.log.Error("dlq send failed", "offset", poison[i].Offset, "err", err)
			return outcomeTransient
		}
		c.log.Warn("message routed to DLQ", "offset", poison[i].Offset, "reason", reasons[i])
	}

	if len(valid) > 0 {
		applied, err := c.stats.RecordEvents(ctx, valid)
		if err != nil {
			c.log.Error("record events failed", "count", len(valid), "err", err)
			return outcomeTransient
		}
		if c.sink != nil {
			if err := c.sink.InsertEvents(ctx, valid); err != nil {
				c.log.Error("sink insert failed", "count", len(valid), "err", err)
				return outcomeTransient
			}
		}
		c.log.Debug("batch processed", "valid", len(valid), "applied", applied, "poison", len(poison))
	}

	return outcomeOK
}

func (c *Consumer) parse(m kafka.Message) (domain.Event, string) {
	var em eventMessage
	if err := json.Unmarshal(m.Value, &em); err != nil {
		return domain.Event{}, "unmarshal: " + err.Error()
	}
	id, err := uuid.Parse(em.ID)
	if err != nil {
		return domain.Event{}, "invalid id: " + err.Error()
	}
	if em.EventType == "" {
		return domain.Event{}, "empty event_type"
	}

	ev := domain.Event{ID: id, EventType: em.EventType}
	if c.sink != nil {
		userID, err := uuid.Parse(em.UserID)
		if err != nil {
			return domain.Event{}, "invalid user_id: " + err.Error()
		}
		payload, err := decodePayload(em.Payload)
		if err != nil {
			return domain.Event{}, "invalid payload: " + err.Error()
		}
		ev.UserID = userID
		ev.Payload = payload
		ev.CreatedAt = em.CreatedAt
	}
	return ev, ""
}

func decodePayload(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *Consumer) commit(ctx context.Context, msgs ...kafka.Message) {
	if len(msgs) == 0 {
		return
	}
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.reader.CommitMessages(commitCtx, msgs...); err != nil {
		c.log.Error("commit failed", "err", err)
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	return next
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

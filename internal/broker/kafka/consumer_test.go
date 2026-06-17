package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"events/internal/domain"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)


type mockStatsWriter struct {
	applied int
	err     error
	calls   int
	got     []domain.Event
}

func (m *mockStatsWriter) RecordEvents(_ context.Context, events []domain.Event) (int, error) {
	m.calls++
	m.got = append(m.got, events...)
	if m.err != nil {
		return 0, m.err
	}
	if m.applied > 0 {
		return m.applied, nil
	}
	return len(events), nil
}

type mockDLQ struct {
	calls int
	err   error
}

func (m *mockDLQ) Send(_ context.Context, _ kafka.Message, _ string) error {
	m.calls++
	return m.err
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func msg(t *testing.T, v any) kafka.Message {
	t.Helper()
	return kafka.Message{Value: mustJSON(t, v)}
}

func validMsg(t *testing.T, eventType string) kafka.Message {
	t.Helper()
	return msg(t, map[string]any{"id": uuid.NewString(), "event_type": eventType})
}

func TestConsumer_HandleBatch(t *testing.T) {
	t.Parallel()

	t.Run("valid batch recorded once and committed", func(t *testing.T) {
		t.Parallel()
		sw := &mockStatsWriter{}
		dlq := &mockDLQ{}
		c := NewConsumer(nil, dlq, sw, nil, discardLogger())

		batch := []kafka.Message{validMsg(t, "click"), validMsg(t, "view"), validMsg(t, "click")}
		got := c.handleBatch(context.Background(), batch)

		require.Equal(t, outcomeOK, got)
		require.Equal(t, 1, sw.calls, "one batch call, not per-message")
		require.Len(t, sw.got, 3)
		require.Equal(t, 0, dlq.calls)
	})

	t.Run("db error is transient (no commit)", func(t *testing.T) {
		t.Parallel()
		sw := &mockStatsWriter{err: errors.New("db down")}
		c := NewConsumer(nil, &mockDLQ{}, sw, nil, discardLogger())

		got := c.handleBatch(context.Background(), []kafka.Message{validMsg(t, "click")})
		require.Equal(t, outcomeTransient, got)
	})

	t.Run("poison messages go to DLQ, batch committed", func(t *testing.T) {
		t.Parallel()
		sw := &mockStatsWriter{}
		dlq := &mockDLQ{}
		c := NewConsumer(nil, dlq, sw, nil, discardLogger())

		batch := []kafka.Message{
			{Value: []byte("{not json")},
			msg(t, map[string]any{"id": "nope", "event_type": "click"}),
			msg(t, map[string]any{"id": uuid.NewString(), "event_type": ""}),
		}
		got := c.handleBatch(context.Background(), batch)

		require.Equal(t, outcomeOK, got)
		require.Equal(t, 3, dlq.calls)
		require.Equal(t, 0, sw.calls, "no valid events to record")
	})

}

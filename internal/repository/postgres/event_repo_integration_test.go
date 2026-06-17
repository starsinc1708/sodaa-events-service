
package postgres

import (
	"context"
	"events/internal/domain"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestEventRepo_Create_Integration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run integration test")
	}

	ctx := context.Background()
	pool, err := New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `TRUNCATE events`)
	require.NoError(t, err)

	repo := NewEventPool(pool)

	userID := uuid.New()
	e := &domain.Event{
		ID:        uuid.New(),
		UserID:    userID,
		EventType: "purchase",
		Payload:   nil,
		CreatedAt: time.Now().UTC(),
	}

	t.Run("event written to db", func(t *testing.T) {
		err := repo.Create(ctx, e)
		require.NoError(t, err)

		var eventType string
		err = pool.QueryRow(ctx,
			`SELECT event_type FROM events WHERE id = $1`, e.ID,
		).Scan(&eventType)
		require.NoError(t, err)
		require.Equal(t, "purchase", eventType)

		var payload *map[string]any
		err = pool.QueryRow(ctx,
			`SELECT payload FROM events WHERE id = $1`, e.ID,
		).Scan(&payload)
		require.NoError(t, err)
		require.Nil(t, payload)
	})
}

func TestEventRepo_List_Integration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run integration test")
	}

	ctx := context.Background()
	pool, err := New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `TRUNCATE events`)
	require.NoError(t, err)

	repo := NewEventPool(pool)
	userA, userB := uuid.New(), uuid.New()
	now := time.Now().UTC()

	events := []domain.Event{
		{ID: uuid.New(), UserID: userA, EventType: "click", Payload: map[string]any{}, CreatedAt: now},
		{ID: uuid.New(), UserID: userA, EventType: "view", Payload: map[string]any{}, CreatedAt: now.Add(-time.Second)},
		{ID: uuid.New(), UserID: userB, EventType: "click", Payload: map[string]any{}, CreatedAt: now.Add(-2 * time.Second)},
	}
	for i := range events {
		_, err := pool.Exec(ctx,
			`INSERT INTO events (id, user_id, event_type, payload, created_at) VALUES ($1,$2,$3,$4,$5)`,
			events[i].ID, events[i].UserID, events[i].EventType, events[i].Payload, events[i].CreatedAt,
		)
		require.NoError(t, err)
	}

	t.Run("filter by user_id", func(t *testing.T) {
		got, _, err := repo.List(ctx, domain.EventFilter{UserID: &userA, Limit: 10})
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("filter by event_type", func(t *testing.T) {
		got, _, err := repo.List(ctx, domain.EventFilter{EventType: strPtr("click"), Limit: 10})
		require.NoError(t, err)
		require.Len(t, got, 2)
	})

	t.Run("keyset pagination", func(t *testing.T) {
		page1, token, err := repo.List(ctx, domain.EventFilter{Limit: 2})
		require.NoError(t, err)
		require.Len(t, page1, 2)
		require.NotEmpty(t, token)

		page2, token2, err := repo.List(ctx, domain.EventFilter{Limit: 2, PageToken: token})
		require.NoError(t, err)
		require.Len(t, page2, 1)
		require.Empty(t, token2, "no more pages")
	})

	t.Run("invalid page_token returns error", func(t *testing.T) {
		_, _, err := repo.List(ctx, domain.EventFilter{PageToken: "not-valid-base64"})
		require.ErrorIs(t, err, domain.ErrInvalidPageToken)
	})
}

func strPtr(s string) *string { return &s }

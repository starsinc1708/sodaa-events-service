
package postgres

import (
	"context"
	"events/internal/domain"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestStatsRepo_AggregationAndDedup(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run integration test")
	}

	ctx := context.Background()
	pool, err := New(ctx, dsn)
	require.NoError(t, err)
	defer pool.Close()

	_, err = pool.Exec(ctx, `TRUNCATE events, event_stats, processed_events`)
	require.NoError(t, err)

	repo := NewStatsRepo(pool)

	clickID1, clickID2, viewID := uuid.New(), uuid.New(), uuid.New()
	applied, err := repo.RecordEvents(ctx, []domain.Event{
		{ID: clickID1, EventType: "click"},
		{ID: clickID2, EventType: "click"},
		{ID: viewID, EventType: "view"},
	})
	require.NoError(t, err)
	require.Equal(t, 3, applied, "all three are new")

	applied, err = repo.RecordEvents(ctx, []domain.Event{{ID: clickID1, EventType: "click"}})
	require.NoError(t, err)
	require.Equal(t, 0, applied, "duplicate must be skipped")

	u1, u2 := uuid.New(), uuid.New()
	insertEvent(t, pool, clickID1, u1, "click")
	insertEvent(t, pool, clickID2, u1, "click")
	insertEvent(t, pool, viewID, u2, "view")

	stats, err := repo.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), stats.TotalEvents)
	require.Equal(t, uint64(2), stats.ByType["click"])
	require.Equal(t, uint64(1), stats.ByType["view"])
	require.Equal(t, uint64(2), stats.UniqueUsers)
}

func insertEvent(t *testing.T, pool *pgxpool.Pool, id, userID uuid.UUID, eventType string) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`INSERT INTO events (id, user_id, event_type, payload, created_at)
		 VALUES ($1, $2, $3, '{}'::jsonb, $4)`,
		id, userID, eventType, time.Now().UTC())
	require.NoError(t, err)
}

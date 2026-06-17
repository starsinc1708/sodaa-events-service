
package clickhouse

import (
	"context"
	"events/internal/domain"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClickHouseStats_AggregationDedup(t *testing.T) {
	addr := os.Getenv("TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("set TEST_CLICKHOUSE_ADDR to run integration test")
	}

	ctx := context.Background()
	conn, err := New(ctx, addr, "analytics", "app", "app")
	require.NoError(t, err)
	defer conn.Close()

	repo := NewStatsRepo(conn)
	require.NoError(t, repo.EnsureSchema(ctx))
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE events"))
	require.NoError(t, conn.Exec(ctx, "TRUNCATE TABLE event_agg"))

	id1, id2 := uuid.New(), uuid.New()
	u1, u2 := uuid.New(), uuid.New()
	now := time.Now().UTC()

	events := []domain.Event{
		{ID: id1, UserID: u1, EventType: "click", CreatedAt: now},
		{ID: id2, UserID: u2, EventType: "view", CreatedAt: now},
		{ID: id1, UserID: u1, EventType: "click", CreatedAt: now},
	}
	require.NoError(t, repo.InsertEvents(ctx, events))

	stats, err := repo.GetStats(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.TotalEvents, "uniq(id) ignores duplicate")
	require.Equal(t, uint64(1), stats.ByType["click"])
	require.Equal(t, uint64(1), stats.ByType["view"])
	require.Equal(t, uint64(2), stats.UniqueUsers)
}

package clickhouse

import (
	"context"
	"encoding/json"
	"events/internal/domain"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type StatsRepo struct {
	conn driver.Conn
}

func NewStatsRepo(conn driver.Conn) *StatsRepo {
	return &StatsRepo{conn: conn}
}

func (r *StatsRepo) EnsureSchema(ctx context.Context) error {
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS events (
			id         UUID,
			user_id    UUID,
			event_type LowCardinality(String),
			payload    String,
			created_at DateTime64(3, 'UTC')
		) ENGINE = MergeTree
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (event_type, created_at)`,

		`CREATE TABLE IF NOT EXISTS event_agg (
			event_type LowCardinality(String),
			ids        AggregateFunction(uniq, UUID),
			users      AggregateFunction(uniq, UUID)
		) ENGINE = AggregatingMergeTree
		ORDER BY event_type`,

		`CREATE MATERIALIZED VIEW IF NOT EXISTS event_agg_mv TO event_agg AS
			SELECT event_type, uniqState(id) AS ids, uniqState(user_id) AS users
			FROM events
			GROUP BY event_type`,
	} {
		if err := r.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("clickhouse ensure schema: %w", err)
		}
	}
	return nil
}

func (r *StatsRepo) InsertEvents(ctx context.Context, events []domain.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := r.conn.PrepareBatch(ctx,
		"INSERT INTO events (id, user_id, event_type, payload, created_at)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	defer func() { _ = batch.Close() }()

	for i := range events {
		payload, err := json.Marshal(events[i].Payload)
		if err != nil {
			return fmt.Errorf("marshal payload: %w", err)
		}
		if err := batch.Append(events[i].ID, events[i].UserID, events[i].EventType, string(payload), events[i].CreatedAt); err != nil {
			return fmt.Errorf("batch append: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("batch send: %w", err)
	}
	return nil
}

func (r *StatsRepo) GetStats(ctx context.Context) (domain.Stats, error) {
	stats := domain.Stats{ByType: make(map[string]uint64)}

	rows, err := r.conn.Query(ctx,
		`SELECT event_type, uniqMerge(ids) AS cnt FROM event_agg GROUP BY event_type`)
	if err != nil {
		return domain.Stats{}, fmt.Errorf("query by_type: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventType string
		var cnt uint64
		if err := rows.Scan(&eventType, &cnt); err != nil {
			return domain.Stats{}, fmt.Errorf("scan by_type: %w", err)
		}
		stats.ByType[eventType] = cnt
		stats.TotalEvents += cnt
	}
	if err := rows.Err(); err != nil {
		return domain.Stats{}, fmt.Errorf("rows iteration: %w", err)
	}

	if err := r.conn.QueryRow(ctx,
		`SELECT uniqMerge(users) FROM event_agg`,
	).Scan(&stats.UniqueUsers); err != nil {
		return domain.Stats{}, fmt.Errorf("query unique_users: %w", err)
	}

	return stats, nil
}

var (
	_ domain.StatsRepository = (*StatsRepo)(nil)
	_ domain.EventSink       = (*StatsRepo)(nil)
)

package postgres

import (
	"context"
	"events/internal/domain"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type StatsRepo struct {
	pool *pgxpool.Pool
}

func NewStatsRepo(pool *pgxpool.Pool) *StatsRepo {
	return &StatsRepo{pool: pool}
}

func (r *StatsRepo) GetStats(ctx context.Context) (domain.Stats, error) {
	stats := domain.Stats{ByType: make(map[string]uint64)}

	rows, err := r.pool.Query(ctx, `SELECT event_type, count FROM event_stats`)
	if err != nil {
		return domain.Stats{}, fmt.Errorf("query event_stats: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var eventType string
		var cnt uint64
		if err := rows.Scan(&eventType, &cnt); err != nil {
			return domain.Stats{}, fmt.Errorf("scan event_stats: %w", err)
		}
		stats.ByType[eventType] = cnt
		stats.TotalEvents += cnt
	}
	if err := rows.Err(); err != nil {
		return domain.Stats{}, fmt.Errorf("rows iteration: %w", err)
	}

	if err := r.pool.QueryRow(ctx,
		`SELECT count(DISTINCT user_id)::bigint FROM events`,
	).Scan(&stats.UniqueUsers); err != nil {
		return domain.Stats{}, fmt.Errorf("query unique_users: %w", err)
	}

	return stats, nil
}

func (r *StatsRepo) RecordEvents(ctx context.Context, events []domain.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	ids := make([]uuid.UUID, len(events))
	for i := range events {
		ids[i] = events[i].ID
	}

	rows, err := tx.Query(ctx,
		`INSERT INTO processed_events (event_id)
		 SELECT unnest($1::uuid[])
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`, ids)
	if err != nil {
		return 0, fmt.Errorf("dedup insert: %w", err)
	}
	applied := make(map[uuid.UUID]struct{}, len(events))
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan dedup: %w", err)
		}
		applied[id] = struct{}{}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("dedup rows: %w", err)
	}
	if len(applied) == 0 {
		return 0, nil
	}

	counts := make(map[string]int64)
	for i := range events {
		if _, ok := applied[events[i].ID]; ok {
			counts[events[i].EventType]++
		}
	}
	types := make([]string, 0, len(counts))
	deltas := make([]int64, 0, len(counts))
	for t, c := range counts {
		types = append(types, t)
		deltas = append(deltas, c)
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO event_stats (event_type, count, updated_at)
		 SELECT t, c, now() FROM unnest($1::text[], $2::bigint[]) AS x(t, c)
		 ON CONFLICT (event_type)
		 DO UPDATE SET count = event_stats.count + EXCLUDED.count, updated_at = now()`,
		types, deltas,
	); err != nil {
		return 0, fmt.Errorf("upsert event_stats: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}
	return len(applied), nil
}

var (
	_ domain.StatsRepository = (*StatsRepo)(nil)
	_ domain.StatsWriter     = (*StatsRepo)(nil)
)

package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"events/internal/domain"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type EventRepo struct {
	pool *pgxpool.Pool
}

func NewEventPool(pool *pgxpool.Pool) *EventRepo {
	return &EventRepo{pool: pool}
}

func (r *EventRepo) Create(ctx context.Context, e *domain.Event) error {
	const q = `INSERT INTO events (id, user_id, event_type, payload, created_at)
		VALUES ($1, $2, $3, $4, $5)`
	if _, err := r.pool.Exec(ctx, q, e.ID, e.UserID, e.EventType, e.Payload, e.CreatedAt); err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

type listCursor struct {
	CreatedAt time.Time `json:"c"`
	ID        uuid.UUID `json:"i"`
}

func encodeCursor(c listCursor) (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("marshal cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func decodeCursor(token string) (listCursor, error) {
	b, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return listCursor{}, fmt.Errorf("decode page_token: %w", err)
	}
	var c listCursor
	if err := json.Unmarshal(b, &c); err != nil {
		return listCursor{}, fmt.Errorf("unmarshal page_token: %w", err)
	}
	return c, nil
}

func (r *EventRepo) List(ctx context.Context, f domain.EventFilter) ([]domain.Event, string, error) {
	limit := f.Limit
	if limit <= 0 || limit > 200 {
		limit = 50
	}

	var cur listCursor
	hasCursor := f.PageToken != ""
	if hasCursor {
		c, err := decodeCursor(f.PageToken)
		if err != nil {
			return nil, "", domain.ErrInvalidPageToken
		}
		cur = c
	}

	const q = `
		SELECT id, user_id, event_type, payload, created_at
		FROM events
		WHERE ($1::uuid IS NULL OR user_id = $1)
		  AND ($2::text IS NULL OR event_type = $2)
		  AND (NOT $3::boolean OR (created_at, id) < ($4::timestamptz, $5::uuid))
		ORDER BY created_at DESC, id DESC
		LIMIT $6`

	rows, err := r.pool.Query(ctx, q,
		f.UserID, f.EventType, hasCursor, cur.CreatedAt, cur.ID, limit+1,
	)
	if err != nil {
		return nil, "", fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]domain.Event, 0, limit+1)
	for rows.Next() {
		var e domain.Event
		if err := rows.Scan(&e.ID, &e.UserID, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
			return nil, "", fmt.Errorf("scan event: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("rows iteration: %w", err)
	}

	var nextToken string
	if len(events) > limit {
		events = events[:limit]
		last := events[limit-1]
		nextToken, err = encodeCursor(listCursor{CreatedAt: last.CreatedAt, ID: last.ID})
		if err != nil {
			return nil, "", err
		}
	}

	return events, nextToken, nil
}

func (r *EventRepo) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

var _ domain.EventRepository = (*EventRepo)(nil)

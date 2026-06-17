package domain

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidUserID    = errors.New("invalid user_id")
	ErrEmptyEventType   = errors.New("event_type is required")
	ErrInvalidPageToken = errors.New("invalid page_token")
)

type Event struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	EventType string
	Payload   map[string]any
	CreatedAt time.Time
}

type EventFilter struct {
	UserID    *uuid.UUID
	EventType *string
	Limit     int
	PageToken string
}

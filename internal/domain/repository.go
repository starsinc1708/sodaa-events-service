package domain

import (
	"context"
)

type EventRepository interface {
	Create(ctx context.Context, e *Event) error
	List(ctx context.Context, f EventFilter) ([]Event, string, error)
	Ping(ctx context.Context) error
}

type EventPublisher interface {
	PublishEvent(ctx context.Context, e *Event, topic string) error
}

type Stats struct {
	TotalEvents uint64
	ByType      map[string]uint64
	UniqueUsers uint64
}

type StatsRepository interface {
	GetStats(ctx context.Context) (Stats, error)
}

type StatsWriter interface {
	RecordEvents(ctx context.Context, events []Event) (applied int, err error)
}

type EventSink interface {
	InsertEvents(ctx context.Context, events []Event) error
}

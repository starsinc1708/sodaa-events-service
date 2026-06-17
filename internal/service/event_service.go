package service

import (
	"context"
	"events/internal/domain"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type EventService struct {
	repo      domain.EventRepository
	stats     domain.StatsRepository
	publisher domain.EventPublisher
	topic     string
	log       *slog.Logger
}

type CreateEventInput struct {
	UserID    string
	EventType string
	Payload   map[string]any
}

func NewEventService(
	repo domain.EventRepository,
	stats domain.StatsRepository,
	publisher domain.EventPublisher,
	topic string,
	log *slog.Logger,
) *EventService {
	return &EventService{repo: repo, stats: stats, publisher: publisher, topic: topic, log: log}
}

func (s *EventService) CreateEvent(ctx context.Context, in CreateEventInput) (*domain.Event, error) {
	userID, err := uuid.Parse(in.UserID)
	if err != nil {
		return nil, domain.ErrInvalidUserID
	}
	if in.EventType == "" {
		return nil, domain.ErrEmptyEventType
	}

	e := &domain.Event{
		ID:        uuid.New(),
		UserID:    userID,
		EventType: in.EventType,
		Payload:   in.Payload,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.repo.Create(ctx, e); err != nil {
		return nil, fmt.Errorf("create event: %w", err)
	}

	if err := s.publisher.PublishEvent(ctx, e, s.topic); err != nil {
		s.log.Warn("failed to publish event to kafka", "event_id", e.ID, "err", err)
	}

	return e, nil
}

func (s *EventService) ListEvents(ctx context.Context, f domain.EventFilter) ([]domain.Event, string, error) {
	return s.repo.List(ctx, f)
}

func (s *EventService) GetStats(ctx context.Context) (domain.Stats, error) {
	return s.stats.GetStats(ctx)
}

func (s *EventService) Ping(ctx context.Context) error {
	return s.repo.Ping(ctx)
}

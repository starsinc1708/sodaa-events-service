package grpc

import (
	"context"
	"io"
	"log/slog"
	"testing"

	eventsv1 "events/api/proto"
	"events/internal/domain"
	"events/internal/service"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)


type mockEventRepo struct {
	createErr  error
	gotEvent   *domain.Event
	listEvents []domain.Event
	listToken  string
	listErr    error
	gotFilter  domain.EventFilter
}

func (m *mockEventRepo) Create(_ context.Context, e *domain.Event) error {
	m.gotEvent = e
	return m.createErr
}

func (m *mockEventRepo) List(_ context.Context, f domain.EventFilter) ([]domain.Event, string, error) {
	m.gotFilter = f
	return m.listEvents, m.listToken, m.listErr
}

func (m *mockEventRepo) Ping(_ context.Context) error { return nil }

type mockStatsRepo struct {
	stats domain.Stats
	err   error
}

func (m *mockStatsRepo) GetStats(_ context.Context) (domain.Stats, error) {
	return m.stats, m.err
}

type mockPublisher struct {
	err error
}

func (m *mockPublisher) PublishEvent(_ context.Context, _ *domain.Event, _ string) error {
	return m.err
}

var (
	_ domain.EventRepository = (*mockEventRepo)(nil)
	_ domain.StatsRepository = (*mockStatsRepo)(nil)
	_ domain.EventPublisher  = (*mockPublisher)(nil)
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newHandler(repo domain.EventRepository, stats domain.StatsRepository) *EventHandler {
	return NewEventHandler(
		service.NewEventService(repo, stats, &mockPublisher{}, "events", discardLogger()),
		discardLogger(),
	)
}


func TestEventHandler_CreateEvent(t *testing.T) {
	t.Parallel()
	userID := uuid.New().String()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		repo := &mockEventRepo{}
		h := newHandler(repo, &mockStatsRepo{})

		resp, err := h.CreateEvent(context.Background(), &eventsv1.CreateEventRequest{
			UserId:    userID,
			EventType: "purchase",
		})

		require.NoError(t, err)
		require.Equal(t, userID, resp.GetEvent().GetUserId())
		require.Equal(t, "purchase", resp.GetEvent().GetEventType())
		require.NotEmpty(t, resp.GetEvent().GetId())
		require.Equal(t, "purchase", repo.gotEvent.EventType)
	})

	t.Run("invalid user_id -> InvalidArgument", func(t *testing.T) {
		t.Parallel()
		h := newHandler(&mockEventRepo{}, &mockStatsRepo{})

		_, err := h.CreateEvent(context.Background(), &eventsv1.CreateEventRequest{
			UserId:    "not-a-uuid",
			EventType: "click",
		})

		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("empty event_type -> InvalidArgument", func(t *testing.T) {
		t.Parallel()
		h := newHandler(&mockEventRepo{}, &mockStatsRepo{})

		_, err := h.CreateEvent(context.Background(), &eventsv1.CreateEventRequest{
			UserId:    userID,
			EventType: "",
		})

		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}


func TestEventHandler_ListEvents(t *testing.T) {
	t.Parallel()
	userID := uuid.New()

	t.Run("filters and pagination passed through", func(t *testing.T) {
		t.Parallel()
		repo := &mockEventRepo{
			listEvents: []domain.Event{{ID: uuid.New(), UserID: userID, EventType: "click"}},
			listToken:  "next-page-token",
		}
		h := newHandler(repo, &mockStatsRepo{})

		resp, err := h.ListEvents(context.Background(), &eventsv1.ListEventsRequest{
			UserId:    userID.String(),
			EventType: "click",
			Limit:     25,
			PageToken: "cursor-1",
		})

		require.NoError(t, err)
		require.NotNil(t, repo.gotFilter.UserID)
		require.Equal(t, userID, *repo.gotFilter.UserID)
		require.NotNil(t, repo.gotFilter.EventType)
		require.Equal(t, "click", *repo.gotFilter.EventType)
		require.Equal(t, 25, repo.gotFilter.Limit)
		require.Equal(t, "cursor-1", repo.gotFilter.PageToken)
		require.Equal(t, "next-page-token", resp.GetNextPageToken())
		require.Len(t, resp.GetEvents(), 1)
	})

	t.Run("empty filters are not applied", func(t *testing.T) {
		t.Parallel()
		repo := &mockEventRepo{}
		h := newHandler(repo, &mockStatsRepo{})

		_, err := h.ListEvents(context.Background(), &eventsv1.ListEventsRequest{})

		require.NoError(t, err)
		require.Nil(t, repo.gotFilter.UserID)
		require.Nil(t, repo.gotFilter.EventType)
	})

	t.Run("invalid user_id filter -> InvalidArgument", func(t *testing.T) {
		t.Parallel()
		h := newHandler(&mockEventRepo{}, &mockStatsRepo{})

		_, err := h.ListEvents(context.Background(), &eventsv1.ListEventsRequest{UserId: "bad"})

		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
}


func TestEventHandler_GetStats(t *testing.T) {
	t.Parallel()
	repo := &mockStatsRepo{stats: domain.Stats{
		TotalEvents: 1500,
		ByType:      map[string]uint64{"click": 800, "view": 500, "purchase": 200},
		UniqueUsers: 342,
	}}
	h := newHandler(&mockEventRepo{}, repo)

	resp, err := h.GetStats(context.Background(), &eventsv1.GetStatsRequest{})

	require.NoError(t, err)
	require.Equal(t, uint64(1500), resp.GetTotalEvents())
	require.Equal(t, uint64(342), resp.GetUniqueUsers())
	require.Equal(t, map[string]uint64{"click": 800, "view": 500, "purchase": 200}, resp.GetByType())
}

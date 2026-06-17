package grpc

import (
	"context"
	"errors"
	eventsv1 "events/api/proto"
	"events/internal/domain"
	"events/internal/service"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EventHandler struct {
	eventsv1.UnimplementedEventServiceServer
	svc *service.EventService
	log *slog.Logger
}

func NewEventHandler(svc *service.EventService, log *slog.Logger) *EventHandler {
	return &EventHandler{svc: svc, log: log}
}

func (h *EventHandler) CreateEvent(
	ctx context.Context, req *eventsv1.CreateEventRequest,
) (*eventsv1.CreateEventResponse, error) {
	e, err := h.svc.CreateEvent(ctx, service.CreateEventInput{
		UserID:    req.GetUserId(),
		EventType: req.GetEventType(),
		Payload:   protoStructToMap(req.GetPayload()),
	})
	if err != nil {
		return nil, mapError(h.log, err)
	}

	pe, err := domainToProto(*e)
	if err != nil {
		h.log.Error("CreateEvent: encode failed", "err", err)
		return nil, status.Error(codes.Internal, "failed to encode event")
	}
	return &eventsv1.CreateEventResponse{Event: pe}, nil
}

func (h *EventHandler) ListEvents(
	ctx context.Context, req *eventsv1.ListEventsRequest,
) (*eventsv1.ListEventsResponse, error) {
	filter := domain.EventFilter{
		Limit:     int(req.GetLimit()),
		PageToken: req.GetPageToken(),
	}

	if uid := req.GetUserId(); uid != "" {
		parsed, err := uuid.Parse(uid)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid user_id filter")
		}
		filter.UserID = &parsed
	}
	if et := req.GetEventType(); et != "" {
		filter.EventType = &et
	}

	events, nextPageToken, err := h.svc.ListEvents(ctx, filter)
	if err != nil {
		return nil, mapError(h.log, err)
	}

	out := make([]*eventsv1.Event, 0, len(events))
	for _, e := range events {
		pe, err := domainToProto(e)
		if err != nil {
			h.log.Error("ListEvents: encode failed", "err", err)
			return nil, status.Error(codes.Internal, "failed to encode event")
		}
		out = append(out, pe)
	}

	return &eventsv1.ListEventsResponse{Events: out, NextPageToken: nextPageToken}, nil
}

func (h *EventHandler) GetStats(
	ctx context.Context, _ *eventsv1.GetStatsRequest,
) (*eventsv1.GetStatsResponse, error) {
	stats, err := h.svc.GetStats(ctx)
	if err != nil {
		return nil, mapError(h.log, err)
	}

	return &eventsv1.GetStatsResponse{
		TotalEvents: stats.TotalEvents,
		ByType:      stats.ByType,
		UniqueUsers: stats.UniqueUsers,
	}, nil
}

func mapError(log *slog.Logger, err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidUserID),
		errors.Is(err, domain.ErrEmptyEventType),
		errors.Is(err, domain.ErrInvalidPageToken):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		log.Error("internal error", "err", err)
		return status.Error(codes.Internal, "internal error")
	}
}

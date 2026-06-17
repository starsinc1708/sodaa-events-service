package grpc

import (
	eventsv1 "events/api/proto"
	"events/internal/domain"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func domainToProto(e domain.Event) (*eventsv1.Event, error) {
	var payload *structpb.Struct
	if e.Payload != nil {
		s, err := structpb.NewStruct(e.Payload)
		if err != nil {
			return nil, err
		}
		payload = s
	}

	return &eventsv1.Event{
		Id:        e.ID.String(),
		UserId:    e.UserID.String(),
		EventType: e.EventType,
		Payload:   payload,
		CreatedAt: timestamppb.New(e.CreatedAt),
	}, nil
}

func protoStructToMap(s *structpb.Struct) map[string]any {
	if s == nil {
		return nil
	}
	return s.AsMap()
}

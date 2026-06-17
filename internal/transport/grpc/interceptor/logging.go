package interceptor

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func Logging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := status.Code(err)

		log.LogAttrs(ctx, levelFor(code), "grpc request",
			slog.String("method", info.FullMethod),
			slog.String("code", code.String()),
			slog.Int64("duration_ms", time.Since(start).Milliseconds()),
			slog.String("ip", clientIP(ctx)),
		)
		return resp, err
	}
}

func levelFor(code codes.Code) slog.Level {
	switch code {
	case codes.OK:
		return slog.LevelInfo
	case codes.Internal, codes.Unknown, codes.DataLoss, codes.Unavailable:
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}

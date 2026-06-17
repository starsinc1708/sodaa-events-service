package interceptor

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func peerCtx(t *testing.T, addr string) context.Context {
	t.Helper()
	tcp, err := net.ResolveTCPAddr("tcp", addr)
	require.NoError(t, err)
	return peer.NewContext(context.Background(), &peer.Peer{Addr: tcp})
}

func okHandler(_ context.Context, _ any) (any, error) { return "ok", nil }

var testInfo = &grpc.UnaryServerInfo{FullMethod: "/event.v1.EventService/CreateEvent"}

func TestRateLimiter_TriggersOn61stRequest(t *testing.T) {
	t.Parallel()
	interc := NewRateLimiter(60).Unary()
	ctx := peerCtx(t, "1.2.3.4:5555")

	for i := 1; i <= 60; i++ {
		_, err := interc(ctx, nil, testInfo, okHandler)
		require.NoErrorf(t, err, "request %d should be allowed", i)
	}

	_, err := interc(ctx, nil, testInfo, okHandler)
	require.Error(t, err, "61st request must be rejected")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
}

func TestRateLimiter_PerClientIsolation(t *testing.T) {
	t.Parallel()
	interc := NewRateLimiter(1).Unary()
	clientA := peerCtx(t, "10.0.0.1:1111")
	clientB := peerCtx(t, "10.0.0.2:2222")

	_, err := interc(clientA, nil, testInfo, okHandler)
	require.NoError(t, err)
	_, err = interc(clientA, nil, testInfo, okHandler)
	require.Equal(t, codes.ResourceExhausted, status.Code(err), "client A exhausted")

	_, err = interc(clientB, nil, testInfo, okHandler)
	require.NoError(t, err)
}

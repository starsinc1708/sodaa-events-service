package interceptor

import (
	"context"
	"net"
	"sync"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type RateLimiter struct {
	mu      sync.Mutex
	clients map[string]*client
	limit   rate.Limit
	burst   int
	ttl     time.Duration
}

type client struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func NewRateLimiter(perMinute int) *RateLimiter {
	if perMinute <= 0 {
		perMinute = 60
	}
	return &RateLimiter{
		clients: make(map[string]*client),
		limit:   rate.Limit(float64(perMinute) / 60.0),
		burst:   perMinute,
		ttl:     3 * time.Minute,
	}
}

func (r *RateLimiter) allow(key string) bool {
	r.mu.Lock()
	c, ok := r.clients[key]
	if !ok {
		c = &client{limiter: rate.NewLimiter(r.limit, r.burst)}
		r.clients[key] = c
	}
	c.lastSeen = time.Now()
	r.mu.Unlock()

	return c.limiter.Allow()
}

func (r *RateLimiter) Cleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.mu.Lock()
			for key, c := range r.clients {
				if time.Since(c.lastSeen) > r.ttl {
					delete(r.clients, key)
				}
			}
			r.mu.Unlock()
		}
	}
}

func (r *RateLimiter) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !r.allow(clientIP(ctx)) {
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func clientIP(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.Addr == nil {
		return "unknown"
	}
	if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
		return host
	}
	return p.Addr.String()
}

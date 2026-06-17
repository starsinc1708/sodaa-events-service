package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	eventsv1 "events/api/proto"
	"events/internal/broker/kafka"
	"events/internal/config"
	"events/internal/domain"
	"events/internal/repository/clickhouse"
	"events/internal/repository/postgres"
	"events/internal/service"
	grpctransport "events/internal/transport/grpc"
	"events/internal/transport/grpc/interceptor"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
)

const gracefulTimeout = 15 * time.Second

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("api: fatal", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	eventRepo := postgres.NewEventPool(pool)

	var statsReader domain.StatsRepository = postgres.NewStatsRepo(pool)
	if cfg.ClickHouse.Enabled {
		chConn, err := clickhouse.New(ctx, cfg.ClickHouse.Addr, cfg.ClickHouse.DB, cfg.ClickHouse.User, cfg.ClickHouse.Password)
		if err != nil {
			return fmt.Errorf("connect clickhouse: %w", err)
		}
		defer chConn.Close()

		chRepo := clickhouse.NewStatsRepo(chConn)
		if err := chRepo.EnsureSchema(ctx); err != nil {
			return fmt.Errorf("clickhouse ensure schema: %w", err)
		}
		statsReader = chRepo
		log.Info("GetStats source: clickhouse")
	}

	if err := kafka.EnsureTopics(ctx, cfg.Kafka.Brokers, 1, cfg.Kafka.Topic); err != nil {
		return fmt.Errorf("ensure kafka topics: %w", err)
	}

	producer := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	defer producer.Close()

	svc := service.NewEventService(eventRepo, statsReader, producer, cfg.Kafka.Topic, log)
	handler := grpctransport.NewEventHandler(svc, log)

	rl := interceptor.NewRateLimiter(cfg.RateLimitPerMinute)

	srv := grpc.NewServer(grpc.ChainUnaryInterceptor(
		interceptor.Logging(log),
		interceptor.Recovery(log),
		rl.Unary(),
	))
	eventsv1.RegisterEventServiceServer(srv, handler)

	healthSrv := health.NewServer()
	healthgrpc.RegisterHealthServer(srv, healthSrv)
	setServing(healthSrv, healthgrpc.HealthCheckResponse_SERVING)

	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.GRPCPort, err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); rl.Cleanup(ctx) }()
	wg.Add(1)
	go func() { defer wg.Done(); watchHealth(ctx, pool, healthSrv) }()

	serveErr := make(chan error, 1)
	go func() {
		log.Info("grpc server listening", "addr", lis.Addr().String())
		serveErr <- srv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("grpc serve: %w", err)
	case <-ctx.Done():
		log.Info("shutdown signal received")
	}

	gracefulStop(srv)
	waitTimeout(&wg, gracefulTimeout)
	log.Info("api stopped")
	return nil
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
	}
}

func gracefulStop(srv *grpc.Server) {
	stopped := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(gracefulTimeout):
		srv.Stop()
	}
}

func watchHealth(ctx context.Context, pool *pgxpool.Pool, h *health.Server) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			h.Shutdown()
			return
		case <-ticker.C:
			st := healthgrpc.HealthCheckResponse_SERVING
			pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			if err := pool.Ping(pingCtx); err != nil {
				st = healthgrpc.HealthCheckResponse_NOT_SERVING
			}
			cancel()
			setServing(h, st)
		}
	}
}

func setServing(h *health.Server, st healthgrpc.HealthCheckResponse_ServingStatus) {
	h.SetServingStatus("", st)
	h.SetServingStatus(eventsv1.EventService_ServiceDesc.ServiceName, st)
}

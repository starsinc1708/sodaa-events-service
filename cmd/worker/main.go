package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"events/internal/broker/kafka"
	"events/internal/config"
	"events/internal/domain"
	"events/internal/repository/clickhouse"
	"events/internal/repository/postgres"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("worker: fatal", "err", err)
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

	statsRepo := postgres.NewStatsRepo(pool)

	var sink domain.EventSink
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
		sink = chRepo
		log.Info("clickhouse sink enabled")
	}

	if err := kafka.EnsureTopics(ctx, cfg.Kafka.Brokers, 1, cfg.Kafka.Topic, cfg.Kafka.DLQTopic); err != nil {
		return fmt.Errorf("ensure kafka topics: %w", err)
	}

	reader := kafka.NewReader(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID)
	defer reader.Close()

	dlq := kafka.NewDLQWriter(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic)
	defer dlq.Close()

	consumer := kafka.NewConsumer(reader, dlq, statsRepo, sink, log)

	if err := consumer.Run(ctx); err != nil {
		return fmt.Errorf("consumer run: %w", err)
	}
	return nil
}

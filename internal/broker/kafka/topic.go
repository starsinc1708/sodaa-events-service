package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

func EnsureTopics(ctx context.Context, brokers []string, partitions int, topics ...string) error {
	const attempts = 5
	var lastErr error
	for i := 0; i < attempts; i++ {
		if lastErr = ensureTopicsOnce(ctx, brokers, partitions, topics...); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("ensure topics after %d attempts: %w", attempts, lastErr)
}

func ensureTopicsOnce(ctx context.Context, brokers []string, partitions int, topics ...string) error {
	if len(brokers) == 0 {
		return errors.New("no kafka brokers configured")
	}

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

package config

import "github.com/ilyakaznacheev/cleanenv"

type Config struct {
	GRPCPort    string `env:"GRPC_PORT" env-default:"50051"`
	PostgresDSN string `env:"POSTGRES_DSN" env-required:"true"`

	Kafka struct {
		Brokers  []string `env:"KAFKA_BROKERS" env-separator:"," env-required:"true"`
		Topic    string   `env:"KAFKA_TOPIC" env-default:"events"`
		DLQTopic string   `env:"KAFKA_DLQ_TOPIC" env-default:"events.dlq"`
		GroupID  string   `env:"KAFKA_GROUP_ID" env-default:"event-stats-worker"`
	}

	ClickHouse struct {
		Enabled  bool   `env:"CLICKHOUSE_ENABLED" env-default:"false"`
		Addr     string `env:"CLICKHOUSE_ADDR" env-default:"clickhouse:9000"`
		DB       string `env:"CLICKHOUSE_DB" env-default:"analytics"`
		User     string `env:"CLICKHOUSE_USER" env-default:"app"`
		Password string `env:"CLICKHOUSE_PASSWORD" env-default:"app"`
	}

	RateLimitPerMinute int `env:"RATE_LIMIT_PER_MINUTE" env-default:"60"`
}

func Load() (*Config, error) {
	var cfg Config
	if err := cleanenv.ReadEnv(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

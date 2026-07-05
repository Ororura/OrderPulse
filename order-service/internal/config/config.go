package config

import (
	"log"
	"os"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPPort               string
	DBURL                  string
	KafkaBrokets           []string
	KafkaOrderCreatedTopic string
}

func MustLoad() Config {
	_ = godotenv.Load()

	cfg := Config{
		HTTPPort:               getEnv("HTTP_PORT", "8080"),
		DBURL:                  getEnv("DB_URL", ""),
		KafkaBrokets:           strings.Split(getEnv("KAFKA_BROKETS", "localhost:9092"), ","),
		KafkaOrderCreatedTopic: getEnv("KAFKA_ORDER_CREATED_TOPIC", "orders.created"),
	}

	if cfg.DBURL == "" {
		log.Fatalf("DB_URL is required")
	}

	return cfg
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

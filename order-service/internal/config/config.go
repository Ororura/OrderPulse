package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPPort               string
	DBURL                  string
	KafkaBrokets           []string
	KafkaOrderCreatedTopic string
	RedisAddr              string
	RedisPassword          string
	RedisDB                int
	OrderCacheTTL          time.Duration
}

func MustLoad() Config {
	_ = godotenv.Load()

	redisDB, err := strconv.Atoi(getEnv("REDIS_DB", "0"))
	if err != nil {
		log.Fatalf("REDIS_DB is invalid")
	}

	cacheTTL, err := time.ParseDuration(getEnv("ORDER_CACHE_TTL", "5m"))
	if err != nil {
		log.Fatalf("ORDER_CACHE_TTL is invalid")
	}

	cfg := Config{
		HTTPPort:               getEnv("HTTP_PORT", "8080"),
		DBURL:                  getEnv("DB_URL", ""),
		KafkaBrokets:           strings.Split(getEnv("KAFKA_BROKETS", "localhost:9092"), ","),
		KafkaOrderCreatedTopic: getEnv("KAFKA_ORDER_CREATED_TOPIC", "orders.created"),
		RedisAddr:              getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:          getEnv("REDIS_PASSWORD", ""),
		RedisDB:                redisDB,
		OrderCacheTTL:          cacheTTL,
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

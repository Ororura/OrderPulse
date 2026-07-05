package config

import (
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

type Config struct {
	KafkaBrokers           []string
	KafkaOrderCreatedTopic string
	KafkaConsumerGroup     string

	TelegramBotToken string
	TelegramChatID   int64
}

func MustLoad() Config {
	_ = godotenv.Load()

	chatID, err := strconv.ParseInt(getEnv("TELEGRAM_CHAT_ID", ""), 10, 64)

	if err != nil {
		log.Fatalf("TELEGRAM_CHAT_ID is invalid")
	}

	cfg := Config{
		KafkaBrokers:           strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaOrderCreatedTopic: getEnv("KAFKA_ORDER_CREATED_TOPIC", "orders.created"),
		KafkaConsumerGroup:     getEnv("KAFKA_CONSUMER_GROUP", "notification-service"),

		TelegramBotToken: getEnv("TELEGRAM_BOT_TOKEN", ""),
		TelegramChatID:   chatID,
	}

	if cfg.TelegramBotToken == "" {
		log.Fatalf("TELEGRAM BOT TOKEN is required")
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

package main

import (
	"context"
	"errors"
	"log"
	"notification-service/internal/config"
	"notification-service/internal/consumer"
	"notification-service/internal/telegram"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := config.MustLoad()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	telegramClient := telegram.NewClient(cfg.TelegramBotToken, cfg.TelegramChatID)

	orderConsumer := consumer.NewOrderConsumer(cfg.KafkaBrokers,
		cfg.KafkaOrderCreatedTopic,
		cfg.KafkaConsumerGroup,
		telegramClient)
	defer orderConsumer.Close()

	if err := orderConsumer.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal("consumer error:", err)
	}

	log.Println("notification-service stopped")
}

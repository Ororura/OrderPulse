package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"notification-service/internal/telegram"

	kafkapkg "notification-service/internal/kafka"

	"github.com/segmentio/kafka-go"
)

type OrderConsumer struct {
	reader         *kafka.Reader
	telegramClient *telegram.Client
}

func NewOrderConsumer(brokers []string, topic string, groupID string, telegramClient *telegram.Client) *OrderConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})

	return &OrderConsumer{
		reader:         reader,
		telegramClient: telegramClient,
	}
}

func (c *OrderConsumer) Start(ctx context.Context) error {
	log.Println("notification-service started")

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			log.Println("failed to read kafka message", err)
			continue
		}

		var event kafkapkg.OrderCreatedEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to decode ordder event:", err)
			continue
		}

		text := fmt.Sprintf("🛒 Новый заказ\n\nOrder ID: %d\nUser ID: %d\nProduct ID: %d\nCount: %d\nStatus: %s", event.OrderID,
			event.UserID,
			event.ProductID,
			event.Count,
			event.Status)

		if err := c.telegramClient.SendMessage(ctx, text); err != nil {
			log.Println("failed to send telegram message:", err)
			continue
		}

		log.Printf(
			"telegram notification sent: order_id=%d partition=%d offset=%d",
			event.OrderID,
			msg.Partition,
			msg.Offset,
		)
	}
}

func (c *OrderConsumer) Close() error {
	return c.reader.Close()
}

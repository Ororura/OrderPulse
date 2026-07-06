package consumer

import (
	"context"
	"encoding/json"
	"log"
	"notification-service/internal/domain"

	"github.com/segmentio/kafka-go"
)

type OrderCreatedHandler interface {
	NotifyOrderCreated(ctx context.Context, event domain.OrderCreatedEvent) error
}

type OrderConsumer struct {
	reader  *kafka.Reader
	handler OrderCreatedHandler
}

func NewOrderConsumer(brokers []string, topic string, groupID string, handler OrderCreatedHandler) *OrderConsumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})

	return &OrderConsumer{
		reader:  reader,
		handler: handler,
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

		var event domain.OrderCreatedEvent

		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Println("failed to decode order event:", err)
			continue
		}

		if err := c.handler.NotifyOrderCreated(ctx, event); err != nil {
			log.Println("failed to send order notification:", err)
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

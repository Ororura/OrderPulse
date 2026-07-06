package service

import (
	"context"
	"order-service/internal/kafka"
	"order-service/internal/model"
)

type EventProducer interface {
	PublishOrderCreated(ctx context.Context, event kafka.OrderCreatedEvent) error
}

type OrderRepository interface {
	Create(ctx context.Context, order model.Order) (model.Order, error)
}

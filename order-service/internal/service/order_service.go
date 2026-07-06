package service

import (
	"context"
	"errors"
	"order-service/internal/kafka"
	"order-service/internal/model"
)

type OrderService struct {
	repo     OrderRepository
	producer EventProducer
}

func NewOrderService(repo OrderRepository, producer EventProducer) *OrderService {
	return &OrderService{
		repo, producer,
	}
}

func (s *OrderService) CreateOrder(ctx context.Context, req model.CreateOrderRequest) (model.Order, error) {
	if req.UserID <= 0 {
		return model.Order{}, errors.New("user_id is required")
	}

	if req.ProductID <= 0 {
		return model.Order{}, errors.New("product_id is required")
	}

	if req.Count <= 0 {
		return model.Order{}, errors.New("count must be greater than zero")
	}

	order := model.Order{
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Count:     int(req.Count),
		Status:    "created",
	}

	created, err := s.repo.Create(ctx, order)
	if err != nil {
		return model.Order{}, err
	}

	event := kafka.OrderCreatedEvent{
		OrderID:   created.ID,
		UserID:    created.UserID,
		ProductID: created.ProductID,
		Count:     created.Count,
		Status:    created.Status,
		CreatedAt: created.CreatedAt,
	}

	if err := s.producer.PublishOrderCreated(ctx, event); err != nil {
		return model.Order{}, err
	}

	return created, nil
}

package service

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"order-service/internal/kafka"
	"order-service/internal/model"
)

var (
	ErrInvalidOrderID = errors.New("order_id must be greater than zero")
	ErrOrderNotFound  = errors.New("order not found")
)

type OrderRepository interface {
	Create(ctx context.Context, order model.Order) (model.Order, error)
	GetByID(ctx context.Context, id int64) (model.Order, error)
}

type OrderCreatedPublisher interface {
	PublishOrderCreated(ctx context.Context, event kafka.OrderCreatedEvent) error
}

type OrderCache interface {
	GetOrder(ctx context.Context, id int64) (model.Order, bool, error)
	SetOrder(ctx context.Context, order model.Order) error
}

type OrderService struct {
	repo     OrderRepository
	producer OrderCreatedPublisher
	cache    OrderCache
}

func NewOrderService(repo OrderRepository, producer OrderCreatedPublisher, cache OrderCache) *OrderService {
	return &OrderService{
		repo:     repo,
		producer: producer,
		cache:    cache,
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

	if s.cache != nil {
		if err := s.cache.SetOrder(ctx, created); err != nil {
			log.Println("failed to cache created order:", err)
		}
	}

	return created, nil
}

func (s *OrderService) GetOrder(ctx context.Context, id int64) (model.Order, error) {
	if id <= 0 {
		return model.Order{}, ErrInvalidOrderID
	}

	if s.cache != nil {
		order, ok, err := s.cache.GetOrder(ctx, id)
		if err != nil {
			log.Println("failed to read order cache:", err)
		} else if ok {
			return order, nil
		}
	}

	order, err := s.repo.GetByID(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Order{}, ErrOrderNotFound
	}
	if err != nil {
		return model.Order{}, err
	}

	if s.cache != nil {
		if err := s.cache.SetOrder(ctx, order); err != nil {
			log.Println("failed to cache order:", err)
		}
	}

	return order, nil
}

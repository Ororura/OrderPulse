package service

import (
	"context"
	"database/sql"
	"errors"
	"order-service/internal/kafka"
	"order-service/internal/model"
	"testing"
	"time"
)

type fakeRepository struct {
	order      model.Order
	err        error
	getByIDHit int
}

func (r *fakeRepository) Create(_ context.Context, order model.Order) (model.Order, error) {
	if r.err != nil {
		return model.Order{}, r.err
	}

	order.ID = 1
	order.CreatedAt = time.Now()
	return order, nil
}

func (r *fakeRepository) GetByID(_ context.Context, _ int64) (model.Order, error) {
	r.getByIDHit++
	if r.err != nil {
		return model.Order{}, r.err
	}

	return r.order, nil
}

type fakePublisher struct {
	err error
}

func (p fakePublisher) PublishOrderCreated(_ context.Context, _ kafka.OrderCreatedEvent) error {
	return p.err
}

type fakeCache struct {
	order model.Order
	hit   bool
	err   error
	set   int
	get   int
}

func (c *fakeCache) GetOrder(_ context.Context, _ int64) (model.Order, bool, error) {
	c.get++
	if c.err != nil {
		return model.Order{}, false, c.err
	}

	return c.order, c.hit, nil
}

func (c *fakeCache) SetOrder(_ context.Context, order model.Order) error {
	c.set++
	c.order = order
	return c.err
}

func TestGetOrderReturnsCacheHit(t *testing.T) {
	cached := model.Order{ID: 1, UserID: 10, ProductID: 20, Count: 2, Status: "created"}
	repo := &fakeRepository{}
	cache := &fakeCache{order: cached, hit: true}
	svc := NewOrderService(repo, fakePublisher{}, cache)

	order, err := svc.GetOrder(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}
	if order.ID != cached.ID {
		t.Fatalf("GetOrder() id = %d, want %d", order.ID, cached.ID)
	}
	if repo.getByIDHit != 0 {
		t.Fatalf("repository hit = %d, want 0", repo.getByIDHit)
	}
}

func TestGetOrderCachesRepositoryResultOnMiss(t *testing.T) {
	stored := model.Order{ID: 1, UserID: 10, ProductID: 20, Count: 2, Status: "created"}
	repo := &fakeRepository{order: stored}
	cache := &fakeCache{}
	svc := NewOrderService(repo, fakePublisher{}, cache)

	order, err := svc.GetOrder(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}
	if order.ID != stored.ID {
		t.Fatalf("GetOrder() id = %d, want %d", order.ID, stored.ID)
	}
	if repo.getByIDHit != 1 {
		t.Fatalf("repository hit = %d, want 1", repo.getByIDHit)
	}
	if cache.set != 1 {
		t.Fatalf("cache set = %d, want 1", cache.set)
	}
}

func TestGetOrderFallsBackToRepositoryOnCacheError(t *testing.T) {
	stored := model.Order{ID: 1, UserID: 10, ProductID: 20, Count: 2, Status: "created"}
	repo := &fakeRepository{order: stored}
	cache := &fakeCache{err: errors.New("redis down")}
	svc := NewOrderService(repo, fakePublisher{}, cache)

	order, err := svc.GetOrder(context.Background(), 1)
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}
	if order.ID != stored.ID {
		t.Fatalf("GetOrder() id = %d, want %d", order.ID, stored.ID)
	}
	if repo.getByIDHit != 1 {
		t.Fatalf("repository hit = %d, want 1", repo.getByIDHit)
	}
}

func TestGetOrderMapsNoRowsToNotFound(t *testing.T) {
	repo := &fakeRepository{err: sql.ErrNoRows}
	svc := NewOrderService(repo, fakePublisher{}, nil)

	_, err := svc.GetOrder(context.Background(), 1)
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("GetOrder() error = %v, want %v", err, ErrOrderNotFound)
	}
}

func TestGetOrderRejectsInvalidID(t *testing.T) {
	svc := NewOrderService(&fakeRepository{}, fakePublisher{}, nil)

	_, err := svc.GetOrder(context.Background(), 0)
	if !errors.Is(err, ErrInvalidOrderID) {
		t.Fatalf("GetOrder() error = %v, want %v", err, ErrInvalidOrderID)
	}
}

func TestCreateOrderDoesNotCacheWhenPublishFails(t *testing.T) {
	cache := &fakeCache{}
	svc := NewOrderService(&fakeRepository{}, fakePublisher{err: errors.New("kafka down")}, cache)

	_, err := svc.CreateOrder(context.Background(), model.CreateOrderRequest{UserID: 1, ProductID: 2, Count: 3})
	if err == nil {
		t.Fatal("CreateOrder() error = nil, want error")
	}
	if cache.set != 0 {
		t.Fatalf("cache set = %d, want 0", cache.set)
	}
}

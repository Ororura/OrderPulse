package handler

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"order-service/internal/kafka"
	"order-service/internal/model"
	"order-service/internal/service"
	"testing"
	"time"
)

type handlerRepository struct {
	order model.Order
	err   error
}

func (r handlerRepository) Create(_ context.Context, order model.Order) (model.Order, error) {
	return order, nil
}

func (r handlerRepository) GetByID(_ context.Context, _ int64) (model.Order, error) {
	if r.err != nil {
		return model.Order{}, r.err
	}

	return r.order, nil
}

type handlerPublisher struct{}

func (p handlerPublisher) PublishOrderCreated(_ context.Context, _ kafka.OrderCreatedEvent) error {
	return nil
}

func TestGetOrderSuccess(t *testing.T) {
	want := model.Order{ID: 1, UserID: 10, ProductID: 20, Count: 2, Status: "created", CreatedAt: time.Now()}
	h := newTestHandler(handlerRepository{order: want})
	req := httptest.NewRequest(http.MethodGet, "/orders/1", nil)
	req.SetPathValue("id", "1")
	res := httptest.NewRecorder()

	h.GetOrder(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusOK)
	}

	var got model.Order
	if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("id = %d, want %d", got.ID, want.ID)
	}
}

func TestGetOrderInvalidID(t *testing.T) {
	h := newTestHandler(handlerRepository{})
	req := httptest.NewRequest(http.MethodGet, "/orders/abc", nil)
	req.SetPathValue("id", "abc")
	res := httptest.NewRecorder()

	h.GetOrder(res, req)

	if res.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusBadRequest)
	}
}

func TestGetOrderNotFound(t *testing.T) {
	h := newTestHandler(handlerRepository{err: sql.ErrNoRows})
	req := httptest.NewRequest(http.MethodGet, "/orders/1", nil)
	req.SetPathValue("id", "1")
	res := httptest.NewRecorder()

	h.GetOrder(res, req)

	if res.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", res.Code, http.StatusNotFound)
	}
}

func newTestHandler(repo handlerRepository) *OrderHandler {
	svc := service.NewOrderService(repo, handlerPublisher{}, nil)
	return NewOrderHandler(svc)
}

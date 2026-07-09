package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"order-service/internal/model"
	"order-service/internal/service"
	"strconv"
)

type OrderHandler struct {
	service *service.OrderService
}

func NewOrderHandler(service *service.OrderService) *OrderHandler {
	return &OrderHandler{service}
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	var req model.CreateOrderRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid request body",
		})

		return
	}

	order, err := h.service.CreateOrder(r.Context(), req)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "failed to create order",
		})

		return
	}

	resp := model.CreateOrderResponse{
		ID:      order.ID,
		Status:  order.Status,
		Message: "order created",
	}

	writeJSON(w, http.StatusCreated, resp)
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "invalid order id",
		})

		return
	}

	order, err := h.service.GetOrder(r.Context(), id)
	if errors.Is(err, service.ErrOrderNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{
			"error": "order not found",
		})

		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "failed to get order",
		})

		return
	}

	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(data)
}

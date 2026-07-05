package model

import "time"

type Order struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	ProductID int64     `json:"product_id"`
	Count     int       `json:"count"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateOrderRequest struct {
	UserID    int64 `json:"user_id"`
	ProductID int64 `json:"product_id"`
	Count     int64 `json:"count"`
}

type CreateOrderResponse struct {
	ID      int64  `json:"id"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

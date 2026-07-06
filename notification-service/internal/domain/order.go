package domain

import "time"

type OrderCreatedEvent struct {
	OrderID   int64     `json:"order_id"`
	UserID    int64     `json:"user_id"`
	ProductID int64     `json:"product_id"`
	Count     int       `json:"count"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

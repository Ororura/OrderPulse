package repository

import (
	"context"
	"database/sql"
	"order-service/internal/model"
)

type PostgresOrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(db *sql.DB) *PostgresOrderRepository {
	return &PostgresOrderRepository{db: db}
}

func (r *PostgresOrderRepository) Create(ctx context.Context, order model.Order) (model.Order, error) {
	query := `
		INSERT INTO orders (user_id, product_id, count, status)
		VALUES ($1, $2, $3, $4)
		RETURNING id, user_id, product_id, count, status, created_at
	`

	var created model.Order

	err := r.db.QueryRowContext(
		ctx,
		query,
		order.UserID,
		order.ProductID,
		order.Count,
		order.Status,
	).Scan(&created.ID, &created.UserID, &created.ProductID, &created.Count, &created.Status, &created.CreatedAt)

	if err != nil {
		return model.Order{}, err
	}

	return created, nil
}

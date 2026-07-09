package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"order-service/internal/model"
	"time"

	"github.com/redis/go-redis/v9"
)

type OrderCache struct {
	client *redis.Client
	ttl    time.Duration
}

func NewOrderCache(addr string, password string, db int, ttl time.Duration) *OrderCache {
	return &OrderCache{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
		ttl: ttl,
	}
}

func (c *OrderCache) GetOrder(ctx context.Context, id int64) (model.Order, bool, error) {
	value, err := c.client.Get(ctx, orderKey(id)).Result()
	if err == redis.Nil {
		return model.Order{}, false, nil
	}
	if err != nil {
		return model.Order{}, false, err
	}

	var order model.Order
	if err := json.Unmarshal([]byte(value), &order); err != nil {
		return model.Order{}, false, err
	}

	return order, true, nil
}

func (c *OrderCache) SetOrder(ctx context.Context, order model.Order) error {
	value, err := json.Marshal(order)
	if err != nil {
		return err
	}

	return c.client.Set(ctx, orderKey(order.ID), value, c.ttl).Err()
}

func (c *OrderCache) Close() error {
	return c.client.Close()
}

func orderKey(id int64) string {
	return fmt.Sprintf("order:%d", id)
}

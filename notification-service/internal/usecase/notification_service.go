package usecase

import (
	"context"
	"fmt"
	"notification-service/internal/domain"
)

type MessageSender interface {
	SendMessage(ctx context.Context, text string) error
}

type NotificationService struct {
	sender MessageSender
}

func NewNotificationService(sender MessageSender) *NotificationService {
	return &NotificationService{
		sender: sender,
	}
}

func (s *NotificationService) NotifyOrderCreated(ctx context.Context, event domain.OrderCreatedEvent) error {
	text := fmt.Sprintf(
		"🛒 Новый заказ\n\nOrder ID: %d\nUser ID: %d\nProduct ID: %d\nCount: %d\nStatus: %s",
		event.OrderID,
		event.UserID,
		event.ProductID,
		event.Count,
		event.Status,
	)

	return s.sender.SendMessage(ctx, text)
}

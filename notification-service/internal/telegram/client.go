package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Client struct {
	botToken   string
	chatID     int64
	httpClient *http.Client
}

type sendMessageRequest struct {
	ChatID int64  `json:"chat_id"`
	Text   string `json:"text"`
}

func NewClient(botToken string, chatID int64) *Client {
	return &Client{
		botToken: botToken,
		chatID:   chatID,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *Client) SendMessage(ctx context.Context, text string) error {
	url := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", c.botToken)

	body, err := json.Marshal(sendMessageRequest{
		ChatID: c.chatID,
		Text:   text,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("telegram returned status: %s", resp.Status)
	}

	return nil
}

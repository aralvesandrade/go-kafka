package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"

	"estudos.com/mysql-kafka/internal/domain"
)

// Consumer wraps a kafka.Reader to read user events.
type Consumer struct {
	reader *kafka.Reader
}

// NewConsumer returns a Consumer configured for the given broker, topic, and group.
func NewConsumer(broker, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker},
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

// Read fetches the next message and unmarshals its JSON value into a domain.User.
// It returns the raw kafka.Message alongside so callers can commit it.
func (c *Consumer) Read(ctx context.Context) (domain.User, kafka.Message, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return domain.User{}, kafka.Message{}, fmt.Errorf("kafka consumer: fetch message: %w", err)
	}

	var payload struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		return domain.User{}, msg, fmt.Errorf("kafka consumer: unmarshal: %w", err)
	}

	return domain.User{ID: payload.ID, Name: payload.Name}, msg, nil
}

// Commit commits the offset for the given message.
func (c *Consumer) Commit(ctx context.Context, msg kafka.Message) error {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka consumer: commit: %w", err)
	}
	return nil
}

// Close shuts down the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}

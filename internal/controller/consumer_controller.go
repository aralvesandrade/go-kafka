package controller

import (
	"context"
	"errors"
	"log/slog"

	"github.com/segmentio/kafka-go"

	"estudos.com/mysql-kafka/internal/domain"
)

// MessageConsumer is the interface satisfied by kafka.Consumer.
type MessageConsumer interface {
	Read(ctx context.Context) (domain.User, kafka.Message, error)
	Commit(ctx context.Context, msg kafka.Message) error
}

// ConsumerController orchestrates the consume → log pipeline.
type ConsumerController struct {
	consumer MessageConsumer
	log      *slog.Logger
}

// NewConsumerController returns a new ConsumerController.
func NewConsumerController(consumer MessageConsumer, log *slog.Logger) *ConsumerController {
	return &ConsumerController{consumer: consumer, log: log}
}

// Run reads from Kafka until the context is cancelled.
func (c *ConsumerController) Run(ctx context.Context) error {
	for {
		user, msg, err := c.consumer.Read(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				c.log.InfoContext(ctx, "consumer shutting down", "op", "kafka_consume")
				return nil
			}
			// Malformed message: log error, commit offset, continue.
			c.log.ErrorContext(ctx, "message read error", "op", "kafka_consume", "error", err)
			if commitErr := c.consumer.Commit(context.Background(), msg); commitErr != nil {
				c.log.ErrorContext(ctx, "commit failed after read error", "op", "kafka_consume", "error", commitErr)
			}
			continue
		}

		c.log.InfoContext(ctx, "user consumed", "op", "kafka_consume", "id", user.ID, "name", user.Name)

		if err := c.consumer.Commit(context.Background(), msg); err != nil {
			c.log.ErrorContext(ctx, "commit failed", "op", "kafka_consume", "id", user.ID, "error", err)
		}
	}
}

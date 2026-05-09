package controller

import (
	"context"
	"fmt"
	"log/slog"

	"estudos.com/mysql-kafka/internal/domain"
)

// APIClient is the interface satisfied by apiclient.Client.
type APIClient interface {
	FetchUsers(ctx context.Context) ([]domain.User, error)
}

// UserSaver is the interface satisfied by repository.MySQLUserRepository.
type UserSaver interface {
	Save(ctx context.Context, user domain.User) (domain.User, error)
}

// MessagePublisher is the interface satisfied by kafka.Producer.
type MessagePublisher interface {
	Publish(ctx context.Context, user domain.User) error
}

// ProducerController orchestrates the fetch → save → publish pipeline.
type ProducerController struct {
	client   APIClient
	repo     UserSaver
	producer MessagePublisher
	log      *slog.Logger
}

// NewProducerController returns a new ProducerController.
func NewProducerController(
	client APIClient,
	repo UserSaver,
	producer MessagePublisher,
	log *slog.Logger,
) *ProducerController {
	return &ProducerController{
		client:   client,
		repo:     repo,
		producer: producer,
		log:      log,
	}
}

// Run executes the full fetch → save → publish cycle.
func (c *ProducerController) Run(ctx context.Context) error {
	users, err := c.client.FetchUsers(ctx)
	if err != nil {
		c.log.ErrorContext(ctx, "api fetch failed", "op", "api_fetch", "error", err)
		return fmt.Errorf("producer controller: fetch users: %w", err)
	}
	c.log.InfoContext(ctx, "api fetch complete", "op", "api_fetch", "count", len(users))

	if len(users) == 0 {
		c.log.InfoContext(ctx, "no users to process, exiting cleanly", "op", "api_fetch")
		return nil
	}

	total := 0
	for _, u := range users {
		saved, err := c.repo.Save(ctx, u)
		if err != nil {
			c.log.ErrorContext(ctx, "db insert failed", "op", "db_insert", "name", u.Name, "error", err)
			return fmt.Errorf("producer controller: save user %q: %w", u.Name, err)
		}
		c.log.InfoContext(ctx, "user saved", "op", "db_insert", "id", saved.ID, "name", saved.Name)

		if err := c.producer.Publish(ctx, saved); err != nil {
			c.log.ErrorContext(ctx, "kafka publish failed", "op", "kafka_publish", "id", saved.ID, "name", saved.Name, "error", err)
			// Non-fatal: log and continue per spec edge case.
			continue
		}
		c.log.InfoContext(ctx, "message published", "op", "kafka_publish", "id", saved.ID, "name", saved.Name)
		total++
	}

	c.log.InfoContext(ctx, "producer run complete", "op", "summary", "total", total)
	return nil
}

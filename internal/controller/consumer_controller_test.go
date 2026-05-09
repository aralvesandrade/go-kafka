package controller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"estudos.com/mysql-kafka/internal/controller"
	"estudos.com/mysql-kafka/internal/domain"
)

// --- Fake consumer ---

type fakeConsumer struct {
	messages []consumerMsg
	idx      int
}

type consumerMsg struct {
	user domain.User
	msg  kafka.Message
	err  error
}

func (f *fakeConsumer) Read(ctx context.Context) (domain.User, kafka.Message, error) {
	if ctx.Err() != nil {
		return domain.User{}, kafka.Message{}, ctx.Err()
	}
	if f.idx >= len(f.messages) {
		// Block until context cancelled.
		<-ctx.Done()
		return domain.User{}, kafka.Message{}, ctx.Err()
	}
	m := f.messages[f.idx]
	f.idx++
	return m.user, m.msg, m.err
}

func (f *fakeConsumer) Commit(_ context.Context, _ kafka.Message) error {
	return nil
}

// --- Tests ---

func TestConsumerController_NormalMessage(t *testing.T) {
	consumer := &fakeConsumer{
		messages: []consumerMsg{
			{user: domain.User{ID: 1, Name: "Alice"}, msg: kafka.Message{}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctrl := controller.NewConsumerController(consumer, slog.Default())

	// Cancel after first message so the loop exits.
	go func() {
		// Wait until message processed (idx advances).
		for consumer.idx < 1 {
		}
		cancel()
	}()

	err := ctrl.Run(ctx)
	require.NoError(t, err)
}

func TestConsumerController_MalformedMessage_Continues(t *testing.T) {
	consumer := &fakeConsumer{
		messages: []consumerMsg{
			{err: errors.New("unmarshal error"), msg: kafka.Message{}},
			{user: domain.User{ID: 2, Name: "Bob"}, msg: kafka.Message{}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctrl := controller.NewConsumerController(consumer, slog.Default())

	go func() {
		for consumer.idx < 2 {
		}
		cancel()
	}()

	err := ctrl.Run(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, consumer.idx)
}

func TestConsumerController_ContextCancel(t *testing.T) {
	consumer := &fakeConsumer{messages: []consumerMsg{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	ctrl := controller.NewConsumerController(consumer, slog.Default())
	err := ctrl.Run(ctx)
	assert.NoError(t, err)
}

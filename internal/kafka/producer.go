package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"

	"estudos.com/mysql-kafka/internal/domain"
)

// Producer wraps a kafka.Writer to publish user events.
type Producer struct {
	broker string
	topic  string
	writer *kafka.Writer
}

// NewProducer returns a Producer configured to write to the given broker and topic.
func NewProducer(broker, topic string) *Producer {
	return &Producer{
		broker: broker,
		topic:  topic,
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(broker),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

// EnsureTopic creates the topic on the broker if it does not already exist.
func (p *Producer) EnsureTopic(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", p.broker)
	if err != nil {
		return fmt.Errorf("kafka: dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka: get controller: %w", err)
	}

	ctrlConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("kafka: dial controller: %w", err)
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil {
		return fmt.Errorf("kafka: create topic: %w", err)
	}
	return nil
}

// Publish marshals a domain.User to JSON and writes it to Kafka.
func (p *Producer) Publish(ctx context.Context, user domain.User) error {
	value, err := json.Marshal(struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}{ID: user.ID, Name: user.Name})
	if err != nil {
		return fmt.Errorf("kafka producer: marshal: %w", err)
	}
	msg := kafka.Message{
		Key:   []byte(strconv.FormatInt(user.ID, 10)),
		Value: value,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("kafka producer: write message: %w", err)
	}
	return nil
}

// Close shuts down the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}

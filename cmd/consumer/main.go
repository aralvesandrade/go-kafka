package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"estudos.com/mysql-kafka/config"
	"estudos.com/mysql-kafka/internal/controller"
	"estudos.com/mysql-kafka/internal/kafka"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	consumer := kafka.NewConsumer(cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID)
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("consumer starting", "op", "kafka_consume", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)

	ctrl := controller.NewConsumerController(consumer, slog.Default())
	if err := ctrl.Run(ctx); err != nil {
		slog.Error("consumer run failed", "error", err)
		os.Exit(1)
	}
}

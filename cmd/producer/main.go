package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"estudos.com/mysql-kafka/cmd/producer/config"
	"estudos.com/mysql-kafka/internal/apiclient"
	"estudos.com/mysql-kafka/internal/controller"
	"estudos.com/mysql-kafka/internal/kafka"
	"estudos.com/mysql-kafka/internal/repository"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("mysql", cfg.DBDSN)
	if err != nil {
		slog.Error("db open failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		slog.Error("db ping failed", "error", err)
		os.Exit(1)
	}

	apiClient := apiclient.NewClient(cfg.APIURL)
	repo := repository.NewMySQLUserRepository(db)
	producer := kafka.NewProducer(cfg.KafkaBroker, cfg.KafkaTopic)
	defer producer.Close()

	if err := producer.EnsureTopic(context.Background()); err != nil {
		slog.Error("kafka ensure topic failed", "error", err)
		os.Exit(1)
	}

	ctrl := controller.NewProducerController(apiClient, repo, producer, slog.Default())
	if err := ctrl.Run(context.Background()); err != nil {
		slog.Error("producer run failed", "error", err)
		os.Exit(1)
	}
}

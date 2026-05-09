package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"estudos.com/mysql-kafka/cmd/monitor/config"
	"estudos.com/mysql-kafka/internal/controller"
	internalkafka "estudos.com/mysql-kafka/internal/kafka"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "error", err)
		os.Exit(1)
	}

	addr := cfg.PORT
	if addr == "" {
		addr = ":8081"
	}

	lagChecker := internalkafka.NewLagChecker(cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID)
	ctrl := controller.NewMonitorController(lagChecker, slog.Default())

	mux := http.NewServeMux()
	ctrl.RegisterRoutes(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", addr),
		Handler: mux,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		if err := server.Shutdown(context.Background()); err != nil {
			slog.Error("monitor shutdown error", "error", err)
		}
	}()

	slog.Info("monitor starting", "op", "monitor_start", "addr", addr, "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("monitor server failed", "error", err)
		os.Exit(1)
	}
}

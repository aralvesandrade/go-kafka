package controller

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"

	internalkafka "estudos.com/mysql-kafka/internal/kafka"
)

// LagReader is the interface for fetching consumer group lag from Kafka.
type LagReader interface {
	ReadLag(ctx context.Context) (internalkafka.LagReport, error)
}

// MonitorController handles HTTP requests for Kafka lag metrics.
type MonitorController struct {
	reader LagReader
	log    *slog.Logger
}

// NewMonitorController returns a new MonitorController.
func NewMonitorController(reader LagReader, log *slog.Logger) *MonitorController {
	return &MonitorController{reader: reader, log: log}
}

// RegisterRoutes registers the controller's routes on the given mux.
func (m *MonitorController) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /metrics/lag", m.handleLag)
}

func (m *MonitorController) handleLag(w http.ResponseWriter, r *http.Request) {
	report, err := m.reader.ReadLag(r.Context())
	if err != nil {
		m.log.ErrorContext(r.Context(), "lag read error", "op", "monitor_lag", "error", err)
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}

	m.log.InfoContext(r.Context(), "lag fetched", "op", "monitor_lag", "total_lag", report.TotalLag)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(report) //nolint:errcheck
}

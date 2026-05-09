package controller_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"estudos.com/mysql-kafka/internal/controller"
	internalkafka "estudos.com/mysql-kafka/internal/kafka"
)

// --- Fake LagReader ---

type fakeLagReader struct {
	report internalkafka.LagReport
	err    error
}

func (f *fakeLagReader) ReadLag(_ context.Context) (internalkafka.LagReport, error) {
	return f.report, f.err
}

// --- Tests ---

func TestMonitorController_Success(t *testing.T) {
	report := internalkafka.LagReport{
		Topic: "users",
		Group: "user-ingestion-group",
		Partitions: []internalkafka.PartitionLag{
			{Partition: 0, LastOffset: 150, CommittedOffset: 145, Lag: 5},
		},
		TotalLag: 5,
	}

	ctrl := controller.NewMonitorController(&fakeLagReader{report: report}, slog.Default())
	mux := http.NewServeMux()
	ctrl.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics/lag", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got internalkafka.LagReport
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, "users", got.Topic)
	assert.Equal(t, "user-ingestion-group", got.Group)
	assert.Equal(t, int64(5), got.TotalLag)
	assert.Len(t, got.Partitions, 1)
	assert.Equal(t, int64(150), got.Partitions[0].LastOffset)
	assert.Equal(t, int64(145), got.Partitions[0].CommittedOffset)
	assert.Equal(t, int64(5), got.Partitions[0].Lag)
}

func TestMonitorController_KafkaError_Returns503(t *testing.T) {
	ctrl := controller.NewMonitorController(
		&fakeLagReader{err: errors.New("connection refused")},
		slog.Default(),
	)
	mux := http.NewServeMux()
	ctrl.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics/lag", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}

func TestMonitorController_TotalLag_MultiplePartitions(t *testing.T) {
	report := internalkafka.LagReport{
		Topic: "users",
		Group: "user-ingestion-group",
		Partitions: []internalkafka.PartitionLag{
			{Partition: 0, LastOffset: 100, CommittedOffset: 90, Lag: 10},
			{Partition: 1, LastOffset: 200, CommittedOffset: 185, Lag: 15},
			{Partition: 2, LastOffset: 50, CommittedOffset: 50, Lag: 0},
		},
		TotalLag: 25,
	}

	ctrl := controller.NewMonitorController(&fakeLagReader{report: report}, slog.Default())
	mux := http.NewServeMux()
	ctrl.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/metrics/lag", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var got internalkafka.LagReport
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&got))
	assert.Equal(t, int64(25), got.TotalLag)
	assert.Len(t, got.Partitions, 3)
}

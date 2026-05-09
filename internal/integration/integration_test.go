package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"estudos.com/mysql-kafka/internal/apiclient"
	"estudos.com/mysql-kafka/internal/controller"
	kafkawrapper "estudos.com/mysql-kafka/internal/kafka"
	"estudos.com/mysql-kafka/internal/repository"
)

// TestIntegration_FullPipeline tests the full fetch → save → publish → consume cycle.
// Skipped unless INTEGRATION=true is set.
func TestIntegration_FullPipeline(t *testing.T) {
	if os.Getenv("INTEGRATION") != "true" {
		t.Skip("skipping integration test; set INTEGRATION=true to run")
	}

	const numRecords = 100
	broker := envOrDefault("KAFKA_BROKER", "localhost:9092")
	topic := envOrDefault("KAFKA_TOPIC", "users-integration-test")
	groupID := envOrDefault("KAFKA_GROUP_ID", "integration-test-group")
	dbDSN := envOrDefault("DB_DSN", "root:secret@tcp(localhost:3306)/appdb")

	// 1. Set up a fake users HTTP server returning 100 records.
	fakeUsers := make([]map[string]any, numRecords)
	for i := range fakeUsers {
		fakeUsers[i] = map[string]any{"name": fmt.Sprintf("User-%d", i+1)}
	}
	body, _ := json.Marshal(fakeUsers)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer srv.Close()

	// 2. Open DB and ensure schema exists.
	db, err := sql.Open("mysql", dbDSN)
	require.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM users")
	require.NoError(t, err)

	// 3. Run the producer pipeline.
	start := time.Now()
	apiClient := apiclient.NewClient(srv.URL)
	repo := repository.NewMySQLUserRepository(db)
	producer := kafkawrapper.NewProducer(broker, topic)
	defer producer.Close()

	prodCtrl := controller.NewProducerController(apiClient, repo, producer, noopLogger(t))
	err = prodCtrl.Run(context.Background())
	require.NoError(t, err)

	// 4. Assert 100 rows in MySQL.
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, numRecords, count, "expected %d rows in MySQL", numRecords)

	// 5. Assert 100 messages in Kafka by consuming them.
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MaxWait:     2 * time.Second,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	consumed := 0
	for consumed < numRecords {
		_, err := reader.ReadMessage(ctx)
		if err != nil {
			break
		}
		consumed++
	}
	assert.Equal(t, numRecords, consumed, "expected %d Kafka messages", numRecords)

	elapsed := time.Since(start)
	assert.Less(t, elapsed, 10*time.Second, "full cycle must complete in under 10s (SC-001)")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// noopLogger returns a *slog.Logger backed by a discard handler to keep test output clean.
func noopLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

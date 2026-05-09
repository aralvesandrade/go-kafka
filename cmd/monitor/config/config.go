package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration loaded from environment variables.
type Config struct {
	PORT         string
	KafkaBroker  string
	KafkaTopic   string
	KafkaGroupID string
}

// Load reads all required environment variables and returns a Config.
// Returns a descriptive error if any required variable is missing.
func Load() (Config, error) {
	err := godotenv.Load()
	if err != nil {
		return Config{}, fmt.Errorf("error loading .env file: %w", err)
	}

	required := []string{"MONITOR_ADDR", "KAFKA_BROKER", "KAFKA_TOPIC", "KAFKA_GROUP_ID"}
	for _, key := range required {
		if os.Getenv(key) == "" {
			return Config{}, fmt.Errorf("required environment variable %s is not set", key)
		}
	}
	return Config{
		PORT:         os.Getenv("MONITOR_ADDR"),
		KafkaBroker:  os.Getenv("KAFKA_BROKER"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
	}, nil
}

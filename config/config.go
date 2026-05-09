package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

// Config holds all runtime configuration loaded from environment variables.
type Config struct {
	APIURL       string
	DBDSN        string
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

	required := []string{"API_URL", "DB_DSN", "KAFKA_BROKER", "KAFKA_TOPIC", "KAFKA_GROUP_ID"}
	for _, key := range required {
		if os.Getenv(key) == "" {
			return Config{}, fmt.Errorf("required environment variable %s is not set", key)
		}
	}
	return Config{
		APIURL:       os.Getenv("API_URL"),
		DBDSN:        os.Getenv("DB_DSN"),
		KafkaBroker:  os.Getenv("KAFKA_BROKER"),
		KafkaTopic:   os.Getenv("KAFKA_TOPIC"),
		KafkaGroupID: os.Getenv("KAFKA_GROUP_ID"),
	}, nil
}

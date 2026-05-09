package domain

// User represents a person fetched from the API and persisted to MySQL.
type User struct {
	ID   int64
	Name string
}

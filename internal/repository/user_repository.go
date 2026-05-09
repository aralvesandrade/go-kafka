package repository

import (
	"context"
	"database/sql"
	"fmt"

	"estudos.com/mysql-kafka/internal/domain"
)

// UserRepository defines the persistence interface for User records.
type UserRepository interface {
	Save(ctx context.Context, user domain.User) (domain.User, error)
}

// MySQLUserRepository implements UserRepository backed by MySQL.
type MySQLUserRepository struct {
	db *sql.DB
}

// NewMySQLUserRepository returns a new MySQLUserRepository using the given DB.
func NewMySQLUserRepository(db *sql.DB) *MySQLUserRepository {
	return &MySQLUserRepository{db: db}
}

// Save inserts a user record into MySQL and returns the saved user with its assigned ID.
func (r *MySQLUserRepository) Save(ctx context.Context, user domain.User) (domain.User, error) {
	result, err := r.db.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", user.Name)
	if err != nil {
		return domain.User{}, fmt.Errorf("repository: insert user: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return domain.User{}, fmt.Errorf("repository: last insert id: %w", err)
	}
	return domain.User{ID: id, Name: user.Name}, nil
}

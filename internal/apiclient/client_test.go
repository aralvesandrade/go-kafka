package apiclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"estudos.com/mysql-kafka/internal/apiclient"
)

func TestFetchUsers_ValidList(t *testing.T) {
	payload := []map[string]any{
		{"nome": "Alice", "age": 30},
		{"nome": "Bob", "extra": "ignored"},
	}
	body, _ := json.Marshal(payload)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}))
	defer srv.Close()

	client := apiclient.NewClient(srv.URL)
	users, err := client.FetchUsers(context.Background())

	require.NoError(t, err)
	assert.Len(t, users, 2)
	assert.Equal(t, "Alice", users[0].Name)
	assert.Equal(t, "Bob", users[1].Name)
}

func TestFetchUsers_EmptyList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	client := apiclient.NewClient(srv.URL)
	users, err := client.FetchUsers(context.Background())

	require.NoError(t, err)
	assert.Empty(t, users)
}

func TestFetchUsers_HTTPError(t *testing.T) {
	client := apiclient.NewClient("http://127.0.0.1:0") // unreachable
	users, err := client.FetchUsers(context.Background())

	assert.Error(t, err)
	assert.Nil(t, users)
}

func TestFetchUsers_DecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`not-json`))
	}))
	defer srv.Close()

	client := apiclient.NewClient(srv.URL)
	users, err := client.FetchUsers(context.Background())

	assert.Error(t, err)
	assert.Nil(t, users)
}

func TestFetchUsers_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := apiclient.NewClient(srv.URL)
	users, err := client.FetchUsers(context.Background())

	assert.Error(t, err)
	assert.Nil(t, users)
}

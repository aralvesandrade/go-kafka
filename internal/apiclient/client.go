package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"estudos.com/mysql-kafka/internal/domain"
)

type apiUser struct {
	Name string `json:"nome"`
}

// Client fetches users from the fake users HTTP API.
type Client struct {
	apiURL     string
	httpClient *http.Client
}

// NewClient returns a new Client configured for the given API URL.
func NewClient(apiURL string) *Client {
	return &Client{
		apiURL:     apiURL,
		httpClient: &http.Client{},
	}
}

// FetchUsers makes a GET request to the API and returns the parsed users.
func (c *Client) FetchUsers(ctx context.Context) ([]domain.User, error) {
	apiURL := fmt.Sprintf("%s/fake/users", c.apiURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("apiclient: build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("apiclient: do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("apiclient: unexpected status code %d", resp.StatusCode)
	}

	var apiUsers []apiUser
	if err := json.NewDecoder(resp.Body).Decode(&apiUsers); err != nil {
		return nil, fmt.Errorf("apiclient: decode response: %w", err)
	}

	users := make([]domain.User, 0, len(apiUsers))
	for _, u := range apiUsers {
		users = append(users, domain.User{Name: u.Name})
	}
	return users, nil
}

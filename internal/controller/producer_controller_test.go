package controller_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"estudos.com/mysql-kafka/internal/controller"
	"estudos.com/mysql-kafka/internal/domain"
)

// --- Fakes ---

type fakeAPIClient struct {
	users []domain.User
	err   error
}

func (f *fakeAPIClient) FetchUsers(_ context.Context) ([]domain.User, error) {
	return f.users, f.err
}

type fakeRepo struct {
	savedUsers []domain.User
	err        error
	nextID     int64
}

func (f *fakeRepo) Save(_ context.Context, u domain.User) (domain.User, error) {
	if f.err != nil {
		return domain.User{}, f.err
	}
	f.nextID++
	saved := domain.User{ID: f.nextID, Name: u.Name}
	f.savedUsers = append(f.savedUsers, saved)
	return saved, nil
}

type fakeProducer struct {
	published []domain.User
	err       error
}

func (f *fakeProducer) Publish(_ context.Context, u domain.User) error {
	if f.err != nil {
		return f.err
	}
	f.published = append(f.published, u)
	return nil
}

// producerControllerForTest constructs a ProducerController using the fake dependencies.
func producerControllerForTest(
	client controller.APIClient,
	repo controller.UserSaver,
	producer controller.MessagePublisher,
) *controller.ProducerController {
	return controller.NewProducerController(client, repo, producer, slog.Default())
}

// --- Tests ---

func TestProducerController_EmptyList(t *testing.T) {
	client := &fakeAPIClient{users: []domain.User{}}
	repo := &fakeRepo{}
	prod := &fakeProducer{}

	ctrl := producerControllerForTest(client, repo, prod)
	err := ctrl.Run(context.Background())

	require.NoError(t, err)
	assert.Empty(t, repo.savedUsers)
	assert.Empty(t, prod.published)
}

func TestProducerController_FetchError(t *testing.T) {
	client := &fakeAPIClient{err: errors.New("api down")}
	repo := &fakeRepo{}
	prod := &fakeProducer{}

	ctrl := producerControllerForTest(client, repo, prod)
	err := ctrl.Run(context.Background())

	assert.Error(t, err)
	assert.Empty(t, repo.savedUsers)
}

func TestProducerController_SaveAndPublish(t *testing.T) {
	client := &fakeAPIClient{users: []domain.User{{Name: "Alice"}, {Name: "Bob"}}}
	repo := &fakeRepo{}
	prod := &fakeProducer{}

	ctrl := producerControllerForTest(client, repo, prod)
	err := ctrl.Run(context.Background())

	require.NoError(t, err)
	assert.Len(t, repo.savedUsers, 2)
	assert.Len(t, prod.published, 2)
}

func TestProducerController_PublishError_Continues(t *testing.T) {
	client := &fakeAPIClient{users: []domain.User{{Name: "Alice"}, {Name: "Bob"}}}
	repo := &fakeRepo{}
	prod := &fakeProducer{err: errors.New("broker down")}

	ctrl := producerControllerForTest(client, repo, prod)
	err := ctrl.Run(context.Background())

	// Publish errors are non-fatal; all records saved.
	require.NoError(t, err)
	assert.Len(t, repo.savedUsers, 2)
}

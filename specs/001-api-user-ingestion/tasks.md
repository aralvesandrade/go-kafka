# Tasks: API User Ingestion Pipeline

**Feature**: 001-api-user-ingestion | **Date**: 2026-05-09 | **Plan**: [plan.md](plan.md)

Tasks are ordered by dependency: each task may only reference code produced by earlier tasks.
Mark each task `done` as it is completed.

---

## TASK-001 — Infrastructure: Docker Compose

**Status**: `done`  
**Refs**: quickstart.md §1, FR-008

Create `docker-compose.yml` at the repository root that starts:
- MySQL 8 on port `3306` (user `root`, password `secret`, database `appdb`)
- Kafka (with KRaft or Zookeeper) on port `9092`

No source code is involved. This is the prerequisite for all integration tests.

**Acceptance**:
- `docker compose up -d` starts both services without errors.
- MySQL is reachable at `localhost:3306` and Kafka at `localhost:9092`.

---

## TASK-002 — Config Package

**Status**: `done`  
**Refs**: FR-008, plan.md §Source Code

Create `config/config.go` with a `Config` struct and a `Load()` function that reads all
required values from environment variables. No defaults for secrets; fail fast if a required
var is missing.

**Environment variables**:

| Var | Description |
|-----|-------------|
| `API_URL` | Full URL of the fake users API endpoint |
| `DB_DSN` | MySQL DSN (`user:pass@tcp(host:port)/db`) |
| `KAFKA_BROKER` | Kafka broker address (`host:port`) |
| `KAFKA_TOPIC` | Kafka topic name |
| `KAFKA_GROUP_ID` | Consumer group ID (consumer only) |

**Acceptance**:
- `Load()` returns a populated `Config` and `nil` error when all vars are set.
- `Load()` returns a descriptive error when any required var is missing.

---

## TASK-003 — Domain Type

**Status**: `done`  
**Refs**: data-model.md §User, FR-002

Create `internal/domain/user.go` with the `User` struct:

```go
package domain

type User struct {
    ID   int64
    Name string
}
```

No methods, no logic. This is a pure data carrier shared by all layers.

**Acceptance**:
- Package compiles with `go build ./internal/domain/...`.

---

## TASK-004 — API Client

**Status**: `done`  
**Refs**: FR-001, FR-002, FR-010, data-model.md §API Response Record

Create `internal/apiclient/client.go`:
- `Client` struct holding the base URL and an `*http.Client`.
- `NewClient(apiURL string) *Client` constructor.
- `FetchUsers(ctx context.Context) ([]domain.User, error)` method that:
  - Makes a GET request to the configured URL.
  - Decodes the JSON array response.
  - Maps only the `name` field to `domain.User{Name: ...}`.
  - Returns an error (not panic) on any HTTP or decode failure.

**Acceptance**:
- Returns `[]domain.User` with correct names when the API returns a valid list.
- Returns `(nil, error)` when the API is unreachable.
- Returns `([]domain.User{}, nil)` (empty slice, no error) when the API returns `[]`.

---

## TASK-005 — Repository Layer

**Status**: `done`  
**Refs**: FR-003, FR-010, data-model.md §User (MySQL DDL)

Create `internal/repository/user_repository.go`:
- `UserRepository` interface with `Save(ctx context.Context, user domain.User) (domain.User, error)`.
- `MySQLUserRepository` struct implementing `UserRepository`, backed by `*sql.DB`.
- `NewMySQLUserRepository(db *sql.DB) *MySQLUserRepository` constructor.
- `Save` executes `INSERT INTO users (name) VALUES (?)`, retrieves `LastInsertId`, and returns
  the updated `domain.User{ID: <id>, Name: <name>}`.

**Acceptance**:
- `Save` returns the user with a non-zero `ID` on success.
- `Save` returns an error (not panic) when the DB is unavailable.

---

## TASK-006 — Kafka Producer Wrapper

**Status**: `done`  
**Refs**: FR-004, contracts/kafka-message.md, FR-010

Create `internal/kafka/producer.go`:
- `Producer` struct wrapping `*kafka.Writer` (segmentio/kafka-go).
- `NewProducer(broker, topic string) *Producer` constructor.
- `Publish(ctx context.Context, user domain.User) error` method that:
  - Marshals `{"id": user.ID, "name": user.Name}` to JSON.
  - Sets the message key to `strconv.FormatInt(user.ID, 10)`.
  - Calls `Writer.WriteMessages`.
- `Close() error` method that closes the writer.

**Acceptance**:
- `Publish` sends one message with the correct key and JSON value.
- `Publish` returns an error (not panic) if the broker is unreachable.

---

## TASK-007 — Kafka Consumer Wrapper

**Status**: `done`  
**Refs**: FR-005, FR-006, contracts/kafka-message.md, FR-010

Create `internal/kafka/consumer.go`:
- `Consumer` struct wrapping `*kafka.Reader` (segmentio/kafka-go).
- `NewConsumer(broker, topic, groupID string) *Consumer` constructor.
- `Read(ctx context.Context) (domain.User, error)` method that fetches one message and
  unmarshals the JSON value into `domain.User`.
- `Commit(ctx context.Context, msg kafka.Message) error` method that commits the offset.
- `Close() error` method that closes the reader.

**Acceptance**:
- `Read` returns a `domain.User` with correct `ID` and `Name` for a valid message.
- `Read` returns an error for a malformed JSON message (caller decides to skip or stop).
- `Close` does not block indefinitely.

---

## TASK-008 — Producer Controller

**Status**: `done`  
**Refs**: FR-001–FR-004, FR-007, FR-009, FR-010

Create `internal/controller/producer_controller.go`:
- `ProducerController` struct holding `apiclient.Client`, `repository.UserRepository`,
  `kafka.Producer`, and `*slog.Logger`.
- `NewProducerController(...)` constructor.
- `Run(ctx context.Context) error` method that:
  1. Calls `apiclient.FetchUsers` — logs `op=api_fetch`.
  2. If the list is empty, logs a summary and returns `nil` (clean exit, SC from FR-009).
  3. For each user: calls `repository.Save` — logs `op=db_insert`; on success calls
     `kafka.Publish` — logs `op=kafka_publish`; on publish error logs and continues (per edge
     case in spec).
  4. Logs a final summary (`total` count).
  5. Returns the first fatal error (fetch failure or unrecoverable DB error); non-fatal
     publish errors do not abort the loop.

**Acceptance**:
- `Run` returns `nil` and produces no DB/Kafka side effects when the API returns an empty list.
- `Run` returns a non-nil error when `FetchUsers` fails.
- `Run` processes all records and logs each step when the API returns a valid list.

---

## TASK-009 — Consumer Controller

**Status**: `done`  
**Refs**: FR-005, FR-006, FR-007, FR-010, spec.md §User Story 2

Create `internal/controller/consumer_controller.go`:
- `ConsumerController` struct holding `kafka.Consumer` and `*slog.Logger`.
- `NewConsumerController(...)` constructor.
- `Run(ctx context.Context) error` method that loops until `ctx` is cancelled:
  1. Calls `consumer.Read` — on malformed message: logs error, commits offset, continues.
  2. Logs `op=kafka_consume` with `id` and `name`.
  3. Calls `consumer.Commit`.
  4. On `ctx.Err()`, exits cleanly.

**Acceptance**:
- `Run` logs each received user and commits the offset.
- `Run` skips malformed messages without crashing (spec §User Story 2, scenario 2).
- `Run` exits when the context is cancelled (SIGINT/SIGTERM).

---

## TASK-010 — Producer Entry Point

**Status**: `done`  
**Refs**: plan.md §Dual Entry Points, FR-008, quickstart.md §5

Create `cmd/producer/main.go`:
1. Call `config.Load()` — exit with error log if it fails.
2. Open `*sql.DB` with the MySQL DSN — exit on error.
3. Instantiate `apiclient.NewClient`, `repository.NewMySQLUserRepository`,
   `kafka.NewProducer`.
4. Instantiate `controller.NewProducerController` with a `slog.Default()` logger.
5. Call `controller.Run(context.Background())` — exit with non-zero status on error.
6. Defer `kafka.Producer.Close()` and `db.Close()`.

**Acceptance**:
- `go build ./cmd/producer` succeeds.
- Binary exits 0 after a successful run and non-zero on configuration or fetch errors.

---

## TASK-011 — Consumer Entry Point

**Status**: `done`  
**Refs**: plan.md §Dual Entry Points, FR-008, quickstart.md §6

Create `cmd/consumer/main.go`:
1. Call `config.Load()` — exit with error log if it fails.
2. Instantiate `kafka.NewConsumer`.
3. Instantiate `controller.NewConsumerController` with a `slog.Default()` logger.
4. Set up OS signal handling (`SIGINT`, `SIGTERM`) to cancel the root context.
5. Call `controller.Run(ctx)` — log and exit on error.
6. Defer `kafka.Consumer.Close()`.

**Acceptance**:
- `go build ./cmd/consumer` succeeds.
- Consumer starts and logs startup message (`op=kafka_consume`, topic, group).
- `Ctrl+C` triggers a clean shutdown.

---

## TASK-012 — Unit Tests

**Status**: `done`  
**Refs**: spec.md §User Scenarios, SC-001–SC-005

Write unit tests using `testify/assert` for the components below. Use interfaces/mocks or
`httptest` to avoid real infrastructure.

| File | What to test |
|------|-------------|
| `internal/apiclient/client_test.go` | Valid list, empty list, HTTP error, decode error |
| `internal/repository/user_repository_test.go` | Save success, Save DB error (use `database/sql` mock or real DB in container) |
| `internal/controller/producer_controller_test.go` | Empty list → clean exit; fetch error → error returned; per-user save+publish loop |
| `internal/controller/consumer_controller_test.go` | Normal message logged + committed; malformed JSON skipped; context cancel exits loop |

**Acceptance**:
- `go test ./...` passes with no failures.
- Coverage on controller and apiclient packages ≥ 80 %.

---

## TASK-013 — Integration Test / Smoke Test

**Status**: `done`  
**Refs**: SC-001, SC-002, SC-003, quickstart.md

Write a shell script or `TestMain`-based integration test (skipped unless `INTEGRATION=true`)
that:
1. Assumes Docker Compose services are running.
2. Starts a local fake users HTTP server (e.g., via `httptest.NewServer`) returning 100 records.
3. Runs the producer binary.
4. Asserts MySQL contains 100 rows.
5. Asserts 100 Kafka messages were published to the topic.
6. Runs the consumer for 5 seconds and asserts 100 log entries containing `op=kafka_consume`.
7. Full cycle must complete in under 10 seconds (SC-001).

**Acceptance**:
- `INTEGRATION=true go test ./... -run Integration` passes end-to-end.
- SC-001 time budget is met on the local dev machine.

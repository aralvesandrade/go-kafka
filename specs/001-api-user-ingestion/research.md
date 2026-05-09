# Research: API User Ingestion Pipeline

**Feature**: 001-api-user-ingestion  
**Date**: 2026-05-09  
**Status**: Complete — all unknowns resolved

## 1. Kafka Client Library

**Decision**: `github.com/segmentio/kafka-go`  
**Rationale**: Pure Go, no CGO dependency (unlike `confluent-kafka-go`), idiomatic API with
`kafka.Writer` and `kafka.Reader`, actively maintained, and well-suited for simple
producer/consumer use cases without a full consumer-group framework overhead.  
**Alternatives considered**:
- `confluent-kafka-go` — wraps librdkafka via CGO; adds native build complexity; better
  for high-throughput production systems but overkill for this scope.
- `Shopify/sarama` — full-featured but larger API surface; more configuration required.

## 2. MySQL Driver

**Decision**: `github.com/go-sql-driver/mysql` + `database/sql` stdlib  
**Rationale**: The de-facto standard MySQL driver for Go; integrates with `database/sql` so
the repository layer can use the standard interface (`*sql.DB`), keeping the implementation
swappable. No ORM needed — a single `INSERT` statement suffices for this scope.  
**Alternatives considered**:
- `GORM` — ORM overhead not justified for a single-table insert; violates Simplicity First.
- `sqlx` — useful for scanning but unnecessary when only inserting.

## 3. HTTP Client for Fake Users API

**Decision**: `net/http` stdlib (`http.Get`)  
**Rationale**: The fake API is a single GET endpoint returning JSON. The stdlib HTTP client
is sufficient; no retry middleware, auth headers, or connection pooling beyond defaults are
required for v1.  
**Alternatives considered**:
- `resty` / `go-resty` — adds a dependency for no benefit at this scope.

## 4. Configuration Strategy

**Decision**: Environment variables loaded into a single `config.Config` struct at startup  
**Rationale**: Simplest approach with zero external dependencies; satisfies FR-008. All
required variables are documented in `quickstart.md`. The program exits with a clear error
message if any required variable is missing.  
**Required variables**:

| Variable | Description | Example |
|----------|-------------|---------|
| `API_URL` | Fake users API base URL | `http://localhost:3000/fake/users` |
| `DB_DSN` | MySQL DSN | `user:pass@tcp(localhost:3306)/dbname` |
| `KAFKA_BROKER` | Kafka broker address | `localhost:9092` |
| `KAFKA_TOPIC` | Kafka topic name | `users` |

## 5. Kafka Message Format

**Decision**: JSON-encoded payload `{"id": <int>, "name": "<string>"}`  
**Rationale**: Human-readable, easy to inspect with CLI tools (`kafka-console-consumer`),
and requires no schema registry for this scope. The `id` field is the MySQL auto-increment
primary key, providing correlation between the DB row and the Kafka event.  
**Alternatives considered**:
- Protobuf / Avro — adds schema management complexity; not justified for a simple demo pipeline.
- Plain text (name only) — loses the correlation ID, making debugging harder.

## 6. Producer Execution Model

**Decision**: One-shot batch — fetch all records, process sequentially, exit  
**Rationale**: Spec assumption states the producer is a batch process (not a long-running
poll loop). Sequential processing keeps the code simple and avoids concurrency complexity
for v1.  
**Alternatives considered**:
- Goroutine fan-out per record — adds complexity and race conditions; YAGNI.
- Polling loop — out of scope for v1 per spec assumptions.

## 7. Consumer Execution Model

**Decision**: Long-running loop with `kafka.Reader`, graceful shutdown on SIGINT/SIGTERM  
**Rationale**: The consumer must run continuously (spec assumption). `kafka.Reader` manages
offset commits automatically after successful `FetchMessage` + `CommitMessages`. Signal
handling via `os/signal` and `context.WithCancel` enables clean shutdown.  
**Alternatives considered**:
- Manual offset management — unnecessary complexity; `kafka.Reader` auto-commit is sufficient.

## 8. Logging

**Decision**: `log/slog` (Go 1.21+ stdlib), structured JSON output to stdout  
**Rationale**: Mandated by Constitution Principle V. `slog` is stdlib, zero dependencies,
and produces structured output compatible with log aggregators. JSON handler used so log
fields are machine-parseable.  
**Log fields per operation**:

| Operation | Required fields |
|-----------|----------------|
| API fetch | `op`, `url`, `count` |
| DB insert | `op`, `name`, `id` (returned insert ID) |
| Kafka publish | `op`, `topic`, `id`, `name` |
| Kafka consume | `op`, `topic`, `id`, `name` |
| Any error | `op`, `error` (error string) |

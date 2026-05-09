# Implementation Plan: API User Ingestion Pipeline

**Branch**: `001-api-user-ingestion` | **Date**: 2026-05-09 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `specs/001-api-user-ingestion/spec.md`

## Summary

Build a Go pipeline with two binary entry points. The **producer** (`cmd/producer`) fetches
user records from a fake HTTP API, persists each user's `name` to MySQL via the repository
layer, and then publishes one Kafka message per inserted record. The **consumer**
(`cmd/consumer`) runs as a long-lived worker, reads from the same Kafka topic, and logs each
received name. Both binaries share internal packages organized in controller and repository
layers as mandated by the constitution.

## Technical Context

**Language/Version**: Go 1.26 (module `estudos.com/mysql-kafka`)
**Primary Dependencies**: `github.com/segmentio/kafka-go` (Kafka), `github.com/go-sql-driver/mysql` (MySQL driver), `database/sql` (stdlib), `net/http` (stdlib), `log/slog` (stdlib)
**Storage**: MySQL 8 — single table `users(id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL)`
**Testing**: `go test ./...` + `testify/assert`; integration tests require Docker Compose
**Target Platform**: Linux server (local dev on Linux/macOS)
**Project Type**: Two CLI binaries (producer + consumer) sharing internal libraries
**Performance Goals**: Full fetch-save-publish cycle for 100 records in under 10 seconds (SC-001)
**Constraints**: No hard-coded config — all values via environment variables; no silent failures
**Scale/Scope**: Single Kafka topic, single consumer group, single MySQL table; batch producer (one-shot), long-running consumer

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Layered Architecture | ✅ PASS | `internal/controller` and `internal/repository` packages defined; no cross-layer bypass |
| II. Dual Entry Points | ✅ PASS | Exactly `cmd/producer/main.go` and `cmd/consumer/main.go` |
| III. Simplicity First | ✅ PASS | No speculative abstractions; each construct serves ≥1 concrete requirement |
| IV. Separation of Concerns | ✅ PASS | Kafka → `internal/kafka`; MySQL → `internal/repository`; domain types → `internal/domain` |
| V. Observability | ✅ PASS | `log/slog` used throughout; every operation and error produces a structured log entry |

**Gate result**: All principles pass. Proceeding to Phase 0.

## Project Structure

### Documentation (this feature)

```text
specs/001-api-user-ingestion/
├── plan.md          # This file
├── research.md      # Phase 0 output
├── data-model.md    # Phase 1 output
├── quickstart.md    # Phase 1 output
├── contracts/       # Phase 1 output
│   └── kafka-message.md
└── tasks.md         # Phase 2 output (speckit.tasks)
```

### Source Code (repository root)

```text
cmd/
├── producer/
│   └── main.go          # Entry point: fetch → save → publish
└── consumer/
    └── main.go          # Entry point: consume → log

internal/
├── domain/
│   └── user.go          # User struct (shared domain type)
├── apiclient/
│   └── client.go        # HTTP client for fake users API
├── controller/
│   ├── producer_controller.go   # Orchestrates fetch → save → publish
│   └── consumer_controller.go  # Orchestrates consume → log
├── repository/
│   └── user_repository.go      # MySQL INSERT + interface
└── kafka/
    ├── producer.go      # Kafka writer wrapper
    └── consumer.go      # Kafka reader wrapper

config/
└── config.go            # Env-var loading

docker-compose.yml       # MySQL + Kafka for local dev & integration tests
go.mod
go.sum
main.go                  # (empty, unused — entry points are under cmd/)
```

**Structure Decision**: Single Go module with two `cmd/` entry points and shared `internal/`
packages. This is idiomatic Go and satisfies Principles I (layers), II (dual entry points),
and IV (separation of concerns) without over-engineering.

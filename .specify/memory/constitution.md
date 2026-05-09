<!--
SYNC IMPACT REPORT
Version change: (new) → 1.0.0
Added sections: Core Principles, Technology Stack, Development Workflow, Governance
Removed sections: none (initial version)
Templates requiring updates:
  - .specify/templates/plan-template.md ✅ aligned (no changes required)
  - .specify/templates/spec-template.md ✅ aligned (no changes required)
  - .specify/templates/tasks-template.md ✅ aligned (no changes required)
Follow-up TODOs: none
-->

# go-mysql-kafka Constitution

## Core Principles

### I. Layered Architecture (NON-NEGOTIABLE)
The codebase MUST be organized in distinct layers: **controller** and **repository**.
Controllers handle incoming data/events and orchestrate business logic; repositories handle all
persistence (MySQL) operations. No layer MUST bypass another — controllers MUST NOT access the
database directly, and repositories MUST NOT contain business logic. Each layer MUST be
independently testable via interfaces.

### II. Dual Entry Points
The project MUST expose exactly two `main` packages: `cmd/producer/main.go` and
`cmd/consumer/main.go`. The producer entry point publishes messages to Kafka; the consumer
entry point reads from Kafka and persists data to MySQL. No additional entry points MUST be
created without a constitution amendment.

### III. Simplicity First (YAGNI)
The design MUST start simple and grow only when proven necessary. Abstractions MUST be
introduced only when they serve at least two concrete use cases. Generic utilities, excessive
interfaces, and speculative features are prohibited. Each new construct MUST justify its existence
against an existing, concrete requirement.

### IV. Separation of Concerns
Each package MUST have a single, well-defined responsibility. Kafka interaction logic MUST reside
in dedicated packages (e.g., `internal/kafka`). MySQL interaction MUST reside in repository
packages. Business/domain types MUST live in a shared `internal/domain` or `internal/model`
package. Cross-cutting concerns (config, logging) are handled in dedicated packages and injected
via constructors.

### V. Observability
All significant operations (message published, message consumed, DB write, errors) MUST produce
structured log entries. Logs MUST include at minimum: timestamp, level, operation name, and
relevant identifiers (e.g., message key, record ID). `log/slog` (stdlib) is the preferred logging
backend; no silent failures are permitted.

## Technology Stack

- **Language**: Go 1.21+
- **Message Broker**: Apache Kafka (via `github.com/segmentio/kafka-go` or `confluent-kafka-go`)
- **Database**: MySQL (via `database/sql` + `github.com/go-sql-driver/mysql`)
- **Configuration**: Environment variables or a single config struct loaded at startup
- **Testing**: `testing` stdlib + `testify` for assertions; integration tests use Docker Compose
- **Build**: Standard `go build ./cmd/producer` and `go build ./cmd/consumer`

## Development Workflow

- Features MUST be developed on feature branches; direct commits to `main` are prohibited.
- Each PR MUST include tests covering the changed layer(s).
- The layered boundary is enforced at review time: reviewers MUST reject any PR where a controller
  directly accesses a database driver or where a repository contains business logic.
- Integration tests (Kafka + MySQL) MUST be runnable via `docker compose up` + `go test ./...`.
- Breaking changes to the producer/consumer contract MUST be agreed upon before implementation.

## Governance

This constitution supersedes all other verbal or written practices for this repository. Amendments
require a documented rationale, a version bump following semantic versioning (MAJOR for principle
removal/redefinition; MINOR for new principle or section; PATCH for wording clarifications), and
must be reflected in this file before merging. All PRs and code reviews MUST verify compliance
with the Core Principles above. Complexity MUST be justified; any deviation from Simplicity First
MUST be noted in the PR description.

**Version**: 1.0.0 | **Ratified**: 2026-05-09 | **Last Amended**: 2026-05-09

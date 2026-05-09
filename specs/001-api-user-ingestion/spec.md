# Feature Specification: API User Ingestion Pipeline

**Feature Branch**: `001-api-user-ingestion`  
**Created**: 2026-05-09  
**Status**: Draft  
**Input**: User description: "será um projeto que irá ler dados de uma api, por exemplo: localhost:3000/fake/users, irá retornar dados fake da api, no caso ler apenas o campo nome e salvar numa base mysql, depois de salvo no mysql, esses dados devem ser enviados para kafka e depois consumidos por um worker"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Producer Fetches and Persists Users (Priority: P1)

An operator starts the **producer** process. It calls the fake users API, extracts the `name`
field from each returned record, saves each name to the MySQL database, and then publishes
an event to Kafka for each saved record.

**Why this priority**: This is the core data-entry point of the pipeline. Without it no data
enters the system at all. Every downstream behavior depends on this story working correctly.

**Independent Test**: Start the producer against a running fake-users API and a MySQL instance.
After execution the MySQL `users` table must contain the fetched names and the Kafka topic must
have one message per inserted record.

**Acceptance Scenarios**:

1. **Given** the fake users API is available and returns a list with a `name` field,
   **When** the producer runs,
   **Then** each name is saved as a row in MySQL and a corresponding Kafka message is published.

2. **Given** the API returns an empty list,
   **When** the producer runs,
   **Then** no rows are written to MySQL and no Kafka messages are published; the process exits cleanly.

3. **Given** the API is unreachable,
   **When** the producer runs,
   **Then** the process logs a descriptive error and exits with a non-zero status; MySQL and Kafka are untouched.

---

### User Story 2 - Consumer Processes Kafka Messages (Priority: P2)

An operator starts the **consumer** (worker) process. It continuously reads messages from the
Kafka topic produced in User Story 1 and processes each one (logs the received name and
acknowledges the message).

**Why this priority**: This closes the pipeline loop. The consumer is the observable end of
the data flow and proves the Kafka integration is end-to-end functional.

**Independent Test**: Manually publish a Kafka message with a user name payload and start the
consumer. The consumer must log the received name and commit the offset.

**Acceptance Scenarios**:

1. **Given** the Kafka topic has messages with user names,
   **When** the consumer is running,
   **Then** each message is read in order, logged, and its offset committed.

2. **Given** a malformed message is on the topic,
   **When** the consumer reads it,
   **Then** the message is logged as an error and skipped; the consumer continues without crashing.

3. **Given** the Kafka broker is temporarily unavailable,
   **When** the consumer is running,
   **Then** the consumer retries with backoff and resumes processing once the broker is reachable again.

---

### User Story 3 - Observability of the Full Pipeline (Priority: P3)

An operator can inspect structured logs from both the producer and consumer to understand what
the pipeline did: which names were fetched, how many records were saved, which messages were
published, and which were consumed.

**Why this priority**: Operational visibility is required to diagnose issues and confirm the
pipeline is healthy, but it does not block the core data flow.

**Independent Test**: Run the full pipeline end-to-end and inspect stdout/log output. Every
significant action (API call, DB insert, Kafka publish, Kafka consume) must appear as a
structured log entry.

**Acceptance Scenarios**:

1. **Given** the pipeline runs successfully,
   **When** an operator inspects the logs,
   **Then** each step (fetch, save, publish, consume) is represented by at least one log entry
   containing the operation name, a relevant identifier (e.g., name or record ID), and a timestamp.

2. **Given** any step fails,
   **When** an operator inspects the logs,
   **Then** the error log entry includes the operation name, the cause, and enough context to
   reproduce or investigate the issue.

---

### Edge Cases

- What happens when the API returns duplicate names? → Each record is persisted and published
  independently; deduplication is out of scope for v1.
- What happens when the MySQL `users` table does not exist? → The producer logs an error and
  exits; schema migration is a prerequisite, not part of this feature.
- What happens when the Kafka topic does not exist? → Behaviour follows the broker's
  `auto.create.topics.enable` setting; the producer logs the result regardless.
- What happens when a DB insert succeeds but the Kafka publish fails? → The message is logged
  as a publish error; the record remains in MySQL; retry logic is out of scope for v1.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The producer MUST call the configured fake users API endpoint and read the response.
- **FR-002**: The producer MUST extract only the `name` field from each record returned by the API.
- **FR-003**: The producer MUST save each extracted name as a row in the MySQL `users` table before publishing to Kafka.
- **FR-004**: After a successful MySQL insert, the producer MUST publish one message per record to the configured Kafka topic.
- **FR-005**: The consumer MUST read messages from the Kafka topic continuously until stopped.
- **FR-006**: The consumer MUST log each received message's content and commit the Kafka offset after processing.
- **FR-007**: Both the producer and consumer MUST emit structured log entries for every significant operation and all errors.
- **FR-008**: Configuration (API URL, MySQL DSN, Kafka broker address, topic name) MUST be supplied via environment variables; no hard-coded values are permitted.
- **FR-009**: The producer MUST exit cleanly (status 0) when the API returns an empty list.
- **FR-010**: Neither the producer nor the consumer MUST crash silently; all errors MUST be logged before the process exits or continues.

### Key Entities

- **User**: Represents a person fetched from the fake API. Relevant attribute: `name` (string). Stored in MySQL and referenced in Kafka messages.
- **Kafka Message**: An event published after a User is persisted. Contains at minimum the user's name and a record identifier to correlate with the MySQL row.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: The producer completes a full fetch-save-publish cycle for a 100-record API response in under 10 seconds on a local development machine.
- **SC-002**: Every name returned by the API is present in MySQL after the producer finishes, with zero missing records.
- **SC-003**: Every Kafka message published by the producer is consumed and logged by the consumer with zero message loss under normal operating conditions.
- **SC-004**: Every error scenario (API unreachable, DB failure, Kafka publish failure) produces at least one log entry that identifies the failing operation and its cause.
- **SC-005**: Both processes start up and are ready to operate in under 3 seconds from launch.

## Assumptions

- The fake users API (e.g., `localhost:3000/fake/users`) returns a JSON array where each object contains at least a `name` string field.
- The MySQL database and the `users` table schema exist before the producer runs; schema creation is a prerequisite and not part of this feature.
- The Kafka broker and topic exist (or auto-creation is enabled) before the processes start.
- The producer is designed for batch/one-shot execution (runs once, processes all records, exits); continuous polling is out of scope for v1.
- The consumer is designed for long-running operation and is stopped via an OS signal (SIGINT/SIGTERM).
- A single Kafka consumer group is sufficient; competing consumers are out of scope for v1.
- Both processes run on Linux/macOS; Windows support is out of scope.

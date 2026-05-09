# Quickstart: API User Ingestion Pipeline

**Feature**: 001-api-user-ingestion  
**Date**: 2026-05-09

## Prerequisites

- Go 1.21+
- Docker & Docker Compose
- A running fake users API (e.g., `json-server`, `faker-api`, or any HTTP server returning
  a JSON array of objects with a `name` field at the configured URL)

---

## 1. Start Infrastructure (MySQL + Kafka)

```bash
docker compose up -d
```

This starts:
- **MySQL 8** on port `3306` (user: `root`, password: `secret`, database: `appdb`)
- **Kafka** on port `9092` (with Zookeeper or KRaft depending on the compose file)

Wait ~10 seconds for both services to be healthy before running the binaries.

---

## 2. Create the Database Schema

```bash
docker exec -i <mysql-container> mysql -uroot -psecret appdb <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
SQL
```

Replace `<mysql-container>` with the actual container name (check with `docker ps`).

---

## 3. Configure Environment Variables

Both binaries share the same set of environment variables. Export them in your shell or
create a `.env` file:

```bash
export API_URL="http://localhost:3000/fake/users"
export DB_DSN="root:secret@tcp(localhost:3306)/appdb"
export KAFKA_BROKER="localhost:9092"
export KAFKA_TOPIC="users"
export KAFKA_GROUP_ID="user-ingestion-group"   # consumer only
```

---

## 4. Build the Binaries

```bash
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
```

---

## 5. Run the Producer

```bash
./bin/producer
```

Expected output (structured JSON logs to stdout):

```json
{"time":"...","level":"INFO","msg":"fetching users","op":"api_fetch","url":"http://localhost:3000/fake/users"}
{"time":"...","level":"INFO","msg":"users fetched","op":"api_fetch","count":10}
{"time":"...","level":"INFO","msg":"user saved","op":"db_insert","name":"Alice","id":1}
{"time":"...","level":"INFO","msg":"event published","op":"kafka_publish","topic":"users","id":1,"name":"Alice"}
...
{"time":"...","level":"INFO","msg":"producer finished","total":10}
```

The producer exits after processing all records.

---

## 6. Run the Consumer

```bash
./bin/consumer
```

Expected output:

```json
{"time":"...","level":"INFO","msg":"consumer started","op":"kafka_consume","topic":"users","group":"user-ingestion-group"}
{"time":"...","level":"INFO","msg":"message received","op":"kafka_consume","id":1,"name":"Alice"}
...
```

Stop with `Ctrl+C` (SIGINT). The consumer logs a shutdown message and exits cleanly.

---

## 7. Running Tests

```bash
# Unit tests (no infrastructure needed)
go test ./internal/...

# Integration tests (requires docker compose up -d first)
go test ./... -tags integration
```

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `dial tcp: connection refused` on DB | MySQL not ready | Wait 10s after `docker compose up -d` |
| `dial tcp: connection refused` on Kafka | Kafka not ready | Wait 15s; check `docker compose logs kafka` |
| `API_URL: missing required env var` | Env not set | Export the variables from step 3 |
| Producer exits with 0 records | API returned empty list | Check your fake API is running and reachable |
| Consumer sees no messages | Producer not yet run | Run the producer first |

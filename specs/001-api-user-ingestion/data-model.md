# Data Model: API User Ingestion Pipeline

**Feature**: 001-api-user-ingestion  
**Date**: 2026-05-09

## Entities

### User

Represents a person fetched from the fake users API. The canonical source of truth is the
MySQL database; the Kafka message carries a snapshot of the same data.

| Field | Type | Constraints | Description |
|-------|------|-------------|-------------|
| `id` | `int64` | Auto-increment, primary key | Unique identifier assigned by MySQL on insert |
| `name` | `string` | NOT NULL, max 255 chars | Person's name extracted from the API response |

**Go type** (`internal/domain/user.go`):

```go
package domain

type User struct {
    ID   int64
    Name string
}
```

**MySQL DDL**:

```sql
CREATE TABLE IF NOT EXISTS users (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
```

---

### API Response Record

Represents one element of the JSON array returned by the fake users API. Only the `name`
field is consumed; all other fields are ignored (per FR-002).

| Field | Type | Notes |
|-------|------|-------|
| `name` | `string` | The only field read from the API response |
| *(others)* | any | Ignored — not mapped to any domain type |

**Go type** (anonymous, used only inside `internal/apiclient`):

```go
type apiUser struct {
    Name string `json:"name"`
}
```

---

### Kafka Message Payload

The event published to Kafka after a successful MySQL insert. Encodes the domain `User`
(with its assigned database ID) as JSON.

| Field | Type | Description |
|-------|------|-------------|
| `id` | `int64` | MySQL-assigned primary key — correlates event to DB row |
| `name` | `string` | User's name, identical to the stored value |

**JSON schema**:

```json
{
  "id":   1,
  "name": "Alice"
}
```

**Kafka message key**: `strconv.FormatInt(user.ID, 10)` (string representation of the numeric
ID) — enables partition affinity per user record.

---

## State Transitions

```
API Response
    │
    ▼ extract name
domain.User{Name: "..."}
    │
    ▼ repository.Save() → assigns ID
domain.User{ID: 42, Name: "..."}
    │
    ▼ kafka.Producer.Publish()
KafkaMessage{id: 42, name: "..."}
    │
    ▼ kafka.Consumer.Fetch()
Logged & offset committed
```

## Validation Rules

- `name` MUST NOT be empty; records with an empty name are skipped and logged as warnings by
  the producer controller.
- `name` longer than 255 characters is truncated to 255 before insert (logged as a warning).
- A MySQL insert failure is logged as an error; the failed record is skipped and the producer
  continues with the remaining records.

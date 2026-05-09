# Contract: Kafka Message — User Event

**Topic**: configured via `KAFKA_TOPIC` env var (default suggestion: `users`)  
**Direction**: Producer → Consumer  
**Format**: JSON (UTF-8)  
**Encoding**: Kafka message value is a JSON object; message key is the user ID as a UTF-8 string.

---

## Message Structure

### Key

```
"42"
```

The MySQL-assigned primary key encoded as a decimal string. Provides partition affinity so
all events for the same user land on the same partition (useful if the pipeline is extended
to emit multiple events per user in the future).

### Value (JSON)

```json
{
  "id":   42,
  "name": "Alice"
}
```

| Field | JSON type | Required | Description |
|-------|-----------|----------|-------------|
| `id` | `number` (integer) | Yes | MySQL auto-increment primary key of the persisted user row |
| `name` | `string` | Yes | User's name as stored in MySQL; max 255 characters |

---

## Producer Guarantees

- One message is published **per successfully inserted MySQL row**.
- A message is **never published** if the DB insert failed.
- Message key is always set to the string representation of `id`.
- If `kafka.Writer.WriteMessages` fails, the error is logged (`op=kafka_publish`, `error=<reason>`) and the record is skipped; no retry in v1.

## Consumer Expectations

- Consumer reads messages sequentially from offset (earliest by default for a new group).
- After successful processing (logging the name), `CommitMessages` is called to advance the offset.
- Malformed JSON (non-parseable value) is logged as an error (`op=kafka_consume`, `error=<reason>`) and the message is skipped; the offset is still committed to avoid blocking.
- Consumer group ID is configured via `KAFKA_GROUP_ID` env var (default: `user-ingestion-group`).

---

## Example Flow

```
Producer publishes:
  Key:   "1"
  Value: {"id":1,"name":"Alice"}

Consumer receives:
  Logs: op=kafka_consume id=1 name=Alice
  Commits offset
```

---

## Versioning

This contract is at **v1**. Any change to field names or types constitutes a breaking change
and requires a constitution amendment and a new contract version.

# go-mysql-kafka

Pipeline em Go que busca usuários de uma API HTTP, persiste no MySQL e publica no Kafka. Três binários independentes compartilham pacotes internos organizados em camadas.

## Visão Geral

- **Producer** (`cmd/producer`): busca registros de usuários de uma API fake, salva cada `name` no MySQL e publica uma mensagem Kafka por registro inserido.
- **Consumer** (`cmd/consumer`): worker de longa duração que lê do mesmo tópico Kafka e loga cada nome recebido.
- **Monitor** (`cmd/monitor`): servidor HTTP que expõe o lag do consumer group via `GET /metrics/lag`.

## Estrutura do Projeto

```text
cmd/
├── producer/
│   ├── main.go               # Entrada: fetch → save → publish
│   ├── config/config.go      # Config do producer (API_URL, DB_DSN, KAFKA_*)
│   └── .env
├── consumer/
│   ├── main.go               # Entrada: consume → log
│   ├── config/config.go      # Config do consumer (KAFKA_*)
│   └── .env
└── monitor/
    ├── main.go               # Entrada: servidor HTTP de métricas
    ├── config/config.go      # Config do monitor (KAFKA_*, MONITOR_ADDR)
    └── .env

internal/
├── domain/user.go            # Struct User (tipo de domínio compartilhado)
├── apiclient/client.go       # Cliente HTTP para a API de usuários
├── controller/
│   ├── producer_controller.go
│   ├── consumer_controller.go
│   └── monitor_controller.go # Handler HTTP GET /metrics/lag
├── repository/user_repository.go  # INSERT MySQL + interface
└── kafka/
    ├── producer.go           # Wrapper do Kafka writer
    ├── consumer.go           # Wrapper do Kafka reader
    └── lag.go                # LagChecker: consulta offsets via kafka.Client
```

## Pré-requisitos

- Go 1.21+
- Docker & Docker Compose

## Quickstart

### 1. Subir infraestrutura (MySQL + Kafka)

```bash
docker compose up -d
```

Aguarde ~10 segundos para os serviços ficarem saudáveis.

### 2. Criar schema do banco

```bash
docker exec -i <mysql-container> mysql -uroot -psecret appdb <<'SQL'
CREATE TABLE IF NOT EXISTS users (
    id   INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);
SQL
```

Substitua `<mysql-container>` pelo nome real do container (`docker ps`).

### 3. Configurar variáveis de ambiente

Cada binário lê o `.env` do próprio diretório. Os arquivos já estão preenchidos com os defaults locais. Para sobrescrever, edite os arquivos ou exporte as vars antes de rodar.

**Producer** (`cmd/producer/.env`):
```env
API_URL=http://localhost:3000
DB_DSN=root:secret@tcp(localhost:3306)/appdb
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=users
```

**Consumer** (`cmd/consumer/.env`):
```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=users
KAFKA_GROUP_ID=user-ingestion-group
```

**Monitor** (`cmd/monitor/.env`):
```env
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=users
KAFKA_GROUP_ID=user-ingestion-group
MONITOR_ADDR=:8081
```

### 4. Build

```bash
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
go build -o bin/monitor  ./cmd/monitor
```

### 5. Rodar o producer

```bash
cd cmd/producer && ../../bin/producer
```

### 6. Rodar o consumer

```bash
cd cmd/consumer && ../../bin/consumer
```

### 7. Rodar o monitor

```bash
cd cmd/monitor && ../../bin/monitor
```

O monitor sobe na porta definida em `MONITOR_ADDR` (default `:8081`).

## API do Monitor

### `GET /metrics/lag`

Retorna o lag atual do consumer group por partição.

**200 OK:**
```json
{
  "topic": "users",
  "group": "user-ingestion-group",
  "partitions": [
    {
      "partition": 0,
      "last_offset": 150,
      "committed_offset": 145,
      "lag": 5
    }
  ],
  "total_lag": 5
}
```

**503 Service Unavailable:** quando não consegue conectar ao broker Kafka.

## Testes

```bash
go test ./...
```

Testes de integração requerem Docker Compose rodando.

## Dependências Principais

| Pacote | Uso |
|--------|-----|
| `github.com/segmentio/kafka-go` | Cliente Kafka (producer, consumer, lag) |
| `github.com/go-sql-driver/mysql` | Driver MySQL |
| `github.com/joho/godotenv` | Carregamento de `.env` |
| `github.com/stretchr/testify` | Assertions nos testes |

## Infraestrutura (docker-compose)

### Subir

```bash
docker compose up -d
```

### Desligar

```bash
docker compose down
```

Para remover também os volumes (banco de dados):

```bash
docker compose down -v
```

| Serviço | Imagem | Porta |
|---------|--------|-------|
| MySQL | `mysql:8` | `3306` |
| Kafka | `apache/kafka:3.7.0` | `9092` |

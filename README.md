# go-mysql-kafka

Pipeline em Go que busca usuários de uma API HTTP, persiste no MySQL e publica no Kafka. Dois binários independentes compartilham pacotes internos organizados em camadas.

## Visão Geral

- **Producer** (`cmd/producer`): busca registros de usuários de uma API fake, salva cada `name` no MySQL e publica uma mensagem Kafka por registro inserido.
- **Consumer** (`cmd/consumer`): worker de longa duração que lê do mesmo tópico Kafka e loga cada nome recebido.

## Estrutura do Projeto

```text
cmd/
├── producer/main.go          # Entrada: fetch → save → publish
└── consumer/main.go          # Entrada: consume → log

internal/
├── domain/user.go            # Struct User (tipo de domínio compartilhado)
├── apiclient/client.go       # Cliente HTTP para a API de usuários
├── controller/
│   ├── producer_controller.go
│   └── consumer_controller.go
├── repository/user_repository.go  # INSERT MySQL + interface
└── kafka/
    ├── producer.go           # Wrapper do Kafka writer
    └── consumer.go           # Wrapper do Kafka reader

config/config.go              # Carregamento de variáveis de ambiente
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

```bash
export API_URL="http://localhost:3000/fake/users"
export DB_DSN="root:secret@tcp(localhost:3306)/appdb"
export KAFKA_BROKER="localhost:9092"
export KAFKA_TOPIC="users"
export KAFKA_GROUP_ID="user-ingestion-group"   # apenas consumer
```

### 4. Build

```bash
go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
```

### 5. Rodar o producer

```bash
./bin/producer
```

### 6. Rodar o consumer

```bash
./bin/consumer
```

## Testes

```bash
go test ./...
```

Testes de integração requerem Docker Compose rodando.

## Dependências Principais

| Pacote | Uso |
|--------|-----|
| `github.com/segmentio/kafka-go` | Cliente Kafka |
| `github.com/go-sql-driver/mysql` | Driver MySQL |
| `github.com/stretchr/testify` | Assertions nos testes |

## Infraestrutura (docker-compose)

| Serviço | Imagem | Porta |
|---------|--------|-------|
| MySQL | `mysql:8` | `3306` |
| Kafka | `apache/kafka:3.7.0` | `9092` |

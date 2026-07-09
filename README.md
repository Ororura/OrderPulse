# Order Go

Order Go is a small event-driven Go project that demonstrates order creation, persistence in PostgreSQL, Redis-backed order read caching, event publishing to Kafka, and Telegram notifications from a separate consumer service.

The repository contains two independent Go services:

- `order-service` - exposes an HTTP API for creating and reading orders, stores them in PostgreSQL, caches reads in Redis, and publishes `orders.created` events to Kafka.
- `notification-service` - consumes `orders.created` events from Kafka and sends a Telegram message for each created order.

---

## English

### Architecture

```text
Client
  |
  | POST /orders
  v
order-service
  |
  | INSERT INTO orders
  v
PostgreSQL
  |
  | publish orders.created
  v
Kafka
  |
  | consume orders.created
  v
notification-service
  |
  | Telegram Bot API
  v
Telegram chat
```

### Repository Structure

```text
.
├── order-service
│   ├── cmd/main.go
│   ├── docker-compose.yml
│   ├── migrations
│   │   ├── 000001_create_orders.up.sql
│   │   └── 000001_create_orders.down.sql
│   └── internal
│       ├── config
│       ├── database
│       ├── handler
│       ├── kafka
│       ├── model
│       ├── repository
│       └── service
└── notification-service
    ├── cmd/main.go
    └── internal
        ├── config
        ├── consumer
        ├── kafka
        └── telegram
```

### Technology Stack

- Go `1.25.6`
- PostgreSQL `16`
- Redis `7`
- Apache Kafka
- `segmentio/kafka-go` for Kafka producer and consumer logic
- `lib/pq` PostgreSQL driver
- `joho/godotenv` for local `.env` files
- Telegram Bot API for notifications
- Docker Compose for local infrastructure

### Services

#### order-service

Responsibilities:

- Accepts order creation requests over HTTP.
- Serves single-order reads with Redis cache-aside lookup.
- Validates the request payload.
- Persists valid orders to PostgreSQL.
- Publishes an `orders.created` Kafka event after the order is saved.
- Provides a health check endpoint.

HTTP endpoints:

| Method | Path | Description |
| --- | --- | --- |
| `POST` | `/orders` | Create a new order |
| `GET` | `/orders/{id}` | Get an order by ID |
| `GET` | `/health` | Health check, returns `ok` |

Create order request:

```json
{
  "user_id": 1,
  "product_id": 10,
  "count": 2
}
```

Get order response:

```json
{
  "id": 1,
  "user_id": 1,
  "product_id": 10,
  "count": 2,
  "status": "created",
  "created_at": "2026-07-05T12:00:00Z"
}
```

Successful response:

```json
{
  "id": 1,
  "status": "created",
  "message": "order created"
}
```

Validation rules:

- `user_id` must be greater than `0`.
- `product_id` must be greater than `0`.
- `count` must be greater than `0`.

#### notification-service

Responsibilities:

- Connects to Kafka as a consumer.
- Reads messages from the `orders.created` topic.
- Decodes order creation events.
- Sends a formatted Telegram notification.

Example Telegram message:

```text
New order

Order ID: 1
User ID: 1
Product ID: 10
Count: 2
Status: created
```

### Kafka Event Contract

Topic:

```text
orders.created
```

Message key:

```text
order_id
```

Message value:

```json
{
  "order_id": 1,
  "user_id": 1,
  "product_id": 10,
  "count": 2,
  "status": "created",
  "created_at": "2026-07-05T12:00:00Z"
}
```

### Database Schema

The order table is created by `order-service/migrations/000001_create_orders.up.sql`.

```sql
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    count INT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Configuration

Both services load environment variables from the current environment and from a local `.env` file.

#### order-service

Example file: `order-service/.env.example`

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `HTTP_PORT` | No | `8080` | HTTP server port |
| `DB_URL` | Yes | - | PostgreSQL connection string |
| `KAFKA_BROKETS` | No | `localhost:9092` | Kafka broker list used by the current code |
| `KAFKA_ORDER_CREATED_TOPIC` | No | `orders.created` | Kafka topic for created order events |
| `REDIS_ADDR` | No | `localhost:6379` | Redis address for order read cache |
| `REDIS_PASSWORD` | No | empty | Redis password |
| `REDIS_DB` | No | `0` | Redis database number |
| `ORDER_CACHE_TTL` | No | `5m` | TTL for cached order JSON |

Important: the current `order-service` code reads `KAFKA_BROKETS` with this exact spelling. The provided `.env.example` uses `KAFKA_BROKERS`, so either update the code or use `KAFKA_BROKETS` in the actual `.env` file until the typo is fixed.

Suggested local `.env`:

```env
HTTP_PORT=8080
DB_URL=postgres://pg:pg@localhost:5432/orders?sslmode=disable
KAFKA_BROKETS=localhost:9092
KAFKA_ORDER_CREATED_TOPIC=orders.created
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
ORDER_CACHE_TTL=5m
```

#### notification-service

Example file: `notification-service/env.example`

| Variable | Required | Default | Description |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | No | `localhost:9092` | Kafka broker list |
| `KAFKA_ORDER_CREATED_TOPIC` | No | `orders.created` | Kafka topic to consume |
| `KAFKA_CONSUMER_GROUP` | No | `notification-service` | Kafka consumer group ID |
| `TELEGRAM_BOT_TOKEN` | Yes | - | Telegram bot token |
| `TELEGRAM_CHAT_ID` | Yes | - | Telegram chat ID |

### Local Development

#### Prerequisites

- Go installed
- Docker and Docker Compose installed
- A Telegram bot token
- A Telegram chat ID

#### 1. Start Infrastructure

Run PostgreSQL, Redis, Kafka, and Kafka UI:

```bash
cd order-service
docker compose up -d
```

Local services:

| Service | URL |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| Redis | `localhost:6379` |
| Kafka | `localhost:9092` |
| Kafka UI | `http://localhost:8081` |

#### 2. Apply Database Migration

Use any PostgreSQL migration tool or run the SQL file manually.

Manual example:

```bash
psql "postgres://pg:pg@localhost:5432/orders?sslmode=disable" \
  -f migrations/000001_create_orders.up.sql
```

Rollback:

```bash
psql "postgres://pg:pg@localhost:5432/orders?sslmode=disable" \
  -f migrations/000001_create_orders.down.sql
```

#### 3. Configure order-service

```bash
cd order-service
cp .env.example .env
```

Because the current code expects `KAFKA_BROKETS`, adjust `.env` if needed:

```env
KAFKA_BROKETS=localhost:9092
```

#### 4. Run order-service

```bash
cd order-service
go run ./cmd
```

Health check:

```bash
curl http://localhost:8080/health
```

Create an order:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"product_id":10,"count":2}'
```

Get an order:

```bash
curl http://localhost:8080/orders/1
```

#### 5. Configure notification-service

```bash
cd notification-service
cp env.example .env
```

Edit `.env`:

```env
KAFKA_BROKERS=localhost:9092
KAFKA_ORDER_CREATED_TOPIC=orders.created
KAFKA_CONSUMER_GROUP=notification-service
TELEGRAM_BOT_TOKEN=<your-bot-token>
TELEGRAM_CHAT_ID=<your-chat-id>
```

#### 6. Run notification-service

```bash
cd notification-service
go run ./cmd
```

After creating an order through `order-service`, the consumer should receive the Kafka event and send a Telegram message.

### Useful Commands

Run Go formatting:

```bash
gofmt -w order-service notification-service
```

Download dependencies:

```bash
cd order-service && go mod download
cd ../notification-service && go mod download
```

Run services:

```bash
cd order-service && go run ./cmd
cd notification-service && go run ./cmd
```

Stop local infrastructure:

```bash
cd order-service
docker compose down
```

Stop local infrastructure and remove PostgreSQL data:

```bash
cd order-service
docker compose down -v
```

### Operational Notes

- `order-service` shuts down gracefully on `SIGINT` or `SIGTERM`.
- `notification-service` also uses signal-aware context cancellation.
- `GET /orders/{id}` uses Redis as a cache-aside layer. Redis read/write errors are logged and PostgreSQL remains the source of truth.
- If publishing to Kafka fails after the database insert succeeds, the current implementation returns an error to the HTTP client, but the order remains stored in PostgreSQL.
- `order-service` has unit tests for the `GET /orders/{id}` handler and cache-aside service behavior.
- Kafka UI is available at `http://localhost:8081` and can be used to inspect topics and messages.

---

## Русский

### Архитектура

```text
Клиент
  |
  | POST /orders
  v
order-service
  |
  | INSERT INTO orders
  v
PostgreSQL
  |
  | publish orders.created
  v
Kafka
  |
  | consume orders.created
  v
notification-service
  |
  | Telegram Bot API
  v
Telegram-чат
```

### Структура Репозитория

```text
.
├── order-service
│   ├── cmd/main.go
│   ├── docker-compose.yml
│   ├── migrations
│   │   ├── 000001_create_orders.up.sql
│   │   └── 000001_create_orders.down.sql
│   └── internal
│       ├── config
│       ├── database
│       ├── handler
│       ├── kafka
│       ├── model
│       ├── repository
│       └── service
└── notification-service
    ├── cmd/main.go
    └── internal
        ├── config
        ├── consumer
        ├── kafka
        └── telegram
```

### Технологии

- Go `1.25.6`
- PostgreSQL `16`
- Redis `7`
- Apache Kafka
- `segmentio/kafka-go` для Kafka producer и consumer
- `lib/pq` как PostgreSQL-драйвер
- `joho/godotenv` для локальных `.env` файлов
- Telegram Bot API для отправки уведомлений
- Docker Compose для локальной инфраструктуры

### Сервисы

#### order-service

Задачи сервиса:

- Принимает HTTP-запросы на создание заказов.
- Отдает чтение одного заказа через Redis cache-aside.
- Валидирует тело запроса.
- Сохраняет корректные заказы в PostgreSQL.
- Публикует Kafka-событие `orders.created` после сохранения заказа.
- Предоставляет health check endpoint.

HTTP endpoints:

| Метод | Путь | Описание |
| --- | --- | --- |
| `POST` | `/orders` | Создать новый заказ |
| `GET` | `/orders/{id}` | Получить заказ по ID |
| `GET` | `/health` | Проверка состояния сервиса, возвращает `ok` |

Запрос на создание заказа:

```json
{
  "user_id": 1,
  "product_id": 10,
  "count": 2
}
```

Ответ чтения заказа:

```json
{
  "id": 1,
  "user_id": 1,
  "product_id": 10,
  "count": 2,
  "status": "created",
  "created_at": "2026-07-05T12:00:00Z"
}
```

Успешный ответ:

```json
{
  "id": 1,
  "status": "created",
  "message": "order created"
}
```

Правила валидации:

- `user_id` должен быть больше `0`.
- `product_id` должен быть больше `0`.
- `count` должен быть больше `0`.

#### notification-service

Задачи сервиса:

- Подключается к Kafka как consumer.
- Читает сообщения из топика `orders.created`.
- Декодирует события создания заказа.
- Отправляет уведомление в Telegram.

Пример Telegram-сообщения:

```text
Новый заказ

Order ID: 1
User ID: 1
Product ID: 10
Count: 2
Status: created
```

### Контракт Kafka-События

Топик:

```text
orders.created
```

Ключ сообщения:

```text
order_id
```

Тело сообщения:

```json
{
  "order_id": 1,
  "user_id": 1,
  "product_id": 10,
  "count": 2,
  "status": "created",
  "created_at": "2026-07-05T12:00:00Z"
}
```

### Схема Базы Данных

Таблица заказов создается миграцией `order-service/migrations/000001_create_orders.up.sql`.

```sql
CREATE TABLE IF NOT EXISTS orders (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    count INT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### Конфигурация

Оба сервиса читают переменные окружения из системного окружения и локального `.env` файла.

#### order-service

Файл-пример: `order-service/.env.example`

| Переменная | Обязательная | Значение по умолчанию | Описание |
| --- | --- | --- | --- |
| `HTTP_PORT` | Нет | `8080` | Порт HTTP-сервера |
| `DB_URL` | Да | - | Строка подключения к PostgreSQL |
| `KAFKA_BROKETS` | Нет | `localhost:9092` | Список Kafka brokers, который сейчас читает код |
| `KAFKA_ORDER_CREATED_TOPIC` | Нет | `orders.created` | Kafka-топик для событий создания заказа |
| `REDIS_ADDR` | Нет | `localhost:6379` | Адрес Redis для кэша чтения заказов |
| `REDIS_PASSWORD` | Нет | пусто | Пароль Redis |
| `REDIS_DB` | Нет | `0` | Номер базы Redis |
| `ORDER_CACHE_TTL` | Нет | `5m` | TTL JSON заказа в кэше |

Важно: текущий код `order-service` читает переменную `KAFKA_BROKETS` именно с таким написанием. В `.env.example` сейчас указано `KAFKA_BROKERS`, поэтому до исправления опечатки в коде используйте `KAFKA_BROKETS` в реальном `.env` файле или поправьте код.

Рекомендуемый локальный `.env`:

```env
HTTP_PORT=8080
DB_URL=postgres://pg:pg@localhost:5432/orders?sslmode=disable
KAFKA_BROKETS=localhost:9092
KAFKA_ORDER_CREATED_TOPIC=orders.created
REDIS_ADDR=localhost:6379
REDIS_PASSWORD=
REDIS_DB=0
ORDER_CACHE_TTL=5m
```

#### notification-service

Файл-пример: `notification-service/env.example`

| Переменная | Обязательная | Значение по умолчанию | Описание |
| --- | --- | --- | --- |
| `KAFKA_BROKERS` | Нет | `localhost:9092` | Список Kafka brokers |
| `KAFKA_ORDER_CREATED_TOPIC` | Нет | `orders.created` | Kafka-топик для чтения |
| `KAFKA_CONSUMER_GROUP` | Нет | `notification-service` | ID consumer group |
| `TELEGRAM_BOT_TOKEN` | Да | - | Токен Telegram-бота |
| `TELEGRAM_CHAT_ID` | Да | - | ID Telegram-чата |

### Локальная Разработка

#### Требования

- Установленный Go
- Установленные Docker и Docker Compose
- Токен Telegram-бота
- ID Telegram-чата

#### 1. Запустить Инфраструктуру

Запустите PostgreSQL, Redis, Kafka и Kafka UI:

```bash
cd order-service
docker compose up -d
```

Локальные сервисы:

| Сервис | URL |
| --- | --- |
| PostgreSQL | `localhost:5432` |
| Redis | `localhost:6379` |
| Kafka | `localhost:9092` |
| Kafka UI | `http://localhost:8081` |

#### 2. Применить Миграцию Базы Данных

Можно использовать любой инструмент для PostgreSQL-миграций или выполнить SQL-файл вручную.

Пример ручного запуска:

```bash
psql "postgres://pg:pg@localhost:5432/orders?sslmode=disable" \
  -f migrations/000001_create_orders.up.sql
```

Откат миграции:

```bash
psql "postgres://pg:pg@localhost:5432/orders?sslmode=disable" \
  -f migrations/000001_create_orders.down.sql
```

#### 3. Настроить order-service

```bash
cd order-service
cp .env.example .env
```

Так как текущий код ожидает `KAFKA_BROKETS`, при необходимости измените `.env`:

```env
KAFKA_BROKETS=localhost:9092
```

#### 4. Запустить order-service

```bash
cd order-service
go run ./cmd
```

Проверка состояния:

```bash
curl http://localhost:8080/health
```

Создание заказа:

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"user_id":1,"product_id":10,"count":2}'
```

Получение заказа:

```bash
curl http://localhost:8080/orders/1
```

#### 5. Настроить notification-service

```bash
cd notification-service
cp env.example .env
```

Отредактируйте `.env`:

```env
KAFKA_BROKERS=localhost:9092
KAFKA_ORDER_CREATED_TOPIC=orders.created
KAFKA_CONSUMER_GROUP=notification-service
TELEGRAM_BOT_TOKEN=<your-bot-token>
TELEGRAM_CHAT_ID=<your-chat-id>
```

#### 6. Запустить notification-service

```bash
cd notification-service
go run ./cmd
```

После создания заказа через `order-service` consumer должен получить Kafka-событие и отправить сообщение в Telegram.

### Полезные Команды

Форматирование Go-кода:

```bash
gofmt -w order-service notification-service
```

Загрузка зависимостей:

```bash
cd order-service && go mod download
cd ../notification-service && go mod download
```

Запуск сервисов:

```bash
cd order-service && go run ./cmd
cd notification-service && go run ./cmd
```

Остановка локальной инфраструктуры:

```bash
cd order-service
docker compose down
```

Остановка инфраструктуры с удалением данных PostgreSQL:

```bash
cd order-service
docker compose down -v
```

### Эксплуатационные Заметки

- `order-service` корректно завершает работу по `SIGINT` или `SIGTERM`.
- `notification-service` также использует context cancellation при получении сигнала остановки.
- `GET /orders/{id}` использует Redis как cache-aside слой. Ошибки чтения/записи Redis логируются, а PostgreSQL остается источником истины.
- Если публикация в Kafka завершится ошибкой после успешного INSERT в базу, текущая реализация вернет ошибку HTTP-клиенту, но заказ останется сохраненным в PostgreSQL.
- В `order-service` есть unit-тесты для `GET /orders/{id}` handler и cache-aside поведения service.
- Kafka UI доступен по адресу `http://localhost:8081`; через него удобно проверять топики и сообщения.

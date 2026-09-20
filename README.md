# Logger Service

A centralized event logging service built with Go. The service receives structured
application events over WebSocket, validates and sanitizes them, and persists them
as JSON Lines files.

The current implementation uses a single canonical event model:

```text
contracts.Event
        ↓
security.RedactEvent
        ↓
services.EventProcessorService
        ↓
ports.EventStore
        ↓
storage.JSONFileStorage
```

Legacy `LogEntry`-based HTTP logging has been removed in favor of the event
pipeline.

## Features

- **Canonical event model**: all ingestion, validation, storage, and publication
  use `contracts.Event`.
- **WebSocket producer protocol**: producers connect to `/ws/events`, perform a
  hello handshake, and submit event messages.
- **Structured JSON event storage**: accepted events are persisted as JSON Lines.
- **Validation and normalization**: event levels, required fields, timestamps, and
  metadata are validated before storage.
- **Sensitive data protection**: messages, event fields, context, and metadata are
  sanitized/redacted before validation and persistence.
- **Local JSONL storage backend**: events are stored in weekly rotated JSONL files.
- **Health endpoints**: liveness and readiness endpoints are available for
  deployment platforms.
- **Rate limiting**: process-local fixed-window rate limiting protects HTTP/WebSocket
  entrypoints.
- **Configurable runtime**: configuration is loaded through Viper from defaults and
  environment variables.

## Architecture

This service follows a simplified hexagonal architecture centered on the event
pipeline.

```text
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
│                                                             │
│  cmd/server                                                │
│  WebSocket endpoint: /ws/events                            │
│  Health endpoints: /health, /health/live, /health/ready    │
└─────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────┐
│                    WebSocket Adapter                        │
│                                                             │
│  internal/websockets                                       │
│  - authenticates API key                                   │
│  - validates origin policy                                 │
│  - performs protocol handshake                             │
│  - reads event messages                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────┐
│                    Domain Services                          │
│                                                             │
│  internal/domain/services                                  │
│  - redacts sensitive values                                │
│  - validates event shape                                   │
│  - maps compatibility levels                               │
│  - normalizes timestamps and text                          │
│  - generates event IDs                                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────┐
│                    Ports                                    │
│                                                             │
│  internal/ports                                             │
│  - EventProcessor                                          │
│  - EventStore                                              │
│  - EventPublisher                                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              v
┌─────────────────────────────────────────────────────────────┐
│                    Storage Adapter                          │
│                                                             │
│  internal/adapters/storage                                 │
│  - JSONFileStorage                                         │
│  - JSON Lines persistence                                  │
│  - duplicate event detection                               │
│  - query support                                           │
│  - health checks                                           │
│  - retention rotation                                      │
└─────────────────────────────────────────────────────────────┘
```

## Current Package Layout

```text
cmd/server
  Application bootstrap and HTTP server setup.

internal/contracts
  Canonical event and WebSocket protocol contracts.

internal/domain/config
  Runtime configuration structures.

internal/domain/services
  Event processing: validation, normalization, enrichment, redaction integration.

internal/ports
  Event-oriented interfaces:
  - EventProcessor
  - EventStore
  - EventPublisher

internal/adapters/config
  Viper-based configuration loader.

internal/adapters/storage
  JSON Lines event storage implementation.

internal/adapters/websocket
  Bounded event handler, publisher, subscriptions, and queue processing.

internal/security
  Redaction, rate limiting, and connection limiting utilities.

internal/websockets
  WebSocket protocol handler and hub support.
```

## Event Model

The canonical event type is `contracts.Event`.

```json
{
  "eventId": "evt-123",
  "timestamp": "2026-09-20T12:30:00Z",
  "level": "info",
  "service": "orders",
  "component": "checkout-api",
  "eventType": "order.created",
  "message": "Order created successfully",
  "code": "ORDER_CREATED",
  "sessionId": "session-123",
  "userId": "user-456",
  "correlationId": "corr-789",
  "traceId": "trace-abc",
  "spanId": "span-def",
  "context": {
    "path": "/orders",
    "method": "POST"
  },
  "metadata": {
    "orderId": "order-123"
  }
}
```

### Required Fields

The event processor requires:

- `level`
- `service`
- `eventType`
- `message`

### Supported Event Levels

Canonical levels:

- `trace`
- `debug`
- `info`
- `warn`
- `error`
- `fatal`

Compatibility mappings:

- `information` → `info`
- `warning` → `warn`

Unsupported levels are rejected.

## Sensitive Data Redaction

Before validation and persistence, events pass through redaction.

The redactor sanitizes:

- `service`
- `component`
- `eventType`
- `message`
- `code`
- `sessionId`
- `userId`
- `correlationId`
- `traceId`
- `spanId`
- `context`
- `metadata`

Sensitive patterns such as tokens, bearer credentials, passwords, secrets, API keys,
authorization headers, cookies, and credentials are redacted.

Example:

```json
{
  "message": "request failed token=abc123"
}
```

is persisted as:

```json
{
  "message": "request failed [REDACTED]"
}
```

Sensitive keys in `context` or `metadata` are replaced with:

```text
[REDACTED]
```

## WebSocket API

### Endpoint

```text
GET /ws/events
```

Producer clients connect using WebSocket.

If `LOGGER_API_KEY` is configured, clients must send:

```text
X-API-Key: <api-key>
```

When `LOGGER_API_KEY` is empty, localhost access can be allowed for local
development.

### Producer Handshake

After connecting, the first message must be a `hello` message.

```json
{
  "type": "hello",
  "mode": "producer",
  "client": "orders-service",
  "version": "1.0"
}
```

The server responds with:

```json
{
  "type": "ready",
  "connectionId": "20260920T123000.000000000",
  "mode": "producer",
  "serverTime": "2026-09-20T12:30:00Z"
}
```

### Submit Event

After the handshake, producers send event messages.

```json
{
  "type": "event",
  "requestId": "request-1",
  "event": {
    "level": "info",
    "service": "orders",
    "component": "checkout-api",
    "eventType": "order.created",
    "message": "Order created successfully",
    "code": "ORDER_CREATED",
    "correlationId": "corr-123",
    "context": {
      "path": "/orders",
      "method": "POST"
    },
    "metadata": {
      "orderId": "order-123"
    }
  }
}
```

Successful response:

```json
{
  "type": "ack",
  "requestId": "request-1",
  "eventId": "evt-generated-or-provided",
  "status": "accepted",
  "timestamp": "2026-09-20T12:30:00Z"
}
```

Error response:

```json
{
  "type": "error",
  "requestId": "request-1",
  "code": "INVALID_MESSAGE",
  "message": "message is invalid"
}
```

## Health Endpoints

### GET /health

Checks service health, including storage availability.

```json
{
  "status": "healthy",
  "timestamp": "2026-09-20T12:30:00Z",
  "service": "logger-service"
}
```

If storage is unavailable:

```json
{
  "status": "unhealthy",
  "error": "storage unavailable"
}
```

### GET /health/live

Liveness check.

```json
{
  "status": "alive",
  "timestamp": "2026-09-20T12:30:00Z"
}
```

### GET /health/ready

Readiness check. Returns `200` when storage is available and `503` otherwise.

```json
{
  "status": "ready",
  "timestamp": "2026-09-20T12:30:00Z"
}
```

## Removed Legacy Endpoints

The previous legacy `LogEntry` REST API has been removed from the current
architecture.

The following endpoints are no longer part of the service contract:

```text
POST /logs
GET /logs
GET /logs/stream
GET /services
GET /stats
```

Use the WebSocket event protocol at `/ws/events` instead.

## Storage

Events are persisted as JSON Lines files.

Current file naming pattern:

```text
events_{YYYYMMDD}_{NN}.jsonl
```

Example:

```text
events_20260914_00.jsonl
```

Files are grouped by the start of the storage week. When `STORAGE_MAX_FILE_SIZE`
is exceeded, a new numbered file is created for the same week.

Old files are removed by retention rotation according to `STORAGE_ROTATION_DAYS`.

## Configuration

The service is configured through environment variables.

| Variable                  | Default      | Description                          |
| ------------------------- | ------------ | ------------------------------------ |
| `SERVER_PORT`             | `8080`       | HTTP server port                     |
| `SERVER_HOST`             | `0.0.0.0`    | HTTP server host                     |
| `SERVER_READ_TIMEOUT`     | `15s`        | Request read timeout                 |
| `SERVER_WRITE_TIMEOUT`    | `15s`        | Response write timeout               |
| `SERVER_IDLE_TIMEOUT`     | `60s`        | Connection idle timeout              |
| `SERVER_MAX_BODY_BYTES`   | `1048576`    | Maximum accepted HTTP request body   |
| `SERVER_MAX_CONNECTIONS`  | `100`        | Maximum concurrent connections       |
| `STORAGE_DATA_DIR`        | `./logs`     | Directory for JSONL event files      |
| `STORAGE_ROTATION_DAYS`   | `7`          | Days before event files expire       |
| `STORAGE_MAX_FILE_SIZE`   | `104857600`  | Max event file size in bytes         |
| `STORAGE_BUFFER_SIZE`     | `1000`       | Reserved buffer size setting         |
| `LOGGING_LEVEL`           | `info`       | Service logging/event level setting  |
| `LOGGING_FORMAT`          | `json`       | Log format                           |
| `LOGGING_OUTPUT_PATH`     | `stdout`     | Log output path                      |
| `RATE_LIMIT_ENABLED`      | `true`       | Enable process-local rate limiting   |
| `RATE_LIMIT_REQUESTS_PER` | `1000`       | Requests per rate-limit window       |
| `RATE_LIMIT_WINDOW`       | `1m`         | Rate-limit window                    |
| `LOGGER_API_KEY`          | unset        | API key required for WebSocket usage |

Example local configuration:

```dotenv
SERVER_HOST=0.0.0.0
SERVER_PORT=8080
SERVER_MAX_BODY_BYTES=1048576
SERVER_MAX_CONNECTIONS=100
RATE_LIMIT_ENABLED=true
RATE_LIMIT_REQUESTS_PER=1000
RATE_LIMIT_WINDOW=1m
STORAGE_DATA_DIR=./logs
STORAGE_ROTATION_DAYS=7
STORAGE_MAX_FILE_SIZE=104857600
LOGGER_API_KEY=local-development-key
```

## Development

### Prerequisites

- Go 1.24 or later
- Docker, optional

### Run Locally

```bash
go run cmd/server/main.go
```

### Run Tests

```bash
go test ./...
```

Useful focused test runs:

```bash
go test ./internal/security
go test ./internal/domain/services
go test ./internal/adapters/storage
go test ./internal/websockets
go test ./internal/adapters/websocket
```

## Manual WebSocket Test

You can test with a WebSocket client such as `websocat`.

Start the service:

```bash
LOGGER_API_KEY=local-development-key go run cmd/server/main.go
```

Connect:

```bash
websocat -H='X-API-Key: local-development-key' ws://localhost:8080/ws/events
```

Send hello:

```json
{
  "type": "hello",
  "mode": "producer",
  "client": "manual-test",
  "version": "1.0"
}
```

Send event:

```json
{
  "type": "event",
  "requestId": "request-1",
  "event": {
    "level": "info",
    "service": "manual-test",
    "eventType": "manual.event",
    "message": "manual event received",
    "metadata": {
      "source": "websocat"
    }
  }
}
```

Expected response:

```json
{
  "type": "ack",
  "requestId": "request-1",
  "eventId": "evt-generated-or-provided",
  "status": "accepted",
  "timestamp": "2026-09-20T12:30:00Z"
}
```

## Production Deployment Notes

- Run the service on a private network.
- Require `LOGGER_API_KEY` or enforce equivalent authentication at a private
  gateway.
- Do not expose `/ws/events` publicly without authentication, TLS, and gateway-level
  rate limiting.
- Terminate TLS at the gateway or ingress unless the service is extended with native
  TLS.
- Persist `STORAGE_DATA_DIR` on durable storage.
- Use one writer instance per JSONL data directory.
- The built-in rate limiter is process-local. For multiple replicas, enforce global
  rate limits at the gateway or replace it with a shared limiter.
- JSONL storage is suitable for a single-instance deployment. For horizontal scale,
  add a centralized event store before increasing writer replicas.

Recommended traffic flow:

```text
Application producers
        |
        v
Private gateway / ingress
TLS, authentication, global rate limit
        |
        v
Logger service
WebSocket event ingestion
        |
        v
JSONL event storage
```

## Docker

Build:

```bash
docker build -t logger-service:latest .
```

Run:

```bash
docker run -d \
  --name logger-service \
  -p 8080:8080 \
  -v "$(pwd)/logs:/app/logs" \
  -e LOGGER_API_KEY=change-me \
  -e STORAGE_DATA_DIR=/app/logs \
  -e RATE_LIMIT_ENABLED=true \
  -e RATE_LIMIT_REQUESTS_PER=1000 \
  logger-service:latest
```

For production, bind the port only on a private interface or place the container
behind an authenticated private gateway.

## Kubernetes Notes

Use a `Secret` for `LOGGER_API_KEY`.

Expose the service as `ClusterIP` unless an internal load balancer is required.

Mount durable storage for JSONL files:

```yaml
volumeMounts:
  - name: logs
    mountPath: /app/logs
volumes:
  - name: logs
    persistentVolumeClaim:
      claimName: logger-logs-pvc
```

Inject configuration:

```yaml
env:
  - name: LOGGER_API_KEY
    valueFrom:
      secretKeyRef:
        name: logger-service-secrets
        key: api-key
  - name: STORAGE_DATA_DIR
    value: /app/logs
  - name: RATE_LIMIT_ENABLED
    value: "true"
  - name: RATE_LIMIT_REQUESTS_PER
    value: "1000"
```

## Roadmap

- Add a query/read API for persisted events.
- Add monitor-mode WebSocket subscriptions for live event streams.
- Add distributed storage backends such as Redis Streams, Kafka, PostgreSQL,
  Elasticsearch, Loki, or object storage.
- Add gateway-integrated authentication and authorization.
- Add structured metrics and tracing.
- Add native TLS option for direct deployments.
- Add distributed rate limiting for multi-replica deployments.

## License

Add your license here.

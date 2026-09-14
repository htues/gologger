# Global Log and Business Event Digest Service Blueprint

## 1. Purpose

Build a centralized Go service for structured, domain-level events and application log entries emitted by multiple services.

The service provides a durable, searchable record of meaningful application activity such as:

- `user_created`
- `payment_declined`
- `permission_denied`
- `order_completed`

It is not intended to replace an observability platform. OpenTelemetry and the infrastructure observability stack remain responsible for automatically captured technical telemetry such as HTTP spans, database calls, cache calls, host metrics, and generic infrastructure failures.

## 2. Boundary With Observability

| Concern       | Global Log and Event Service                   | OTel / Observability Stack                         |
| ------------- | ---------------------------------------------- | -------------------------------------------------- |
| Primary data  | Domain events and application log entries      | Metrics, traces, infrastructure and technical logs |
| Emission      | Explicitly emitted by application code         | Automatic instrumentation and platform collectors  |
| Audience      | Product, support, audit, and application teams | SRE and on-call teams                              |
| Main question | "What meaningful business thing happened?"     | "Is the system healthy and where is it slow?"      |
| Examples      | `payment_declined`, `user_created`             | HTTP 500, SQL latency, Redis timeout, CPU usage    |

The services connect through shared identifiers, especially `traceId`, `spanId`, and `correlationId`. A domain event may link back to the distributed trace in the observability platform, but generic infrastructure failures should not be duplicated here unless they carry meaningful business context.

## 3. Goals

The service must:

- Receive events through one canonical WebSocket protocol.
- Validate JSON messages and reject malformed or unsafe payloads.
- Support `trace`, `debug`, `info`, `warn`, `error`, and `fatal` levels, plus `off` for filtering configuration.
- Preserve the event digest levels `information`, `warning`, and `error` as a compatibility mapping.
- Enrich events with server-generated identifiers and timestamps.
- Include request and identity context when available.
- Persist accepted events behind a replaceable storage port.
- Acknowledge a producer only after successful persistence.
- Support concurrent producers and optional monitoring consumers.
- Provide bounded queues and explicit backpressure behavior.
- Sanitize sensitive data before storage, publication, and internal logging.
- Support asynchronous, buffered processing without silently discarding producer events.
- Shut down gracefully and report events that could not be persisted.

The initial implementation prioritizes correctness and a stable contract over production-scale querying and distribution.

## 4. Non-Goals

The service will not:

- Replace OpenTelemetry, a metrics backend, a trace backend, or a log collector.
- Automatically capture every HTTP, database, or infrastructure failure.
- Accept unauthenticated public traffic in a production deployment.
- Promise exactly-once delivery across arbitrary storage providers.
- Expose database-specific behavior to the domain layer.

## 5. Protocol and Transport

### 5.1 Canonical endpoint

```text
/ws/events
```

The endpoint supports two connection modes:

- `producer`: submits events for validation and persistence.
- `monitor`: receives accepted events matching a subscription.

Every WebSocket frame contains exactly one JSON protocol message. A connection has one read pump and one write pump; no other goroutine may write directly to its socket.

HTTP is not used for event ingestion. Optional HTTP endpoints are limited to infrastructure and operational concerns:

```text
/health/live
/health/ready
/metrics
```

A future authenticated query API may expose stored events, but it must not create a second write contract. REST, GraphQL, or another protocol can be added as an adapter over the same application ports later.

### 5.2 Authentication and handshake

1. The client opens `/ws/events`.
2. The server validates `X-API-Key` or, only for constrained clients, a query parameter.
3. The server applies the configured origin policy and connection limit.
4. The server assigns a connection ID.
5. The client sends a `hello` message.
6. The server validates the requested mode and replies with `ready`.

Example:

```json
{
  "type": "hello",
  "mode": "producer",
  "client": "payments-service",
  "version": "1"
}
```

Response:

```json
{
  "type": "ready",
  "connectionId": "conn-123",
  "mode": "producer",
  "serverTime": "2026-09-13T14:30:00Z"
}
```

Authentication is disabled only for explicitly controlled local development. Production deployments must use a secret managed outside source control, TLS, strict origin validation, and a private network or firewall until centralized authentication is available.

## 6. Event Model

### 6.1 Event envelope

```json
{
  "type": "event",
  "requestId": "req-001",
  "event": {
    "eventId": "01JABC123XYZ",
    "timestamp": "2026-09-13T14:30:00Z",
    "level": "info",
    "service": "payments",
    "component": "checkout",
    "eventType": "payment_declined",
    "message": "Payment provider rejected the transaction",
    "code": "PAYMENT_DECLINED",
    "sessionId": "sess-123",
    "userId": "user-456",
    "correlationId": "req-12345",
    "traceId": "trace-98765",
    "spanId": "span-111",
    "context": {
      "path": "/checkout",
      "method": "POST",
      "domain": "payments.example.com",
      "requiredPermission": "payments.write",
      "cookiePresent": true
    },
    "metadata": {
      "orderId": "order-456",
      "provider": "example-provider"
    }
  }
}
```

### 6.2 Required and optional fields

| Field           | Required | Description                                             |
| --------------- | -------- | ------------------------------------------------------- |
| `eventId`       | No       | Client identifier or server-generated unique identifier |
| `timestamp`     | No       | RFC3339 event time; server fills it when absent         |
| `level`         | Yes      | `trace`, `debug`, `info`, `warn`, `error`, or `fatal`   |
| `service`       | Yes      | Originating service                                     |
| `component`     | No       | Originating module or component                         |
| `eventType`     | Yes      | Machine-readable domain event name                      |
| `message`       | Yes      | Human-readable description                              |
| `code`          | No       | Stable application-specific code                        |
| `sessionId`     | No       | Session identifier when available                       |
| `userId`        | No       | User identifier when available; never a secret          |
| `correlationId` | No       | Request or workflow correlation identifier              |
| `traceId`       | No       | Distributed trace identifier                            |
| `spanId`        | No       | Distributed span identifier                             |
| `context`       | No       | Request and authorization context                       |
| `metadata`      | No       | Structured domain data with configured size limits      |

The canonical digest levels `information`, `warning`, and `error` map to `info`, `warn`, and `error`. New producers should use the canonical logging levels. `off` is a configuration value meaning that a logger emits nothing; it is not a valid event level.

Validation must reject unknown levels, missing required fields, invalid timestamps, oversized payloads, invalid UTF-8, and metadata containing prohibited sensitive fields. Server enrichment must not overwrite trusted identifiers without an explicit policy.

### 6.3 Context injection

Client libraries or middleware should populate `sessionId`, `userId`, request path, method, domain, required permission, cookie presence, correlation ID, trace ID, and span ID automatically. The service validates and stores this context but does not infer identity from arbitrary payload text.

## 7. Protocol Messages

### Event submission

```json
{
  "type": "event",
  "requestId": "req-001",
  "event": {
    "level": "info",
    "service": "users",
    "eventType": "user_created",
    "message": "User created successfully"
  }
}
```

### Successful acknowledgment

An acknowledgment is sent only after the event has been accepted and persisted by the active storage provider.

```json
{
  "type": "ack",
  "requestId": "req-001",
  "eventId": "01JABC123XYZ",
  "status": "accepted",
  "timestamp": "2026-09-13T14:30:00Z"
}
```

### Rejection

```json
{
  "type": "error",
  "requestId": "req-001",
  "code": "INVALID_EVENT",
  "message": "The event level is required"
}
```

Stable protocol error codes include:

```text
UNAUTHORIZED
FORBIDDEN
INVALID_JSON
INVALID_MESSAGE
INVALID_EVENT
UNSUPPORTED_EVENT_LEVEL
MISSING_REQUIRED_FIELD
MESSAGE_TOO_LARGE
RATE_LIMITED
STORAGE_UNAVAILABLE
INTERNAL_ERROR
SERVER_SHUTTING_DOWN
```

Errors must not expose credentials, database details, stack traces, or internal topology.

### Monitor subscription

```json
{
  "type": "subscribe",
  "requestId": "sub-001",
  "filters": {
    "levels": ["warn", "error"],
    "services": ["payments"],
    "eventTypes": ["payment_declined"]
  }
}
```

Response:

```json
{
  "type": "subscription_ack",
  "requestId": "sub-001",
  "status": "subscribed"
}
```

Published events use the event envelope and contain only data permitted by the monitor's authorization and redaction policy.

## 8. Architecture

```text
Producer WebSocket
        |
        v
WebSocket Handler -> Connection Manager / Hub
        |                         |
        v                         v
Event Processor              Monitor Clients
        |
        +--> Decode and validate
        +--> Enrich and sanitize
        +--> Apply level policy
        +--> Persist through EventStore
        +--> Publish accepted event
        +--> Return ack or protocol error
```

Suggested repository structure:

```text
cmd/server/main.go
internal/adapters/config/
internal/adapters/http/       # health, metrics, and future query adapters
internal/adapters/storage/    # memory, JSON file, Redis, and future providers
internal/adapters/websocket/  # handler, client, and protocol adapter
internal/contracts/           # wire and application contracts
internal/domain/entities/     # event and log-entry entities
internal/domain/services/     # processor and logging policy
internal/ports/               # storage, processor, and publisher interfaces
internal/websockets/          # hub and client lifecycle
pkg/ids/                      # identifier generation
```

The hub must coordinate clients and publication but must not depend on Redis, files, or another concrete database. Domain services depend on ports, not adapters.

## 9. Storage and Durability

The primary storage abstraction is:

```go
type EventStore interface {
    Store(ctx context.Context, event contracts.Event) error
    Get(ctx context.Context, eventID string) (contracts.Event, error)
    Query(ctx context.Context, filter contracts.EventFilter) ([]contracts.Event, error)
    Health(ctx context.Context) error
    Close() error
}
```

Initial providers:

1. In-memory storage for unit and integration tests.
2. JSON file storage for local development and early deployments.
3. Redis Streams, a document store, Loki, OpenSearch, or PostgreSQL JSONB as later adapters.

The active provider is selected through configuration and isolated behind the port. Storage-specific settings are required only for the selected provider.

Durability semantics:

- The producer receives an acknowledgment only after `Store` succeeds.
- Storage failure returns `STORAGE_UNAVAILABLE` or `INTERNAL_ERROR` and never silently discards the event.
- `eventId` supports idempotency. Providers should treat repeated writes of the same ID as the same event where feasible.
- The service documents at-least-once processing unless the selected provider can guarantee stronger semantics.
- A bounded processing queue applies backpressure. When full, the service rejects new submissions with a stable error rather than dropping producer events.

JSON file storage must support configurable directory, weekly rotation by default, maximum file size, safe file permissions, flush behavior, and cleanup policy. Rotation should be adjustable without changing domain code.

## 10. Logging Policy

The service's own operational logs use structured JSON and the levels `trace`, `debug`, `info`, `warn`, `error`, and `fatal`. The configured global level filters operational output. Critical lifecycle, security, and persistence failures bypass ordinary verbosity filtering as appropriate.

Operational logs and business events are separate concepts:

- Operational logs describe what the service itself is doing.
- Business events describe what an application domain is doing.

Both share structured fields and correlation identifiers, but they must not be conflated in storage or dashboards. Stack traces and request details belong in operational error logs when useful; they should not be copied into business event messages by default.

## 11. Security and Privacy

The service must implement:

- API-key authentication initially, with a migration path to centralized AuthService authorization.
- TLS for network traffic in production.
- Strict configurable WebSocket origin validation.
- Maximum frame, metadata, and aggregate payload sizes.
- Read and write deadlines, connection limits, and per-client rate limits.
- Input validation and output redaction.
- Secret and PII filtering before persistence and publication.
- Protection against log injection, including control-character handling.
- Audit records for administrative access and future event queries.
- No passwords, tokens, session secrets, raw cookies, or unnecessary sensitive PII in events.

The service should default to deny for unrecognized origins, API keys, monitor subscriptions, and query access.

## 12. Concurrency, Buffering, and Backpressure

Each connection has:

- One read pump.
- One write pump.
- One bounded outbound queue.

Processing uses bounded internal queues and asynchronous workers so slow storage does not block socket reads indefinitely. Queue sizes and worker counts are configurable.

Producer events are never silently dropped. When the processing queue is full, the service returns a backpressure error or applies a configured admission policy. For slow monitor clients, the initial policy is to disconnect the client with an explicit reason after its outbound queue is full; monitor publication must not block producer persistence.

## 13. Configuration

Configuration comes from environment variables or the existing configuration adapter. Recommended settings include:

```text
SERVER_HOST
SERVER_PORT
SERVER_READ_TIMEOUT
SERVER_WRITE_TIMEOUT
SERVER_IDLE_TIMEOUT
WEBSOCKET_PATH
WEBSOCKET_MAX_MESSAGE_SIZE
WEBSOCKET_MAX_CONNECTIONS
WEBSOCKET_SEND_QUEUE_SIZE
AUTH_ENABLED
API_KEY
STORAGE_PROVIDER
STORAGE_TIMEOUT
STORAGE_QUEUE_SIZE
STORAGE_DATA_DIR
STORAGE_ROTATION_DAYS
STORAGE_MAX_FILE_SIZE
RATE_LIMIT_ENABLED
RATE_LIMIT_EVENTS_PER_SECOND
SHUTDOWN_TIMEOUT
LOGGING_LEVEL
LOGGING_FORMAT
LOGGING_OUTPUT_PATH
```

Provider-specific settings remain isolated:

```text
REDIS_URL
REDIS_STREAM
JSON_STORAGE_PATH
```

The configuration layer should provide sensible local defaults and fail clearly when a selected provider lacks required settings.

## 14. Health, Metrics, and Observability

Optional infrastructure endpoints:

- `/health/live`: process is running.
- `/health/ready`: configuration and selected storage are available.
- `/metrics`: operational metrics for the monitoring system.

Recommended metrics:

- Active connections, producers, and monitors.
- Events received, accepted, rejected, persisted, and failed.
- Processing latency and storage latency.
- Processing queue depth and rejected submissions due to backpressure.
- Authentication failures and rate-limit violations.
- Dropped or disconnected monitor clients.
- Connection duration and shutdown drain duration.

The service should export trace and correlation identifiers in its operational logs so domain events can be followed into the external observability stack.

## 15. Lifecycle and Graceful Shutdown

Shutdown sequence:

1. Stop accepting new WebSocket connections.
2. Notify connected clients with `SERVER_SHUTTING_DOWN`.
3. Stop accepting new producer events.
4. Drain the bounded processing queue until empty or the shutdown deadline expires.
5. Persist pending events and record failures.
6. Close monitor connections.
7. Close the storage provider.
8. Flush operational logs and metrics.
9. Exit after the configured timeout.

Events that cannot be persisted before the deadline must be counted and reported through operational logs and metrics.

## 16. Testing Strategy

Use Go's built-in `testing` package.

### Contract tests

- Valid `trace`, `debug`, `info`, `warn`, `error`, and `fatal` events.
- Compatibility mapping for `information`, `warning`, and `error`.
- Unknown levels and `off` used as an event level.
- Missing required fields.
- Invalid timestamps and malformed JSON.
- Oversized metadata and prohibited sensitive fields.

### Processor tests

- ID and timestamp enrichment.
- Context preservation and sanitization.
- Configured level filtering.
- Critical event behavior.
- Storage success and storage failure.
- Acknowledgment only after persistence.
- Duplicate event handling.
- Backpressure behavior.

### WebSocket tests

- Successful authentication and handshake.
- Failed authentication and origin rejection.
- Producer registration and event submission.
- Monitor registration and filtered subscription.
- Malformed messages and size limits.
- Concurrent clients and single-writer guarantees.
- Slow monitor behavior.
- Client disconnect and graceful shutdown.

### Storage contract tests

Every provider must be tested for storing, retrieving, querying, idempotency, health checks, rotation where applicable, and closing resources. Most unit tests use the in-memory store; integration tests exercise JSON file storage and later production providers.

## 17. Delivery Plan

### Phase 1: Core MVP

1. Define event and protocol contracts.
2. Implement validation, level mapping, enrichment, and sanitization.
3. Define the `EventStore` port.
4. Implement in-memory storage and processor tests.
5. Implement JSON file storage with weekly rotation.
6. Implement WebSocket authentication, handshake, producer mode, acknowledgments, and protocol errors.
7. Add bounded queues and graceful shutdown.

### Phase 2: Monitoring and operations

1. Add monitor subscriptions and filtered publication.
2. Add health endpoints and metrics.
3. Add rate limiting, connection limits, and stronger redaction.
4. Add client libraries or middleware for Node.js and Go producers.
5. Document deployment, secrets, and private-network requirements.

### Phase 3: Scale and integration

1. Add Redis Streams or another production storage adapter.
2. Add authenticated query access without changing the event write contract.
3. Add centralized AuthService integration and audit access logs.
4. Add trace propagation and OpenTelemetry instrumentation.
5. Add load tests, retention policies, and operational dashboards.

## 18. Acceptance Criteria for the First Working Version

The first release is complete when it can:

- Accept authenticated producer connections at `/ws/events`.
- Complete the `hello` and `ready` handshake.
- Validate and enrich information, warning, and error-compatible events.
- Accept canonical `trace` through `fatal` levels according to configuration.
- Persist events in memory and JSON files through the same `EventStore` port.
- Return an acknowledgment only after storage succeeds.
- Return stable protocol errors for malformed, invalid, oversized, or unauthorized requests.
- Preserve `sessionId`, `userId`, request context, `correlationId`, `traceId`, and `spanId` when provided.
- Redact configured secrets and sensitive fields.
- Apply bounded queues and explicit producer backpressure.
- Shut down without accepting new work and report undrained events.
- Pass contract, processor, storage, and WebSocket integration tests.

This design keeps the service focused: applications explicitly send meaningful domain activity here, while OTel and the surrounding observability platform retain ownership of technical system telemetry.

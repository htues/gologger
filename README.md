# Logger Service

A microservice-based logging system built with Go using hexagonal architecture. This service provides centralized logging capabilities for multiple applications and services.

## Features

- **Structured Logging**: JSON-based log format with configurable levels
- **Service Isolation**: Separate log files per service with weekly rotation
- **Real-time Streaming**: WebSocket support for live log monitoring
- **RESTful API**: HTTP endpoints for log ingestion and retrieval
- **Buffered Storage**: Asynchronous, non-blocking log storage
- **Configurable**: Environment-based configuration with hot reload support
- **Containerized**: Docker support for easy deployment

## Architecture

This service follows hexagonal architecture principles:

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  HTTP Adapter  │  WebSocket Adapter  │  Config Adapter     │
├─────────────────────────────────────────────────────────────┤
│                    Domain Layer                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   Entities  │  │   Services  │  │   Ports     │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
├─────────────────────────────────────────────────────────────┤
│                    Infrastructure Layer                     │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │JSON Storage │  │HTTP Handler │  │Viper Config │        │
│  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────┘
```

## API Endpoints

### POST /logs
Log a new entry.

**Request Body:**
```json
{
  "event_timestamp": "2024-01-15T10:30:00Z",
  "level": "info",
  "serviceName": "user-service",
  "data": {
    "code": "USER_CREATED",
    "message": "User created successfully",
    "context": {
      "userId": "12345",
      "sessionId": "sess_abc123",
      "endpoint": "/api/users",
      "method": "POST",
      "domain": "example.com"
    },
    "extra": {
      "userEmail": "user@example.com"
    }
  }
}
```

**Response:**
```json
{
  "id": "20240115103000_abc12345",
  "timestamp": "2024-01-15T10:30:00Z",
  "status": "success"
}
```

### GET /logs
Retrieve logs with optional filtering.

**Query Parameters:**
- `serviceName` (optional): Filter by service name
- `level` (optional): Filter by log level
- `limit` (optional): Maximum number of logs to return (default: 100)

**Response:**
```json
{
  "logs": [...],
  "count": 50
}
```

### GET /logs/stream
WebSocket endpoint for real-time log streaming.

**Query Parameters:**
- `serviceName` (optional): Filter by service name
- `level` (optional): Filter by log level

### GET /services
Get list of all services that have logged entries.

**Response:**
```json
{
  "services": ["user-service", "auth-service", "payment-service"],
  "count": 3
}
```

### GET /stats
Get logging statistics.

**Query Parameters:**
- `serviceName` (optional): Get stats for specific service

**Response:**
```json
{
  "totalEntries": 1500,
  "entriesByLevel": {
    "info": 1000,
    "error": 50,
    "warn": 450
  },
  "entriesByService": {
    "user-service": 800,
    "auth-service": 700
  }
}
```

### GET /health
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T10:30:00Z",
  "service": "logger-service"
}
```

## Configuration

The service is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP server port |
| `SERVER_HOST` | `0.0.0.0` | HTTP server host |
| `SERVER_READ_TIMEOUT` | `15s` | Request read timeout |
| `SERVER_WRITE_TIMEOUT` | `15s` | Response write timeout |
| `SERVER_IDLE_TIMEOUT` | `60s` | Connection idle timeout |
| `STORAGE_DATA_DIR` | `./logs` | Directory for log files |
| `STORAGE_ROTATION_DAYS` | `7` | Days before log rotation |
| `STORAGE_MAX_FILE_SIZE` | `104857600` | Max file size in bytes (100MB) |
| `STORAGE_BUFFER_SIZE` | `1000` | Buffer size for log entries |
| `LOGGING_LEVEL` | `info` | Global log level |
| `LOGGING_FORMAT` | `json` | Log format |
| `LOGGING_OUTPUT_PATH` | `stdout` | Log output path |
| `RATE_LIMIT_ENABLED` | `true` | Enable rate limiting |
| `RATE_LIMIT_REQUESTS_PER` | `1000` | Requests per window |
| `RATE_LIMIT_WINDOW` | `1m` | Rate limit window |

## Log Levels

Supported log levels (in order of severity):
- `trace` - Most verbose
- `debug` - Debug information
- `info` - General information
- `warn` - Warnings
- `error` - Errors
- `fatal` - Fatal errors
- `off` - Disable logging

## File Storage

Logs are stored in JSON files with the following naming convention:
```
{serviceName}_{MMDDYY}.json
```

Example: `user-service_011524.json`

Files are rotated weekly (on Sundays) and old files are automatically cleaned up based on the `STORAGE_ROTATION_DAYS` configuration.

## Development

### Prerequisites
- Go 1.22.2 or later
- Docker (optional)

### Local Development

1. Clone the repository:
```bash
git clone <repository-url>
cd gologgermservice
```

2. Install dependencies:
```bash
go mod download
```

3. Run the service:
```bash
go run cmd/server/main.go
```

### Docker Development

1. Build and run with Docker Compose:
```bash
docker-compose up --build
```

2. Or build and run manually:
```bash
docker build -t logger-service .
docker run -p 8080:8080 -v $(pwd)/logs:/app/logs logger-service
```

## Testing

### Manual Testing

1. Start the service
2. Send a test log entry:
```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{
    "level": "info",
    "serviceName": "test-service",
    "data": {
      "message": "Test log entry",
      "context": {
        "userId": "123",
        "sessionId": "sess_456"
      }
    }
  }'
```

3. Retrieve logs:
```bash
curl http://localhost:8080/logs?serviceName=test-service
```

4. Check health:
```bash
curl http://localhost:8080/health
```

## Production Deployment

### Docker Deployment

1. Build production image:
```bash
docker build -t logger-service:latest .
```

2. Run with production configuration:
```bash
docker run -d \
  --name logger-service \
  -p 8080:8080 \
  -v /var/log/logger-service:/app/logs \
  -e LOGGING_LEVEL=warn \
  -e RATE_LIMIT_REQUESTS_PER=5000 \
  logger-service:latest
```

### Kubernetes Deployment

Create a deployment manifest:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: logger-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: logger-service
  template:
    metadata:
      labels:
        app: logger-service
    spec:
      containers:
      - name: logger-service
        image: logger-service:latest
        ports:
        - containerPort: 8080
        env:
        - name: LOGGING_LEVEL
          value: "warn"
        - name: RATE_LIMIT_REQUESTS_PER
          value: "5000"
        volumeMounts:
        - name: logs
          mountPath: /app/logs
      volumes:
      - name: logs
        persistentVolumeClaim:
          claimName: logger-logs-pvc
```

## Monitoring

The service exposes a health check endpoint at `/health` that can be used by load balancers and monitoring systems.

### Metrics to Monitor
- Request rate and response times
- Storage usage and file rotation
- Error rates and log levels
- Buffer utilization

## Future Enhancements

- [ ] Authentication and authorization
- [ ] Elasticsearch/Loki integration
- [ ] GraphQL API
- [ ] Advanced filtering and search
- [ ] Log aggregation and analytics
- [ ] Service mesh integration (Kafka, RabbitMQ)
- [ ] Metrics and monitoring integration
- [ ] Multi-region support

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

[Add your license here] 
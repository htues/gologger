package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hftamayo/gologger/internal/domain/entities"
	"github.com/hftamayo/gologger/internal/ports"
	"github.com/hftamayo/gologger/internal/security"
	"go.uber.org/zap"
)

type fakeLoggerService struct {
	entries        []entities.LogEntry
	requestedLimit int
	logEntryError  error
	logs           []entities.LogEntry
}

func (service *fakeLoggerService) LogEntry(
	_ context.Context,
	entry entities.LogEntry,
) error {
	if service.logEntryError != nil {
		return service.logEntryError
	}

	service.entries = append(service.entries, entry)
	return nil
}

func (service *fakeLoggerService) GetLogs(
	_ context.Context,
	_ string,
	_ entities.LogLevel,
	limit int,
) ([]entities.LogEntry, error) {
	service.requestedLimit = limit
	return service.logs, nil
}

func (service *fakeLoggerService) StreamLogs(
	_ context.Context,
	_ string,
	_ entities.LogLevel,
) (<-chan entities.LogEntry, error) {
	stream := make(chan entities.LogEntry)
	close(stream)
	return stream, nil
}

func (service *fakeLoggerService) GetServiceNames(
	context.Context,
) ([]string, error) {
	return nil, nil
}

func (service *fakeLoggerService) GetLogStats(
	context.Context,
	string,
) (*ports.LogStats, error) {
	return &ports.LogStats{}, nil
}

func newTestHandler(service *fakeLoggerService) *Handler {
	return &Handler{
		loggerService: service,
		logger:        zap.NewNop(),
		metrics:       newMetrics(),
	}
}

func TestLogEntryAcceptsValidRequestAndRedactsData(t *testing.T) {
	service := &fakeLoggerService{}
	handler := newTestHandler(service)

	request := httptest.NewRequest(
		http.MethodPost,
		"/logs",
		strings.NewReader(`{
            "level": "error",
            "serviceName": "orders-service",
            "data": {
                "message": "token=fixture-value\nvisible",
                "extra": {
                    "pass" : "fixture-value"
                }
            }
        }`),
	)
	recorder := httptest.NewRecorder()

	handler.LogEntry(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", recorder.Code)
	}

	if len(service.entries) != 1 {
		t.Fatalf("expected one stored entry, got %d", len(service.entries))
	}

	if service.entries[0].Data.Message != "[REDACTED] visible" {
		t.Fatalf(
			"expected redacted message, got %q",
			service.entries[0].Data.Message,
		)
	}
}

func TestLogEntryRejectsInvalidJSON(t *testing.T) {
	service := &fakeLoggerService{}
	handler := newTestHandler(service)

	request := httptest.NewRequest(
		http.MethodPost,
		"/logs",
		strings.NewReader(`{"level":`),
	)
	recorder := httptest.NewRecorder()

	handler.LogEntry(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", recorder.Code)
	}

	if len(service.entries) != 0 {
		t.Fatalf("expected no stored entries, got %d", len(service.entries))
	}
}

func TestLogEntryRejectsMissingServiceName(t *testing.T) {
	service := &fakeLoggerService{}
	handler := newTestHandler(service)

	request := httptest.NewRequest(
		http.MethodPost,
		"/logs",
		strings.NewReader(`{
            "level": "info",
            "data": {
                "message": "fixture message"
            }
        }`),
	)
	recorder := httptest.NewRecorder()

	handler.LogEntry(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", recorder.Code)
	}

	if len(service.entries) != 0 {
		t.Fatalf("expected no stored entries, got %d", len(service.entries))
	}
}

func TestLogEntryReturnsInternalServerError(t *testing.T) {
	service := &fakeLoggerService{
		logEntryError: errors.New("storage unavailable"),
	}
	handler := newTestHandler(service)

	request := httptest.NewRequest(
		http.MethodPost,
		"/logs",
		strings.NewReader(`{
            "level": "error",
            "serviceName": "orders-service",
            "data": {
                "message": "fixture message"
            }
        }`),
	)
	recorder := httptest.NewRecorder()

	handler.LogEntry(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", recorder.Code)
	}
}

func TestGetLogsUsesDefaultLimit(t *testing.T) {
	service := &fakeLoggerService{
		logs: []entities.LogEntry{
			{
				ServiceName: "orders-service",
				Data: entities.LogData{
					Message: "fixture message",
				},
			},
		},
	}
	handler := newTestHandler(service)

	request := httptest.NewRequest(
		http.MethodGet,
		"/logs?limit=invalid",
		nil,
	)
	recorder := httptest.NewRecorder()

	handler.GetLogs(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", recorder.Code)
	}

	if service.requestedLimit != 100 {
		t.Fatalf(
			"expected default limit 100, got %d",
			service.requestedLimit,
		)
	}
}

func TestStreamLogsRejectsWhenConnectionLimitIsReached(t *testing.T) {
	service := &fakeLoggerService{}
	handler := newTestHandler(service)

	limiter := security.NewConnectionLimiter(1)
	handler.SetConnectionLimiter(limiter)

	if !limiter.TryAcquire() {
		t.Fatal("expected first connection slot to be available")
	}
	defer limiter.Release()

	request := httptest.NewRequest(
		http.MethodGet,
		"/logs/stream",
		nil,
	)
	recorder := httptest.NewRecorder()

	handler.StreamLogs(recorder, request)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status 503, got %d", recorder.Code)
	}
}

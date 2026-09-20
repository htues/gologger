package websockets

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
)

type testEventStore struct {
	stored []contracts.Event
	err    error
}

func (store *testEventStore) Store(
	_ context.Context,
	event contracts.Event,
) error {
	if store.err != nil {
		return store.err
	}

	store.stored = append(store.stored, event)
	return nil
}

func (*testEventStore) Get(
	context.Context,
	string,
) (contracts.Event, error) {
	return contracts.Event{}, errors.New("not implemented")
}

func (*testEventStore) Query(
	context.Context,
	contracts.EventFilter,
) ([]contracts.Event, error) {
	return nil, errors.New("not implemented")
}

func (*testEventStore) Health(context.Context) error {
	return nil
}

func (*testEventStore) Close() error {
	return nil
}

type testEventProcessor struct {
	err error
}

func (processor testEventProcessor) Process(
	_ context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	if processor.err != nil {
		return contracts.Event{}, processor.err
	}

	if event.EventID == "" {
		event.EventID = "fixture-event-id"
	}

	return event, nil
}

func TestHandlerRejectsInvalidAPIKey(t *testing.T) {
	handler := NewHandler(
		&testEventStore{},
		testEventProcessor{},
		"fixture-api-key",
	)

	request := httptest.NewRequest(
		http.MethodGet,
		"/ws/events",
		nil,
	)
	request.Header.Set("X-API-Key", "wrong-key")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf(
			"expected status 401, got %d",
			recorder.Code,
		)
	}
}

func TestHandlerRejectsDisallowedOrigin(t *testing.T) {
	handler := NewHandler(
		&testEventStore{},
		testEventProcessor{},
		"fixture-api-key",
	)
	handler.AllowedOrigins["https://allowed.example"] = struct{}{}

	request := httptest.NewRequest(
		http.MethodGet,
		"/ws/events",
		nil,
	)
	request.Header.Set("X-API-Key", "fixture-api-key")
	request.Header.Set("Origin", "https://blocked.example")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf(
			"expected status 403, got %d",
			recorder.Code,
		)
	}
}

func TestHandlerRejectsMissingDependencies(t *testing.T) {
	handler := NewHandler(nil, nil, "fixture-api-key")

	request := httptest.NewRequest(
		http.MethodGet,
		"/ws/events",
		nil,
	)
	request.Header.Set("X-API-Key", "fixture-api-key")

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf(
			"expected status 500, got %d",
			recorder.Code,
		)
	}
}

func TestHandlerAllowsConfiguredOrigin(t *testing.T) {
	handler := NewHandler(nil, nil, "fixture-api-key")
	handler.AllowedOrigins["https://allowed.example"] = struct{}{}

	request := httptest.NewRequest(
		http.MethodGet,
		"/ws/events",
		nil,
	)
	request.Header.Set("Origin", "https://allowed.example")

	if !handler.originAllowed(request) {
		t.Fatal("expected configured origin to be allowed")
	}
}

func TestHandlerAllowsLocalhostWhenConfigured(t *testing.T) {
	handler := NewHandler(nil, nil, "")
	handler.AllowLocalhost = true

	request := httptest.NewRequest(
		http.MethodGet,
		"/ws/events",
		nil,
	)
	request.Host = "localhost:8080"

	if !handler.authenticate(request) {
		t.Fatal("expected localhost authentication to succeed")
	}

	if !handler.originAllowed(request) {
		t.Fatal("expected localhost origin to be allowed")
	}
}

func TestHandlerUsesDefaultReadLimit(t *testing.T) {
	handler := &Handler{}

	if got := handler.readLimit(); got != defaultReadLimit {
		t.Fatalf(
			"readLimit() = %d, want %d",
			got,
			defaultReadLimit,
		)
	}
}

func TestHandlerUsesConfiguredReadLimit(t *testing.T) {
	handler := &Handler{
		ReadLimit: 2048,
	}

	if got := handler.readLimit(); got != 2048 {
		t.Fatalf("readLimit() = %d, want 2048", got)
	}
}

func TestNewConnectionUsesDefaultQueueSize(t *testing.T) {
	handler := &Handler{
		WriteQueueSize: 0,
	}

	connection := newConnection(handler, nil)

	if cap(connection.send) != defaultWriteQueue {
		t.Fatalf(
			"queue capacity = %d, want %d",
			cap(connection.send),
			defaultWriteQueue,
		)
	}
}

func TestHandlerCompletesProducerHandshake(t *testing.T) {
	handler := NewHandler(
		&testEventStore{},
		testEventProcessor{},
		"fixture-api-key",
	)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	client, response, err := dialHandler(server, "fixture-api-key")
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	defer response.Body.Close()
	defer client.Close()

	if response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf(
			"expected status 101, got %d",
			response.StatusCode,
		)
	}

	if err := client.WriteJSON(contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "fixture-producer",
		Version: "test",
	}); err != nil {
		t.Fatalf("writing hello failed: %v", err)
	}

	var ready contracts.ReadyMessage
	if err := client.ReadJSON(&ready); err != nil {
		t.Fatalf("reading ready message failed: %v", err)
	}

	if ready.Type != contracts.MessageTypeReady {
		t.Fatalf("expected ready message, got %q", ready.Type)
	}

	if ready.ConnectionID == "" {
		t.Fatal("expected connection ID")
	}

	if ready.Mode != contracts.ConnectionModeProducer {
		t.Fatalf("expected producer mode, got %q", ready.Mode)
	}
}

func TestHandlerProcessesEventAndReturnsAck(t *testing.T) {
	store := &testEventStore{}
	handler := NewHandler(
		store,
		testEventProcessor{},
		"fixture-api-key",
	)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	client, response, err := dialHandler(server, "fixture-api-key")
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	defer response.Body.Close()
	defer client.Close()

	writeHello(t, client)
	readReady(t, client)

	if err := client.WriteJSON(contracts.EventMessage{
		Type:      contracts.MessageTypeEvent,
		RequestID: "request-1",
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "orders",
			EventType: "order_created",
			Message:   "fixture event",
		},
	}); err != nil {
		t.Fatalf("writing event failed: %v", err)
	}

	var ack contracts.AckMessage
	if err := client.ReadJSON(&ack); err != nil {
		t.Fatalf("reading ack failed: %v", err)
	}

	if ack.Type != contracts.MessageTypeAck {
		t.Fatalf("expected ack message, got %q", ack.Type)
	}

	if ack.RequestID != "request-1" {
		t.Fatalf("expected request ID request-1, got %q", ack.RequestID)
	}

	if ack.Status != contracts.AckStatusAccepted {
		t.Fatalf("expected accepted status, got %q", ack.Status)
	}

	if len(store.stored) != 1 {
		t.Fatalf("expected one stored event, got %d", len(store.stored))
	}
}

func TestHandlerReturnsStorageError(t *testing.T) {
	store := &testEventStore{
		err: errors.New("fixture storage failure"),
	}
	handler := NewHandler(
		store,
		testEventProcessor{},
		"fixture-api-key",
	)

	server := httptest.NewServer(http.HandlerFunc(handler.ServeHTTP))
	defer server.Close()

	client, response, err := dialHandler(server, "fixture-api-key")
	if err != nil {
		t.Fatalf("Dial returned error: %v", err)
	}
	defer response.Body.Close()
	defer client.Close()

	writeHello(t, client)
	readReady(t, client)

	if err := client.WriteJSON(contracts.EventMessage{
		Type:      contracts.MessageTypeEvent,
		RequestID: "request-1",
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "orders",
			EventType: "order_created",
			Message:   "fixture event",
		},
	}); err != nil {
		t.Fatalf("writing event failed: %v", err)
	}

	var message contracts.ErrorMessage
	if err := client.ReadJSON(&message); err != nil {
		t.Fatalf("reading error failed: %v", err)
	}

	if message.Code != contracts.ProtocolErrorStorageUnavailable {
		t.Fatalf(
			"expected storage error code %q, got %q",
			contracts.ProtocolErrorStorageUnavailable,
			message.Code,
		)
	}
}

func funcURL(serverURL string) string {
	serverURL = "ws" + serverURL[len("http"):]
	return serverURL
}

func dialHandler(
	server *httptest.Server,
	apiKey string,
) (*websocket.Conn, *http.Response, error) {
	header := http.Header{}
	header.Set("X-API-Key", apiKey)

	return websocket.DefaultDialer.Dial(
		funcURL(server.URL),
		header,
	)
}

func writeHello(t *testing.T, client *websocket.Conn) {
	t.Helper()

	if err := client.WriteJSON(contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "fixture-producer",
		Version: "test",
	}); err != nil {
		t.Fatalf("writing hello failed: %v", err)
	}
}

func readReady(t *testing.T, client *websocket.Conn) {
	t.Helper()

	_ = client.SetReadDeadline(time.Now().Add(time.Second))

	var ready contracts.ReadyMessage
	if err := client.ReadJSON(&ready); err != nil {
		t.Fatalf("reading ready failed: %v", err)
	}

	if ready.Type != contracts.MessageTypeReady {
		t.Fatalf("expected ready message, got %q", ready.Type)
	}
}

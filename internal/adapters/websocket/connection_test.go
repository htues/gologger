package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
)

type connectionTestProcessor struct {
	mu          sync.Mutex
	processFn   func(ctx context.Context, event contracts.Event) (contracts.Event, error)
	processSeen []contracts.Event
}

func (processor *connectionTestProcessor) Process(
	ctx context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	processor.mu.Lock()
	processor.processSeen = append(processor.processSeen, event)
	processor.mu.Unlock()

	if processor.processFn != nil {
		return processor.processFn(ctx, event)
	}

	event.EventID = "processed-event-id"
	return event, nil
}

type connectionTestStore struct {
	mu      sync.Mutex
	storeFn func(ctx context.Context, event contracts.Event) error
	stored  []contracts.Event
	closed  bool
	healthy bool
}

func (store *connectionTestStore) Store(
	ctx context.Context,
	event contracts.Event,
) error {
	store.mu.Lock()
	store.stored = append(store.stored, event)
	store.mu.Unlock()

	if store.storeFn != nil {
		return store.storeFn(ctx, event)
	}

	return nil
}

func (store *connectionTestStore) Get(
	ctx context.Context,
	eventID string,
) (contracts.Event, error) {
	return contracts.Event{}, nil
}

func (store *connectionTestStore) Query(
	ctx context.Context,
	filter contracts.EventFilter,
) ([]contracts.Event, error) {
	return nil, nil
}

func (store *connectionTestStore) Health(ctx context.Context) error {
	return nil
}

func (store *connectionTestStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.closed = true
	return nil
}

func TestConnectionPerformsProducerHandshake(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "producer-client",
		Version: "1.0.0",
	})

	var ready contracts.ReadyMessage
	readConnectionTestJSON(t, socket, &ready)

	if ready.Type != contracts.MessageTypeReady {
		t.Fatalf("expected ready message type, got %q", ready.Type)
	}

	if ready.Mode != contracts.ConnectionModeProducer {
		t.Fatalf("expected producer mode, got %q", ready.Mode)
	}

	if strings.TrimSpace(ready.ConnectionID) == "" {
		t.Fatal("expected connection ID to be populated")
	}

	if ready.ServerTime.IsZero() {
		t.Fatal("expected server time to be populated")
	}
}

func TestConnectionProducerEventReturnsAck(t *testing.T) {
	processor := &connectionTestProcessor{
		processFn: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			event.EventID = "event-123"
			return event, nil
		},
	}
	store := &connectionTestStore{}

	handler, cleanup := newConnectionTestHandler(t, processor, store)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "producer-client",
		Version: "1.0.0",
	})

	var ready contracts.ReadyMessage
	readConnectionTestJSON(t, socket, &ready)

	writeConnectionTestJSON(t, socket, contracts.EventMessage{
		Type:      contracts.MessageTypeEvent,
		RequestID: "request-123",
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "api",
			EventType: "created",
			Message:   "created event",
		},
	})

	var ack contracts.AckMessage
	readConnectionTestJSON(t, socket, &ack)

	if ack.Type != contracts.MessageTypeAck {
		t.Fatalf("expected ack message type, got %q", ack.Type)
	}

	if ack.RequestID != "request-123" {
		t.Fatalf("expected request ID request-123, got %q", ack.RequestID)
	}

	if ack.EventID != "event-123" {
		t.Fatalf("expected event ID event-123, got %q", ack.EventID)
	}

	if ack.Status != contracts.AckStatusAccepted {
		t.Fatalf("expected accepted status, got %q", ack.Status)
	}

	store.mu.Lock()
	stored := append([]contracts.Event(nil), store.stored...)
	store.mu.Unlock()

	if len(stored) != 1 {
		t.Fatalf("expected one stored event, got %d", len(stored))
	}

	if stored[0].EventID != "event-123" {
		t.Fatalf("expected stored processed event, got event ID %q", stored[0].EventID)
	}
}

func TestConnectionProducerEventWithMissingRequestIDReturnsError(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "producer-client",
		Version: "1.0.0",
	})

	var ready contracts.ReadyMessage
	readConnectionTestJSON(t, socket, &ready)

	writeConnectionTestJSON(t, socket, contracts.EventMessage{
		Type: contracts.MessageTypeEvent,
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "api",
			EventType: "created",
			Message:   "created event",
		},
	})

	var errorMessage contracts.ErrorMessage
	readConnectionTestJSON(t, socket, &errorMessage)

	if errorMessage.Type != contracts.MessageTypeError {
		t.Fatalf("expected error message type, got %q", errorMessage.Type)
	}

	if errorMessage.Code != contracts.ProtocolErrorMissingRequiredField {
		t.Fatalf(
			"expected missing required field error, got %q",
			errorMessage.Code,
		)
	}
}

func TestConnectionProducerEventStoreErrorReturnsStorageUnavailable(t *testing.T) {
	storeErr := errors.New("store unavailable")

	processor := &connectionTestProcessor{
		processFn: func(ctx context.Context, event contracts.Event) (contracts.Event, error) {
			event.EventID = "event-456"
			return event, nil
		},
	}
	store := &connectionTestStore{
		storeFn: func(ctx context.Context, event contracts.Event) error {
			return storeErr
		},
	}

	handler, cleanup := newConnectionTestHandler(t, processor, store)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "producer-client",
		Version: "1.0.0",
	})

	var ready contracts.ReadyMessage
	readConnectionTestJSON(t, socket, &ready)

	writeConnectionTestJSON(t, socket, contracts.EventMessage{
		Type:      contracts.MessageTypeEvent,
		RequestID: "request-456",
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "api",
			EventType: "created",
			Message:   "created event",
		},
	})

	var errorMessage contracts.ErrorMessage
	readConnectionTestJSON(t, socket, &errorMessage)

	if errorMessage.Type != contracts.MessageTypeError {
		t.Fatalf("expected error message type, got %q", errorMessage.Type)
	}

	if errorMessage.RequestID != "request-456" {
		t.Fatalf("expected request ID request-456, got %q", errorMessage.RequestID)
	}

	if errorMessage.Code != contracts.ProtocolErrorInternal {
		t.Fatalf("expected internal error, got %q", errorMessage.Code)
	}
}

func TestConnectionRejectsInvalidFirstMessage(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.EventMessage{
		Type:      contracts.MessageTypeEvent,
		RequestID: "request-789",
		Event: contracts.Event{
			Level:     contracts.EventLevelInfo,
			Service:   "api",
			EventType: "created",
			Message:   "created event",
		},
	})

	var errorMessage contracts.ErrorMessage
	readConnectionTestJSON(t, socket, &errorMessage)

	if errorMessage.Type != contracts.MessageTypeError {
		t.Fatalf("expected error message type, got %q", errorMessage.Type)
	}

	if errorMessage.Code != contracts.ProtocolErrorInvalidMessage {
		t.Fatalf("expected invalid message error, got %q", errorMessage.Code)
	}
}

func TestConnectionRejectsHelloWithoutClient(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeProducer,
		Client:  "   ",
		Version: "1.0.0",
	})

	var errorMessage contracts.ErrorMessage
	readConnectionTestJSON(t, socket, &errorMessage)

	if errorMessage.Type != contracts.MessageTypeError {
		t.Fatalf("expected error message type, got %q", errorMessage.Type)
	}

	if errorMessage.Code != contracts.ProtocolErrorMissingRequiredField {
		t.Fatalf(
			"expected missing required field error, got %q",
			errorMessage.Code,
		)
	}
}

func TestConnectionMonitorReceivesPublishedEvent(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	writeConnectionTestJSON(t, socket, contracts.HelloMessage{
		Type:    contracts.MessageTypeHello,
		Mode:    contracts.ConnectionModeMonitor,
		Client:  "monitor-client",
		Version: "1.0.0",
	})

	var ready contracts.ReadyMessage
	readConnectionTestJSON(t, socket, &ready)

	if ready.Mode != contracts.ConnectionModeMonitor {
		t.Fatalf("expected monitor mode, got %q", ready.Mode)
	}

	writeConnectionTestJSON(t, socket, contracts.SubscribeMessage{
		Type:      contracts.MessageTypeSubscribe,
		RequestID: "subscription-123",
		Filters: contracts.EventFilter{
			Services: []string{"api"},
		},
	})

	waitForConnectionTestSubscription(t, handler)

	handler.dispatcher.publisher.Publish(contracts.Event{
		EventID:   "published-event-123",
		Level:     contracts.EventLevelInfo,
		Service:   "api",
		EventType: "created",
		Message:   "published event",
	})

	var eventMessage contracts.EventMessage
	readConnectionTestJSON(t, socket, &eventMessage)

	if eventMessage.Type != contracts.MessageTypeEvent {
		t.Fatalf("expected event message type, got %q", eventMessage.Type)
	}

	if eventMessage.Event.EventID != "published-event-123" {
		t.Fatalf(
			"expected published event ID, got %q",
			eventMessage.Event.EventID,
		)
	}
}

func TestConnectionSendReturnsFalseWhenOutboundQueueIsFull(t *testing.T) {
	handler, cleanup := newConnectionTestHandler(t, nil, nil)
	defer cleanup()

	handler.WriteQueueSize = 1

	socket, closeClient := dialConnectionTestSocket(t, handler)
	defer closeClient()

	clientSocket := waitForConnectionTestServerSocket(t, handler)

	clientSocket.outbound <- outboundMessage{
		value: contracts.ErrorMessage{
			Type:    contracts.MessageTypeError,
			Code:    contracts.ProtocolErrorInternal,
			Message: "already queued",
		},
	}

	sent := clientSocket.send(outboundMessage{
		value: contracts.ErrorMessage{
			Type:    contracts.MessageTypeError,
			Code:    contracts.ProtocolErrorInternal,
			Message: "queue full",
		},
	})

	if sent {
		t.Fatal("expected send to return false when outbound queue is full")
	}

	_ = socket.Close()
}

func TestNewConnectionUsesDefaultWriteQueueWhenConfiguredQueueIsInvalid(t *testing.T) {
	handler := &Handler{
		WriteQueueSize: 0,
	}

	client := newConnection(handler, nil)

	if cap(client.outbound) != defaultWriteQueue {
		t.Fatalf(
			"expected default write queue size %d, got %d",
			defaultWriteQueue,
			cap(client.outbound),
		)
	}
}

func TestNewConnectionUsesConfiguredWriteQueue(t *testing.T) {
	handler := &Handler{
		WriteQueueSize: 7,
	}

	client := newConnection(handler, nil)

	if cap(client.outbound) != 7 {
		t.Fatalf("expected configured write queue size 7, got %d", cap(client.outbound))
	}
}

func newConnectionTestHandler(
	t *testing.T,
	processor *connectionTestProcessor,
	store *connectionTestStore,
) (*Handler, func()) {
	t.Helper()

	if processor == nil {
		processor = &connectionTestProcessor{}
	}

	if store == nil {
		store = &connectionTestStore{}
	}

	dispatcher, err := NewDispatcher(processor, store, 8, 1)
	if err != nil {
		t.Fatalf("expected dispatcher, got error: %v", err)
	}

	handler := &Handler{
		dispatcher:     dispatcher,
		ReadLimit:      defaultReadLimit,
		WriteQueueSize: defaultWriteQueue,
		AllowedOrigins: make(map[string]struct{}),
		AllowLocalhost: true,
	}

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		_, _ = dispatcher.Shutdown(ctx)
	}

	return handler, cleanup
}

func dialConnectionTestSocket(
	t *testing.T,
	handler *Handler,
) (*websocket.Conn, func()) {
	t.Helper()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(request *http.Request) bool {
			return true
		},
	}

	server := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			socket, err := upgrader.Upgrade(writer, request, nil)
			if err != nil {
				return
			}

			client := newConnection(handler, socket)
			request = request.WithContext(
				context.WithValue(
					request.Context(),
					connectionTestClientContextKey{},
					client,
				),
			)

			connectionTestSocketRegistry.store(handler, client)
			client.run()
		},
	))

	url := "ws" + strings.TrimPrefix(server.URL, "http")

	socket, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		server.Close()
		t.Fatalf("expected websocket dial to succeed, got error: %v", err)
	}

	cleanup := func() {
		_ = socket.Close()
		server.Close()
		connectionTestSocketRegistry.delete(handler)
	}

	return socket, cleanup
}

func writeConnectionTestJSON(
	t *testing.T,
	socket *websocket.Conn,
	value any,
) {
	t.Helper()

	if err := socket.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("expected set write deadline to succeed, got error: %v", err)
	}

	if err := socket.WriteJSON(value); err != nil {
		t.Fatalf("expected write JSON to succeed, got error: %v", err)
	}
}

func readConnectionTestJSON(
	t *testing.T,
	socket *websocket.Conn,
	value any,
) {
	t.Helper()

	if err := socket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("expected set read deadline to succeed, got error: %v", err)
	}

	_, data, err := socket.ReadMessage()
	if err != nil {
		t.Fatalf("expected read message to succeed, got error: %v", err)
	}

	if err := json.Unmarshal(data, value); err != nil {
		t.Fatalf("expected JSON unmarshal to succeed, got error: %v", err)
	}
}

func waitForConnectionTestServerSocket(
	t *testing.T,
	handler *Handler,
) *connection {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		if client := connectionTestSocketRegistry.load(handler); client != nil {
			return client
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("timed out waiting for server connection")
	return nil
}

type connectionTestClientContextKey struct{}

var connectionTestSocketRegistry = &connectionTestRegistry{
	values: make(map[*Handler]*connection),
}

type connectionTestRegistry struct {
	mu     sync.Mutex
	values map[*Handler]*connection
}

func (registry *connectionTestRegistry) store(
	handler *Handler,
	client *connection,
) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	registry.values[handler] = client
}

func (registry *connectionTestRegistry) load(
	handler *Handler,
) *connection {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	return registry.values[handler]
}

func (registry *connectionTestRegistry) delete(
	handler *Handler,
) {
	registry.mu.Lock()
	defer registry.mu.Unlock()

	delete(registry.values, handler)
}

func waitForConnectionTestSubscription(
	t *testing.T,
	handler *Handler,
) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		publisher, ok := handler.dispatcher.publisher.(*eventPublisher)
		if !ok {
			t.Fatal("expected dispatcher publisher to be *eventPublisher")
		}

		publisher.mu.RLock()
		count := len(publisher.subscriptions)
		publisher.mu.RUnlock()

		if count > 0 {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("timed out waiting for monitor subscription")
}

package websocket

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/ports"
)

const (
	defaultReadLimit  = 1024 * 1024
	defaultWriteQueue = 32

	defaultQueueSize   = 1024
	defaultWorkerCount = 4
)

type Handler struct {
	dispatcher *Dispatcher

	APIKey         string
	ReadLimit      int64
	WriteQueueSize int
	AllowedOrigins map[string]struct{}
	AllowLocalhost bool
}

func NewHandler(
	store ports.EventStore,
	processor ports.EventProcessor,
	apiKey string,
) (*Handler, error) {
	return NewHandlerWithDispatcherConfig(
		store,
		processor,
		apiKey,
		defaultQueueSize,
		defaultWorkerCount,
	)
}

func NewHandlerWithDispatcherConfig(
	store ports.EventStore,
	processor ports.EventProcessor,
	apiKey string,
	queueSize int,
	workerCount int,
) (*Handler, error) {
	dispatcher, err := NewDispatcher(
		processor,
		store,
		queueSize,
		workerCount,
	)
	if err != nil {
		return nil, err
	}

	return &Handler{
		dispatcher:     dispatcher,
		APIKey:         apiKey,
		ReadLimit:      defaultReadLimit,
		WriteQueueSize: defaultWriteQueue,
		AllowedOrigins: make(map[string]struct{}),
		AllowLocalhost: false,
	}, nil
}

func (handler *Handler) ServeHTTP(
	writer http.ResponseWriter,
	request *http.Request,
) {
	if !handler.authenticate(request) {
		writeHTTPError(
			writer,
			http.StatusUnauthorized,
			contracts.ProtocolErrorUnauthorized,
		)
		return
	}

	if !handler.originAllowed(request) {
		writeHTTPError(
			writer,
			http.StatusForbidden,
			contracts.ProtocolErrorForbidden,
		)
		return
	}

	if handler.dispatcher == nil {
		writeHTTPError(
			writer,
			http.StatusInternalServerError,
			contracts.ProtocolErrorInternal,
		)
		return
	}

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4096,
		WriteBufferSize: 4096,
		CheckOrigin: func(request *http.Request) bool {
			return handler.originAllowed(request)
		},
	}

	connection, err := upgrader.Upgrade(writer, request, nil)
	if err != nil {
		return
	}

	client := newConnection(handler, connection)
	client.run()
}

func (handler *Handler) Shutdown(
	request *http.Request,
) {
	// Intentionally not implemented here.
	// Keep graceful dispatcher shutdown at the application/server lifecycle layer:
	// handler.dispatcher.Shutdown(ctx)
}

func (handler *Handler) authenticate(
	request *http.Request,
) bool {
	if handler.APIKey == "" {
		return handler.AllowLocalhost && isLocalhost(request)
	}

	provided := request.Header.Get("X-API-Key")
	return provided != "" && provided == handler.APIKey
}

func (handler *Handler) originAllowed(
	request *http.Request,
) bool {
	origin := request.Header.Get("Origin")

	if origin == "" {
		return true
	}

	if _, allowed := handler.AllowedOrigins[origin]; allowed {
		return true
	}

	return handler.AllowLocalhost && isLocalhost(request)
}

func (handler *Handler) readLimit() int64 {
	if handler.ReadLimit <= 0 {
		return defaultReadLimit
	}

	return handler.ReadLimit
}

func readProtocolMessage(
	socket *websocket.Conn,
) (contracts.MessageType, any, error) {
	messageType, data, err := socket.ReadMessage()
	if err != nil {
		return "", nil, err
	}

	if messageType != websocket.TextMessage {
		return "", nil, errors.New("only text messages are supported")
	}

	return decodeMessage(data)
}

func isLocalhost(request *http.Request) bool {
	host := request.Host
	if index := strings.LastIndex(host, ":"); index >= 0 {
		host = host[:index]
	}

	return host == "localhost" ||
		host == "127.0.0.1" ||
		host == "::1"
}

func protocolError(err error) string {
	switch {
	case errors.Is(err, ErrInvalidJSON):
		return contracts.ProtocolErrorInvalidJSON

	case errors.Is(err, ErrInvalidMessage):
		return contracts.ProtocolErrorInvalidMessage

	case errors.Is(err, ErrUnsupportedType):
		return contracts.ProtocolErrorInvalidMessage

	case strings.Contains(err.Error(), "first message"):
		return contracts.ProtocolErrorInvalidMessage

	case strings.Contains(err.Error(), "required"):
		return contracts.ProtocolErrorMissingRequiredField

	default:
		return contracts.ProtocolErrorInternal
	}
}

func publicErrorMessage(code string) string {
	switch code {
	case contracts.ProtocolErrorUnauthorized:
		return "authentication failed"

	case contracts.ProtocolErrorForbidden:
		return "access denied"

	case contracts.ProtocolErrorInvalidJSON:
		return "message contains invalid JSON"

	case contracts.ProtocolErrorInvalidMessage:
		return "message is invalid"

	case contracts.ProtocolErrorMissingRequiredField:
		return "a required field is missing"

	case contracts.ProtocolErrorStorageUnavailable:
		return "event could not be persisted"

	case contracts.ProtocolErrorUnsupportedEventLevel:
		return "event level is not supported"

	case contracts.ProtocolErrorInvalidEvent:
		return "event is invalid"

	default:
		return "request could not be processed"
	}
}

func writeHTTPError(
	writer http.ResponseWriter,
	status int,
	code string,
) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)

	_ = json.NewEncoder(writer).Encode(
		contracts.ErrorMessage{
			Type:    contracts.MessageTypeError,
			Code:    code,
			Message: publicErrorMessage(code),
		},
	)
}

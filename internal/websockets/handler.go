package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/ports"
)

const (
    defaultReadLimit  = 1024 * 1024
    defaultWriteQueue = 32

    writeWait  = 10 * time.Second
    pongWait   = 60 * time.Second
    pingPeriod = (pongWait * 9) / 10
)

type EventProcessor interface {
    Process(
        ctx context.Context,
        event contracts.Event,
    ) (contracts.Event, error)
}

type Handler struct {
    Store     ports.EventStore
    Processor EventProcessor

    APIKey          string
    ReadLimit       int64
    WriteQueueSize  int
    AllowedOrigins  map[string]struct{}
    AllowLocalhost  bool
}

func NewHandler(
    store ports.EventStore,
    processor EventProcessor,
    apiKey string,
) *Handler {
    return &Handler{
        Store:          store,
        Processor:      processor,
        APIKey:         apiKey,
        ReadLimit:      defaultReadLimit,
        WriteQueueSize: defaultWriteQueue,
        AllowedOrigins: make(map[string]struct{}),
        AllowLocalhost: false,
    }
}

func (handler *Handler) ServeHTTP(
    writer http.ResponseWriter,
    request *http.Request,
) {
    if !handler.authenticate(request) {
        writeHTTPError(writer, http.StatusUnauthorized, "UNAUTHORIZED")
        return
    }

    if !handler.originAllowed(request) {
        writeHTTPError(writer, http.StatusForbidden, "FORBIDDEN")
        return
    }

    if handler.Store == nil || handler.Processor == nil {
        writeHTTPError(writer, http.StatusInternalServerError, "INTERNAL_ERROR")
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

type connection struct {
    handler *Handler
    socket  *websocket.Conn
    send    chan outboundMessage
}

type outboundMessage struct {
    value any
}

func newConnection(
    handler *Handler,
    socket *websocket.Conn,
) *connection {
    queueSize := handler.WriteQueueSize
    if queueSize <= 0 {
        queueSize = defaultWriteQueue
    }

    return &connection{
        handler: handler,
        socket:  socket,
        send:    make(chan outboundMessage, queueSize),
    }
}

func (client *connection) run() {
    defer client.socket.Close()

    client.socket.SetReadLimit(client.handler.readLimit())

    if err := client.performHandshake(); err != nil {
        client.sendError("", protocolError(err))
        return
    }

    go client.writePump()
    client.readPump()
}

func (client *connection) performHandshake() error {
    messageType, payload, err := readProtocolMessage(client.socket)
    if err != nil {
        return err
    }

    if messageType != contracts.MessageTypeHello {
        return errors.New("first message must be hello")
    }

    hello, ok := payload.(*contracts.HelloMessage)
    if !ok {
        return errors.New("invalid hello message")
    }

    if hello.Mode != contracts.ConnectionModeProducer {
        return errors.New("only producer mode is supported")
    }

    if strings.TrimSpace(hello.Client) == "" {
        return errors.New("client is required")
    }

    ready := contracts.ReadyMessage{
        Type:         contracts.MessageTypeReady,
        ConnectionID: newConnectionID(),
        Mode:         contracts.ConnectionModeProducer,
        ServerTime:   time.Now().UTC(),
    }

    data, err := json.Marshal(ready)
    if err != nil {
        return err
    }

    // The write pump starts immediately after this handshake response.
    if err := client.socket.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
        return err
    }

    return client.socket.WriteMessage(websocket.TextMessage, data)
}

func (client *connection) readPump() {
    defer close(client.send)

    client.socket.SetReadDeadline(time.Now().Add(pongWait))
    client.socket.SetPongHandler(func(string) error {
        return client.socket.SetReadDeadline(time.Now().Add(pongWait))
    })

    for {
        messageType, payload, err := readProtocolMessage(client.socket)
        if err != nil {
            client.sendError("", protocolError(err))
            return
        }

        switch messageType {
        case contracts.MessageTypeEvent:
            client.handleEvent(payload.(*contracts.EventMessage))

        default:
            client.sendError("", contracts.ProtocolErrorInvalidMessage)
            return
        }
    }
}

func (client *connection) handleEvent(
    message *contracts.EventMessage,
) {
    if strings.TrimSpace(message.RequestID) == "" {
        client.sendError(
            "",
            contracts.ProtocolErrorMissingRequiredField,
        )
        return
    }

    processed, err := client.handler.Processor.Process(
        context.Background(),
        message.Event,
    )
    if err != nil {
        client.sendError(message.RequestID, protocolError(err))
        return
    }

    if err := client.handler.Store.Store(
        context.Background(),
        processed,
    ); err != nil {
        client.sendError(
            message.RequestID,
            contracts.ProtocolErrorStorageUnavailable,
        )
        return
    }

    client.send(outboundMessage{
        value: contracts.AckMessage{
            Type:      contracts.MessageTypeAck,
            RequestID: message.RequestID,
            EventID:   processed.EventID,
            Status:    contracts.AckStatusAccepted,
            Timestamp: time.Now().UTC(),
        },
    })
}

func (client *connection) writePump() {
    ticker := time.NewTicker(pingPeriod)
    defer ticker.Stop()

    for {
        select {
        case message, ok := <-client.send:
            if !ok {
                return
            }

            if err := client.writeJSON(message.value); err != nil {
                return
            }

        case <-ticker.C:
            if err := client.socket.SetWriteDeadline(
                time.Now().Add(writeWait),
            ); err != nil {
                return
            }

            if err := client.socket.WriteMessage(
                websocket.PingMessage,
                nil,
            ); err != nil {
                return
            }
        }
    }
}

func (client *connection) writeJSON(value any) error {
    if err := client.socket.SetWriteDeadline(
        time.Now().Add(writeWait),
    ); err != nil {
        return err
    }

    return client.socket.WriteJSON(value)
}

func (client *connection) sendError(
    requestID string,
    code string,
) {
    message := contracts.ErrorMessage{
        Type:      contracts.MessageTypeError,
        RequestID: requestID,
        Code:      code,
        Message:   publicErrorMessage(code),
    }

    select {
    case client.send <- outboundMessage{value: message}:
    default:
        _ = client.socket.WriteControl(
            websocket.CloseMessage,
            websocket.FormatCloseMessage(
                websocket.CloseTryAgainLater,
                "outbound queue is full",
            ),
            time.Now().Add(writeWait),
        )
    }
}

func (client *connection) send(message outboundMessage) {
    select {
    case client.send <- message:
    default:
        _ = client.socket.WriteControl(
            websocket.CloseMessage,
            websocket.FormatCloseMessage(
                websocket.CloseTryAgainLater,
                "outbound queue is full",
            ),
            time.Now().Add(writeWait),
        )
    }
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

func newConnectionID() string {
    return time.Now().UTC().Format("20060102T150405.000000000")
}
package websocket

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/hftamayo/gologger/internal/contracts"
)

const (
	writeWait = 10 * time.Second
	pongWait  = 60 * time.Second

	pingPeriod = (pongWait * 9) / 10
)

type connection struct {
	handler  *Handler
	socket   *websocket.Conn
	outbound chan outboundMessage
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
		handler:  handler,
		socket:   socket,
		outbound: make(chan outboundMessage, queueSize),
	}
}

func (client *connection) run() {
	defer client.socket.Close()

	client.socket.SetReadLimit(client.handler.readLimit())

	hello, err := client.performHandshake()
	if err != nil {
		client.writeError("", protocolError(err))
		return
	}

	go client.writePump()

	switch hello.Mode {
	case contracts.ConnectionModeProducer:
		client.readProducerMessages()

	case contracts.ConnectionModeMonitor:
		client.readMonitorMessages()

	default:
		client.sendError("", contracts.ProtocolErrorInvalidMessage)
	}
}

func (client *connection) performHandshake() (*contracts.HelloMessage, error) {
	messageType, payload, err := readProtocolMessage(client.socket)
	if err != nil {
		return nil, err
	}

	if messageType != contracts.MessageTypeHello {
		return nil, errors.New("first message must be hello")
	}

	hello, ok := payload.(*contracts.HelloMessage)
	if !ok {
		return nil, errors.New("invalid hello message")
	}

	if strings.TrimSpace(hello.Client) == "" {
		return nil, errors.New("client is required")
	}

	switch hello.Mode {
	case contracts.ConnectionModeProducer, contracts.ConnectionModeMonitor:
	default:
		return nil, errors.New("unsupported connection mode")
	}

	ready := contracts.ReadyMessage{
		Type:         contracts.MessageTypeReady,
		ConnectionID: newConnectionID(),
		Mode:         hello.Mode,
		ServerTime:   time.Now().UTC(),
	}

	data, err := json.Marshal(ready)
	if err != nil {
		return nil, err
	}

	if err := client.socket.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
		return nil, err
	}

	if err := client.socket.WriteMessage(websocket.TextMessage, data); err != nil {
		return nil, err
	}

	return hello, nil
}

func (client *connection) readProducerMessages() {
	defer close(client.outbound)

	client.configureReadDeadlines()

	for {
		messageType, payload, err := readProtocolMessage(client.socket)
		if err != nil {
			client.sendError("", protocolError(err))
			return
		}

		switch messageType {
		case contracts.MessageTypeEvent:
			message, ok := payload.(*contracts.EventMessage)
			if !ok {
				client.sendError("", contracts.ProtocolErrorInvalidMessage)
				return
			}

			client.handleEvent(message)

		default:
			client.sendError("", contracts.ProtocolErrorInvalidMessage)
			return
		}
	}
}

func (client *connection) readMonitorMessages() {
	defer close(client.outbound)

	client.configureReadDeadlines()

	for {
		messageType, payload, err := readProtocolMessage(client.socket)
		if err != nil {
			client.sendError("", protocolError(err))
			return
		}

		switch messageType {
		case contracts.MessageTypeSubscribe:
			message, ok := payload.(*contracts.SubscribeMessage)
			if !ok {
				client.sendError("", contracts.ProtocolErrorInvalidMessage)
				return
			}

			client.handleSubscribe(message)
			return

		default:
			client.sendError("", contracts.ProtocolErrorInvalidMessage)
			return
		}
	}
}

func (client *connection) configureReadDeadlines() {
	client.socket.SetReadDeadline(time.Now().Add(pongWait))
	client.socket.SetPongHandler(func(string) error {
		return client.socket.SetReadDeadline(time.Now().Add(pongWait))
	})
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

	result, err := client.handler.dispatcher.Submit(
		context.Background(),
		message.Event,
	)
	if err != nil {
		client.sendEventSubmissionError(message.RequestID, err)
		return
	}

	select {
	case outcome, ok := <-result:
		if !ok {
			client.sendError(
				message.RequestID,
				contracts.ProtocolErrorInternal,
			)
			return
		}

		if outcome.Err != nil {
			client.sendError(
				message.RequestID,
				protocolError(outcome.Err),
			)
			return
		}

		client.send(outboundMessage{
			value: contracts.AckMessage{
				Type:      contracts.MessageTypeAck,
				RequestID: message.RequestID,
				EventID:   outcome.Event.EventID,
				Status:    contracts.AckStatusAccepted,
				Timestamp: time.Now().UTC(),
			},
		})

	case <-time.After(writeWait):
		client.sendError(
			message.RequestID,
			contracts.ProtocolErrorInternal,
		)
	}
}

func (client *connection) sendEventSubmissionError(
	requestID string,
	err error,
) {
	switch {
	case errors.Is(err, ErrQueueFull):
		client.sendError(
			requestID,
			contracts.ProtocolErrorStorageUnavailable,
		)

	case errors.Is(err, ErrHandlerClosed):
		client.sendError(
			requestID,
			contracts.ProtocolErrorStorageUnavailable,
		)

	default:
		client.sendError(
			requestID,
			protocolError(err),
		)
	}
}

func (client *connection) handleSubscribe(
	message *contracts.SubscribeMessage,
) {
	subscription := client.handler.dispatcher.Subscribe(message.Filters)
	defer subscription.Close()

	for event := range subscription.Events() {
		if !client.send(outboundMessage{
			value: contracts.EventMessage{
				Type:  contracts.MessageTypeEvent,
				Event: event,
			},
		}) {
			return
		}
	}
}

func (client *connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-client.outbound:
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
	client.send(outboundMessage{
		value: contracts.ErrorMessage{
			Type:      contracts.MessageTypeError,
			RequestID: requestID,
			Code:      code,
			Message:   publicErrorMessage(code),
		},
	})
}

func (client *connection) writeError(
	requestID string,
	code string,
) {
	_ = client.writeJSON(contracts.ErrorMessage{
		Type:      contracts.MessageTypeError,
		RequestID: requestID,
		Code:      code,
		Message:   publicErrorMessage(code),
	})
}

func (client *connection) send(message outboundMessage) bool {
	select {
	case client.outbound <- message:
		return true

	default:
		_ = client.socket.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(
				websocket.CloseTryAgainLater,
				"outbound queue is full",
			),
			time.Now().Add(writeWait),
		)

		return false
	}
}

func newConnectionID() string {
	return time.Now().UTC().Format("20060102T150405.000000000")
}

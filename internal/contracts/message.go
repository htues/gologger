package contracts

import "time"

// MessageType identifies a WebSocket protocol message.
type MessageType string

const (
	MessageTypeHello           MessageType = "hello"
	MessageTypeReady           MessageType = "ready"
	MessageTypeEvent           MessageType = "event"
	MessageTypeAck             MessageType = "ack"
	MessageTypeError           MessageType = "error"
	MessageTypeSubscribe       MessageType = "subscribe"
	MessageTypeSubscriptionAck MessageType = "subscription_ack"
)

// ConnectionMode identifies the role requested during the handshake.
type ConnectionMode string

const (
	ConnectionModeProducer ConnectionMode = "producer"
	ConnectionModeMonitor  ConnectionMode = "monitor"
)

// HelloMessage starts a producer or monitor connection.
type HelloMessage struct {
	Type    MessageType    `json:"type"`
	Mode    ConnectionMode `json:"mode"`
	Client  string         `json:"client"`
	Version string         `json:"version"`
}

// ReadyMessage confirms a successful connection handshake.
type ReadyMessage struct {
	Type         MessageType    `json:"type"`
	ConnectionID string         `json:"connectionId"`
	Mode         ConnectionMode `json:"mode"`
	ServerTime   time.Time      `json:"serverTime"`
}

// EventMessage submits one event for validation and persistence.
type EventMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"requestId"`
	Event     Event       `json:"event"`
}

// AckMessage confirms that an event was persisted.
type AckMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"requestId"`
	EventID   string      `json:"eventId"`
	Status    string      `json:"status"`
	Timestamp time.Time   `json:"timestamp"`
}

// ErrorMessage reports a stable protocol error without internal details.
type ErrorMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"requestId,omitempty"`
	Code      string      `json:"code"`
	Message   string      `json:"message"`
}

// SubscribeMessage requests filtered monitor publication.
type SubscribeMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"requestId"`
	Filters   EventFilter `json:"filters"`
}

// SubscriptionAckMessage confirms a monitor subscription.
type SubscriptionAckMessage struct {
	Type      MessageType `json:"type"`
	RequestID string      `json:"requestId"`
	Status    string      `json:"status"`
}

const (
	AckStatusAccepted            = "accepted"
	SubscriptionStatusSubscribed = "subscribed"
)

const (
	ProtocolErrorUnauthorized          = "UNAUTHORIZED"
	ProtocolErrorForbidden             = "FORBIDDEN"
	ProtocolErrorInvalidJSON           = "INVALID_JSON"
	ProtocolErrorInvalidMessage        = "INVALID_MESSAGE"
	ProtocolErrorInvalidEvent          = "INVALID_EVENT"
	ProtocolErrorUnsupportedEventLevel = "UNSUPPORTED_EVENT_LEVEL"
	ProtocolErrorMissingRequiredField  = "MISSING_REQUIRED_FIELD"
	ProtocolErrorMessageTooLarge       = "MESSAGE_TOO_LARGE"
	ProtocolErrorRateLimited           = "RATE_LIMITED"
	ProtocolErrorStorageUnavailable    = "STORAGE_UNAVAILABLE"
	ProtocolErrorInternal              = "INTERNAL_ERROR"
	ProtocolErrorServerShuttingDown    = "SERVER_SHUTTING_DOWN"
)

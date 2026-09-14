package contracts

import "time"

// EventLevel is the severity used by an application event.
type EventLevel string

const (
	EventLevelTrace       EventLevel = "trace"
	EventLevelDebug       EventLevel = "debug"
	EventLevelInfo        EventLevel = "info"
	EventLevelWarn        EventLevel = "warn"
	EventLevelError       EventLevel = "error"
	EventLevelFatal       EventLevel = "fatal"
	EventLevelInformation EventLevel = "information"
	EventLevelWarning     EventLevel = "warning"
)

// Event is the canonical domain event exchanged by producers and monitors.
type Event struct {
	EventID       string         `json:"eventId,omitempty"`
	Timestamp     time.Time      `json:"timestamp,omitempty"`
	Level         EventLevel     `json:"level"`
	Service       string         `json:"service"`
	Component     string         `json:"component,omitempty"`
	EventType     string         `json:"eventType"`
	Message       string         `json:"message"`
	Code          string         `json:"code,omitempty"`
	SessionID     string         `json:"sessionId,omitempty"`
	UserID        string         `json:"userId,omitempty"`
	CorrelationID string         `json:"correlationId,omitempty"`
	TraceID       string         `json:"traceId,omitempty"`
	SpanID        string         `json:"spanId,omitempty"`
	Context       map[string]any `json:"context,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
}

// EventFilter selects events for a monitor or a storage query.
type EventFilter struct {
	Levels     []EventLevel `json:"levels,omitempty"`
	Services   []string     `json:"services,omitempty"`
	EventTypes []string     `json:"eventTypes,omitempty"`
}

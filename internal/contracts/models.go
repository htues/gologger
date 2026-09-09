package contracts

import "time"

// LogLevel defines the severity
type LogLevel string

const (
	Trace   LogLevel = "trace"
	Debug   LogLevel = "debug"
	Info    LogLevel = "info"
	Warning LogLevel = "warning"
	Warn    LogLevel = "warn"
	Error   LogLevel = "error"
	Fatal   LogLevel = "fatal"
)

// ApplicationLogEvent is the contract matching the backend ApplicationLogEventDto.
type ApplicationLogEvent struct {
	Timestamp     time.Time              `json:"timestamp"`
	Severity      string                 `json:"severity"`
	EventType     string                 `json:"eventType"`
	EventCode     string                 `json:"eventCode"`
	Message       string                 `json:"message"`
	Detail        string                 `json:"detail,omitempty"`
	StatusCode    *int                   `json:"statusCode,omitempty"`
	CorrelationID string                 `json:"correlationId,omitempty"`
	TraceID       string                 `json:"traceId,omitempty"`
	Path          string                 `json:"path,omitempty"`
	HTTPMethod    string                 `json:"httpMethod,omitempty"`
	Source        string                 `json:"source,omitempty"`
	Context       map[string]interface{} `json:"context,omitempty"`
}

// Session represents the active user data
type Session struct {
	User      string    `json:"user"`
	Timestamp time.Time `json:"timestamp"`
	IPAddress string    `json:"ipaddress"`
}

// LogEntry is the primary contract for all incoming/outgoing data
type LogEntry struct {
	Timestamp     time.Time `json:"timestamp"`
	CorrelationID string    `json:"correlationId"`
	Level         LogLevel  `json:"level"`
	Project       string    `json:"project"`             // e.g., "absences"
	System        string    `json:"system"`              // e.g., "frontend", "backend"
	Module        string    `json:"module"`              // e.g., "AddCompanies"
	Action        string    `json:"action"`              // e.g., "saveHandler"
	ErrorType     string    `json:"errorType,omitempty"` // unauth, validation, etc.
	ErrorCode     int       `json:"errorCode,omitempty"`
	ErrorMessage  string    `json:"errorMessage,omitempty"`
	ActiveSession Session   `json:"activeSession"`
}

package entities

import (
	"errors"
	"time"
)

var (
	// ErrInvalidLogEntry is returned when a log entry is invalid
	ErrInvalidLogEntry = errors.New("invalid log entry")
)

// LogLevel represents the severity level of a log entry
type LogLevel string

const (
	LogLevelTrace LogLevel = "trace"
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelFatal LogLevel = "fatal"
	LogLevelOff   LogLevel = "off"
)

// LogContext contains contextual information about the log entry
type LogContext struct {
	UserID              string `json:"userId,omitempty"`
	SessionID           string `json:"sessionId,omitempty"`
	Endpoint            string `json:"endpoint,omitempty"`
	Method              string `json:"method,omitempty"`
	Domain              string `json:"domain,omitempty"`
	RequiredPermission  string `json:"requiredPermission,omitempty"`
	CookiePresent       bool   `json:"cookiePresent,omitempty"`
}

// LogData contains the actual log information
type LogData struct {
	Code    string      `json:"code,omitempty"`
	Message string      `json:"message"`
	Context LogContext  `json:"context,omitempty"`
	Extra   interface{} `json:"extra,omitempty"`
}

// LogEntry represents a single log entry in the system
type LogEntry struct {
	ID             string    `json:"id"`
	Timestamp      time.Time `json:"timestamp"`
	EventTimestamp time.Time `json:"event_timestamp"`
	Level          LogLevel  `json:"level"`
	ServiceName    string    `json:"serviceName"`
	Data           LogData   `json:"data"`
}

// IsValid checks if the log entry has required fields
func (le *LogEntry) IsValid() bool {
	return le.ServiceName != "" && le.Data.Message != "" && le.Level != LogLevelOff
}

// GetLogLevel returns the LogLevel as a string
func (le *LogEntry) GetLogLevel() string {
	return string(le.Level)
}

// IsLevelEnabled checks if the given level should be logged based on current level
func (le *LogEntry) IsLevelEnabled(currentLevel LogLevel) bool {
	levels := map[LogLevel]int{
		LogLevelTrace: 0,
		LogLevelDebug: 1,
		LogLevelInfo:  2,
		LogLevelWarn:  3,
		LogLevelError: 4,
		LogLevelFatal: 5,
		LogLevelOff:   6,
	}

	current, exists := levels[currentLevel]
	if !exists {
		return false
	}

	entry, exists := levels[le.Level]
	if !exists {
		return false
	}

	return entry >= current
} 
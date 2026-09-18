package entities_test

import "testing"

func TestLogEntryIsValid(t *testing.T) {
    tests := []struct {
        name  string
        entry LogEntry
        valid bool
    }{
        {
            name: "valid entry",
            entry: LogEntry{
                ServiceName: "orders-service",
                Level:       LogLevelInfo,
                Data: LogData{
                    Message: "fixture message",
                },
            },
            valid: true,
        },
        {
            name: "missing service name",
            entry: LogEntry{
                Level: LogLevelInfo,
                Data: LogData{
                    Message: "fixture message",
                },
            },
            valid: false,
        },
        {
            name: "missing message",
            entry: LogEntry{
                ServiceName: "orders-service",
                Level:       LogLevelInfo,
            },
            valid: false,
        },
        {
            name: "off level",
            entry: LogEntry{
                ServiceName: "orders-service",
                Level:       LogLevelOff,
                Data: LogData{
                    Message: "fixture message",
                },
            },
            valid: false,
        },
        {
            name: "unknown level",
            entry: LogEntry{
                ServiceName: "orders-service",
                Level:       LogLevel("unknown"),
                Data: LogData{
                    Message: "fixture message",
                },
            },
            valid: true,
        },
    }

    for _, test := range tests {
        t.Run(test.name, func(t *testing.T) {
            if got := test.entry.IsValid(); got != test.valid {
                t.Fatalf("IsValid() = %v, want %v", got, test.valid)
            }
        })
    }
}

func TestLogEntryGetLogLevel(t *testing.T) {
    entry := LogEntry{
        Level: LogLevelWarn,
    }

    if got := entry.GetLogLevel(); got != "warn" {
        t.Fatalf("GetLogLevel() = %q, want %q", got, "warn")
    }
}

func TestLogEntryIsLevelEnabled(t *testing.T) {
    tests := []struct {
        name          string
        entryLevel    LogLevel
        currentLevel  LogLevel
        enabled       bool
    }{
        {
            name:         "info entry at info threshold",
            entryLevel:   LogLevelInfo,
            currentLevel: LogLevelInfo,
            enabled:      true,
        },
        {
            name:         "error entry at info threshold",
            entryLevel:   LogLevelError,
            currentLevel: LogLevelInfo,
            enabled:      true,
        },
        {
            name:         "debug entry at info threshold",
            entryLevel:   LogLevelDebug,
            currentLevel: LogLevelInfo,
            enabled:      false,
        },
        {
            name:         "trace entry at trace threshold",
            entryLevel:   LogLevelTrace,
            currentLevel: LogLevelTrace,
            enabled:      true,
        },
        {
            name:         "fatal entry at error threshold",
            entryLevel:   LogLevelFatal,
            currentLevel: LogLevelError,
            enabled:      true,
        },
        {
            name:         "all entries disabled at off threshold",
            entryLevel:   LogLevelFatal,
            currentLevel: LogLevelOff,
            enabled:      false,
        },
        {
            name:         "unknown entry level",
            entryLevel:   LogLevel("unknown"),
            currentLevel: LogLevelInfo,
            enabled:      false,
        },
        {
            name:         "unknown current level",
            entryLevel:   LogLevelInfo,
            currentLevel: LogLevel("unknown"),
            enabled:      false,
        },
    }

    for _, test := range tests {
        t.Run(test.name, func(t *testing.T) {
            entry := LogEntry{
                Level: test.entryLevel,
            }

            if got := entry.IsLevelEnabled(test.currentLevel); got != test.enabled {
                t.Fatalf(
                    "IsLevelEnabled(%q) = %v, want %v",
                    test.currentLevel,
                    got,
                    test.enabled,
                )
            }
        })
    }
}
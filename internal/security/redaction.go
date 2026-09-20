package security

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/hftamayo/gologger/internal/contracts"
)

const redactedValue = "[REDACTED]"

var sensitiveValue = regexp.MustCompile(`(?i)(bearer\s+|token\s*[=:]\s*|password\s*[=:]\s*|secret\s*[=:]\s*|api[-_ ]?key\s*[=:]\s*)[^\s,;]+`)
var sensitiveKey = regexp.MustCompile(`(?i)(pass(word)?|secret|token|api[-_ ]?key|authorization|cookie|credential)`)

func RedactEvent(event contracts.Event) contracts.Event {
	event.Service = sanitize(event.Service)
	event.Component = sanitize(event.Component)
	event.EventType = sanitize(event.EventType)
	event.Message = sanitize(event.Message)
	event.Code = sanitize(event.Code)
	event.SessionID = sanitize(event.SessionID)
	event.UserID = sanitize(event.UserID)
	event.CorrelationID = sanitize(event.CorrelationID)
	event.TraceID = sanitize(event.TraceID)
	event.SpanID = sanitize(event.SpanID)
	event.Context = redactMap(event.Context)
	event.Metadata = redactMap(event.Metadata)

	return event
}

func redactMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}

	redacted := make(map[string]any, len(values))
	for key, value := range values {
		redacted[sanitize(key)] = redactValue(value, key)
	}

	return redacted
}

func redactValue(value any, key string) any {
	if sensitiveKey.MatchString(key) {
		return redactedValue
	}

	switch typed := value.(type) {
	case string:
		return sanitize(typed)

	case map[string]any:
		return redactMap(typed)

	case []any:
		redacted := make([]any, len(typed))
		for index, item := range typed {
			redacted[index] = redactValue(item, key)
		}
		return redacted

	default:
		return value
	}
}

func sanitize(value string) string {
	value = sensitiveValue.ReplaceAllString(value, redactedValue)

	var builder strings.Builder
	builder.Grow(len(value))

	for _, character := range value {
		if unicode.IsControl(character) && character != '\t' {
			continue
		}

		builder.WriteRune(character)
	}

	return strings.TrimSpace(builder.String())
}

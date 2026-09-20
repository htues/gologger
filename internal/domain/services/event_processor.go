package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/hftamayo/gologger/internal/contracts"
	"github.com/hftamayo/gologger/internal/security"
)

type EventProcessorService struct{}

const (
	maxServiceLength   = 128
	maxComponentLength = 128
	maxEventTypeLength = 128
	maxMessageLength   = 4096
	maxMetadataBytes   = 64 * 1024
	maxMapDepth        = 8
)

var (
	ErrInvalidEvent          = errors.New("invalid event")
	ErrUnsupportedEventLevel = errors.New("unsupported event level")
	ErrMissingRequiredField  = errors.New("missing required event field")
	ErrSensitiveField        = errors.New("sensitive field is not allowed")
	ErrMetadataTooLarge      = errors.New("event metadata is too large")
	ErrInvalidUTF8           = errors.New("event contains invalid UTF-8")
	ErrNestedValueTooDeep    = errors.New("event data is nested too deeply")
)

var prohibitedFields = map[string]struct{}{
	"authorization": {},
	"cookie":        {},
	"cookies":       {},
	"password":      {},
	"passwd":        {},
	"secret":        {},
	"token":         {},
	"accesstoken":   {},
	"refreshtoken":  {},
	"apikey":        {},
	"privatekey":    {},
	"sessionsecret": {},
}

// ProcessEvent validates, normalizes, enriches, and sanitizes an event.
func ProcessEvent(event contracts.Event, now func() time.Time) (contracts.Event, error) {
	if now == nil {
		now = time.Now
	}

	event = security.RedactEvent(event)

	if err := validateEvent(event); err != nil {
		return contracts.Event{}, err
	}

	event.Level = MapEventLevel(event.Level)

	if event.EventID == "" {
		event.EventID = generateEventID()
	}

	if event.Timestamp.IsZero() {
		event.Timestamp = now().UTC()
	} else {
		event.Timestamp = event.Timestamp.UTC()
	}

	event.Service = sanitizeText(event.Service)
	event.Component = sanitizeText(event.Component)
	event.EventType = sanitizeText(event.EventType)
	event.Message = sanitizeText(event.Message)
	event.Code = sanitizeText(event.Code)
	event.SessionID = sanitizeText(event.SessionID)
	event.UserID = sanitizeText(event.UserID)
	event.CorrelationID = sanitizeText(event.CorrelationID)
	event.TraceID = sanitizeText(event.TraceID)
	event.SpanID = sanitizeText(event.SpanID)

	var err error
	event.Context, err = sanitizeMap(event.Context, 0)
	if err != nil {
		return contracts.Event{}, err
	}

	event.Metadata, err = sanitizeMap(event.Metadata, 0)
	if err != nil {
		return contracts.Event{}, err
	}

	if err := validateMetadataSize(event); err != nil {
		return contracts.Event{}, err
	}

	return event, nil
}

// MapEventLevel converts compatibility levels to canonical levels.
func MapEventLevel(level contracts.EventLevel) contracts.EventLevel {
	switch strings.ToLower(strings.TrimSpace(string(level))) {
	case "information":
		return contracts.EventLevelInfo
	case "warning":
		return contracts.EventLevelWarn
	default:
		return contracts.EventLevel(strings.ToLower(strings.TrimSpace(string(level))))
	}
}

// validateEvent checks required fields and accepted event levels.
func validateEvent(event contracts.Event) error {
	if !utf8.ValidString(string(event.Level)) ||
		!utf8.ValidString(event.Service) ||
		!utf8.ValidString(event.EventType) ||
		!utf8.ValidString(event.Message) {
		return ErrInvalidUTF8
	}

	switch MapEventLevel(event.Level) {
	case contracts.EventLevelTrace,
		contracts.EventLevelDebug,
		contracts.EventLevelInfo,
		contracts.EventLevelWarn,
		contracts.EventLevelError,
		contracts.EventLevelFatal:
	default:
		if event.Level == "" {
			return fmt.Errorf("%w: level", ErrMissingRequiredField)
		}

		return fmt.Errorf("%w: %q", ErrUnsupportedEventLevel, event.Level)
	}

	requiredFields := []struct {
		name  string
		value string
		limit int
	}{
		{"service", event.Service, maxServiceLength},
		{"eventType", event.EventType, maxEventTypeLength},
		{"message", event.Message, maxMessageLength},
	}

	for _, field := range requiredFields {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("%w: %s", ErrMissingRequiredField, field.name)
		}

		if utf8.RuneCountInString(field.value) > field.limit {
			return fmt.Errorf("%w: %s is too long", ErrInvalidEvent, field.name)
		}
	}

	if utf8.RuneCountInString(event.Component) > maxComponentLength {
		return fmt.Errorf("%w: component is too long", ErrInvalidEvent)
	}

	if event.Timestamp.IsZero() == false &&
		event.Timestamp.Year() < 2000 {
		return fmt.Errorf("%w: timestamp is invalid", ErrInvalidEvent)
	}

	return nil
}

func validateMetadataSize(event contracts.Event) error {
	payload := struct {
		Context  map[string]any `json:"context,omitempty"`
		Metadata map[string]any `json:"metadata,omitempty"`
	}{
		Context:  event.Context,
		Metadata: event.Metadata,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("%w: cannot encode metadata: %v", ErrInvalidEvent, err)
	}

	if len(data) > maxMetadataBytes {
		return ErrMetadataTooLarge
	}

	return nil
}

func sanitizeMap(values map[string]any, depth int) (map[string]any, error) {
	if values == nil {
		return nil, nil
	}

	if depth >= maxMapDepth {
		return nil, ErrNestedValueTooDeep
	}

	sanitized := make(map[string]any, len(values))

	for key, value := range values {
		if !utf8.ValidString(key) {
			return nil, ErrInvalidUTF8
		}

		normalizedKey := normalizeFieldName(key)
		if _, prohibited := prohibitedFields[normalizedKey]; prohibited {
			return nil, fmt.Errorf("%w: %s", ErrSensitiveField, key)
		}

		cleanKey := sanitizeText(key)
		cleanValue, err := sanitizeValue(value, depth+1)
		if err != nil {
			return nil, err
		}

		sanitized[cleanKey] = cleanValue
	}

	return sanitized, nil
}

func sanitizeValue(value any, depth int) (any, error) {
	switch typed := value.(type) {
	case string:
		if !utf8.ValidString(typed) {
			return nil, ErrInvalidUTF8
		}

		return sanitizeText(typed), nil

	case map[string]any:
		return sanitizeMap(typed, depth)

	case []any:
		if depth >= maxMapDepth {
			return nil, ErrNestedValueTooDeep
		}

		sanitized := make([]any, len(typed))
		for index, item := range typed {
			cleanItem, err := sanitizeValue(item, depth+1)
			if err != nil {
				return nil, err
			}

			sanitized[index] = cleanItem
		}

		return sanitized, nil

	case json.Number, bool, float64, float32, int, int32, int64:
		return typed, nil

	case nil:
		return nil, nil

	default:
		return nil, fmt.Errorf("%w: unsupported metadata value %T", ErrInvalidEvent, value)
	}
}

func sanitizeText(value string) string {
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

func normalizeFieldName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, "_", "")
	value = strings.ReplaceAll(value, " ", "")

	return value
}

func generateEventID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return fmt.Sprintf("evt-%d", time.Now().UnixNano())
	}

	return "evt-" + hex.EncodeToString(buffer)
}

func (EventProcessorService) Process(
	ctx context.Context,
	event contracts.Event,
) (contracts.Event, error) {
	if err := ctx.Err(); err != nil {
		return contracts.Event{}, err
	}

	return ProcessEvent(event, time.Now)
}

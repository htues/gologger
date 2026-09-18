package security

import (
	"regexp"
	"strings"
	"unicode"

	"github.com/hftamayo/gologger/internal/domain/entities"
)

var sensitiveValue = regexp.MustCompile(`(?i)(bearer\s+|token\s*[=:]\s*|password\s*[=:]\s*|secret\s*[=:]\s*|api[-_ ]?key\s*[=:]\s*)[^\s,;]+`)
var sensitiveKey = regexp.MustCompile(`(?i)(pass(word)?|secret|token|api[-_ ]?key|authorization|cookie|credential)`)

func RedactLogData(data entities.LogData) entities.LogData {
	data.Code = sanitize(data.Code)
	data.Message = sanitize(data.Message)
	data.Context.UserID = sanitize(data.Context.UserID)
	data.Context.SessionID = sanitize(data.Context.SessionID)
	data.Context.Endpoint = sanitize(data.Context.Endpoint)
	data.Context.Method = sanitize(data.Context.Method)
	data.Context.Domain = sanitize(data.Context.Domain)
	data.Context.RequiredPermission = sanitize(data.Context.RequiredPermission)
	data.Extra = redactValue(data.Extra, "")
	return data
}

func sanitize(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) && r != '\t' {
			return -1
		}
		return r
	}, value)
	return sensitiveValue.ReplaceAllString(value, "[REDACTED]")
}

func redactValue(value any, key string) any {
	if sensitiveKey.MatchString(key) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case string:
		return sanitize(typed)
	case map[string]any:
		result := make(map[string]any, len(typed))
		for name, nested := range typed {
			result[name] = redactValue(nested, name)
		}
		return result
	case []any:
		result := make([]any, len(typed))
		for index, nested := range typed {
			result[index] = redactValue(nested, "")
		}
		return result
	default:
		return value
	}
}

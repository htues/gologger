package websocket

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hftamayo/gologger/internal/contracts"
)

var (
    ErrInvalidJSON     = errors.New("invalid JSON")
    ErrInvalidMessage  = errors.New("invalid protocol message")
    ErrUnsupportedType = errors.New("unsupported message type")
)

type envelope struct {
    Type json.RawMessage `json:"type"`
}

func decodeMessage(data []byte) (contracts.MessageType, any, error) {
    decoder := json.NewDecoder(bytes.NewReader(data))
    decoder.DisallowUnknownFields()

    var raw map[string]json.RawMessage
    if err := decoder.Decode(&raw); err != nil {
        return "", nil, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
    }

    if len(raw) == 0 {
        return "", nil, ErrInvalidMessage
    }

    typeValue, ok := raw["type"]
    if !ok {
        return "", nil, fmt.Errorf("%w: missing type", ErrInvalidMessage)
    }

    var messageType contracts.MessageType
    if err := json.Unmarshal(typeValue, &messageType); err != nil {
        return "", nil, fmt.Errorf("%w: invalid type", ErrInvalidMessage)
    }

    var message any

    switch messageType {
    case contracts.MessageTypeHello:
        message = &contracts.HelloMessage{}

    case contracts.MessageTypeEvent:
        message = &contracts.EventMessage{}

    case contracts.MessageTypeSubscribe:
        message = &contracts.SubscribeMessage{}

    default:
        return "", nil, fmt.Errorf("%w: %s", ErrUnsupportedType, messageType)
    }

    if err := json.Unmarshal(data, message); err != nil {
        return "", nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
    }

    return messageType, message, nil
}
package ports

import (
	"context"

	"github.com/hftamayo/gologger/internal/contracts"
)

// EventProcessor validates, enriches, sanitizes, and normalizes events.
type EventProcessor interface {
	Process(ctx context.Context, event contracts.Event) (contracts.Event, error)
}

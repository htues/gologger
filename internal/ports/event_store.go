package ports

import (
	"context"

	"github.com/hftamayo/gologger/internal/contracts"
)

// EventStore persists and queries accepted application events.
type EventStore interface {
    Store(ctx context.Context, event contracts.Event) error
    Get(ctx context.Context, eventID string) (contracts.Event, error)
    Query(ctx context.Context, filter contracts.EventFilter) ([]contracts.Event, error)
    Health(ctx context.Context) error
    Close() error
}
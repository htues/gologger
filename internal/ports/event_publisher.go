package ports

import (
	"github.com/hftamayo/gologger/internal/contracts"
)

type EventSubscription interface {
	Events() <-chan contracts.Event
	Close()
}

type EventPublisher interface {
	Subscribe(filter contracts.EventFilter) EventSubscription
	Publish(event contracts.Event)
}

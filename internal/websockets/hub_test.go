package websockets

import (
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

func TestHubRegistersClient(t *testing.T) {
	hub := NewHub()
	client := &Client{
		send: make(chan contracts.Event),
	}

	go hub.Run()

	hub.register <- client

	waitForHub(t, func() bool {
		return hub.clients[client]
	})
}

func TestHubUnregistersClientAndClosesSendChannel(t *testing.T) {
	hub := NewHub()
	client := &Client{
		send: make(chan contracts.Event),
	}

	hub.clients[client] = true

	go hub.Run()

	hub.unregister <- client

	waitForHub(t, func() bool {
		_, registered := hub.clients[client]
		return !registered
	})

	select {
	case _, open := <-client.send:
		if open {
			t.Fatal("expected client send channel to be closed")
		}
	default:
		t.Fatal("expected client send channel to be closed")
	}
}

func TestHubIgnoresUnregisteredClient(t *testing.T) {
	hub := NewHub()
	client := &Client{
		send: make(chan contracts.Event),
	}

	go hub.Run()

	hub.unregister <- client

	time.Sleep(10 * time.Millisecond)

	if _, registered := hub.clients[client]; registered {
		t.Fatal("expected unregistered client to remain absent")
	}

	select {
	case _, open := <-client.send:
		if !open {
			t.Fatal("send channel should not be closed for unknown client")
		}
	default:
	}
}

func TestHubAcceptsBroadcastMessages(t *testing.T) {
	hub := NewHub()

	go hub.Run()

	hub.broadcast <- contracts.Event{
		Level:     contracts.EventLevelInfo,
		Service:   "fixture-service",
		EventType: "fixture.event",
		Message:   "fixture message",
	}
}

func waitForHub(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(time.Second)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	t.Fatal("hub condition was not satisfied before timeout")
}

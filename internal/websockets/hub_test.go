package websockets

import (
	"testing"
	"time"

	"github.com/hftamayo/gologger/internal/contracts"
)

func TestNewHubInitializesChannelsAndClients(t *testing.T) {
    hub := NewHub()

    if hub == nil {
        t.Fatal("expected hub to be initialized")
    }

    if hub.broadcast == nil {
        t.Fatal("expected broadcast channel to be initialized")
    }

    if hub.register == nil {
        t.Fatal("expected register channel to be initialized")
    }

    if hub.unregister == nil {
        t.Fatal("expected unregister channel to be initialized")
    }

    if hub.clients == nil {
        t.Fatal("expected clients map to be initialized")
    }

    if len(hub.clients) != 0 {
        t.Fatalf("expected empty clients map, got %d clients", len(hub.clients))
    }
}

func TestHubRegistersClient(t *testing.T) {
    hub := NewHub()
    client := &Client{
        send: make(chan []byte),
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
        send: make(chan []byte),
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
        send: make(chan []byte),
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

    hub.broadcast <- contracts.LogEntry{
        Level:   contracts.Info,
        Service: "fixture-service",
        Message: "fixture message",
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
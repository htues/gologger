package websockets

import "github.com/hftamayo/gologger/internal/contracts"

type Client struct {
	send chan contracts.Event
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan contracts.Event
	register   chan *Client
	unregister chan *Client
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan contracts.Event),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}

		case event := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- event:
				default:
				}
			}
		}
	}
}

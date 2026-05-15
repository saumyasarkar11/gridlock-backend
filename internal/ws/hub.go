package ws

import (
	"sync"
)

// Hub maintains active WebSocket clients and fan-out channels.
type Hub struct {
	mu sync.RWMutex

	clients map[*Client]struct{}

	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 1024),
	}
}

// Run processes hub events until shutdown closes channels (optional pattern: ctx cancel closes hub).
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			if c != nil {
				h.clients[c] = struct{}{}
			}
			h.mu.Unlock()

		case c := <-h.unregister:
			if c == nil {
				continue
			}
			h.mu.Lock()
			if _, ok := h.clients[c]; ok {
				delete(h.clients, c)
				close(c.send)
			}
			h.mu.Unlock()

		case message, ok := <-h.broadcast:
			if !ok {
				return
			}
			h.mu.RLock()
			for c := range h.clients {
				select {
				case c.send <- message:
				default:
					go func(cl *Client) {
						h.unregister <- cl
					}(c)
				}
			}
			h.mu.RUnlock()
		}
	}
}

// Register queues a client for registration (non-blocking if hub.Run is active).
func (h *Hub) Register(c *Client) {
	h.register <- c
}

// Unregister queues a client for removal.
func (h *Hub) Unregister(c *Client) {
	select {
	case h.unregister <- c:
	default:
	}
}

// Broadcast sends a message to all connected clients.
func (h *Hub) Broadcast(b []byte) {
	h.broadcast <- b
}

// ClientCount returns number of connected sockets (approximate for metrics).
func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

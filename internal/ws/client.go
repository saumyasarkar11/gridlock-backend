package ws

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"gridlock/internal/models"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 4096
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // tighten in production behind known origins
	},
}

// ClaimHandler processes validated claim actions from a client connection.
type ClaimHandler func(ctx context.Context, userID string, cellID int, c *Client)

// Client is one websocket connection bound to a single user.
type Client struct {
	hub *Hub

	conn *websocket.Conn

	send chan []byte

	userID string

	onClaim ClaimHandler

	onDisconnect func(ctx context.Context, userID string)

	log *slog.Logger

	// writeMu serializes websocket writes (gorilla requires one writer at a time).
	writeMu sync.Mutex
}

func NewClient(
	hub *Hub,
	conn *websocket.Conn,
	userID string,
	onClaim ClaimHandler,
	onDisconnect func(ctx context.Context, userID string),
	log *slog.Logger,
) *Client {
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		hub:          hub,
		conn:         conn,
		send:         make(chan []byte, 256),
		userID:       userID,
		onClaim:      onClaim,
		onDisconnect: onDisconnect,
		log:          log,
	}
}

// UserID returns the authenticated user for this socket.
func (c *Client) UserID() string {
	return c.userID
}

// Send enqueues a message to this client only (reject path).
func (c *Client) Send(b []byte) {
	select {
	case c.send <- b:
	default:
		c.hub.Unregister(c)
	}
}

// ReadPump pumps inbound messages until disconnect.
func (c *Client) ReadPump(ctx context.Context) {
	defer func() {
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if c.onDisconnect != nil {
			c.onDisconnect(dctx, c.userID)
		}
		cancel()
		c.hub.Unregister(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, payload, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				c.log.Debug("websocket read error", "err", err)
			}
			return
		}

		var msg struct {
			Type   string `json:"type"`
			CellID int    `json:"cellId"`
			UserID string `json:"userId"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			c.log.Warn("invalid ws json", "err", err)
			continue
		}
		if msg.Type != "claim" {
			c.log.Warn("unknown ws message type", "type", msg.Type)
			continue
		}
		// Bind claims to authenticated socket user (ignore spoofed userId in payload for authorization).
		if c.onClaim != nil {
			c.onClaim(ctx, c.userID, msg.CellID, c)
		}
	}
}

// WritePump pumps outbound messages until the send channel is closed.
func (c *Client) WritePump(ctx context.Context) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			c.writeMu.Lock()
			err := c.conn.WriteMessage(websocket.TextMessage, message)
			c.writeMu.Unlock()
			if err != nil {
				return
			}

		case <-ticker.C:
			c.writeMu.Lock()
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			err := c.conn.WriteMessage(websocket.PingMessage, nil)
			c.writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// ServeWS upgrades HTTP to WebSocket and starts pumps.
func ServeWS(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	hub *Hub,
	userID string,
	onConnect func(ctx context.Context, userID string),
	onClaim ClaimHandler,
	onDisconnect func(ctx context.Context, userID string),
	log *slog.Logger,
) error {
	if userID == "" {
		return errors.New("missing user")
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return err
	}
	if onConnect != nil {
		onConnect(ctx, userID)
	}
	client := NewClient(hub, conn, userID, onClaim, onDisconnect, log)
	hub.Register(client)

	go client.WritePump(ctx)
	go client.ReadPump(ctx)
	return nil
}

// Marshal helpers for outbound payloads.
func CellUpdateMessage(cellID int, userID, color string) ([]byte, error) {
	return json.Marshal(models.CellUpdate{
		Type:   "cell_update",
		CellID: cellID,
		UserID: userID,
		Color:  color,
	})
}

func RejectMessage(cellID int) ([]byte, error) {
	return json.Marshal(models.Reject{Type: "reject", CellID: cellID})
}

func PresenceMessage(userID, status string) ([]byte, error) {
	return json.Marshal(models.Presence{Type: "presence", UserID: userID, Status: status})
}

func LeaderboardUpdateMessage() ([]byte, error) {
	return json.Marshal(models.LeaderboardUpdate{Type: "leaderboard_update"})
}

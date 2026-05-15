package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"gridlock/internal/models"
)

const (
	KeyGridState  = "grid:state"
	KeyGridUsers  = "grid:users"
	KeyGridEvents = "grid:events"
	KeyColorSeq   = "grid:color_seq"
	WSTokenPrefix = "ws:token:"
	UserKeyPrefix = "user:"
)

// Client wraps go-redis with Gridlock-specific operations.
type Client struct {
	rdb *goredis.Client
}

func New(addr, password string, db int) *Client {
	return &Client{
		rdb: goredis.NewClient(&goredis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		}),
	}
}

func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

func (c *Client) Close() error {
	return c.rdb.Close()
}

func userKey(id string) string {
	return UserKeyPrefix + id
}

func wsTokenKey(token string) string {
	return WSTokenPrefix + token
}

// SaveUser stores user metadata (does not set online status by itself).
func (c *Client) SaveUser(ctx context.Context, u *models.User) error {
	key := userKey(u.ID)
	pipe := c.rdb.TxPipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"firstName":  u.FirstName,
		"color":      u.Color,
		"avatar":     u.Avatar,
		"isOnline":   boolToInt(u.IsOnline),
		"lastSeenAt": u.LastSeenAt.UnixMilli(),
	})
	pipe.SAdd(ctx, KeyGridUsers, u.ID)
	_, err := pipe.Exec(ctx)
	return err
}

// NextColorSequence returns a monotonic index for deterministic color assignment.
func (c *Client) NextColorSequence(ctx context.Context) (int64, error) {
	return c.rdb.Incr(ctx, KeyColorSeq).Result()
}

// SaveWSToken maps a short-lived session token to a user id.
func (c *Client) SaveWSToken(ctx context.Context, token, userID string, ttl time.Duration) error {
	return c.rdb.Set(ctx, wsTokenKey(token), userID, ttl).Err()
}

// ResolveWSToken returns user id for token or empty if missing/expired.
func (c *Client) ResolveWSToken(ctx context.Context, token string) (string, error) {
	v, err := c.rdb.Get(ctx, wsTokenKey(token)).Result()
	if errors.Is(err, goredis.Nil) {
		return "", nil
	}
	return v, err
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func intToBool(i int64) bool {
	return i != 0
}

// GetUser loads user metadata from Redis.
func (c *Client) GetUser(ctx context.Context, id string) (*models.User, error) {
	key := userKey(id)
	m, err := c.rdb.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(m) == 0 {
		return nil, nil
	}
	u := &models.User{ID: id}
	u.FirstName = m["firstName"]
	u.Color = m["color"]
	u.Avatar = m["avatar"]
	if v, ok := m["isOnline"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			u.IsOnline = intToBool(n)
		}
	}
	if v, ok := m["lastSeenAt"]; ok {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			u.LastSeenAt = time.UnixMilli(n)
		}
	}
	return u, nil
}

// SetOnline updates presence fields for a user.
func (c *Client) SetOnline(ctx context.Context, userID string, online bool) error {
	now := time.Now().UTC()
	return c.rdb.HSet(ctx, userKey(userID), map[string]interface{}{
		"isOnline":   boolToInt(online),
		"lastSeenAt": now.UnixMilli(),
	}).Err()
}

// TryClaimCell atomically assigns ownership if the cell is empty.
// Returns true if this user won the cell.
func (c *Client) TryClaimCell(ctx context.Context, cellID int, userID string) (bool, error) {
	field := strconv.Itoa(cellID)
	n, err := c.rdb.HSetNX(ctx, KeyGridState, field, userID).Result()
	if err != nil {
		return false, err
	}
	return n, nil
}

// GetGridOwnership returns raw cellId string -> userId.
func (c *Client) GetGridOwnership(ctx context.Context) (map[string]string, error) {
	return c.rdb.HGetAll(ctx, KeyGridState).Result()
}

// ClearGrid removes all cell ownership (admin).
func (c *Client) ClearGrid(ctx context.Context) error {
	return c.rdb.Del(ctx, KeyGridState).Err()
}

// AllUserIDs returns known users (joined at least once).
func (c *Client) AllUserIDs(ctx context.Context) ([]string, error) {
	return c.rdb.SMembers(ctx, KeyGridUsers).Result()
}

// PublishGridEvent publishes a JSON event for subscribers (multi-node).
func (c *Client) PublishGridEvent(ctx context.Context, ev *models.GridEvent) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return c.rdb.Publish(ctx, KeyGridEvents, b).Err()
}

// SubscribeGridEvents listens for grid events until context is cancelled.
func (c *Client) SubscribeGridEvents(ctx context.Context, handler func(payload []byte)) error {
	sub := c.rdb.Subscribe(ctx, KeyGridEvents)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return errors.New("redis subscription channel closed")
			}
			if msg == nil {
				continue
			}
			if strings.TrimSpace(msg.Payload) == "" {
				continue
			}
			handler([]byte(msg.Payload))
		}
	}
}

// RDB exposes the underlying client for advanced use (optional); keep internal for tests if needed.
func (c *Client) RDB() *goredis.Client {
	return c.rdb
}

// ErrNil is redis.Nil for handlers.
var ErrNil = goredis.Nil

// FormatUserKey is exported for tests/logging.
func FormatUserKey(id string) string {
	return fmt.Sprintf("%s%s", UserKeyPrefix, id)
}

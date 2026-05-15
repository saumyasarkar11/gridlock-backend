package models

import "time"

// User is persisted metadata for a player (ownership survives disconnect).
type User struct {
	ID         string    `json:"userId"`
	FirstName  string    `json:"firstName"`
	Color      string    `json:"color"`
	Avatar     string    `json:"avatar"`
	IsOnline   bool      `json:"isOnline"`
	LastSeenAt time.Time `json:"lastSeenAt,omitempty"`
}

// JoinResponse is returned by POST /api/join.
type JoinResponse struct {
	UserID    string `json:"userId"`
	FirstName string `json:"firstName"`
	Color     string `json:"color"`
	Avatar    string `json:"avatar"`
	WSToken   string `json:"wsToken"`
}

// GridCell is one cell in a REST grid snapshot.
type GridCell struct {
	CellID int    `json:"cellId"`
	UserID string `json:"userId,omitempty"`
	Color  string `json:"color,omitempty"`
}

// GridResponse is returned by GET /api/grid.
type GridResponse struct {
	Cells []GridCell `json:"cells"`
}

// LeaderboardEntry is one row on the leaderboard.
type LeaderboardEntry struct {
	UserID         string `json:"userId"`
	FirstName      string `json:"firstName"`
	Avatar         string `json:"avatar"`
	Color          string `json:"color"`
	OwnedCellCount int    `json:"ownedCellCount"`
	IsOnline       bool   `json:"isOnline"`
}

// LeaderboardResponse is returned by GET /api/leaderboard.
type LeaderboardResponse struct {
	Entries []LeaderboardEntry `json:"entries"`
}

// ClientClaim is a WebSocket claim from the client.
type ClientClaim struct {
	Type   string `json:"type"`
	CellID int    `json:"cellId"`
	UserID string `json:"userId"`
}

// CellUpdate is broadcast when a cell is claimed.
type CellUpdate struct {
	Type   string `json:"type"`
	CellID int    `json:"cellId"`
	UserID string `json:"userId"`
	Color  string `json:"color"`
}

// Reject is sent only to the claimant when a claim loses the race.
type Reject struct {
	Type   string `json:"type"`
	CellID int    `json:"cellId"`
}

// Presence is broadcast when a user goes online/offline.
type Presence struct {
	Type   string `json:"type"`
	UserID string `json:"userId"`
	Status string `json:"status"` // "online" | "offline"
}

// LeaderboardUpdate notifies clients to refresh leaderboard (REST).
type LeaderboardUpdate struct {
	Type string `json:"type"`
}

// GridEvent is published on Redis for multi-node fan-out.
type GridEvent struct {
	Origin    string `json:"origin"`
	EventType string `json:"eventType"`
	// CellUpdate fields when EventType == "cell_update"
	CellID int    `json:"cellId,omitempty"`
	UserID string `json:"userId,omitempty"`
	Color  string `json:"color,omitempty"`
}

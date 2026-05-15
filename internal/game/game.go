package game

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"gridlock/internal/models"
	"gridlock/internal/redis"
)

var animals = []string{
	"fox", "panda", "tiger", "owl", "whale", "wolf", "bear", "eagle",
	"otter", "rabbit", "deer", "lion", "hawk", "dolphin", "koala", "raven",
}

// Service contains core domain logic for Gridlock.
type Service struct {
	Redis       *redis.Client
	InstanceID  string
	WSTokenTTL  time.Duration
	AdminAPIKey string
}

// Join creates a new user with deterministic unique color and random avatar.
func (s *Service) Join(ctx context.Context, firstName string) (*models.JoinResponse, error) {
	if s.Redis == nil {
		return nil, fmt.Errorf("redis client is nil")
	}
	firstName = trimName(firstName)
	if firstName == "" {
		return nil, fmt.Errorf("firstName is required")
	}

	seq, err := s.Redis.NextColorSequence(ctx)
	if err != nil {
		return nil, err
	}
	color := hslFromSequence(seq)

	id := uuid.NewString()
	avatar, err := pickAnimal()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	u := &models.User{
		ID:         id,
		FirstName:  firstName,
		Color:      color,
		Avatar:     avatar,
		IsOnline:   false,
		LastSeenAt: now,
	}
	if err := s.Redis.SaveUser(ctx, u); err != nil {
		return nil, err
	}

	token, err := randomToken(32)
	if err != nil {
		return nil, err
	}
	if err := s.Redis.SaveWSToken(ctx, token, id, s.wsTokenTTLOrDefault()); err != nil {
		return nil, err
	}

	return &models.JoinResponse{
		UserID:    id,
		FirstName: firstName,
		Color:     color,
		Avatar:    avatar,
		WSToken:   token,
	}, nil
}

func (s *Service) wsTokenTTLOrDefault() time.Duration {
	if s.WSTokenTTL > 0 {
		return s.WSTokenTTL
	}
	return 24 * time.Hour
}

// ResolveWSToken validates a websocket token.
func (s *Service) ResolveWSToken(ctx context.Context, token string) (string, error) {
	return s.Redis.ResolveWSToken(ctx, token)
}

// GridSnapshot returns REST snapshot with colors resolved.
func (s *Service) GridSnapshot(ctx context.Context) (*models.GridResponse, error) {
	raw, err := s.Redis.GetGridOwnership(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]models.GridCell, 0, len(raw))
	for k, userID := range raw {
		cid, err := strconv.Atoi(k)
		if err != nil {
			continue
		}
		cell := models.GridCell{CellID: cid, UserID: userID}
		if userID != "" {
			u, err := s.Redis.GetUser(ctx, userID)
			if err != nil {
				return nil, err
			}
			if u != nil {
				cell.Color = u.Color
			}
		}
		out = append(out, cell)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CellID < out[j].CellID })
	return &models.GridResponse{Cells: out}, nil
}

// Leaderboard builds sorted leaderboard from grid ownership + user metadata.
func (s *Service) Leaderboard(ctx context.Context) (*models.LeaderboardResponse, error) {
	raw, err := s.Redis.GetGridOwnership(ctx)
	if err != nil {
		return nil, err
	}
	counts := map[string]int{}
	for _, uid := range raw {
		if uid == "" {
			continue
		}
		counts[uid]++
	}

	userIDs, err := s.Redis.AllUserIDs(ctx)
	if err != nil {
		return nil, err
	}
	for _, id := range userIDs {
		if _, ok := counts[id]; !ok {
			counts[id] = 0
		}
	}

	entries := make([]models.LeaderboardEntry, 0, len(counts))
	for uid, n := range counts {
		u, err := s.Redis.GetUser(ctx, uid)
		if err != nil {
			return nil, err
		}
		if u == nil {
			continue
		}
		entries = append(entries, models.LeaderboardEntry{
			UserID:         uid,
			FirstName:      u.FirstName,
			Avatar:         u.Avatar,
			Color:          u.Color,
			OwnedCellCount: n,
			IsOnline:       u.IsOnline,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].OwnedCellCount != entries[j].OwnedCellCount {
			return entries[i].OwnedCellCount > entries[j].OwnedCellCount
		}
		return entries[i].UserID < entries[j].UserID
	})
	return &models.LeaderboardResponse{Entries: entries}, nil
}

// ResetGrid clears ownership (admin).
func (s *Service) ResetGrid(ctx context.Context) error {
	return s.Redis.ClearGrid(ctx)
}

// TryClaim applies atomic ownership rules and returns broadcast payload on success.
func (s *Service) TryClaim(ctx context.Context, userID string, cellID int) (ok bool, ownerColor string, err error) {
	if cellID < 0 {
		return false, "", fmt.Errorf("invalid cellId")
	}
	u, err := s.Redis.GetUser(ctx, userID)
	if err != nil {
		return false, "", err
	}
	if u == nil {
		return false, "", fmt.Errorf("unknown user")
	}

	won, err := s.Redis.TryClaimCell(ctx, cellID, userID)
	if err != nil {
		return false, "", err
	}
	if !won {
		return false, "", nil
	}
	return true, u.Color, nil
}

// SetPresence updates online flag (used on connect/disconnect).
func (s *Service) SetPresence(ctx context.Context, userID string, online bool) error {
	return s.Redis.SetOnline(ctx, userID, online)
}

func trimName(s string) string {
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > 64 {
		r = r[:64]
	}
	return string(r)
}

func pickAnimal() (string, error) {
	var b [1]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return animals[int(b[0])%len(animals)], nil
}

func randomToken(nBytes int) (string, error) {
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

// hslFromSequence generates visually separated HSL colors with comfortable contrast.
// Uses golden-angle hue stepping and clamps saturation/lightness for readability.
func hslFromSequence(seq int64) string {
	const golden = 137.508
	hue := math.Mod(float64(seq)*golden, 360.0)
	if hue < 0 {
		hue += 360
	}
	// vary S/L slightly while staying in readable ranges (not too light)
	sat := 62.0 + float64((seq/17)%4)*6.0  // 62-80
	light := 40.0 + float64((seq/5)%3)*7.0 // 40-54
	return hslToHex(hue, sat, light)
}

func hslToHex(h, s, l float64) string {
	h = math.Mod(h, 360) / 360.0
	s /= 100
	l /= 100

	q := l
	if l < 0.5 {
		q = l * (1 + s)
	} else {
		q = l + s - l*s
	}
	p := 2*l - q

	r := hueToRGB(p, q, h+1.0/3.0)
	g := hueToRGB(p, q, h)
	b := hueToRGB(p, q, h-1.0/3.0)
	return fmt.Sprintf("#%02x%02x%02x", clamp255(r), clamp255(g), clamp255(b))
}

func hueToRGB(p, q, t float64) float64 {
	for t < 0 {
		t += 1
	}
	for t > 1 {
		t -= 1
	}
	if t < 1.0/6.0 {
		return p + (q-p)*6*t
	}
	if t < 0.5 {
		return q
	}
	if t < 2.0/3.0 {
		return p + (q-p)*(2.0/3.0-t)*6
	}
	return p
}

func clamp255(x float64) int {
	v := int(math.Round(x * 255))
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return v
}

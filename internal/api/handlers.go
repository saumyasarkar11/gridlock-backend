package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"gridlock/internal/game"
	"gridlock/internal/models"
	"gridlock/internal/redis"
	"gridlock/internal/ws"
)

// Deps wires HTTP handlers to core services.
type Deps struct {
	Log         *slog.Logger
	Game        *game.Service
	Hub         *ws.Hub
	Redis       *redis.Client
	InstanceID  string
	AdminAPIKey string
}

func (d *Deps) log() *slog.Logger {
	if d == nil || d.Log == nil {
		return slog.Default()
	}
	return d.Log
}

// Router returns the application HTTP mux.
func (d *Deps) Router(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", d.handleHealth)
	mux.HandleFunc("POST /api/join", d.withCORS(d.handleJoin))
	mux.HandleFunc("GET /api/grid", d.withCORS(d.handleGetGrid))
	mux.HandleFunc("GET /api/leaderboard", d.withCORS(d.handleLeaderboard))
	mux.HandleFunc("POST /api/reset", d.withCORS(d.handleReset))
	mux.HandleFunc("GET /ws", d.withCORS(d.handleWS()))
	return d.withRequestContext(ctx, mux)
}

func (d *Deps) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type joinRequest struct {
	FirstName string `json:"firstName"`
}

func (d *Deps) handleJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var body joinRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	out, err := d.Game.Join(r.Context(), body.FirstName)
	if err != nil {
		d.log().Warn("join failed", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (d *Deps) handleGetGrid(w http.ResponseWriter, r *http.Request) {
	grid, err := d.Game.GridSnapshot(r.Context())
	if err != nil {
		d.log().Error("grid snapshot failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, grid)
}

func (d *Deps) handleLeaderboard(w http.ResponseWriter, r *http.Request) {
	lb, err := d.Game.Leaderboard(r.Context())
	if err != nil {
		d.log().Error("leaderboard failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, lb)
}

func (d *Deps) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !d.adminAuthorized(r) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if err := d.Game.ResetGrid(r.Context()); err != nil {
		d.log().Error("reset failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if b, err := ws.LeaderboardUpdateMessage(); err == nil {
		d.Hub.Broadcast(b)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (d *Deps) adminAuthorized(r *http.Request) bool {
	if d.AdminAPIKey == "" {
		return false
	}
	got := strings.TrimSpace(r.Header.Get("X-Admin-Key"))
	want := d.AdminAPIKey
	if len(got) != len(want) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

func (d *Deps) handleWS() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		token := strings.TrimSpace(r.URL.Query().Get("token"))
		if token == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		userID, err := d.Game.ResolveWSToken(r.Context(), token)
		if err != nil {
			d.log().Error("token resolve failed", "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if userID == "" {
			http.Error(w, "invalid token", http.StatusUnauthorized)
			return
		}

		ctx := r.Context()
		if err := ws.ServeWS(
			ctx,
			w,
			r,
			d.Hub,
			userID,
			d.wsOnConnect(),
			d.wsOnClaim,
			d.wsOnDisconnect(),
			d.log(),
		); err != nil {
			d.log().Warn("websocket upgrade failed", "err", err)
		}
	}
}

func (d *Deps) wsOnConnect() func(ctx context.Context, userID string) {
	return func(ctx context.Context, userID string) {
		if err := d.Game.SetPresence(ctx, userID, true); err != nil {
			d.log().Error("set online failed", "userId", userID, "err", err)
			return
		}
		b, err := ws.PresenceMessage(userID, "online")
		if err != nil {
			d.log().Error("presence marshal failed", "err", err)
			return
		}
		d.Hub.Broadcast(b)
	}
}

func (d *Deps) wsOnDisconnect() func(ctx context.Context, userID string) {
	return func(ctx context.Context, userID string) {
		if err := d.Game.SetPresence(ctx, userID, false); err != nil {
			d.log().Error("set offline failed", "userId", userID, "err", err)
			return
		}
		b, err := ws.PresenceMessage(userID, "offline")
		if err != nil {
			d.log().Error("presence marshal failed", "err", err)
			return
		}
		d.Hub.Broadcast(b)
	}
}

func (d *Deps) wsOnClaim(ctx context.Context, userID string, cellID int, c *ws.Client) {
	ok, color, err := d.Game.TryClaim(ctx, userID, cellID)
	if err != nil {
		d.log().Warn("claim failed", "userId", userID, "cellId", cellID, "err", err)
		return
	}
	if !ok {
		b, err := ws.RejectMessage(cellID)
		if err != nil {
			d.log().Error("reject marshal failed", "err", err)
			return
		}
		c.Send(b)
		return
	}

	cellPayload, err := ws.CellUpdateMessage(cellID, userID, color)
	if err != nil {
		d.log().Error("cell update marshal failed", "err", err)
		return
	}
	d.Hub.Broadcast(cellPayload)

	ev := &models.GridEvent{
		Origin:    d.InstanceID,
		EventType: "cell_update",
		CellID:    cellID,
		UserID:    userID,
		Color:     color,
	}
	if err := d.Redis.PublishGridEvent(ctx, ev); err != nil {
		d.log().Error("publish grid event failed", "err", err)
	}

	lb, err := ws.LeaderboardUpdateMessage()
	if err != nil {
		d.log().Error("leaderboard update marshal failed", "err", err)
		return
	}
	d.Hub.Broadcast(lb)
}

func (d *Deps) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Admin-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (d *Deps) withRequestContext(base context.Context, next http.Handler) http.Handler {
	_ = base
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 55*time.Second)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Default().Error("encode json failed", "err", err)
	}
}

// ListenAndServe starts the HTTP server (helper for tests / small binaries).
func ListenAndServe(ctx context.Context, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return ctx.Err()
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	return nil
}

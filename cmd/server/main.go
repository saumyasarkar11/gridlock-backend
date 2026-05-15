package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"gridlock/internal/api"
	"gridlock/internal/game"
	"gridlock/internal/models"
	"gridlock/internal/redis"
	"gridlock/internal/ws"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	redisAddr := getenv("REDIS_ADDR", "127.0.0.1:6379")
	redisPass := os.Getenv("REDIS_PASSWORD")
	redisDB, _ := strconv.Atoi(getenv("REDIS_DB", "0"))
	httpAddr := getenv("HTTP_ADDR", ":8080")
	adminKey := os.Getenv("GRIDLOCK_ADMIN_KEY")
	instanceID := getenv("GRIDLOCK_INSTANCE_ID", uuid.NewString())

	rdb := redis.New(redisAddr, redisPass, redisDB)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := rdb.Ping(ctx); err != nil {
		log.Error("redis ping failed", "addr", redisAddr, "err", err)
		os.Exit(1)
	}

	hub := ws.NewHub()
	go hub.Run()

	wsTTL := 24 * time.Hour
	if h := getenv("WS_TOKEN_TTL_HOURS", ""); h != "" {
		if n, err := strconv.Atoi(h); err == nil && n > 0 {
			wsTTL = time.Duration(n) * time.Hour
		}
	}

	gameSvc := &game.Service{
		Redis:      rdb,
		WSTokenTTL: wsTTL,
	}

	deps := &api.Deps{
		Log:         log,
		Game:        gameSvc,
		Hub:         hub,
		Redis:       rdb,
		InstanceID:  instanceID,
		AdminAPIKey: adminKey,
	}

	go runGridSubscriber(ctx, log, rdb, instanceID, hub)

	handler := deps.Router(ctx)
	log.Info("gridlock listening", "addr", httpAddr, "instanceId", instanceID)
	if err := api.ListenAndServe(ctx, httpAddr, handler); err != nil && err != context.Canceled {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
	_ = rdb.Close()
}

func runGridSubscriber(ctx context.Context, log *slog.Logger, rdb *redis.Client, instanceID string, hub *ws.Hub) {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		err := rdb.SubscribeGridEvents(ctx, func(payload []byte) {
			var ev models.GridEvent
			if err := json.Unmarshal(payload, &ev); err != nil {
				log.Warn("grid event decode failed", "err", err)
				return
			}
			if ev.Origin == instanceID {
				return
			}
			if ev.EventType != "cell_update" {
				return
			}
			b, err := ws.CellUpdateMessage(ev.CellID, ev.UserID, ev.Color)
			if err != nil {
				log.Warn("cell update marshal failed", "err", err)
				return
			}
			hub.Broadcast(b)
			lb, err := ws.LeaderboardUpdateMessage()
			if err != nil {
				return
			}
			hub.Broadcast(lb)
		})
		if ctx.Err() != nil {
			return
		}
		log.Warn("grid subscriber stopped, restarting", "err", err, "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

# Gridlock backend

Production-oriented Go service for **Gridlock**, a real-time shared grid where users claim cells. REST covers snapshots and onboarding; **WebSockets are only used for live grid claims, presence, and leaderboard refresh signals**.

## Tech stack

- Go 1.22+
- `net/http` (no Gin/Fiber/Echo)
- `github.com/gorilla/websocket`
- Redis via `github.com/redis/go-redis/v9`
- Structured logging with `log/slog`

## Folder structure

```text
backend/
  cmd/server/main.go          # process entry: wiring, signals, subscriber
  internal/
    api/handlers.go           # REST + WebSocket upgrade route
    game/game.go              # domain: join, claim, leaderboard, snapshots
    redis/redis.go            # Redis keys, atomic claim, pub/sub
    ws/hub.go                 # fan-out hub
    ws/client.go              # connection read/write pumps
    models/models.go          # shared DTOs / wire types
  go.mod
  .env.example
  README.md
```

## Redis schema

| Key / channel | Type | Purpose |
|---------------|------|---------|
| `grid:state` | **HASH** | Field `cellId` (string) → `userId`. Authoritative ownership. |
| `user:<userId>` | **HASH** | `firstName`, `color`, `avatar`, `isOnline` (`0`/`1`), `lastSeenAt` (unix ms). |
| `grid:users` | **SET** | All user ids that have joined (drives leaderboard rows with zero cells). |
| `grid:color_seq` | **STRING** (INCR) | Monotonic sequence for deterministic distinct HSL colors. |
| `ws:token:<token>` | **STRING** with TTL | Maps short-lived WebSocket token → `userId` (not JWT). |
| `grid:events` | **PUB/SUB channel** | JSON `GridEvent` payloads for cross-node replay (same process skips self-origin). |

**Conflict resolution:** successful claims use **`HSETNX`** on `grid:state` so exactly one writer wins per cell; losers get a WebSocket `reject` message only to that client.

## WebSocket hub

Canonical pattern in `internal/ws/hub.go`:

- `register` / `unregister` / `broadcast` channels
- `Run()` goroutine owns the `clients` map under a mutex
- `Broadcast` delivers JSON payloads to every connected client’s outbound buffer

Clients are goroutines with `ReadPump` (claims) and `WritePump` (fan-out + ping).

## Claim flow (atomic + pub/sub)

1. Client sends `{ "type": "claim", "cellId": <n>, "userId": "<ignored for auth>" }` over the socket.  
   The server **binds the claim to the authenticated user** from `?token=...` (the `userId` field in the message is ignored for authorization).
2. `HSETNX grid:state <cellId> <userId>`  
   - `1` → success: broadcast `cell_update`, publish `grid:events` JSON (includes `origin` instance id), broadcast `leaderboard_update`.  
   - `0` → failure: send `reject` **only** to that socket.

**Same-node optimization:** the winning path broadcasts immediately to the local hub **and** publishes to Redis. The in-process subscriber ignores messages where `origin` equals this server’s `GRIDLOCK_INSTANCE_ID` to avoid duplicate local delivery; another node would only receive via Redis and then broadcast locally.

## REST API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/join` | Body: `{"firstName":"..."}`. Creates user (UUID, HSL color, random animal avatar), stores in Redis, returns `userId`, `firstName`, `color`, `avatar`, `wsToken`. |
| `GET` | `/api/grid` | Full grid snapshot: cells with `cellId`, `userId`, `color`. |
| `GET` | `/api/leaderboard` | Users sorted by `ownedCellCount` desc; includes `firstName`, `avatar`, `color`, `ownedCellCount`, `isOnline`. |
| `POST` | `/api/reset` | Clears `grid:state`. Header `X-Admin-Key` must match `GRIDLOCK_ADMIN_KEY`. Broadcasts `leaderboard_update` to live clients (refetch grid + leaderboard via REST). |
| `GET` | `/health` | `{"status":"ok"}`. |

## WebSocket

After join, connect:

`ws://localhost:8080/ws?token=<wsToken>`

Message types match the product spec (`cell_update`, `reject`, `presence`, `leaderboard_update`).

**Ownership vs presence:** disconnect sets `isOnline=false` and broadcasts `presence` offline; **cells stay owned** in `grid:state`.

## Configuration

See `.env.example`. Environment variables:

- `HTTP_ADDR` — listen address (default `:8080`).
- `REDIS_ADDR`, `REDIS_PASSWORD`, `REDIS_DB` — Redis client options.
- `GRIDLOCK_ADMIN_KEY` — required for `/api/reset` (constant-time compare); if unset, reset is always forbidden.
- `GRIDLOCK_INSTANCE_ID` — optional; defaults to a random UUID per process (used for pub/sub de-duplication).
- `WS_TOKEN_TTL_HOURS` — token TTL (default 24).

## Local development

1. Install **Go 1.22+** and **Redis** (local or Docker).
2. Copy env file (optional):

   ```bash
   cp .env.example .env
   ```

   Set `GRIDLOCK_ADMIN_KEY` if you need `/api/reset`.

3. From `backend/`:

   ```bash
   go run ./cmd/server
   ```

4. Smoke test:

   ```bash
   curl -s -X POST localhost:8080/api/join -H "Content-Type: application/json" -d "{\"firstName\":\"Ada\"}"
   curl -s localhost:8080/api/grid
   curl -s localhost:8080/api/leaderboard
   ```

5. WebSocket: use a client or browser with the returned `wsToken` query parameter.

## Production notes

- Terminate TLS at a reverse proxy (recommended).
- Restrict `Access-Control-Allow-Origin` in `internal/api/handlers.go` instead of `*` when you have fixed front-end origins.
- Tune Redis persistence (AOF/RDB) according to how durable you need cell ownership to be across Redis restarts.

# Gridlock Backend

Backend service for Gridlock, a real-time shared grid application where users claim cells and see updates live across connected clients.

## Tech Stack

* Go 1.22+
* `net/http`
* `github.com/gorilla/websocket`
* Redis via `github.com/redis/go-redis/v9`
* Structured logging with `log/slog`

## Folder Structure

```text
backend/
  cmd/server/main.go          # Process entrypoint
  internal/
    api/handlers.go           # REST handlers + WebSocket upgrade route
    game/game.go              # Join, claim, leaderboard, snapshots
    redis/redis.go            # Redis operations and pub/sub
    ws/hub.go                 # WebSocket hub
    ws/client.go              # Connection read/write pumps
    models/models.go          # Shared DTOs and message types
  go.mod
  .env.example
  README.md
```

## Architecture Decisions

* Redis HASH (`grid:state`) stores authoritative ownership state
* Redis Pub/Sub distributes transient real-time events between nodes
* REST is used for onboarding and snapshot retrieval
* WebSockets are limited to live updates and presence events
* Ownership persists after disconnect
* Presence is tracked separately from ownership
* Cell claims are resolved atomically using Redis `HSETNX`

## Redis Schema

| Key / Channel      | Type            | Purpose                                                          |
| ------------------ | --------------- | ---------------------------------------------------------------- |
| `grid:state`       | HASH            | Field `cellId` → `userId`. Stores authoritative ownership state. |
| `user:<userId>`    | HASH            | Stores `firstName`, `color`, `avatar`, `isOnline`, `lastSeenAt`. |
| `grid:users`       | SET             | Tracks all users who have joined.                                |
| `grid:color_seq`   | STRING (INCR)   | Generates deterministic unique HSL colors.                       |
| `ws:token:<token>` | STRING with TTL | Maps temporary WebSocket token → `userId`.                       |
| `grid:events`      | PUB/SUB channel | Broadcasts transient real-time grid events.                      |

## Conflict Resolution

Cell ownership uses Redis atomic operations:

```text
HSETNX grid:state <cellId> <userId>
```

* `1` → claim succeeds
* `0` → claim rejected because another user already owns the cell

This guarantees that only one user can successfully claim a cell during concurrent clicks.

## WebSocket Hub

The WebSocket hub manages:

* connected clients
* registration and disconnects
* outbound broadcasts

Each client connection runs:

* `ReadPump` for incoming claim messages
* `WritePump` for outbound broadcasts and ping/pong handling

## Claim Flow

1. Client sends:

```json
{
  "type": "claim",
  "cellId": 42
}
```

2. Backend attempts:

```text
HSETNX grid:state 42 <userId>
```

3. If successful:

   * broadcast `cell_update` locally
   * publish event to Redis `grid:events`
   * broadcast `leaderboard_update`

4. If rejected:

   * send `reject` only to that socket

Successful claims are broadcast locally immediately and also published to Redis.
The Redis subscriber ignores events originating from the same instance to avoid duplicate broadcasts.

## REST API

| Method | Path               | Description                                                            |
| ------ | ------------------ | ---------------------------------------------------------------------- |
| `POST` | `/api/join`        | Creates a user and returns `userId`, `color`, `avatar`, and `wsToken`. |
| `GET`  | `/api/grid`        | Returns current grid snapshot.                                         |
| `GET`  | `/api/leaderboard` | Returns users sorted by owned cell count.                              |
| `POST` | `/api/reset`       | Clears the grid. Requires `X-Admin-Key`.                               |
| `GET`  | `/health`          | Health check endpoint.                                                 |

## WebSocket

After joining:

```text
ws://localhost:8080/ws?token=<wsToken>
```

WebSocket message types:

* `cell_update`
* `reject`
* `presence`
* `leaderboard_update`

Disconnecting a client:

* marks user as offline
* broadcasts presence update
* preserves owned cells

## Configuration

Environment variables:

| Variable               | Description                                    |
| ---------------------- | ---------------------------------------------- |
| `HTTP_ADDR`            | HTTP listen address                            |
| `REDIS_ADDR`           | Redis address                                  |
| `REDIS_PASSWORD`       | Redis password                                 |
| `REDIS_DB`             | Redis database                                 |
| `GRIDLOCK_ADMIN_KEY`   | Admin key for `/api/reset`                     |
| `GRIDLOCK_INSTANCE_ID` | Instance identifier for pub/sub de-duplication |
| `WS_TOKEN_TTL_HOURS`   | WebSocket token expiration                     |

## Local Development

Install:

* Go 1.22+
* Redis

Copy environment file:

```bash
cp .env.example .env
```

Run backend:

```bash
go run ./cmd/server
```

Smoke test:

```bash
curl -X POST localhost:8080/api/join \
  -H "Content-Type: application/json" \
  -d '{"firstName":"Ada"}'

curl localhost:8080/api/grid

curl localhost:8080/api/leaderboard
```

## Deployment Notes

* Redis should only be accessible locally
* TLS termination should happen at a reverse proxy such as Nginx
* Restrict CORS origins when frontend origin is fixed
* WebSocket traffic should be proxied through Nginx using upgrade headers

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this service does

Backend for [mytonprovider.org](https://mytonprovider.org) — a TON Storage provider monitoring service. It discovers providers by scanning the TON master contract's transaction history, probes each provider via ADNL/DHT, collects telemetry, verifies storage proofs (downloads a random torrent piece and validates its Merkle proof), computes ratings, and exposes a REST API for the frontend.

## Commands

### Run locally
```bash
# Start Postgres (schema applied automatically via docker-init-db.sh)
docker compose up -d

# Reset DB (drops all data)
docker compose down -v && docker compose up -d

# Run the coordinator
env $(cat .coordinator.env | grep -v '^#' | xargs) go run ./cmd/coordinator

# Run an agent
env $(cat .agent.env | grep -v '^#' | xargs) go run ./cmd/agent

# Run a second agent on different ports
AGENT_PORT=9092 AGENT_ADNL_PORT=16169 \
env $(cat .agent.env | grep -v '^#' | xargs) go run ./cmd/agent
```

Use `-tags=debug` on the coordinator when running without nginx — builds `router_debug.go` instead of `router.go`, adding permissive CORS headers and OPTIONS handling:
```bash
go run -tags=debug ./cmd/coordinator
```

### Build, test, lint
```bash
go build ./...
go test ./...
golangci-lint run ./...   # default settings, no config file
```

No Makefile; use `go` commands directly.

### Environment variables

**Coordinator** (see `.coordinator.env`):

| Variable | Required | Default | Notes |
|---|---|---|---|
| `DB_HOST/PORT/USER/PASSWORD/NAME` | yes | — | Postgres connection |
| `MASTER_ADDRESS` | yes | — | TON master contract address |
| `TON_CONFIG_URL` | yes | global.config.json URL | Lite-server config |
| `INTERNAL_TOKEN` | no | `""` | Shared secret for agent API; empty disables auth |
| `SYSTEM_ACCESS_TOKENS` | no | `""` | Comma-separated MD5-hashed tokens for `/metrics` and admin routes |
| `SYSTEM_PORT` | no | `9090` | HTTP listen port |

**Agent** (see `.agent.env`):

| Variable | Required | Default |
|---|---|---|
| `COORDINATOR_URL` | yes | — |
| `TON_CONFIG_URL` | yes | — |
| `INTERNAL_TOKEN` | no | `""` |
| `AGENT_PORT` | no | `9091` |
| `AGENT_ADNL_PORT` | no | `16168` |
| `SYSTEM_KEY` | no | auto-generated | ed25519 private key seed |

## Architecture

The system is split into two binaries:

### Coordinator (`cmd/coordinator`)
Single instance. Owns all DB state, all cursor management, and all TON lite-client calls. Orchestrates agent work: reads input from DB, splits across registered agents, writes aggregated results back.

**Infrastructure**: TON lite client, Postgres, HTTP server. No ADNL/DHT.

### Agent (`cmd/agent`)
N instances. Lightweight — no DB, no TON lite client. Handles only ADNL/DHT/RLDP network work. Registers with the coordinator on startup and sends a heartbeat every 30 s; the coordinator evicts agents not seen within 90 s. If an agent is evicted, the heartbeat loop re-registers automatically.

**Infrastructure**: ADNL gateway, DHT client, RLDP. No DB.

### Layer stack (coordinator, top → bottom)
```
HTTP handler  (pkg/httpServer)
    ↓
Service layer (pkg/services/providers) ← cache middleware wraps the service
    ↓
Repository    (pkg/repositories/providers, /system) ← metrics middleware wraps the repo
    ↓
PostgreSQL via pgx/v5 pool
```

Each layer is a Go interface. Metrics and caching are added via the **decorator pattern** — `NewMetrics(...)` and `NewCacheMiddleware(...)` wrap the real implementation without changing the interface.

### Workers
All background work uses the self-scheduling pattern: each worker method returns `(interval time.Duration, err error)` and the harness in `pkg/workers/workers.go` sleeps for that interval then calls again.

| Worker | Runs on | Key tasks |
|---|---|---|
| `providersMaster.CollectNewProviders` | coordinator | Scans master wallet txs for `tsp-{pubkey}` messages; advances `masterWalletLastLT` cursor in `system.params` |
| `providersMaster.CollectProvidersNewStorageContracts` | coordinator | Scans provider wallet txs; advances per-provider `last_tx_lt` cursors |
| `providersMaster.DistributeProviderPing` | coordinator (delegates) | Gets all pubkeys from DB, splits across agents via `agentClient.DistributePing`, writes status+rate results to DB |
| `providersMaster.DistributeStoreProof` | coordinator (delegates) | Phase 1 (coordinator): TON `GetProvidersInfo` → remove rejected contracts; Phase 2 (agents): IP resolution + RLDP proof checks; writes IPs + reason codes to DB |
| `providersMaster.UpdateUptime` | coordinator | Pure SQL aggregation from `statuses_history` |
| `providersMaster.UpdateRating` | coordinator | Pure SQL formula (uptime, hardware, network, price) |
| `providersMaster.UpdateIPInfo` | coordinator | HTTP calls to `ifconfig.co` for geo-info |
| `telemetry` | coordinator | Flushes in-memory telemetry/benchmark buffers to Postgres |
| `cleaner` | coordinator | Deletes history rows older than `SYSTEM_STORE_HISTORY_DAYS` (default 90) |

### Agent work distribution and failure handling

`pkg/agentClient/client.go` implements **two-pass dispatch**:

1. **Pass 1**: all chunks sent to all agents in parallel.
2. **Pass 2**: failed chunks redistributed round-robin to agents that succeeded in pass 1 (one retry).
3. If **all agents fail**: `ErrAllAgentsFailed` → coordinator aborts the cycle and returns `failureInterval`.
4. If **pass 2 retry fails**: partial result accepted with a `WARN` log. Effect depends on workflow:
   - `DistributeProviderPing`: harmless — missed providers skip one 1-minute status cycle.
   - `DistributeStoreProof` phase 1 (TON call, coordinator-side): **hard abort** — coordinator must not proceed with a stale contract list.
   - `DistributeStoreProof` phase 2 (agents): old `reason_timestamp` values persist for up to 24 h (the `UpdateStatuses` window).

### Agent API

Coordinator exposes (authenticated with `X-Internal-Token`):
- `POST /internal/v1/agents` — agent registers, coordinator returns `{"id": "uuid"}`
- `POST /internal/v1/agents/:id/heartbeat` — keeps agent alive

Agent exposes (authenticated with `X-Internal-Token`):
- `POST /internal/v1/workers/ping-providers` — ADNL ping + `GetStorageRates` per pubkey
- `POST /internal/v1/workers/check-proofs` — IP resolution (ADNL/DHT) + RLDP proof verification per bag

### Caching
`NewCacheMiddleware` in `pkg/services/providers/cache.go` holds three in-memory buffers shared between HTTP and the telemetry worker:
- `telemetryBuffer` — latest `TelemetryRequest` per pubkey; drained by `telemetry.UpdateTelemetry`
- `benchmarksBuffer` — same for benchmarks
- `latestTelemetryBuffer` — raw JSON bytes; served directly by `GET /api/v1/providers` (avoids a DB round-trip)

### TON / ADNL integration
- `pkg/clients/ton` wraps `tonutils-go` for lite-client calls (transactions, contract state) — coordinator only
- `pkg/agentServer/workers.go` holds all ADNL/DHT/RLDP logic (moved from `providersMaster`): `PingProviders`, `CheckProofs`, IP resolution helpers, `checkPiece`
- Provider IP discovery: first tries `VerifyStorageADNLProof` + DHT lookup; falls back to scanning overlay DHT nodes for matching IP

### Database schema
Single migration file: `db/init.sql`. All tables live in the `providers` schema (except `system.reason_codes` and `system.params`). Heavy use of `jsonb_array_elements` bulk upserts — pass a Go slice directly as `$1::jsonb` and let Postgres unpack it. Several triggers archive historical rows on update/delete.

### Proof verification flow
1. Coordinator: fetch active contracts from DB, call TON `GetProvidersInfo`, delete rejected contracts
2. Agents: for each provider pubkey, resolve IP via `VerifyStorageADNLProof` + DHT (overlay fallback if needed)
3. Agents: open ADNL connection, ping, then for each bag: fetch `TorrentInfo` (validate hash == `bag_id`), fetch a random piece, verify Merkle proof against `RootHash`; record `ReasonCode`
4. Coordinator: write IPs + reason codes to DB, run `UpdateStatuses` SQL (aggregates last 24 h of reason codes into provider-level `status`)

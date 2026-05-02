# mytonprovider-backend

**[Русская версия](README.ru.md)**

Backend service for mytonprovider.org — a TON Storage providers monitoring service.

## Description

This backend:
- Discovers TON Storage providers by scanning the master contract's transaction history
- Monitors provider availability, performs health checks via ADNL protocol
- Verifies storage proofs (downloads a random bag piece and validates its Merkle proof)
- Handles telemetry data submitted by providers
- Computes provider ratings
- Exposes REST API endpoints for the frontend
- Exposes Prometheus metrics

## Architecture

The system runs as two separate binaries:

| Binary | Role |
|---|---|
| **coordinator** | Single instance. Owns all DB state, cursor management, TON lite-client calls, and orchestration. |
| **agent** | N instances. Stateless ADNL/DHT/RLDP worker. No DB connection. |

Agents register with the coordinator on startup and send a heartbeat every 30 s. The coordinator distributes provider ping and storage proof work across registered agents, aggregates results, and writes them to the database.

Both binaries share `INTERNAL_TOKEN` as a shared secret for their HTTP API (`X-Internal-Token` header). Leave it empty to disable authentication (local dev only).

## Installation & Setup

### Coordinator server (Debian 12)

1. **Forward SSH keys from your local machine**

```bash
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/init_server_connection.sh
USERNAME=root PASSWORD=supersecretpassword HOST=123.45.67.89 bash init_server_connection.sh
```

2. **Log in and download the setup script**

```bash
ssh root@123.45.67.89
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/setup_server.sh
```

3. **Run setup**

```bash
PG_USER=pguser PG_PASSWORD=secret PG_DB=providerdb \
NEWFRONTENDUSER=jdfront \
NEWSUDOUSER=johndoe NEWUSER_PASSWORD=newsecurepassword \
INTERNAL_TOKEN=$(openssl rand -hex 32) \
bash ./setup_server.sh
```

### Agent server (Debian 12)

Run on each additional server that will handle ADNL work:

```bash
wget https://raw.githubusercontent.com/dearjohndoe/mytonprovider-backend/refs/heads/master/scripts/setup_agent.sh
COORDINATOR_URL=http://<coordinator-ip>:9090 \
TON_CONFIG_URL=https://ton-blockchain.github.io/global.config.json \
INTERNAL_TOKEN=<shared-secret> \
NEWSUDOUSER=agentuser \
bash ./setup_agent.sh
```

Then open the ADNL port (default 16168):
```bash
ufw allow 16168/udp
```

## Local Development

### Env files

| File | Purpose |
|---|---|
| `.postgres.env` | Postgres credentials for `docker compose` and `scripts/init_db.sh` |
| `.coordinator.env` | All coordinator env vars |
| `.agent.env` | All agent env vars |

Copy and edit before first run — the defaults work for local dev without changes except setting `INTERNAL_TOKEN` to the same value in both coordinator and agent files.

### Database

```bash
docker compose up -d

# Reset (drops all data)
docker compose down -v && docker compose up -d
```

### Running locally

```bash
# Coordinator
env $(grep -v '^#' .coordinator.env | xargs) go run ./cmd/coordinator

# Agent (in a second terminal)
env $(grep -v '^#' .agent.env | xargs) go run ./cmd/agent

# Second agent on different ports
AGENT_PORT=9092 AGENT_ADNL_PORT=16169 \
env $(grep -v '^#' .agent.env | xargs) go run ./cmd/agent
```

Use `-tags=debug` on the coordinator to enable CORS headers and OPTIONS handling (needed when running without nginx):

```bash
env $(grep -v '^#' .coordinator.env | xargs) go run -tags=debug ./cmd/coordinator
```

### VS Code

Create `.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Coordinator",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/coordinator",
            "buildFlags": "-tags=debug",
            "env": {}
        },
        {
            "name": "Agent",
            "type": "go",
            "request": "launch",
            "mode": "auto",
            "program": "${workspaceFolder}/cmd/agent",
            "env": {}
        }
    ]
}
```

## Project Structure

```
cmd/
├── coordinator/       # Coordinator binary (DB, TON, orchestration)
└── agent/             # Agent binary (ADNL/DHT/RLDP)
pkg/
├── agentClient/       # HTTP client for coordinator→agent calls (wire types, retry logic)
├── agentRegistry/     # In-memory agent registry with heartbeat eviction
├── agentServer/       # Agent HTTP handlers and ADNL/proof-check workers
├── cache/             # Simple TTL cache
├── clients/           # External clients (TON lite-client, ifconfig.co)
├── httpServer/        # Fiber HTTP handlers (public API + internal agent routes)
├── models/            # DB and API types
├── repositories/      # All Postgres queries
├── services/          # Business logic (providers search, telemetry)
└── workers/           # Background worker harness and individual workers
db/                    # init.sql — single migration file
scripts/               # Server setup and utility scripts
```

## API Endpoints

Public (served by coordinator):
- `POST /api/v1/providers/search` — filtered provider list
- `GET  /api/v1/providers/filters` — filter range metadata
- `POST /api/v1/providers` — telemetry ingestion (from provider nodes)
- `GET  /api/v1/providers` — latest telemetry feed (auth required)
- `POST /api/v1/contracts/statuses` — storage contract proof status
- `POST /api/v1/benchmarks` — benchmark ingestion (from provider nodes)
- `GET  /metrics` — Prometheus metrics (auth required)
- `GET  /health`

Internal (coordinator, authenticated with `X-Internal-Token`):
- `POST /internal/v1/agents` — agent registration
- `POST /internal/v1/agents/:id/heartbeat`

Internal (agent, authenticated with `X-Internal-Token`):
- `POST /internal/v1/workers/ping-providers`
- `POST /internal/v1/workers/check-proofs`

## License

Apache-2.0

This project was created by order of a TON Foundation community member.

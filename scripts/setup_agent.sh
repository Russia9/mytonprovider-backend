#!/bin/bash

# Sets up the TON Storage provider agent on a clean Debian 12 server.
# The agent connects to a running coordinator — this script does NOT set up
# PostgreSQL, Nginx, or server hardening. Run on the agent server only.
#
# Usage:
# COORDINATOR_URL=http://<coordinator-ip>:9090 \
# TON_CONFIG_URL=https://ton-blockchain.github.io/global.config.json \
# INTERNAL_TOKEN=<shared-secret> \
# NEWSUDOUSER=<system-user-to-run-agent> \
# bash setup_agent.sh
#
# Optional vars (with defaults):
#   INTERNAL_TOKEN=""        shared secret with coordinator
#   AGENT_HOST=""            host/IP the coordinator should use to reach the agent
#   AGENT_PORT=9091          HTTP port the agent listens on
#   AGENT_ADNL_PORT=16168    UDP port for ADNL P2P traffic
#   NEWSUDOUSER=""           OS user to run the agent (defaults to root if unset)
#   SYSTEM_LOG_LEVEL=0       0=debug 1=info 2=warn 3=error

set -e

GO_VERSION="1.24.5"
GITHUB_REPO="Russia9/mytonprovider-backend"
GITHUB_BRANCH="master"
WORK_DIR="/tmp/provider-agent"
INSTALL_DIR="/opt/provider-agent"
SERVICE_NAME="provider-agent"

AGENT_PORT="${AGENT_PORT:-9091}"
AGENT_ADNL_PORT="${AGENT_ADNL_PORT:-16168}"
INTERNAL_TOKEN="${INTERNAL_TOKEN:-}"
AGENT_HOST="${AGENT_HOST:-0.0.0.0}"
SYSTEM_LOG_LEVEL="${SYSTEM_LOG_LEVEL:-0}"

RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status()  { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_error()   { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Guards ────────────────────────────────────────────────────────────────────

if [[ $EUID -ne 0 ]]; then
    print_error "This script must be run as root"
    exit 1
fi

if [[ -z "$COORDINATOR_URL" || -z "$TON_CONFIG_URL" ]]; then
    print_error "Missing required environment variables: COORDINATOR_URL, TON_CONFIG_URL"
    echo ""
    echo "Usage example:"
    echo "COORDINATOR_URL=http://1.2.3.4:9090 \\"
    echo "TON_CONFIG_URL=https://ton-blockchain.github.io/global.config.json \\"
    echo "INTERNAL_TOKEN=secret \\"
    echo "AGENT_HOST=5.6.7.8 \\"
    echo "NEWSUDOUSER=agentuser \\"
    echo "bash setup_agent.sh"
    exit 1
fi

# ── Dependencies ──────────────────────────────────────────────────────────────

print_status "Installing system dependencies..."
apt-get update -qq
apt-get install -y wget curl git

# ── Go ────────────────────────────────────────────────────────────────────────

if ! command -v go &>/dev/null && [[ ! -f /usr/local/go/bin/go ]]; then
    print_status "Installing Go $GO_VERSION..."
    wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
    tar -C /usr/local -xzf "go${GO_VERSION}.linux-amd64.tar.gz"
    echo 'export PATH=$PATH:/usr/local/go/bin' >> /etc/profile
    rm "go${GO_VERSION}.linux-amd64.tar.gz"
    print_success "Go installed."
else
    print_status "Go already present, skipping installation."
fi

export PATH=$PATH:/usr/local/go/bin

# ── Clone / update repo ───────────────────────────────────────────────────────

print_status "Fetching source code..."
mkdir -p "$WORK_DIR"
cd "$WORK_DIR"

if [[ -d "mytonprovider-backend/.git" ]]; then
    print_status "Repository exists, pulling latest changes..."
    cd mytonprovider-backend
    git pull origin "$GITHUB_BRANCH"
else
    git clone "https://github.com/${GITHUB_REPO}"
    cd mytonprovider-backend
fi

# ── Build ─────────────────────────────────────────────────────────────────────

print_status "Building agent binary..."
go build -buildvcs=false -o agent ./cmd/agent
print_success "Agent built."

# ── Install ───────────────────────────────────────────────────────────────────

print_status "Installing to $INSTALL_DIR..."
mkdir -p "$INSTALL_DIR"
mv agent "$INSTALL_DIR/"

# ── Write agent.env ───────────────────────────────────────────────────────────

print_status "Writing $INSTALL_DIR/agent.env..."
cat <<EOL > "$INSTALL_DIR/agent.env"
COORDINATOR_URL=${COORDINATOR_URL}
INTERNAL_TOKEN=${INTERNAL_TOKEN}
TON_CONFIG_URL=${TON_CONFIG_URL}
AGENT_HOST=${AGENT_HOST}
AGENT_PORT=${AGENT_PORT}
AGENT_ADNL_PORT=${AGENT_ADNL_PORT}
SYSTEM_LOG_LEVEL=${SYSTEM_LOG_LEVEL}
EOL
chmod 600 "$INSTALL_DIR/agent.env"

# ── Systemd service ───────────────────────────────────────────────────────────

print_status "Writing systemd service..."

USER_LINE=""
if [[ -n "$NEWSUDOUSER" ]]; then
    USER_LINE="User=$NEWSUDOUSER"
fi

cat <<EOL > "/etc/systemd/system/${SERVICE_NAME}.service"
[Unit]
Description=TON Storage Provider Agent
After=network.target

[Service]
${USER_LINE}
EnvironmentFile=${INSTALL_DIR}/agent.env
ExecStart=${INSTALL_DIR}/agent
Restart=on-failure
RestartSec=5s
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOL

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"

sleep 3

if systemctl is-active --quiet "$SERVICE_NAME"; then
    print_success "Agent service started."
else
    print_error "Agent service failed to start. Check: journalctl -u $SERVICE_NAME -n 50"
    exit 1
fi

# ── Summary ───────────────────────────────────────────────────────────────────

SERVER_IP=$(hostname -I | awk '{print $1}')

print_success "Agent setup completed!"
echo ""
echo "Install directory : $INSTALL_DIR"
echo "Config file       : $INSTALL_DIR/agent.env"
echo "Systemd service   : $SERVICE_NAME"
echo "Agent URL         : http://${SERVER_IP}:${AGENT_PORT}"
echo ""
echo "Next steps:"
echo "  1. Open the ADNL port on this server:"
echo "     ufw allow ${AGENT_ADNL_PORT}/udp"
echo "  2. Register the agent with the coordinator by setting COORDINATOR_URL"
echo "     in agent.env and restarting if not already done:"
echo "     systemctl restart $SERVICE_NAME"
echo "  3. Check logs: journalctl -u $SERVICE_NAME -f"
echo ""
echo "Cleanup: rm -rf $WORK_DIR"

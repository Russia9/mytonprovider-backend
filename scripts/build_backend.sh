#!/bin/bash

# Builds the coordinator binary and generates coordinator.env with necessary configurations.

set -e

cd "$WORK_DIR/mytonprovider-backend/"

export PATH=$PATH:/usr/local/go/bin

go build -buildvcs=false -o coordinator ./cmd/coordinator

cat <<EOL > coordinator.env
SYSTEM_PORT=9090
MASTER_ADDRESS=UQB3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d0x0
TON_CONFIG_URL=https://ton-blockchain.github.io/global.config.json
SYSTEM_ACCESS_TOKENS=
BATCH_SIZE=100
DB_HOST=${HOST:-localhost}
DB_PORT=5432
DB_USER=${PG_USER}
DB_PASSWORD=${PG_PASSWORD}
DB_NAME=${PG_DB}
SYSTEM_LOG_LEVEL=0
INTERNAL_TOKEN=${INTERNAL_TOKEN}
EOL

mkdir -p /opt/provider
mv coordinator /opt/provider/
mv coordinator.env /opt/provider/

echo "Coordinator built and coordinator.env created successfully."

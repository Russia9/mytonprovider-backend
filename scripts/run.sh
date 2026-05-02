#!/bin/bash

set -e

cd /opt/provider

env $(grep -v '^#' coordinator.env | xargs) ./coordinator >> /var/log/mytonprovider.app/mytonprovider.app.log 2>&1 &

sleep 5

if pgrep -f "./coordinator" > /dev/null; then
    echo "✅ Coordinator started successfully."
else
    echo "❌ Failed to start coordinator."
    exit 1
fi

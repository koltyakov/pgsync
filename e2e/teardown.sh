#!/bin/bash
# Teardown E2E test environment
# Usage: ./teardown.sh [--volumes]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Determine docker compose command
if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    COMPOSE_CMD="docker compose"
fi

echo "Stopping and removing containers..."

if [[ "$1" == "--volumes" ]]; then
    echo "Also removing volumes..."
    $COMPOSE_CMD down -v
else
    $COMPOSE_CMD down
fi

echo "Done!"

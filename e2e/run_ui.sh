#!/bin/bash
# Run pgsync Web UI against the E2E databases
# Usage: ./run_ui.sh [port]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

PORT="${1:-8080}"
SOURCE_DB="postgres://postgres:postgres@localhost:5433/crm_source?sslmode=disable"
TARGET_DB="postgres://postgres:postgres@localhost:5434/crm_target?sslmode=disable"

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    echo "Error: port must be an integer between 1 and 65535"
    exit 1
fi

echo "Starting pgsync Web UI..."
echo "Source: $SOURCE_DB"
echo "Target: $TARGET_DB"
echo "URL: http://localhost:$PORT"
echo ""

if [ ! -d "web/dist" ]; then
    echo "Web UI assets not built. Running 'make web' first..."
    make web
    echo ""
fi

go run main.go \
    -server \
    -port "$PORT" \
    -source "$SOURCE_DB" \
    -target "$TARGET_DB"

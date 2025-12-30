#!/bin/bash
# Run pgsync E2E test
# Usage: ./run_sync.sh [options]
# Options are passed directly to pgsync

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

SOURCE_DB="postgres://postgres:postgres@localhost:5433/crm_source?sslmode=disable"
TARGET_DB="postgres://postgres:postgres@localhost:5434/crm_target?sslmode=disable"

echo "Running pgsync..."
echo ""

go run main.go \
    --source "$SOURCE_DB" \
    --target "$TARGET_DB" \
    "$@"

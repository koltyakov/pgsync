#!/bin/bash
# E2E Test Environment Setup Script
# Usage: ./setup.sh [small|medium|large|xlarge]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PROFILE="${1:-small}"

echo "=========================================="
echo "pgsync E2E Test Environment Setup"
echo "=========================================="
echo "Profile: $PROFILE"
echo ""

# Check prerequisites
command -v docker >/dev/null 2>&1 || { echo "Error: docker is required"; exit 1; }
command -v docker-compose >/dev/null 2>&1 || command -v docker compose >/dev/null 2>&1 || { echo "Error: docker-compose is required"; exit 1; }

# Determine docker compose command
if command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
else
    COMPOSE_CMD="docker compose"
fi

# Start databases
echo "Starting PostgreSQL containers..."
$COMPOSE_CMD up -d

# Wait for databases to be ready
echo "Waiting for databases to be ready..."
for i in {1..30}; do
    if docker exec pgsync_source pg_isready -U postgres -d crm_source >/dev/null 2>&1 && \
       docker exec pgsync_target pg_isready -U postgres -d crm_target >/dev/null 2>&1; then
        echo "Databases are ready!"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Check if databases are actually ready
if ! docker exec pgsync_source pg_isready -U postgres -d crm_source >/dev/null 2>&1; then
    echo "Error: Source database not ready"
    exit 1
fi

if ! docker exec pgsync_target pg_isready -U postgres -d crm_target >/dev/null 2>&1; then
    echo "Error: Target database not ready"
    exit 1
fi

# Setup Python environment
echo ""
echo "Setting up Python environment..."
cd scripts

if [ ! -d "venv" ]; then
    python3 -m venv venv
fi

source venv/bin/activate
pip install -q -r requirements.txt

# Seed data
echo ""
echo "Seeding source database with '$PROFILE' profile..."
python seed_data.py --profile "$PROFILE"

deactivate
cd ..

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "Connection strings:"
echo "  Source: postgres://postgres:postgres@localhost:5433/crm_source"
echo "  Target: postgres://postgres:postgres@localhost:5434/crm_target"
echo ""
echo "Run pgsync:"
echo "  go run main.go \\"
echo "    --source 'postgres://postgres:postgres@localhost:5433/crm_source?sslmode=disable' \\"
echo "    --target 'postgres://postgres:postgres@localhost:5434/crm_target?sslmode=disable' \\"
echo "    --verbose"
echo ""
echo "Simulate changes:"
echo "  cd scripts && source venv/bin/activate"
echo "  python mutate_data.py --rate medium --duration 60"
echo ""

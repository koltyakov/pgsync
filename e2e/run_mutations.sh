#!/bin/bash
# Run mutation simulator
# Usage: ./run_mutations.sh [rate] [duration]
# rate: slow, medium, fast, burst (default: medium)
# duration: seconds (default: runs until Ctrl+C)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/scripts"

RATE="${1:-medium}"
DURATION="${2:-}"

source venv/bin/activate

echo "Starting mutation simulator (rate: $RATE)..."
echo "Press Ctrl+C to stop"
echo ""

# Pass arguments directly to avoid shell injection
if [ -n "$DURATION" ]; then
    python mutate_data.py --rate "$RATE" --duration "$DURATION"
else
    python mutate_data.py --rate "$RATE"
fi

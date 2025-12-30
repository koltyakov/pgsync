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

ARGS="--rate $RATE"
if [ -n "$DURATION" ]; then
    ARGS="$ARGS --duration $DURATION"
fi

echo "Starting mutation simulator (rate: $RATE)..."
echo "Press Ctrl+C to stop"
echo ""

python mutate_data.py $ARGS

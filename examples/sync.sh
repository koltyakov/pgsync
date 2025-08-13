#!/bin/bash

echo "🚀 Starting tables sync..."
echo ""

# Perform the sync
./pgsync -config ./examples/config.json

echo ""
echo "✅ Sync completed successfully!"

#!/bin/bash
# Clean target database only (truncate all tables)
# Usage: ./clean_target.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TARGET_HOST="${TARGET_HOST:-localhost}"
TARGET_PORT="${TARGET_PORT:-5434}"
TARGET_DB="${TARGET_DB:-crm_target}"
TARGET_USER="${TARGET_USER:-postgres}"
TARGET_PASSWORD="${TARGET_PASSWORD:-postgres}"

echo "Cleaning target database..."

# Check if target is accessible
if ! docker exec pgsync_target pg_isready -U "$TARGET_USER" -d "$TARGET_DB" >/dev/null 2>&1; then
    echo "Error: Target database is not ready"
    exit 1
fi

# Truncate all tables in the target database (in proper order due to FK constraints)
docker exec pgsync_target psql -U "$TARGET_USER" -d "$TARGET_DB" <<'EOF'
-- Disable triggers to allow truncating tables with foreign keys
SET session_replication_role = replica;

-- Truncate all tables in reverse dependency order
TRUNCATE TABLE 
    audit_logs,
    email_logs,
    ticket_comments,
    activities,
    line_items,
    tickets,
    deals,
    opportunities,
    leads,
    price_book_entries,
    contacts,
    team_members,
    organizations,
    products,
    price_books,
    product_categories,
    email_templates,
    pipeline_stages,
    teams,
    users
CASCADE;

-- Re-enable triggers
SET session_replication_role = DEFAULT;

-- Verify all tables are empty
SELECT 
    schemaname || '.' || relname AS table_name,
    n_live_tup AS row_count
FROM pg_stat_user_tables
WHERE n_live_tup > 0
ORDER BY relname;
EOF

echo ""
echo "Target database cleaned!"
echo "You can now run ./run_sync.sh to sync data from source."

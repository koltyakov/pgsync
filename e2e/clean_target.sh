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
# Using CASCADE to handle foreign key dependencies automatically
docker exec pgsync_target psql -U "$TARGET_USER" -d "$TARGET_DB" -c "
TRUNCATE TABLE 
    users,
    teams,
    pipeline_stages,
    email_templates,
    product_categories,
    price_books,
    products,
    organizations,
    team_members,
    contacts,
    price_book_entries,
    leads,
    opportunities,
    deals,
    tickets,
    line_items,
    activities,
    ticket_comments,
    email_logs,
    audit_logs
CASCADE;
"

# Verify all tables are empty
echo ""
echo "Verifying tables are empty..."
docker exec pgsync_target psql -U "$TARGET_USER" -d "$TARGET_DB" -c "
SELECT tablename, 
       (SELECT COUNT(*) FROM public.\"\$1\" WHERE tablename = t.tablename) 
FROM pg_tables t WHERE schemaname = 'public';
" 2>/dev/null || true

# Show actual counts
docker exec pgsync_target psql -U "$TARGET_USER" -d "$TARGET_DB" <<'EOF'
SELECT 'users' AS tbl, COUNT(*) FROM users
UNION ALL SELECT 'teams', COUNT(*) FROM teams
UNION ALL SELECT 'contacts', COUNT(*) FROM contacts
UNION ALL SELECT 'organizations', COUNT(*) FROM organizations
UNION ALL SELECT 'deals', COUNT(*) FROM deals
UNION ALL SELECT 'products', COUNT(*) FROM products
ORDER BY tbl;
EOF

echo ""
echo "Target database cleaned!"
echo "You can now run ./run_sync.sh to sync data from source."

#!/bin/bash
# Compare row counts between source and target databases
# Usage: ./compare_dbs.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SOURCE_HOST="localhost"
SOURCE_PORT="5433"
TARGET_HOST="localhost"
TARGET_PORT="5434"
DB_USER="postgres"
DB_PASS="postgres"
DB_NAME_SOURCE="crm_source"
DB_NAME_TARGET="crm_target"

TABLES=(
    "organizations"
    "contacts"
    "leads"
    "pipeline_stages"
    "opportunities"
    "deals"
    "product_categories"
    "products"
    "price_books"
    "price_book_entries"
    "line_items"
    "tickets"
    "ticket_comments"
    "activities"
    "email_templates"
    "email_logs"
    "users"
    "teams"
    "team_members"
    "audit_logs"
)

echo "=========================================="
echo "Database Comparison: Source vs Target"
echo "=========================================="
printf "%-25s %12s %12s %10s\n" "Table" "Source" "Target" "Match"
echo "----------------------------------------------------------"

total_source=0
total_target=0
all_match=true

for table in "${TABLES[@]}"; do
    source_count=$(PGPASSWORD=$DB_PASS psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d $DB_NAME_SOURCE -t -c "SELECT COUNT(*) FROM $table" 2>/dev/null | tr -d ' ' || echo "0")
    target_count=$(PGPASSWORD=$DB_PASS psql -h $TARGET_HOST -p $TARGET_PORT -U $DB_USER -d $DB_NAME_TARGET -t -c "SELECT COUNT(*) FROM $table" 2>/dev/null | tr -d ' ' || echo "0")
    
    # Handle empty results
    source_count=${source_count:-0}
    target_count=${target_count:-0}
    
    total_source=$((total_source + source_count))
    total_target=$((total_target + target_count))
    
    if [ "$source_count" == "$target_count" ]; then
        match="✓"
    else
        match="✗"
        all_match=false
    fi
    
    printf "%-25s %12s %12s %10s\n" "$table" "$source_count" "$target_count" "$match"
done

echo "----------------------------------------------------------"
printf "%-25s %12s %12s\n" "TOTAL" "$total_source" "$total_target"
echo "=========================================="

if $all_match; then
    echo "✓ All tables match!"
    exit 0
else
    echo "✗ Some tables have mismatched counts"
    exit 1
fi

# pgsync E2E Testing Environment

This directory contains a complete end-to-end testing environment for pgsync, featuring:

- **Docker Compose** with two PostgreSQL 16 instances (source and target)
- **CRM Schema** with 20 tables representing a realistic business application
- **Data Seeding** scripts supporting hundreds to millions of rows
- **Mutation Simulator** for generating continuous inserts, updates, and deletes

## Quick Start

```bash
# 1. Setup environment (starts databases and seeds data)
./setup.sh small    # Options: small, medium, large, xlarge

# 2. Run initial sync
./run_sync.sh # --verbose

# Optional: Start the web UI for interactive sync
./run_ui.sh              # Opens on http://localhost:8080

# 3. Verify sync (compare row counts)
./compare_dbs.sh

# 4. Start mutation simulator (in another terminal)
./run_mutations.sh medium 60   # rate: slow|medium|fast|burst, duration in seconds

# 5. Run sync again to sync changes
./run_sync.sh # --verbose

# 6. Cleanup when done
./teardown.sh --volumes

# Optional: Clean only target DB (keeps source data)
./clean_target.sh
```

`./setup.sh` resets both the source and target databases on each run before reseeding, so it is safe to rerun against existing Docker volumes.

## Directory Structure

```
e2e/
├── docker-compose.yml     # PostgreSQL containers
├── setup.sh              # Setup script
├── teardown.sh           # Cleanup script  
├── clean_target.sh       # Clean target DB only
├── run_sync.sh           # Run pgsync
├── run_ui.sh             # Run pgsync Web UI
├── run_mutations.sh      # Run mutation simulator
├── compare_dbs.sh        # Compare source vs target
├── init/
│   └── 001_schema.sql    # CRM database schema
└── scripts/
    ├── requirements.txt  # Python dependencies
    ├── seed_data.py      # Data seeding script
    └── mutate_data.py    # Mutation simulator
```

## Database Schema

The CRM schema includes the following tables:

### Organization & Contacts
- `organizations` - Companies/accounts
- `contacts` - People at organizations

### Sales Pipeline
- `leads` - Unqualified prospects
- `pipeline_stages` - Sales stages
- `opportunities` - Qualified deals in progress
- `deals` - Won opportunities/contracts

### Products & Pricing
- `product_categories` - Product taxonomy
- `products` - Product catalog
- `price_books` - Pricing tiers
- `price_book_entries` - Product pricing
- `line_items` - Deal line items

### Customer Support
- `tickets` - Support tickets
- `ticket_comments` - Ticket replies

### Activities & Communications
- `activities` - Calls, meetings, tasks
- `email_templates` - Email templates
- `email_logs` - Sent emails

### User Management
- `users` - CRM users
- `teams` - User teams
- `team_members` - Team membership

### Audit
- `audit_logs` - Change tracking

## Data Profiles

| Profile | Organizations | Contacts | Activities | Total Rows |
|---------|--------------|----------|------------|------------|
| small   | 100          | 500      | 1,000      | ~3K        |
| medium  | 5,000        | 25,000   | 50,000     | ~130K      |
| large   | 50,000       | 250,000  | 500,000    | ~1.3M      |
| xlarge  | 200,000      | 1,000,000| 2,000,000  | ~5M        |

## Mutation Profiles

| Profile | Inserts/s | Updates/s | Deletes/s |
|---------|-----------|-----------|-----------|
| slow    | 1         | 2         | 0.1       |
| medium  | 5         | 10        | 0.5       |
| fast    | 20        | 50        | 2         |
| burst   | 100       | 200       | 10        |

## Connection Strings

```bash
# Source Database
postgres://postgres:postgres@localhost:5433/crm_source?sslmode=disable

# Target Database  
postgres://postgres:postgres@localhost:5434/crm_target?sslmode=disable
```

## Testing Scenarios

### Basic Sync Test

```bash
./setup.sh small
./run_sync.sh --verbose
./compare_dbs.sh
```

### Web UI Test

```bash
./setup.sh small
./run_ui.sh
# Open http://localhost:8080
```

### Incremental Sync Test

```bash
./setup.sh small
./run_sync.sh --verbose               # Initial sync
./run_mutations.sh medium 30          # Generate changes for 30s
./run_sync.sh --verbose               # Incremental sync
./compare_dbs.sh                      # Verify
```

### Reconcile Mode Test

```bash
./setup.sh small
./run_sync.sh --verbose               # Initial sync
./run_mutations.sh medium 30          # Generate changes
./run_sync.sh --reconcile --verbose   # Full reconciliation by PK
./compare_dbs.sh                      # Verify all rows match
```

### Performance Test

```bash
./setup.sh large                      # ~1.3M rows
time ./run_sync.sh --parallel 8       # Measure sync time
./compare_dbs.sh
```

### Stress Test

```bash
./setup.sh medium
./run_sync.sh --verbose &             # Run sync in background

# In another terminal:
./run_mutations.sh burst 120          # Heavy mutations for 2 min

# Wait for sync to complete, then:
./compare_dbs.sh
```

### Dry Run Test

```bash
./setup.sh small
./run_sync.sh --dry-run --verbose     # Preview without changes
./compare_dbs.sh                      # Should show differences
```

### Integrity Check Test

```bash
./setup.sh medium
./run_sync.sh --integrity --verbose
cat integrity.csv                     # Check report
```

## Troubleshooting

### Containers not starting

```bash
docker logs pgsync_source
docker logs pgsync_target
```

### Reset environment

```bash
./teardown.sh --volumes
./setup.sh small
```

### Check database connectivity

```bash
docker exec pgsync_source pg_isready -U postgres
docker exec pgsync_target pg_isready -U postgres
```

### Manual database access

```bash
# Source database
docker exec -it pgsync_source psql -U postgres -d crm_source

# Target database
docker exec -it pgsync_target psql -U postgres -d crm_target
```

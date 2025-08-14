# PgSync - PostgreSQL Database Synchronization Tool

A powerful CLI application for synchronizing data between two PostgreSQL databases with intelligent incremental updates and parallel processing.

## Features

- **Incremental Sync**: Uses timestamp columns to sync only changed data
- **Parallel Processing**: Multiple worker goroutines for efficient table synchronization
- **Deleted Row Tracking**: Detects and removes deleted rows from target database
- **Upsert Operations**: Uses PostgreSQL's `ON CONFLICT` for efficient data updates
- **State Management**: SQLite database tracks sync progress and history
- **Batch Processing**: Configurable batch sizes to minimize WAL impact
- **Table Filtering**: Include/exclude specific tables from synchronization
- **Dry Run Mode**: Test synchronization without making changes
- **Timestamp Consistency**: Ensures all rows with same timestamp are synchronized together

## Installation

```bash
git clone https://github.com/koltyakov/pgsync.git
cd pgsync
go mod download
go build -o pgsync
```

## Usage

### Basic Usage

```bash
# Sync all tables from source to target
./pgsync -source "postgres://user:pass@source:5432/db" -target "postgres://user:pass@target:5432/db"

# Sync specific schema
./pgsync -source "..." -target "..." -schema "public"

# Include specific tables only
./pgsync -source "..." -target "..." -include "users,orders,products"

# Exclude specific tables
./pgsync -source "..." -target "..." -exclude "logs,temp_data"

# Custom timestamp column
./pgsync -source "..." -target "..." -timestamp "modified_at"

# Parallel processing with 8 workers
./pgsync -source "..." -target "..." -parallel 8

# Dry run to see what would be synchronized
./pgsync -source "..." -target "..." -dry-run

# Verbose logging
./pgsync -source "..." -target "..." -verbose
```

### Configuration File

Create a `config.json` file:

```json
{
  "sourceDb": "postgres://user:pass@source:5432/sourcedb",
  "targetDb": "postgres://user:pass@target:5432/targetdb",
  "schema": "public",
  "timestampColumn": "updated_at",
  "parallel": 4,
  "batchSize": 1000,
  "includeTables": ["users", "orders"],
  "excludeTables": ["logs"],
  "verbose": true
}
```

Use with: `./pgsync -config config.json`

## Command Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-source` | Source database connection string | Required |
| `-target` | Target database connection string | Required |
| `-schema` | Schema to sync | `public` |
| `-include` | Comma-separated list of tables to include | All tables |
| `-exclude` | Comma-separated list of tables to exclude | None |
| `-timestamp` | Timestamp column name for incremental sync | `updated_at` |
| `-parallel` | Number of parallel sync sessions | `4` |
| `-batch-size` | Batch size for data processing | `1000` |
| `-dry-run` | Perform a dry run without actual changes | `false` |
| `-verbose` | Enable verbose logging | `false` |
| `-config` | Path to configuration file | None |
| `-state-db` | SQLite database path for state management | `./pgsync.db` |

## How It Works

### 1. Table Discovery
- Discovers all tables in the specified schema
- Applies include/exclude filters
- Retrieves table metadata (columns, primary keys, row counts)

### 2. Incremental Synchronization
- Uses timestamp columns to identify changed rows
- Maintains last sync timestamp for each table
- Processes data in batches to minimize memory usage

### 3. Deleted Row Detection
- Compares primary keys between source and target
- Removes rows from target that no longer exist in source

### 4. Timestamp Consistency
- Ensures all rows with the same timestamp are processed together
- Handles bulk updates where multiple rows share the same timestamp

### 5. Parallel Processing
- Distributes tables across multiple worker goroutines
- Load balances based on estimated table complexity

### 6. State Management
- SQLite database tracks sync progress
- Resumes from last successful point on failures
- Maintains sync history and logs

## Database Connection Strings

PostgreSQL connection strings support various formats:

```bash
# Standard format
postgres://username:password@hostname:port/database

# With SSL mode
postgres://user:pass@host:5432/db?sslmode=require

# Local socket
postgres://user:pass@/database?host=/var/run/postgresql

# With additional parameters
postgres://user:pass@host:5432/db?sslmode=disable&connect_timeout=10
```

## Requirements

- PostgreSQL 10+ (both source and target)
- Go 1.21+ (for building)
- Tables must have primary keys for proper upsert operations
- Timestamp columns should be of type `timestamp` or `timestamptz`

## Performance Considerations

- **Batch Size**: Larger batches reduce overhead but increase memory usage
- **Parallel Workers**: More workers can speed up sync but may overwhelm databases
- **Indexes**: Ensure timestamp columns and primary keys are properly indexed
- **WAL Impact**: Batch processing minimizes Write-Ahead Log impact

## Error Handling

- Graceful handling of connection failures
- Continues processing other tables if one fails
- Detailed error logging and reporting
- State preservation allows resume from last successful point

## Monitoring

The SQLite state database contains:
- Sync timestamps for each table
- Detailed sync logs with timing and row counts
- Error history for troubleshooting

Query the state database:
```sql
-- View last sync times
SELECT * FROM sync_state;

-- View sync history
SELECT * FROM sync_log ORDER BY start_time DESC;
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

MIT License - see LICENSE file for details.

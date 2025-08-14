# PgSync - PostgreSQL Database Synchronization Tool

A powerful CLI application for synchronizing data between two PostgreSQL databases with intelligent incremental updates and parallel processing.

## Features

- **Incremental Sync**: Uses timestamp columns to sync only changed data
- **Parallel Processing**: Multiple worker goroutines for efficient table synchronization
- **Deleted Row Tracking**: Detects and removes deleted rows from target database
- **Upsert Operations**: Uses PostgreSQL's `ON CONFLICT` for efficient data updates
- **State Management**: Uses target database timestamps for sync progress tracking
- **Batch Processing**: Configurable batch sizes to minimize WAL impact
- **Table Filtering**: Include/exclude specific tables from synchronization with wildcard support
- **Intelligent Deletion**: Tracks and removes obsolete records
- **Parallel Processing**: Configurable worker pools for optimal performance
- **Progress Tracking**: Real-time sync status and logging
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

# Include tables with wildcards
./pgsync -source "..." -target "..." -include "user_*,order_*"

# Exclude specific tables
./pgsync -source "..." -target "..." -exclude "logs,temp_data"

# Exclude tables with wildcards  
./pgsync -source "..." -target "..." -exclude "temp_*,*_log,audit_*"

# Custom timestamp column
./pgsync -source "..." -target "..." -timestamp "modified_at"

# Parallel processing with 8 workers
./pgsync -source "..." -target "..." -parallel 8

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
| `-include` | Comma-separated list of tables to include (supports wildcards) | All tables |
| `-exclude` | Comma-separated list of tables to exclude (supports wildcards) | None |
| `-timestamp` | Timestamp column name for incremental sync | `updated_at` |
| `-parallel` | Number of parallel sync sessions | `4` |
| `-batch-size` | Batch size for data processing | `1000` |
| `-verbose` | Enable verbose logging | `false` |
| `-config` | Path to configuration file | None |

## Wildcard Patterns

The `-include` and `-exclude` options support standard shell wildcards:

- `*` matches any sequence of characters
- `?` matches any single character  
- `[abc]` matches any character in the set
- `[a-z]` matches any character in the range

**Examples:**
- `user_*` matches `user_profiles`, `user_settings`, etc.
- `*_log` matches `access_log`, `error_log`, etc.
- `temp_???` matches `temp_001`, `temp_abc`, etc.
- `audit_[0-9]*` matches `audit_2023`, `audit_1_data`, etc.

**Note:** Exact table names are always matched first for backward compatibility.

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
- Target database timestamps track sync progress
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

The sync process provides comprehensive logging:
- Sync timestamps for each table tracked via target database
- Detailed sync logs with timing and row counts using ISO timestamps
- Error history and verbose progress reporting

Monitor sync progress through log output:
```
[2024-12-13T15:30:45Z] users - Starting incremental sync from target DB timestamp: 2024-12-13T15:25:30Z
[2024-12-13T15:30:46Z] users - Processed batch, current timestamp: 2024-12-13T15:30:45Z
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

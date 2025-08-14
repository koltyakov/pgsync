## pgsync — PostgreSQL data sync utility

Lightweight, reliable data synchronization for PostgreSQL. pgsync focuses on data-only sync between two Postgres databases using timestamp-based incremental updates, safe upserts, and optional table filtering.

Why this tool
- Simple: data-only sync, no schema changes.
- Predictable: incremental sync based on timestamps.
- Fast: batch processing and configurable parallelism.

Quick start

```bash
git clone https://github.com/koltyakov/pgsync.git
cd pgsync
go build -o pgsync
./pgsync -source "postgres://user:pass@source:5432/db" -target "postgres://user:pass@target:5432/db"
```

Config (JSON)

Create `config.json` to store flags you use often:

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

Command-line examples

- Sync everything in `public`:

```bash
./pgsync -source "..." -target "..."
```

- Only sync tables that start with `user_` and `order_`:

```bash
./pgsync -source "..." -target "..." -include "user_*,order_*"
```

- Exclude temp and log tables:

```bash
./pgsync -source "..." -target "..." -exclude "temp_*,*_log"
```

Flags (short reference)

| Flag | Description | Default |
|------|-------------|---------|
| `-source` | Source DB connection string | required |
| `-target` | Target DB connection string | required |
| `-schema` | Schema to sync | `public` |
| `-include` | Comma list or wildcards of tables to include | all |
| `-exclude` | Comma list or wildcards of tables to exclude | none |
| `-timestamp` | Timestamp column used for incremental sync | `updated_at` |
| `-parallel` | Number of concurrent workers | `4` |
| `-batch-size` | Rows per batch | `1000` |
| `-verbose` | Verbose logging | `false` |
| `-config` | Path to JSON config file | none |

Wildcard patterns

Supports standard shell-style patterns (matched with Go `filepath.Match`):

- `*` — any sequence of characters
- `?` — any single character
- `[set]` / `[range]` — character sets and ranges

Examples: `user_*`, `*_log`, `temp_???`, `audit_[0-9]*`.

Notes and constraints

- pgsync is data-only: it will not create or alter target schemas. Ensure target tables and primary keys exist beforehand.
- Requires proper indexes on timestamp and primary key columns for best performance.
- Use `-parallel` and `-batch-size` to tune throughput vs load on source/target.

How it works (brief)

- Discovers tables in the schema and applies include/exclude filters.
- Uses the target's maximum timestamp (or zero if empty) to pick the starting point.
- Processes rows in timestamp-ordered batches and performs `INSERT ... ON CONFLICT DO UPDATE` upserts.
- Optionally detects and deletes rows present in target but not in source when the target is caught up.

Contributing

- Fork, branch, add tests, submit a pull request.

License

MIT — see `LICENSE`.

# pgsync - PostgreSQL data sync utility

Lightweight, reliable data synchronization for PostgreSQL. pgsync focuses on data-only sync between two Postgres databases using timestamp-based incremental updates, safe upserts, and optional table filtering.

## Why this tool

- Simple: data-only sync, no schema changes.
- Predictable: incremental sync based on timestamps.
- Fast: batch processing and configurable parallelism.

## Quick start

```bash
git clone https://github.com/koltyakov/pgsync.git
cd pgsync
go build -o pgsync
./pgsync \
  -source "postgres://user:pass@source:5432/db" \
  -target "postgres://user:pass@target:5432/db"
```

## Config (JSON)

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
  "verbose": true,
  "integrity": false,
  "reconcile": false,
  "dryRun": false
}
```

## Command-line examples

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

## Flags (short reference)

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
| `-integrity` | Run post-sync integrity checks and write `integrity.csv` | `false` |
| `-reconcile` | Full reconciliation mode (compare all rows by PK) | `false` |
| `-dry-run` | Preview sync operations without making changes | `false` |
| `-config` | Path to JSON config file | none |

### Wildcard patterns

Supports standard shell-style patterns (matched with Go `filepath.Match`):

- `*` — any sequence of characters
- `?` — any single character
- `[set]` / `[range]` — character sets and ranges

Examples: `user_*`, `*_log`, `temp_???`, `audit_[0-9]*`.

### Notes and constraints

- pgsync is data-only: it will not create or alter target schemas. Ensure target tables and primary keys exist beforehand.
- Requires proper indexes on timestamp and primary key columns for best performance.
- Use `-parallel` and `-batch-size` to tune throughput vs load on source/target.

## Sync modes

### Incremental sync (default)

The default mode uses timestamp-based incremental sync:

- Uses the target's maximum timestamp to pick the starting point
- Processes rows in timestamp-ordered batches
- Performs `INSERT ... ON CONFLICT DO UPDATE` upserts
- Optionally detects and deletes rows present in target but not in source

This is the most efficient mode for regular synchronization when all changes update the timestamp column.

### Reconcile mode

Use `-reconcile` for full primary key comparison:

```bash
./pgsync -source "..." -target "..." -reconcile
```

Reconcile mode:

- Compares all primary keys between source and target
- Finds rows missing in target and inserts them
- Finds rows in target that don't exist in source and deletes them
- Ignores timestamps entirely — useful when timestamp-based sync misses changes

Use reconcile mode when:

- You need to ensure full consistency between databases
- Timestamp-based sync can't detect all changes (e.g., direct SQL updates)
- You want periodic full reconciliation as a safety net

**Note:** Reconcile mode loads all primary keys into memory. For very large tables (100M+ rows), consider using table filters to reconcile in batches.

## How it works (brief)

- Discovers tables in the schema and applies include/exclude filters.
- Sorts tables by foreign key dependencies to ensure referential integrity.
- In incremental mode: uses timestamps to find changed rows.
- In reconcile mode: compares all PKs to find missing/extra rows.
- Processes rows in batches and performs `INSERT ... ON CONFLICT DO UPDATE` upserts.

## Integrity checks (optional)

Enable with the `-integrity` flag or `"integrity": true` in JSON config. After a sync finishes, pgsync writes `integrity.csv` with per-table statistics and match indicators:

Columns:

- Table Name
- Source Rows, Target Rows, Rows match
- Source min(id), Target min(id), min(id) match
- Source max(id), Target max(id), max(id) match
- Source min(ts), Target min(ts), min(ts) match
- Source max(ts), Target max(ts), max(ts) match

Details:

- Rows are exact `COUNT(*)` on source and target.
- ID min/max uses a best-effort ID column: single-column primary key if available; otherwise a column named `id` (case-insensitive). If none, ID fields are blank.
- Timestamp min/max are included only if the configured `-timestamp` column exists on the table.
- Values are compared and a "Yes"/"No" is written in the adjacent "match" column.

## Contributing

- Fork, branch, add tests, submit a pull request.

## License

MIT — see [LICENSE](./LICENSE).

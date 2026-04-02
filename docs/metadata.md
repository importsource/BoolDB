# Metadata & Database Management

BoolDB provides a set of commands for inspecting and managing the database's internal structure — tables, columns, indexes, and query plans. This guide covers all available metadata commands.

## Table of Contents

- [Quick Reference](#quick-reference)
- [SHOW TABLES](#show-tables)
- [DESCRIBE / DESC](#describe--desc)
- [SHOW INDEXES](#show-indexes)
- [EXPLAIN](#explain)
- [CREATE INDEX](#create-index)
- [DROP INDEX](#drop-index)
- [DROP TABLE](#drop-table)
- [CLI Shortcuts](#cli-shortcuts)
- [Data Directory Management](#data-directory-management)
- [Practical Workflows](#practical-workflows)

## Quick Reference

| Command | Purpose |
|---------|---------|
| `SHOW TABLES` | List all tables |
| `DESCRIBE table` | Show columns, types, constraints |
| `DESC table` | Alias for DESCRIBE |
| `SHOW INDEXES` | List all indexes across all tables |
| `SHOW INDEXES ON table` | List indexes for one table |
| `EXPLAIN SELECT ...` | Show query execution plan |
| `CREATE INDEX name ON table (col)` | Create a column index |
| `CREATE INDEX name ON table (json_extract(col, '$.path'))` | Create a JSON expression index |
| `DROP INDEX name` | Remove an index |
| `DROP TABLE name` | Remove a table and its indexes |

## SHOW TABLES

List all tables in the database.

```sql
SHOW TABLES;
```

**Output:**

```
 table_name
------------
 events
 orders
 users

(3 row(s))
```

Tables are sorted alphabetically.

**Use cases:**
- Verify a table was created successfully
- See what tables exist after a restart
- Discover the database schema when exploring an unfamiliar database

## DESCRIBE / DESC

Show the column definitions for a table, including name, type, nullability, and primary key status.

```sql
DESCRIBE users;
-- or equivalently:
DESC users;
```

**Output:**

```
 column      | type    | nullable | primary_key
-------------+---------+----------+-------------
 id          | INTEGER | false    | true
 name        | TEXT    | true     | false
 age         | INTEGER | true     | false
 profile     | JSON    | true     | false

(4 row(s))
```

**Column descriptions:**

| Output Column | Description |
|--------------|-------------|
| `column` | Column name |
| `type` | Data type: INTEGER, FLOAT, TEXT, BOOLEAN, or JSON |
| `nullable` | `true` if the column allows NULL values, `false` if NOT NULL |
| `primary_key` | `true` if the column is the primary key |

**Use cases:**
- Check column names and types before writing a query
- Verify constraints (NOT NULL, PRIMARY KEY) are set correctly
- Understand the schema when joining tables

**Error handling:**

```sql
DESCRIBE nonexistent;
-- ERROR: Table 'nonexistent' not found
```

## SHOW INDEXES

List all indexes in the database, or filter by table.

### All indexes

```sql
SHOW INDEXES;
```

**Output:**

```
 table  | index_name     | column  | entries | depth
--------+----------------+---------+---------+-------
 events | idx_event_type | data    | 500     | 2
 users  | idx_user_name  | profile | 100     | 1
 users  | pk_users_id    | id      | 100     | 1

(3 row(s))
```

### Indexes for a specific table

```sql
SHOW INDEXES ON users;
```

**Output:**

```
 table | index_name    | column  | entries | depth
-------+---------------+---------+---------+-------
 users | idx_user_name | profile | 100     | 1
 users | pk_users_id   | id      | 100     | 1

(2 row(s))
```

**Column descriptions:**

| Output Column | Description |
|--------------|-------------|
| `table` | Table the index belongs to |
| `index_name` | Index name (used with DROP INDEX) |
| `column` | Column being indexed. For expression indexes on JSON paths, this shows the source column containing the JSON data |
| `entries` | Number of entries currently in the B+Tree |
| `depth` | B+Tree depth (1 = single leaf, 2+ = multi-level) |

**Understanding depth:**

| Depth | Meaning | Approximate capacity |
|-------|---------|---------------------|
| 0 | Empty index | 0 entries |
| 1 | Single leaf node (root is a leaf) | Up to ~200 entries |
| 2 | Root + leaf nodes | Up to ~50,000 entries |
| 3 | Root + internal + leaf nodes | Up to ~10,000,000 entries |

**Identifying index types:**

- Indexes named `pk_*` are auto-created primary key indexes
- Other indexes are user-created via CREATE INDEX
- Expression indexes (on JSON paths) show the JSON source column in the `column` field. To see the full expression, check the catalog metadata.

## EXPLAIN

Show how BoolDB would execute a query without actually running it. Useful for understanding performance and verifying index usage.

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
```

**Output (sequential scan — no matching index):**

```
 plan
------------------------------
 SeqScan: users
   Filter: id = 1
   Projection: *

(3 row(s))
```

**Output (index scan — matching index found):**

```
 plan
-------------------------------------------------------------
 IndexScan: users using pk_users_id (id Eq 1)
   Filter: id = 1
   Projection: *

(3 row(s))
```

### What EXPLAIN shows

| Line | Meaning |
|------|---------|
| `SeqScan: table` | Full table scan (reads every row) |
| `IndexScan: table using idx (col Op val)` | Index-accelerated lookup |
| `Filter: expression` | WHERE clause being applied |
| `Projection: *` or `Projection: col1, col2` | Which columns are returned |
| `InnerJoin: table ON col1 = col2` | Join operation |
| `LeftJoin: ...` / `RightJoin: ...` | Outer join operations |

### EXPLAIN with JSON filters

```sql
EXPLAIN SELECT * FROM events WHERE json_extract(data, '$.type') = 'click';
```

```
 plan
------------------------------------------------------------
 SeqScan: events
   Filter: json_extract(data, '$.type') = click
   Projection: *
```

### EXPLAIN for other statement types

EXPLAIN works with any statement, not just SELECT:

```sql
EXPLAIN INSERT INTO users VALUES (1, 'Alice', 30);
-- Insert: users (1 row(s))

EXPLAIN UPDATE users SET age = 31 WHERE id = 1;
-- Update: users SET age
--   Filter: id = 1

EXPLAIN DELETE FROM users WHERE age < 18;
-- Delete: users
--   Filter: age < 18
```

### EXPLAIN for joins

```sql
EXPLAIN SELECT * FROM users INNER JOIN orders ON id = user_id;
```

```
 plan
----------------------------------------------
 SeqScan: users
   InnerJoin: orders ON id = user_id
   Projection: *
```

## CREATE INDEX

Create an index to speed up queries on a specific column or JSON path.

### Column index

```sql
CREATE INDEX index_name ON table_name (column_name);
```

```sql
CREATE INDEX idx_age ON users (age);
```

Output:
```
Index 'idx_age' created on age (100 entries, depth 1)
```

### JSON expression index

```sql
CREATE INDEX index_name ON table_name (json_extract(column, '$.path'));
```

```sql
CREATE INDEX idx_city ON users (json_extract(profile, '$.address.city'));
```

Output:
```
Index 'idx_city' created on json_extract(profile, '$.address.city') (100 entries, depth 1)
```

### What happens on CREATE INDEX

1. BoolDB scans all existing rows in the table.
2. For each row, it extracts the key value (column value or json_extract result).
3. Each key is inserted into a new B+Tree.
4. The B+Tree pages are stored in `data.db`.
5. The catalog is updated with the index metadata and root page ID.

### Notes

- PRIMARY KEY columns automatically get an index named `pk_{table}_{column}` on CREATE TABLE.
- Indexes are automatically maintained when you INSERT, UPDATE, or DELETE rows.
- Creating a duplicate index name returns an error.
- Index data persists across server restarts.

## DROP INDEX

Remove an index by name.

```sql
DROP INDEX index_name;
```

```sql
DROP INDEX idx_age;
```

Output:
```
Index 'idx_age' dropped
```

**What happens:**
- The index is removed from memory and the catalog.
- The B+Tree pages in `data.db` become reclaimable (not immediately freed).
- The underlying table data is not affected.
- Queries that previously used this index will fall back to sequential scans.

**Warning:** Dropping a primary key index (`pk_*`) is allowed but not recommended — it may impact query performance on that table.

## DROP TABLE

Remove a table and all its data and indexes.

```sql
DROP TABLE table_name;
```

```sql
DROP TABLE events;
```

Output:
```
Table 'events' dropped
```

**What happens:**
- The table is removed from the catalog.
- All heap pages for the table become reclaimable.
- All indexes on the table are removed (both primary key and user-created).
- This operation cannot be undone.

## CLI Shortcuts

The CLI provides shorthand commands that map to the SQL metadata commands:

| Shortcut | Equivalent SQL | Description |
|----------|---------------|-------------|
| `\dt` | `SHOW TABLES` | List all tables |
| `\di` | `SHOW INDEXES` | List all indexes |
| `\d users` | `DESCRIBE users` | Describe a table |
| `\help` or `\?` | — | Show all available commands |
| `\q` | — | Quit the CLI |

**Example session:**

```
booldb> \dt
 table_name
------------
 events
 users

(2 row(s))

booldb> \d users
 column  | type    | nullable | primary_key
---------+---------+----------+-------------
 id      | INTEGER | false    | true
 name    | TEXT    | true     | false
 profile | JSON    | true     | false

(3 row(s))

booldb> \di
 table  | index_name  | column  | entries | depth
--------+-------------+---------+---------+-------
 users  | pk_users_id | id      | 50      | 1

(1 row(s))
```

## Data Directory Management

### File layout

```
booldb_data/
├── data.db        All table data + B+Tree index pages (4 KB pages)
└── catalog.bin    Table schemas, index metadata, root page IDs
```

Both table heap pages and B+Tree index pages coexist in `data.db`, managed by a unified buffer pool.

### Checking database size

```bash
# Total data file size
ls -lh booldb_data/data.db

# Number of pages (size / 4096)
stat -f%z booldb_data/data.db | awk '{print $1/4096, "pages"}'
# On Linux: stat --format=%s booldb_data/data.db | awk '{print $1/4096, "pages"}'
```

### Backup and restore

```bash
# Backup (stop server first for consistency, or accept a fuzzy snapshot)
cp -r booldb_data/ booldb_data_backup/

# Restore
rm -rf booldb_data/
cp -r booldb_data_backup/ booldb_data/
```

### Starting fresh

```bash
# Stop the server, then:
rm -rf booldb_data/
# Restart the server — it will create a fresh empty database
```

### Custom data directory

```bash
BOOLDB_DATA_DIR=/var/lib/booldb cargo run -p booldb-server
```

## Practical Workflows

### Workflow 1: Exploring an unknown database

```sql
-- 1. What tables exist?
SHOW TABLES;

-- 2. What does each table look like?
DESCRIBE users;
DESCRIBE orders;

-- 3. What indexes are in place?
SHOW INDEXES;

-- 4. Sample some data
SELECT * FROM users;
SELECT * FROM orders;
```

### Workflow 2: Optimizing a slow query

```sql
-- 1. Check the current execution plan
EXPLAIN SELECT * FROM events WHERE json_extract(data, '$.type') = 'purchase';
-- Shows: SeqScan (full table scan)

-- 2. Create an index on the filtered path
CREATE INDEX idx_type ON events (json_extract(data, '$.type'));

-- 3. Verify the index was created
SHOW INDEXES ON events;

-- 4. Check the plan again
EXPLAIN SELECT * FROM events WHERE json_extract(data, '$.type') = 'purchase';
-- May now show: IndexScan
```

### Workflow 3: Schema evolution

```sql
-- 1. Check current schema
DESCRIBE users;

-- 2. You can't ALTER TABLE yet, so create a new table with the new schema
CREATE TABLE users_v2 (id INTEGER PRIMARY KEY, name TEXT, email TEXT, profile JSON);

-- 3. Copy data (manually, via application code)
-- INSERT INTO users_v2 SELECT ... FROM users; (subqueries not yet supported)

-- 4. Drop the old table
DROP TABLE users;

-- 5. Verify
SHOW TABLES;
DESCRIBE users_v2;
```

### Workflow 4: Index management

```sql
-- 1. See all indexes and their sizes
SHOW INDEXES;

-- 2. Find large indexes (high entry count, high depth)
-- Look at the 'entries' and 'depth' columns

-- 3. Drop unused indexes to save space
DROP INDEX idx_rarely_used;

-- 4. Create indexes on frequently filtered columns
CREATE INDEX idx_status ON orders (status);
CREATE INDEX idx_created ON orders (json_extract(data, '$.created_at'));

-- 5. Verify
SHOW INDEXES ON orders;
```

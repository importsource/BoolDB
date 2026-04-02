# BoolDB

A production-grade relational database built from scratch in Rust.

BoolDB features a complete SQL engine, page-based storage with buffer pool management, B-Tree indexes, ACID transaction support with MVCC and Write-Ahead Logging, a TCP server, and an interactive CLI client.

## Quick Start

### Build

```bash
cargo build --release
```

### Start the Server

```bash
cargo run --release -p booldb-server
```

The server listens on `127.0.0.1:5433` by default and stores data in `./booldb_data/`.

### Connect with the CLI

```bash
cargo run --release -p booldb-cli
```

### Run Your First Queries

```
booldb> CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
Table 'users' created

booldb> INSERT INTO users VALUES (1, 'Alice', 30);
1 row(s) affected

booldb> INSERT INTO users VALUES (2, 'Bob', 25);
1 row(s) affected

booldb> SELECT * FROM users WHERE age > 27;
 id | name  | age
----+-------+-----
 1  | Alice | 30

(1 row(s))

booldb> EXPLAIN SELECT * FROM users WHERE age > 27;
 plan
----------------------------------------------
 SeqScan: users
   Filter: age > 30
   Projection: *

(3 row(s))
```

## Table of Contents

- [Architecture](#architecture)
- [SQL Reference](#sql-reference)
- [Configuration](#configuration)
- [Client-Server Protocol](#client-server-protocol)
- [Storage Engine](#storage-engine)
- [Index System](#index-system)
- [Transaction System](#transaction-system)
- [Using BoolDB as a Library](#using-booldb-as-a-library)
- [JSON Support](#json-support)
- [Internals Deep Dive](#internals-deep-dive)
- [Testing](#testing)

## Architecture

BoolDB is organized as a Rust workspace with three crates:

```
BoolDB/
├── booldb-core/       Core database engine (library crate)
├── booldb-server/     TCP server (binary crate)
└── booldb-cli/        Interactive CLI client (binary crate)
```

### Layer Diagram

```
┌─────────────┐   ┌──────────────┐
│  CLI Client  │   │  TCP Client   │
└──────┬───────┘   └──────┬────────┘
       │                  │
       └────────┬─────────┘
                │  TCP (length-prefixed JSON)
       ┌────────┴─────────┐
       │    TCP Server     │  booldb-server
       │  (tokio async)    │
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │   SQL Parser       │  sqlparser-rs
       │   (GenericDialect) │
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │  Query Planner     │  AST → LogicalPlan
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │  Query Optimizer   │  Index selection, EXPLAIN
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │  Executor          │  Plan evaluation
       └────────┬──────────┘
                │
  ┌─────────────┼──────────────┐
  │             │              │
┌─┴──┐   ┌─────┴─────┐   ┌───┴───┐
│ TX │   │  Catalog   │   │ Index │
│Mgr │   │  (schema)  │   │(BTree)│
└─┬──┘   └─────┬─────┘   └───┬───┘
  │             │             │
  └─────────────┼─────────────┘
                │
       ┌────────┴──────────┐
       │    Heap File       │  Row storage
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │   Buffer Pool      │  LRU/Clock page cache
       └────────┬──────────┘
                │
       ┌────────┴──────────┐
       │   Disk Manager     │  Page I/O
       └────────┬──────────┘
                │
            ┌───┴───┐
            │ Disk  │
            └───────┘
```

## SQL Reference

### Data Types

| Type | Rust Mapping | Description |
|------|-------------|-------------|
| `INTEGER` | `i64` | 64-bit signed integer. Aliases: `INT`, `BIGINT`, `SMALLINT`, `TINYINT` |
| `FLOAT` | `f64` | 64-bit floating point. Aliases: `DOUBLE`, `DOUBLE PRECISION`, `REAL` |
| `TEXT` | `String` | Variable-length UTF-8 string. Aliases: `VARCHAR`, `CHAR`, `STRING` |
| `BOOLEAN` | `bool` | `true` or `false` |
| `NULL` | - | Null value (absence of data) |

### CREATE TABLE

Create a new table with typed columns.

```sql
CREATE TABLE table_name (
    column1 TYPE [PRIMARY KEY] [NOT NULL],
    column2 TYPE [NULL],
    ...
);
```

**Examples:**

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    age INTEGER,
    active BOOLEAN
);

CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    product TEXT,
    price FLOAT,
    quantity INTEGER
);
```

**Column constraints:**

| Constraint | Description |
|-----------|-------------|
| `PRIMARY KEY` | Marks the column as the primary key (implies NOT NULL) |
| `NOT NULL` | Column cannot contain NULL values |
| `NULL` | Column allows NULL values (default) |

### DROP TABLE

Remove a table and all its data (including all associated indexes).

```sql
DROP TABLE table_name;
```

### CREATE INDEX

Create an index on a table column to speed up queries that filter on that column.

```sql
CREATE INDEX index_name ON table_name (column_name);
```

**Examples:**

```sql
CREATE INDEX idx_users_name ON users (name);
CREATE INDEX idx_orders_user_id ON orders (user_id);
```

**Notes:**
- PRIMARY KEY columns automatically get an index named `pk_{table}_{column}` on CREATE TABLE.
- Index data is stored on B+Tree pages inside `data.db` alongside table data — no separate files.
- The index is built immediately by scanning all existing rows in the table.
- Indexes are automatically maintained on INSERT, UPDATE, and DELETE.

### DROP INDEX

Remove an index.

```sql
DROP INDEX index_name;
```

**Example:**

```sql
DROP INDEX idx_users_name;
```

This removes the index from memory and the catalog. The underlying table data is not affected. The B+Tree pages in `data.db` become reclaimable.

### INSERT

Insert one or more rows into a table.

```sql
-- Insert with all columns (positional)
INSERT INTO table_name VALUES (val1, val2, ...);

-- Insert with named columns
INSERT INTO table_name (col1, col2) VALUES (val1, val2);

-- Insert multiple rows
INSERT INTO table_name VALUES
    (1, 'Alice', 30),
    (2, 'Bob', 25),
    (3, 'Charlie', 35);
```

**Value syntax:**
- Integers: `42`, `-7`
- Floats: `3.14`, `-0.5`
- Strings: `'single quoted'` or `"double quoted"`
- Booleans: `true`, `false`
- Null: `NULL`

### SELECT

Query rows from one or more tables.

```sql
-- Select all columns
SELECT * FROM table_name;

-- Select specific columns
SELECT col1, col2 FROM table_name;

-- With filtering
SELECT * FROM table_name WHERE condition;

-- With joins
SELECT * FROM left_table
    INNER JOIN right_table ON left_table.col = right_table.col;
```

### WHERE Clause

Filter rows using comparison and logical operators.

**Comparison operators:**

| Operator | Description |
|----------|-------------|
| `=` | Equal |
| `!=` or `<>` | Not equal |
| `<` | Less than |
| `<=` | Less than or equal |
| `>` | Greater than |
| `>=` | Greater than or equal |
| `IS NULL` | Value is NULL |
| `IS NOT NULL` | Value is not NULL |

**Logical operators:**

| Operator | Description |
|----------|-------------|
| `AND` | Both conditions must be true |
| `OR` | Either condition must be true |
| `NOT` | Negates the condition |

**Examples:**

```sql
SELECT * FROM users WHERE age > 25;
SELECT * FROM users WHERE age >= 20 AND age <= 30;
SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob';
SELECT * FROM users WHERE email IS NOT NULL;
SELECT * FROM users WHERE NOT (age < 18);
```

### JOIN

Combine rows from two or more tables based on a related column.

**Supported join types:**

| Type | Description |
|------|-------------|
| `INNER JOIN` | Only matching rows from both tables |
| `LEFT JOIN` / `LEFT OUTER JOIN` | All rows from left + matching from right (NULL-padded) |
| `RIGHT JOIN` / `RIGHT OUTER JOIN` | All rows from right + matching from left (NULL-padded) |

**Syntax:**

```sql
SELECT *
FROM table1
INNER JOIN table2 ON table1.col_a = table2.col_b;

SELECT *
FROM users
LEFT JOIN orders ON users.id = orders.user_id;

SELECT *
FROM orders
RIGHT JOIN users ON orders.user_id = users.id;
```

**Notes:**
- Only equi-joins (`ON col1 = col2`) are supported.
- Multiple joins can be chained in a single query.
- Joined column names can use `table.column` dot notation.

### UPDATE

Modify existing rows.

```sql
UPDATE table_name SET col1 = val1, col2 = val2 WHERE condition;

-- Update all rows (no WHERE)
UPDATE table_name SET col1 = val1;
```

**Examples:**

```sql
UPDATE users SET age = 31 WHERE id = 1;
UPDATE users SET name = 'Robert', age = 26 WHERE name = 'Bob';
UPDATE orders SET quantity = 0 WHERE price > 100;
```

### DELETE

Remove rows from a table.

```sql
DELETE FROM table_name WHERE condition;

-- Delete all rows (no WHERE)
DELETE FROM table_name;
```

**Examples:**

```sql
DELETE FROM users WHERE id = 3;
DELETE FROM orders WHERE quantity = 0;
```

### EXPLAIN

Show the query execution plan without running the query.

```sql
EXPLAIN SELECT * FROM users WHERE id = 1;
EXPLAIN SELECT * FROM users INNER JOIN orders ON id = user_id;
EXPLAIN UPDATE users SET age = 31 WHERE id = 1;
EXPLAIN DELETE FROM users WHERE age < 18;
```

**Example output:**

```
SeqScan: users
  Filter: id = 1
  Projection: *
```

If an index is available on the filtered column:
```
IndexScan: users using idx_users_id (id Eq 1)
  Filter: id = 1
  Projection: *
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BOOLDB_ADDR` | `127.0.0.1:5433` | Server bind address (host:port) |
| `BOOLDB_DATA_DIR` | `./booldb_data` | Directory for database files |

**Examples:**

```bash
# Custom port
BOOLDB_ADDR=0.0.0.0:9999 cargo run -p booldb-server

# Custom data directory
BOOLDB_DATA_DIR=/var/lib/booldb cargo run -p booldb-server

# CLI connecting to a custom address
BOOLDB_ADDR=192.168.1.10:9999 cargo run -p booldb-cli
```

### Data Directory Layout

When the server starts, it creates the following files in the data directory:

```
booldb_data/
├── data.db        Page-based data file (table data + B+Tree index pages)
└── catalog.bin    Serialized catalog (table schemas, index root page IDs)
```

- `data.db` — Fixed-size 4 KB pages containing **both** table heap data and B+Tree index nodes. Heap pages and B+Tree pages are interleaved in the same file, distinguished by their page type header byte. Grows as tables and indexes are populated.
- `catalog.bin` — Binary-encoded catalog with all table schemas and index metadata (including B+Tree root page IDs). Updated on every DDL/DML operation.

This unified file design means a single buffer pool caches both data and index pages, and a single flush persists everything atomically.

## Client-Server Protocol

BoolDB uses a simple length-prefixed JSON protocol over TCP.

### Wire Format

Each message (request or response) is framed as:

```
┌──────────────────┬────────────────────┐
│ 4 bytes (BE u32) │  JSON payload      │
│ payload length   │                    │
└──────────────────┴────────────────────┘
```

- Length is encoded as a **big-endian unsigned 32-bit integer**.
- Maximum message size: **16 MB**.

### Request Format

```json
{
    "sql": "SELECT * FROM users WHERE age > 25"
}
```

### Response Format

**DDL success** (CREATE TABLE, DROP TABLE):
```json
{
    "status": "ok",
    "message": "Table 'users' created"
}
```

**DML success** (INSERT, UPDATE, DELETE):
```json
{
    "status": "ok",
    "message": "3 row(s) affected",
    "rows_affected": 3
}
```

**Query result** (SELECT):
```json
{
    "status": "ok",
    "columns": ["id", "name", "age"],
    "rows": [
        ["1", "Alice", "30"],
        ["2", "Bob", "25"]
    ]
}
```

**Error:**
```json
{
    "status": "error",
    "message": "Table 'users' not found"
}
```

**Notes:**
- All row values are serialized as strings in query responses.
- NULL values are represented as the string `"NULL"`.
- The `columns`, `rows`, `rows_affected`, and `message` fields are optional (omitted when not applicable).

### Writing a Custom Client

Any language that can open a TCP socket and send/receive length-prefixed JSON can connect to BoolDB. Here is a minimal Python example:

```python
import socket, json, struct

def send_query(sock, sql):
    req = json.dumps({"sql": sql}).encode()
    sock.sendall(struct.pack(">I", len(req)))
    sock.sendall(req)

    resp_len = struct.unpack(">I", sock.recv(4))[0]
    data = b""
    while len(data) < resp_len:
        data += sock.recv(resp_len - len(data))
    return json.loads(data)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", 5433))

print(send_query(sock, "CREATE TABLE t (id INTEGER, name TEXT)"))
print(send_query(sock, "INSERT INTO t VALUES (1, 'hello')"))
print(send_query(sock, "SELECT * FROM t"))

sock.close()
```

## CLI Client

The CLI provides an interactive REPL (Read-Eval-Print Loop) with command history.

### Commands

| Command | Description |
|---------|-------------|
| Any SQL statement | Executed on the server |
| `\q`, `quit`, `exit` | Quit the CLI |
| `\dt` | List all tables |
| `\d TABLE` | Describe a table (planned) |
| `\help`, `\?` | Show help |

### Features

- **Command history**: Persisted to `~/.booldb_history` across sessions.
- **Line editing**: Arrow keys, Ctrl-A/E, Ctrl-W, etc. (via rustyline).
- **Formatted output**: Query results displayed as aligned ASCII tables.
- **Error display**: SQL errors shown with descriptive messages.

### Output Format

Query results are displayed as ASCII tables:

```
booldb> SELECT * FROM users;
 id | name    | age
----+---------+-----
 1  | Alice   | 30
 2  | Bob     | 25
 3  | Charlie | 35

(3 row(s))
```

## Storage Engine

### Page Layout

All data is stored in fixed-size **4096-byte (4 KB) pages**. Each page uses a slotted-page format:

```
┌─────────────────────────────────────────────────────┐
│                    Page (4096 bytes)                 │
├─────────┬──────────────┬───────────┬────────────────┤
│ Header  │ Slot Array → │ Free      │ ← Tuple Data   │
│(13 bytes)│              │ Space     │                │
└─────────┴──────────────┴───────────┴────────────────┘
```

**Header (13 bytes):**

| Field | Size | Description |
|-------|------|-------------|
| `page_id` | 4 bytes | Unique page identifier (u32) |
| `page_type` | 1 byte | Type: Heap(1), BTreeInternal(2), BTreeLeaf(3), Catalog(4) |
| `num_slots` | 2 bytes | Number of slot entries |
| `free_space_offset` | 2 bytes | Offset where free space ends |
| `checksum` | 4 bytes | CRC32 checksum of the page |

**Slot Array:**

Each slot is 4 bytes: `[offset: u16][length: u16]`. Slots grow forward from the header. A slot with `(offset=0, length=0)` indicates a deleted tuple whose slot can be reused.

**Tuple Data:**

Tuples are stored backward from the end of the page. New tuples are appended toward the beginning. Rows are serialized using `bincode`.

### Buffer Pool

The buffer pool is a fixed-size in-memory cache of pages (default: **256 pages = 1 MB**).

**Eviction policy:** Clock algorithm (second-chance replacement). Pages are pinned while in use and only eligible for eviction when unpinned.

**Dirty page tracking:** Modified pages are marked dirty and flushed to disk on:
- Explicit flush calls
- Eviction of a dirty page
- Database shutdown (Drop)

### Disk Manager

Maps page IDs to file offsets using a simple formula: `offset = page_id * 4096`. Pages are stored contiguously in a single `data.db` file.

### Heap File

Each table is backed by a heap file — an unordered collection of pages. The heap file:

- Tries to insert into existing pages with free space before allocating new ones.
- Supports full table scans by iterating over all pages.
- Implements delete-then-insert for updates (rows may move between pages).

## Index System

BoolDB includes a disk-based B+Tree index stored on pages inside `data.db` — the same file that holds table data.

### How It Works

B+Tree nodes (both internal and leaf) are stored as individual 4 KB pages, sharing the same file and buffer pool as heap data:

```
data.db
┌─────────┬─────────┬─────────┬─────────┬─────────┬─────────┐
│ Page 0  │ Page 1  │ Page 2  │ Page 3  │ Page 4  │ Page 5  │
│  Heap   │  Heap   │  BTree  │  BTree  │  BTree  │  Heap   │
│ (users) │ (users) │  Leaf   │  Leaf   │Internal │(orders) │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
```

- **Leaf nodes** store sorted keys, RowId lists, and a `next_leaf` pointer for efficient range scans.
- **Internal nodes** store sorted keys and child page pointers.
- The catalog stores each index's **root page ID** — that single pointer is all that's needed to traverse the entire tree on startup.

### Tree Properties

| Property | Value |
|----------|-------|
| Page size | 4 KB |
| Split threshold | ~3600 bytes per node |
| Entries per leaf | ~150–250 (depends on key size) |
| Internal fanout | ~200–400 |
| Depth for 500 rows | 2 |
| Depth for 5,000 rows | 2–3 |
| Depth for 1M rows | ~3 |

### Lifecycle

| Event | Index behavior |
|-------|---------------|
| **CREATE TABLE** (with PK) | Auto-creates `pk_{table}_{column}` B+Tree (empty) |
| **CREATE INDEX** | Builds B+Tree by scanning the table |
| **INSERT / UPDATE / DELETE** | Rebuilds affected B+Tree indexes |
| **DROP INDEX** | Removes from memory and catalog; B+Tree pages become reclaimable |
| **DROP TABLE** | Removes all associated indexes |
| **Startup** | Restores B+Tree from root page ID in catalog (pages already in `data.db`) |
| **Shutdown** | Flushes buffer pool (persists all dirty pages including B+Tree nodes) |

### Key Encoding

Values are encoded into bytes that preserve sort order:

| Type | Prefix | Encoding |
|------|--------|----------|
| NULL | `0x00` | Single byte |
| Boolean | `0x01` | `0x00` (false) or `0x01` (true) |
| Integer | `0x02` | XOR sign-flip + big-endian u64 (preserves i64 ordering) |
| Float | `0x03` | IEEE 754 sign-flip + big-endian u64 (preserves f64 ordering) |
| Text | `0x04` | Raw UTF-8 bytes |

This encoding ensures that byte-level comparison matches the logical sort order across all types.

### Operations

| Operation | Complexity | Description |
|-----------|-----------|-------------|
| Point lookup | O(log n) | Find all rows with a specific key value |
| Range scan | O(log n + k) | Find all rows where key is in [start, end] |
| Insert | O(log n) | Add a key-RowId mapping |
| Delete | O(log n) | Remove a specific key-RowId pair |
| Full scan | O(n) | Iterate all entries in sorted order |

Duplicate keys are supported — multiple RowIds can map to the same key value.

### Index-Aware Optimization

The query optimizer checks whether a WHERE clause references an indexed column. If so, it produces an `IndexScanHint` that the executor can use instead of a full sequential scan. This is visible via `EXPLAIN`.

## Transaction System

BoolDB implements the foundations for ACID transactions.

### MVCC (Multi-Version Concurrency Control)

Each transaction gets:
- A unique **transaction ID** (`tx_id`, monotonically increasing)
- A **start timestamp** (`start_ts`)
- A **snapshot** of active transaction IDs at begin time

**Visibility rules (snapshot isolation):**

A row written by transaction W is visible to transaction R if:
1. W is the same transaction as R (own writes are always visible), **OR**
2. W was committed before R started, **AND** W was not in R's active snapshot

This prevents dirty reads, non-repeatable reads, and phantom reads.

### Transaction States

```
Active → Committed
       → Aborted
```

- **Active**: Transaction is in progress.
- **Committed**: Transaction's changes are permanently visible.
- **Aborted**: Transaction's changes are rolled back.

### Write-Ahead Log (WAL)

The WAL ensures crash recovery by logging changes before they are applied to data pages.

**Log record types:**

| Record | Fields | Description |
|--------|--------|-------------|
| `Begin` | `tx_id` | Transaction started |
| `Commit` | `tx_id` | Transaction committed |
| `Abort` | `tx_id` | Transaction aborted |
| `PageWrite` | `tx_id`, `page_id`, `before_image`, `after_image` | Page was modified |
| `Checkpoint` | `active_tx_ids` | All dirty pages flushed |

**WAL file format:** Sequence of length-prefixed bincode-serialized records.

**Recovery algorithm:**
1. Read all WAL records.
2. Identify committed and aborted transactions.
3. Replay `PageWrite` records from committed transactions (redo).
4. Discard writes from aborted/incomplete transactions.

### Row-Level Locking

The lock manager provides shared and exclusive row-level locks:

| Lock held | Shared request | Exclusive request |
|-----------|---------------|-------------------|
| None | Granted | Granted |
| Shared | Granted | **Blocked** |
| Exclusive | **Blocked** | **Blocked** |

- Multiple transactions can hold shared locks on the same row.
- A transaction can upgrade its own shared lock to exclusive.
- All locks are released when a transaction commits or aborts.

## JSON Support

BoolDB supports storing, querying, and indexing JSON data.

```sql
-- JSON column type
CREATE TABLE events (id INTEGER PRIMARY KEY, data JSON);
INSERT INTO events VALUES (1, '{"name": "Alice", "age": 30, "city": "NYC"}');

-- Extract values with json_extract()
SELECT json_extract(data, '$.name'), json_extract(data, '$.age') FROM events;

-- Filter on JSON paths (supports AND/OR with multiple criteria)
SELECT * FROM events
WHERE json_extract(data, '$.age') > 25
  AND json_extract(data, '$.city') = 'NYC';

-- Expression index for fast JSON path lookups
CREATE INDEX idx_name ON events (json_extract(data, '$.name'));
```

**Supported paths:** `$.field`, `$.nested.field`, `$.array[0]`, `$.array[0].field`

For the full reference including expression indexes, type mapping, multiple filter criteria, and worked examples, see the [JSON Guide](docs/json.md).

## Using BoolDB as a Library

The `booldb-core` crate can be used directly as an embedded database without the TCP server.

### Dependency

```toml
[dependencies]
booldb-core = { path = "booldb-core" }
```

### Example

```rust
use booldb_core::db::Database;
use booldb_core::sql::executor::ExecResult;

fn main() {
    // Open or create a database
    let mut db = Database::open("./my_data").unwrap();

    // Create a table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
        .unwrap();

    // Insert rows
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();

    // Query
    match db.execute("SELECT * FROM users WHERE age > 27").unwrap() {
        ExecResult::Rows { columns, rows } => {
            println!("Columns: {:?}", columns);
            for row in &rows {
                println!("  {:?}", row);
            }
        }
        _ => {}
    }

    // List tables
    println!("Tables: {:?}", db.table_names());

    // Get schema
    let schema = db.table_schema("users").unwrap();
    for col in &schema.columns {
        println!("  {} {:?} nullable={} pk={}", col.name, col.data_type, col.nullable, col.primary_key);
    }
}
```

### API Reference

**`Database::open(path)`** — Open or create a database at the given directory. Creates the directory if it does not exist. Loads the catalog and rebuilds heap files from persisted metadata.

**`Database::execute(sql)`** — Parse and execute a single SQL statement. Returns:
- `ExecResult::Ok { message }` for DDL (CREATE, DROP)
- `ExecResult::RowsAffected { count }` for DML (INSERT, UPDATE, DELETE)
- `ExecResult::Rows { columns, rows }` for queries (SELECT, EXPLAIN)

Automatically persists the catalog and flushes dirty pages after mutations.

**`Database::table_names()`** — Returns a list of all table names.

**`Database::table_schema(name)`** — Returns the schema for a table.

## Internals Deep Dive

### Query Execution Pipeline

```
SQL String
    │
    ▼
┌──────────┐
│  Parser  │  sqlparser-rs (GenericDialect)
└────┬─────┘
     │  Vec<Statement>
     ▼
┌──────────┐
│ Planner  │  Statement → LogicalPlan
└────┬─────┘
     │  LogicalPlan
     ▼
┌──────────┐
│Optimizer │  Analyze plan → QueryHints
└────┬─────┘
     │  LogicalPlan + hints
     ▼
┌──────────┐
│ Executor │  Evaluate plan against storage
└────┬─────┘
     │  ExecResult
     ▼
  Result
```

### Logical Plan Nodes

| Node | Fields | Description |
|------|--------|-------------|
| `CreateTable` | `schema` | Create a new table |
| `DropTable` | `table_name` | Drop a table |
| `Insert` | `table_name`, `columns`, `rows` | Insert rows |
| `Select` | `table_name`, `projection`, `filter`, `joins` | Query with optional filter/joins |
| `Update` | `table_name`, `assignments`, `filter` | Modify matching rows |
| `Delete` | `table_name`, `filter` | Remove matching rows |

### JOIN Implementation

Joins use a **nested loop** algorithm:

```
for each row in left_table:
    for each row in right_table:
        if join_condition matches:
            emit combined row
```

- **INNER JOIN**: Only emits rows where both sides match.
- **LEFT JOIN**: Emits all left rows; unmatched right side is NULL-padded.
- **RIGHT JOIN**: Emits all right rows; unmatched left side is NULL-padded.

Multiple joins can be chained — each join extends the working row set.

### Value Comparison

BoolDB supports cross-type numeric comparison:

| Left | Right | Behavior |
|------|-------|----------|
| Integer | Integer | Direct i64 comparison |
| Float | Float | Direct f64 comparison |
| Integer | Float | Integer cast to f64 |
| Float | Integer | Integer cast to f64 |
| Text | Text | Lexicographic comparison |
| Boolean | Boolean | false < true |
| NULL | NULL | Equal |
| NULL | Any | NULL is less than any non-NULL value |

### Persistence Model

BoolDB persists data in two files:

1. **Data pages** (`data.db`): All row data **and** B+Tree index nodes stored as 4 KB pages. Heap pages and B+Tree pages coexist in the same file, sharing the same buffer pool. Dirty pages are written to disk on flush, eviction, or shutdown.

2. **Catalog** (`catalog.bin`): Table schemas, heap page ID lists, and index metadata (including B+Tree root page IDs). Serialized with bincode after every DDL/DML operation.

On startup, BoolDB:
1. Opens the data file via the disk manager.
2. Loads the catalog from `catalog.bin` (if it exists).
3. Reconstructs heap file objects from the catalog's page ID lists.
4. Reconstructs B+Tree indexes from the root page IDs stored in the catalog — the actual tree nodes are already in `data.db`.
5. If a root page ID is missing (e.g., migrated from an older format), rebuilds the index by scanning the table's heap data.

### Error Types

All operations return `Result<T, BoolDBError>`. Error variants:

| Error | Description |
|-------|-------------|
| `Io` | File system I/O error |
| `PageNotFound` | Referenced page does not exist |
| `BufferPoolFull` | All buffer frames are pinned, cannot evict |
| `PageFull` | Not enough space in page for the tuple |
| `TupleNotFound` | Row at given RowId does not exist or was deleted |
| `TableNotFound` | Referenced table does not exist |
| `TableAlreadyExists` | CREATE TABLE with duplicate name |
| `ColumnNotFound` | Referenced column not in schema |
| `TypeMismatch` | Value does not match expected type |
| `Sql` | General SQL execution error |
| `Parse` | SQL parsing error |
| `Serialization` | Bincode serialization/deserialization error |
| `Transaction` | Transaction state error |
| `LockConflict` | Row-level lock conflict |
| `Internal` | Unexpected internal error |

## Testing

### Run All Tests

```bash
cargo test --workspace
```

### Test Coverage

BoolDB includes **66 unit tests** covering all modules:

| Module | Tests | What's Tested |
|--------|-------|---------------|
| `storage::page` | 6 | Page create, tuple insert/read/delete, slot reuse, page full, iteration |
| `storage::disk` | 3 | Allocate, read/write, persistence across reopen, nonexistent page |
| `storage::buffer` | 3 | New/fetch page, flush and reread, clock eviction |
| `storage::heap` | 5 | Insert/scan, delete, update, count, multi-page spanning |
| `catalog::schema` | 5 | Create/get/drop table, duplicates, serialization, add index |
| `index::btree` | 6 | Insert/search, delete, duplicates, range scan, integer/text ordering |
| `sql::parser` | 4 | Parse CREATE/INSERT/SELECT, data type conversion |
| `sql::planner` | 7 | Plan all statement types (CREATE, INSERT, SELECT, UPDATE, DELETE, DROP, WHERE) |
| `sql::optimizer` | 3 | Index scan hints, unindexed columns, EXPLAIN output |
| `tx::wal` | 3 | Write/read WAL, recovery (committed vs aborted), truncate |
| `tx::mvcc` | 5 | Begin/commit, abort, snapshot isolation, own-write visibility, concurrency |
| `tx::lock` | 6 | Shared/exclusive locks, conflicts, upgrade, release |
| `db` | 10 | Full SQL workflow, WHERE, UPDATE, DELETE, DROP, persistence, JOIN, index persistence, index survives mutations, CREATE/DROP INDEX |

### Integration Testing

Start the server and run queries via the CLI or a custom client:

```bash
# Terminal 1: Start server
BOOLDB_DATA_DIR=/tmp/booldb_test cargo run -p booldb-server

# Terminal 2: Connect with CLI
cargo run -p booldb-cli
```

Then execute:

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);
INSERT INTO users VALUES (1, 'Alice', 30);
INSERT INTO users VALUES (2, 'Bob', 25);
INSERT INTO users VALUES (3, 'Charlie', 35);
SELECT * FROM users;
SELECT * FROM users WHERE age > 27;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE id = 2;
SELECT * FROM users;
DROP TABLE users;
```

## License

MIT

# BoolDB Architecture Guide

This document provides a deep technical walkthrough of BoolDB's internals. It is intended for contributors, students, and anyone who wants to understand how a relational database works from the ground up.

## Table of Contents

1. [Overview](#overview)
2. [Storage Engine](#storage-engine)
   - [Pages](#pages)
   - [Disk Manager](#disk-manager)
   - [Buffer Pool](#buffer-pool)
   - [Heap File](#heap-file)
3. [Catalog System](#catalog-system)
4. [Index Engine](#index-engine)
5. [SQL Pipeline](#sql-pipeline)
   - [Parsing](#parsing)
   - [Planning](#planning)
   - [Optimization](#optimization)
   - [Execution](#execution)
6. [Transaction Engine](#transaction-engine)
   - [MVCC](#mvcc)
   - [Write-Ahead Log](#write-ahead-log)
   - [Lock Manager](#lock-manager)
7. [Networking](#networking)
8. [Key Design Decisions](#key-design-decisions)

---

## Overview

BoolDB is structured as a layered system. Each layer has a well-defined responsibility and communicates with adjacent layers through clean interfaces.

```
                          User
                           │
                    ┌──────┴──────┐
                    │  CLI / TCP  │   Network layer
                    └──────┬──────┘
                           │
                    ┌──────┴──────┐
                    │  Database   │   Coordinator (db.rs)
                    └──┬───┬───┬─┘
                       │   │   │
              ┌────────┘   │   └────────┐
              │            │            │
        ┌─────┴─────┐ ┌───┴───┐ ┌──────┴──────┐
        │SQL Engine │ │Catalog│ │ Transaction │
        │parse/plan/│ │       │ │  WAL/MVCC   │
        │ execute   │ │       │ │   Locks     │
        └─────┬─────┘ └───┬───┘ └──────┬──────┘
              │            │            │
              └────────┬───┘────────────┘
                       │
                ┌──────┴──────┐
                │  Heap File  │   Logical row storage
                └──────┬──────┘
                       │
                ┌──────┴──────┐
                │ Buffer Pool │   Page cache
                └──────┬──────┘
                       │
                ┌──────┴──────┐
                │Disk Manager │   Raw page I/O
                └──────┬──────┘
                       │
                    ┌──┴──┐
                    │File │
                    └─────┘
```

**Source file map:**

| File | Responsibility |
|------|---------------|
| `db.rs` | Top-level coordinator: opens database, routes SQL, manages persistence |
| `types.rs` | Core data types: `Value`, `Row`, `Schema`, `Column`, `RowId` |
| `error.rs` | Error type hierarchy |
| `storage/page.rs` | 4 KB slotted page format |
| `storage/disk.rs` | Page-to-file I/O |
| `storage/buffer.rs` | In-memory page cache with clock eviction |
| `storage/heap.rs` | Unordered row storage across pages |
| `catalog/schema.rs` | Table/index metadata and persistence |
| `index/btree.rs` | B-Tree index with file-based persistence |
| `sql/parser.rs` | SQL text to AST (wraps sqlparser-rs) |
| `sql/planner.rs` | AST to `LogicalPlan` |
| `sql/optimizer.rs` | Plan analysis, index selection, EXPLAIN |
| `sql/executor.rs` | `LogicalPlan` evaluation against storage |
| `tx/wal.rs` | Write-ahead log for crash recovery |
| `tx/mvcc.rs` | Multi-version concurrency control |
| `tx/lock.rs` | Row-level shared/exclusive locks |

---

## Storage Engine

The storage engine is the foundation of BoolDB. It manages how data is organized on disk and cached in memory.

### Pages

**File:** `booldb-core/src/storage/page.rs`

Everything in BoolDB is stored in fixed-size 4096-byte pages. The page is the unit of I/O — reads and writes always operate on complete pages.

#### Slotted Page Format

BoolDB uses the classic "slotted page" design from database textbooks (also used by PostgreSQL):

```
Byte offset:  0                    13                                    4096
              ┌─────────┬──────────┬──────┬──────┬─────────┬─────┬──────┐
              │ Header  │ Slot [0] │Slot[1]│ ... │  Free   │Tuple│Tuple │
              │(13 bytes)│(4 bytes) │       │     │  Space  │ [1] │ [0]  │
              └─────────┴──────────┴──────┴──────┴─────────┴─────┴──────┘
                         ◄── grows right                  grows left ──►
```

**Why slotted pages?**

1. **Variable-length tuples**: Rows can be different sizes.
2. **Stable references**: A `RowId` is `(page_id, slot_id)`. Even if we compact tuples, the slot pointer is updated but the slot index stays the same.
3. **Efficient deletion**: Just zero the slot entry — no data movement needed.
4. **Slot reuse**: New inserts can reuse deleted slots, avoiding unbounded slot array growth.

#### Header Detail

```rust
// Byte layout:
// [0..4]   page_id: u32 (little-endian)
// [4]      page_type: u8
// [5..7]   num_slots: u16 (little-endian)
// [7..9]   free_space_offset: u16 (little-endian)
// [9..13]  checksum: u32 (CRC32, little-endian)
```

The `free_space_offset` tracks where the next tuple will be placed (growing backward from 4096). The available free space is:

```
free_space = free_space_offset - (HEADER_SIZE + num_slots * SLOT_ENTRY_SIZE)
```

#### Checksum

Every page carries a CRC32 checksum. The checksum field is zeroed before computation so it covers the entire page content. This detects corruption from partial writes or disk errors.

```rust
pub fn update_checksum(&mut self) {
    self.data[9..13].copy_from_slice(&[0u8; 4]);  // zero checksum field
    let crc = crc32fast::hash(&self.data);
    self.set_checksum(crc);
}
```

#### Insert Algorithm

```
fn insert_tuple(data):
    needed = data.len() + 4 (slot entry)
    if free_space < needed:
        return None  // page is full

    new_offset = free_space_offset - data.len()
    copy data into page at new_offset
    set free_space_offset = new_offset

    // find a reusable slot (deleted) or append new
    for slot in 0..num_slots:
        if slot is deleted (offset=0, length=0):
            reuse this slot
            return slot_index

    // no reusable slot, append
    set slot[num_slots] = (new_offset, data.len())
    num_slots += 1
    return num_slots - 1
```

### Disk Manager

**File:** `booldb-core/src/storage/disk.rs`

The disk manager provides a simple page-to-file mapping:

```
file offset = page_id * PAGE_SIZE (4096)
```

Pages are stored contiguously in a single file (`data.db`). The file grows by one page at a time when `allocate_page()` is called.

**Operations:**

| Method | Description |
|--------|-------------|
| `open(path)` | Open/create the database file, compute page count from file size |
| `allocate_page()` | Extend file by 4096 bytes, return new page ID |
| `read_page(id)` | Seek to offset, read 4096 bytes into a `Page` |
| `write_page(id, page)` | Seek to offset, write 4096 bytes from `Page` |

The disk manager does **not** cache anything — that is the buffer pool's job.

### Buffer Pool

**File:** `booldb-core/src/storage/buffer.rs`

The buffer pool sits between the executor and disk, keeping frequently-accessed pages in memory.

#### Structure

```rust
struct BufferFrame {
    page: Page,         // the cached page data
    page_id: PageId,    // which page is in this frame
    dirty: bool,        // has this page been modified?
    pin_count: u32,     // number of active users
    reference: bool,    // clock algorithm bit
}
```

The pool maintains a hash map (`page_table: HashMap<PageId, usize>`) for O(1) frame lookup.

#### Pin/Unpin Protocol

Every `fetch_page()` call increments the pin count. The caller **must** call `unpin_page()` when done. A page with `pin_count > 0` cannot be evicted.

This is the standard database buffer pool protocol:

```rust
// Read a page
let page = pool.fetch_page(page_id)?;    // pin_count++
// ... use page ...
pool.unpin_page(page_id, false);          // pin_count--, not dirty

// Modify a page
let page = pool.fetch_page_mut(page_id)?; // pin_count++, marks dirty
// ... modify page ...
pool.unpin_page(page_id, true);            // pin_count--, dirty=true
```

#### Clock Eviction Algorithm

When the pool is full and a new page is needed, the clock algorithm selects a victim:

```
clock_hand = current position in frame array

repeat up to 2 * num_frames times:
    frame = frames[clock_hand]
    if frame.pin_count == 0:
        if frame.reference:
            frame.reference = false   // give second chance
        else:
            if frame.dirty:
                write page to disk    // flush before eviction
            evict this frame
            return frame index
    clock_hand = (clock_hand + 1) % num_frames

return BufferPoolFull error
```

**Why clock over LRU?** The clock algorithm approximates LRU with O(1) overhead per lookup, avoiding the linked-list maintenance that true LRU requires.

### Heap File

**File:** `booldb-core/src/storage/heap.rs`

A heap file is the physical storage for a single table — an unordered collection of pages.

#### Row Lifecycle

```
INSERT: serialize row → try each page → allocate new page if needed → return RowId
GET:    fetch page → read slot → deserialize row
DELETE: fetch page → zero slot → unpin dirty
UPDATE: delete old RowId → insert new row → return new RowId
SCAN:   iterate all pages → iterate all live slots → deserialize each
```

**Important:** UPDATE may return a different RowId if the new row lands on a different page. Any index entries must be updated accordingly.

#### Serialization

Rows are serialized using `bincode`, which produces a compact binary representation:

```rust
pub fn serialize_row(row: &Row) -> Vec<u8> {
    bincode::serialize(row).expect("Row serialization should not fail")
}
```

A typical row like `[Integer(1), Text("Alice"), Integer(30)]` serializes to roughly 30-40 bytes.

---

## Catalog System

**File:** `booldb-core/src/catalog/schema.rs`

The catalog is an in-memory registry of all tables and their metadata.

### Data Structures

```rust
struct Catalog {
    tables: HashMap<String, TableMeta>,
}

struct TableMeta {
    schema: Schema,                      // column definitions
    heap_page_ids: Vec<PageId>,          // which pages belong to this table
    indexes: HashMap<String, IndexMeta>, // named indexes
}

struct IndexMeta {
    name: String,
    table_name: String,
    column_index: usize,    // index into Schema.columns
    root_page_id: PageId,   // (reserved for disk-based B-Tree)
}
```

### Persistence

The catalog is serialized with `bincode` and written to `catalog.bin`:

```rust
// Save
let bytes = catalog.to_bytes();  // bincode::serialize
std::fs::write("catalog.bin", bytes)?;

// Load
let bytes = std::fs::read("catalog.bin")?;
let catalog = Catalog::from_bytes(&bytes)?;  // bincode::deserialize
```

The catalog is saved after every DDL/DML operation. This ensures that the table metadata (including the list of page IDs for each table's heap file) survives restarts.

### Startup Sequence

```
1. Open DiskManager on data.db
2. Create BufferPool
3. Load Catalog from catalog.bin (if exists)
4. For each table in catalog:
     Create HeapFile from stored page_ids
5. For each index in catalog:
     Load from index_{name}.bin
     If file missing/corrupt → rebuild by scanning the heap
6. Ready to accept queries
```

---

## Index Engine

**File:** `booldb-core/src/index/btree.rs`

BoolDB includes a B-Tree index built on Rust's `BTreeMap<Vec<u8>, Vec<RowId>>`, with each index persisted to its own file.

### Persistence

Each index is serialized to an independent `index_{name}.bin` file using bincode:

```rust
// Save
let bytes = index.to_bytes();  // bincode::serialize
std::fs::write("index_pk_users_id.bin", bytes)?;

// Load
let bytes = std::fs::read("index_pk_users_id.bin")?;
let index = BTreeIndex::from_bytes(&bytes)?;
```

**Index lifecycle:**

| Event | Behavior |
|-------|----------|
| `CREATE TABLE` (with PRIMARY KEY) | Auto-creates `pk_{table}_{col}` index + file |
| `CREATE INDEX name ON table (col)` | Builds index from heap scan, saves file |
| `INSERT` / `UPDATE` / `DELETE` | Rebuilds affected indexes, updates files |
| `DROP INDEX name` | Removes from memory, catalog, and deletes file |
| `DROP TABLE` | Deletes all index files for that table |
| Startup | Loads from file; if missing, rebuilds from heap scan |
| Shutdown (`Drop`) | Flushes all indexes to files |

**Resilience:** If an index file is missing or corrupt on startup, BoolDB logs a warning and rebuilds it by scanning the table's heap. This means indexes are never a single point of failure — they can always be reconstructed from the source data.

### Order-Preserving Key Encoding

The key challenge is encoding different types into bytes while preserving their natural sort order. BoolDB uses a type-tagged encoding:

```
NULL:       [0x00]
Boolean:    [0x01, 0x00 or 0x01]
Integer:    [0x02, 8 bytes: XOR-flipped big-endian u64]
Float:      [0x03, 8 bytes: IEEE 754-flipped big-endian u64]
Text:       [0x04, UTF-8 bytes]
```

**Integer encoding trick:** To make signed integers sort correctly in byte order, we XOR with `1 << 63` to flip the sign bit, then store in big-endian:

```rust
let ordered = (*v as u64) ^ (1u64 << 63);
buf.extend_from_slice(&ordered.to_be_bytes());
```

This maps:
- `i64::MIN` (-9223372036854775808) → `0x0000000000000000`
- `-1` → `0x7FFFFFFFFFFFFFFF`
- `0` → `0x8000000000000000`
- `i64::MAX` → `0xFFFFFFFFFFFFFFFF`

**Float encoding trick:** IEEE 754 doubles are transformed so that byte comparison matches numerical order:

```rust
let bits = v.to_bits();
let ordered = if bits & (1u64 << 63) != 0 {
    !bits           // negative: flip all bits
} else {
    bits ^ (1u64 << 63)  // positive: flip sign bit
};
```

### Duplicate Key Support

Multiple RowIds can map to the same key. The index stores `Vec<RowId>` per key:

```rust
entries: BTreeMap<Vec<u8>, Vec<RowId>>
```

This is important for non-unique indexes (e.g., indexing a `name` column where multiple users share the same name).

---

## SQL Pipeline

### Parsing

**File:** `booldb-core/src/sql/parser.rs`

BoolDB uses the `sqlparser-rs` crate (version 0.43) with `GenericDialect` for SQL parsing. This provides a production-quality parser that handles standard SQL syntax.

The parser module wraps sqlparser and provides helper functions:

```rust
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>>
pub fn convert_data_type(sql_type: &SqlDataType) -> Result<DataType>
pub fn convert_column_def(col: &ColumnDef) -> Result<Column>
```

**Type mapping:**

| SQL Type(s) | BoolDB Type |
|------------|-------------|
| INT, INTEGER, BIGINT, SMALLINT, TINYINT | Integer |
| FLOAT, DOUBLE, DOUBLE PRECISION, REAL | Float |
| VARCHAR, CHAR, TEXT, STRING | Text |
| BOOLEAN | Boolean |

### Planning

**File:** `booldb-core/src/sql/planner.rs`

The planner converts a sqlparser AST `Statement` into a `LogicalPlan` — BoolDB's internal representation of what to execute.

```
Statement::CreateTable → LogicalPlan::CreateTable { schema }
Statement::Drop        → LogicalPlan::DropTable { table_name }
Statement::Insert      → LogicalPlan::Insert { table_name, columns, rows }
Statement::Query       → LogicalPlan::Select { table_name, projection, filter, joins }
Statement::Update      → LogicalPlan::Update { table_name, assignments, filter }
Statement::Delete      → LogicalPlan::Delete { table_name, filter }
```

#### Filter Expression Tree

WHERE clauses are parsed into a recursive `FilterExpr`:

```rust
enum FilterExpr {
    Comparison { column, op, value },  // e.g., age > 25
    And(Box<FilterExpr>, Box<FilterExpr>),
    Or(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    IsNull(column),
    IsNotNull(column),
}
```

This allows arbitrary nesting:
```sql
WHERE (age > 25 AND name = 'Alice') OR status IS NOT NULL
```

#### Value Extraction

SQL literals are converted to BoolDB `Value` types:

```rust
pub fn convert_expr_to_value(expr: &Expr) -> Result<Value>
```

- Numbers → `Integer(i64)` or `Float(f64)`
- Strings → `Text(String)`
- `true`/`false` → `Boolean(bool)`
- `NULL` → `Null`
- Unary minus → negated Integer or Float

### Optimization

**File:** `booldb-core/src/sql/optimizer.rs`

The optimizer analyzes a `LogicalPlan` and produces `QueryHints`:

```rust
struct QueryHints {
    index_scan: Option<IndexScanHint>,  // use index instead of seq scan
    needed_columns: Option<Vec<String>>, // for early projection
}
```

**Index selection:** For a simple `WHERE column = value` filter, the optimizer checks the catalog for an index on that column. If found, it suggests an index scan.

**EXPLAIN output:** The `explain()` function produces a human-readable execution plan:

```
SeqScan: users              (or IndexScan: users using idx_name)
  Filter: age > 25 AND name = 'Alice'
  InnerJoin: orders ON id = user_id
  Projection: id, name
```

### Execution

**File:** `booldb-core/src/sql/executor.rs`

The executor takes a `LogicalPlan` and evaluates it against the storage engine.

#### SELECT Execution Flow

```
1. Scan: HeapFile.scan() → Vec<Tuple>
2. Join: For each JoinClause, nested-loop join with right table
3. Filter: Evaluate FilterExpr against each row
4. Project: Select requested columns
5. Return: ExecResult::Rows { columns, rows }
```

#### Filter Evaluation

```rust
pub fn evaluate_filter(filter: &FilterExpr, row: &Row, schema: &Schema) -> bool
```

The evaluator:
1. Looks up the column index in the schema.
2. Extracts the value from the row.
3. Applies the comparison operator using `PartialOrd`.
4. Handles AND/OR/NOT with short-circuit evaluation.

#### JOIN Execution

Joins use the nested-loop algorithm with three modes:

**INNER JOIN:**
```
for left_row in left_table:
    for right_row in right_table:
        if left_row[left_col] == right_row[right_col]:
            emit concat(left_row, right_row)
```

**LEFT JOIN:**
```
for left_row in left_table:
    matched = false
    for right_row in right_table:
        if match:
            emit concat(left_row, right_row)
            matched = true
    if not matched:
        emit concat(left_row, NULLs)
```

**RIGHT JOIN:**
```
for right_row in right_table:
    matched = false
    for left_row in left_table:
        if match:
            emit concat(left_row, right_row)
            matched = true
    if not matched:
        emit concat(NULLs, right_row)
```

---

## Transaction Engine

### MVCC

**File:** `booldb-core/src/tx/mvcc.rs`

BoolDB implements Multi-Version Concurrency Control with snapshot isolation.

#### Transaction Lifecycle

```
begin()  → Transaction { tx_id, start_ts, active_snapshot }
           State: Active

commit() → Record commit timestamp
           State: Committed
           Writes become visible to future transactions

abort()  → Discard all changes
           State: Aborted
           Writes are invisible
```

#### Snapshot Isolation

When a transaction begins, it captures a snapshot of currently active transactions:

```rust
let active_at_start: Vec<u64> = self.active.keys().copied().collect();
```

A row written by transaction W is visible to reader R if:

```
W == R                                       → visible (own writes)
W committed AND commit_ts < R.start_ts
  AND W not in R.active_at_start             → visible
otherwise                                    → not visible
```

This prevents:
- **Dirty reads**: Uncommitted writes are never visible.
- **Non-repeatable reads**: A transaction always sees the same snapshot.
- **Phantom reads**: New rows inserted by concurrent transactions are invisible.

#### Timestamp Ordering

Both `tx_id` and timestamps use `AtomicU64` counters:

```rust
next_tx_id: AtomicU64,  // monotonically increasing transaction IDs
next_ts: AtomicU64,     // monotonically increasing timestamps
```

Each `begin()` consumes one timestamp. Each `commit()` also consumes one timestamp (the commit timestamp). This ensures a total ordering of events.

### Write-Ahead Log

**File:** `booldb-core/src/tx/wal.rs`

The WAL ensures durability by writing log records before modifying data pages.

#### Write Protocol (WAL rule)

```
1. Log the change to WAL (append record)
2. Flush WAL to disk
3. Modify the page in buffer pool
4. (Page written to disk later, on flush/eviction)
```

If the system crashes between steps 2 and 4, the WAL contains enough information to redo the change.

#### Log Record Format

On disk, each record is stored as:

```
[4 bytes: length (u32 LE)][length bytes: bincode-serialized LogRecord]
```

Records are append-only — the WAL file only grows during normal operation.

#### Recovery Algorithm

```
read all WAL records

pass 1 — identify outcomes:
    for each Commit record: add tx_id to committed set
    for each Abort record:  add tx_id to aborted set

pass 2 — redo committed writes:
    for each PageWrite record:
        if tx_id in committed set:
            apply after_image to page
        else:
            skip (uncommitted or aborted)

return list of (page_id, after_image) to apply
```

#### Checkpoint

A checkpoint record indicates that all dirty pages have been flushed. After a checkpoint, the WAL can be truncated because all changes are safely on disk.

```rust
pub fn truncate(&mut self) -> Result<()>
```

### Lock Manager

**File:** `booldb-core/src/tx/lock.rs`

The lock manager provides row-level concurrency control.

#### Lock Compatibility

```
           Request
           Shared    Exclusive
Held:
  None     GRANT     GRANT
  Shared   GRANT     DENY*
  Excl.    DENY      DENY
```

*Exception: If the requesting transaction is the **only** holder of the shared lock, it can upgrade to exclusive.

#### Implementation

```rust
struct LockEntry {
    mode: LockMode,
    holders: Vec<u64>,  // transaction IDs
}

struct LockManager {
    locks: HashMap<RowId, LockEntry>,
}
```

- `acquire(row_id, tx_id, mode)` — Try to acquire a lock. Returns `LockConflict` error on failure.
- `release_all(tx_id)` — Release all locks held by a transaction (called on commit/abort).
- `release(row_id, tx_id)` — Release a specific lock.

---

## Networking

### Server

**File:** `booldb-server/src/server.rs`

The server uses Tokio for async I/O. It accepts TCP connections and spawns a task per client:

```rust
loop {
    let (stream, addr) = listener.accept().await?;
    let db = Arc::clone(&db);
    tokio::spawn(async move {
        session::handle_connection(stream, db).await;
    });
}
```

The database is shared across connections via `Arc<Mutex<Database>>`. The mutex ensures serialized access to the single-threaded database engine.

### Session

**File:** `booldb-server/src/session.rs`

Each client connection runs in a loop:

```
loop:
    read length-prefixed message
    deserialize JSON Request
    lock database mutex
    execute SQL
    unlock mutex
    serialize Response
    write length-prefixed message
```

### Protocol

**File:** `booldb-server/src/protocol.rs`

All messages use a simple framing protocol:

```
[4 bytes: big-endian u32 length][N bytes: JSON payload]
```

This makes it trivial to implement clients in any language.

---

## Key Design Decisions

### Why page-based storage?

Page-based storage is the industry standard for databases (PostgreSQL, MySQL InnoDB, SQLite). Benefits:
- Aligns with OS page size and disk sector size for efficient I/O.
- Buffer pool can cache hot pages in memory.
- Enables page-level locking and logging.

### Why slotted pages?

Compared to fixed-length record formats, slotted pages handle variable-length rows naturally. Compared to log-structured storage, they support efficient in-place updates and random access.

### Why clock eviction instead of LRU?

True LRU requires maintaining a doubly-linked list, which has high overhead per access. The clock algorithm approximates LRU with a simple circular scan, achieving similar hit rates with less bookkeeping.

### Why sqlparser-rs?

Writing a SQL parser is a large, error-prone task. `sqlparser-rs` is mature (used by Apache DataFusion, GlueSQL, and others), handles edge cases correctly, and supports a wide SQL dialect.

### Why bincode for serialization?

Bincode is compact and fast — significantly smaller and faster than JSON or MessagePack for structured Rust data. It is used for both row storage and catalog persistence.

### Why Arc<Mutex<Database>> for the server?

BoolDB's storage engine uses `&mut self` (exclusive access) for most operations. Wrapping in `Mutex` ensures correctness. For a production system, this would be replaced with finer-grained locking or a worker-thread model, but it keeps the initial implementation simple and correct.

### Why nested-loop joins?

Nested-loop is the simplest join algorithm and works correctly for all join types. More advanced algorithms (hash join, sort-merge join) would improve performance for large tables but add significant complexity. Nested-loop is the right starting point.

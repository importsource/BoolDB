# Contributing to BoolDB

This guide helps you get started as a contributor to BoolDB.

## Project Structure

```
BoolDB/
├── Cargo.toml                 Workspace root
├── README.md                  User-facing documentation
├── docs/
│   ├── architecture.md        Deep technical internals
│   ├── tutorial.md            Step-by-step usage guide
│   └── contributing.md        This file
├── booldb-core/               Core database engine (library)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs             Module declarations
│       ├── types.rs           Value, Row, Schema, Column, RowId
│       ├── error.rs           BoolDBError enum
│       ├── db.rs              Database struct (top-level API)
│       ├── storage/
│       │   ├── mod.rs
│       │   ├── page.rs        4 KB slotted page
│       │   ├── disk.rs        File-based page I/O
│       │   ├── buffer.rs      Buffer pool with clock eviction
│       │   └── heap.rs        Unordered row storage
│       ├── catalog/
│       │   ├── mod.rs
│       │   └── schema.rs      Table/index metadata
│       ├── index/
│       │   ├── mod.rs
│       │   └── btree.rs       In-memory B-Tree index
│       ├── sql/
│       │   ├── mod.rs
│       │   ├── parser.rs      SQL parsing (sqlparser-rs)
│       │   ├── planner.rs     AST → LogicalPlan
│       │   ├── optimizer.rs   Query hints, EXPLAIN
│       │   └── executor.rs    Plan evaluation
│       └── tx/
│           ├── mod.rs
│           ├── wal.rs         Write-ahead log
│           ├── mvcc.rs        MVCC transaction manager
│           └── lock.rs        Row-level lock manager
├── booldb-server/             TCP server (binary)
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs            Entry point, env config
│       ├── server.rs          Tokio TCP listener
│       ├── session.rs         Per-connection handler
│       └── protocol.rs        Wire protocol (length-prefixed JSON)
└── booldb-cli/                CLI client (binary)
    ├── Cargo.toml
    └── src/
        └── main.rs            REPL with rustyline
```

## Development Setup

```bash
# Build everything
cargo build --workspace

# Run all tests
cargo test --workspace

# Run tests for a specific crate
cargo test -p booldb-core

# Run a specific test
cargo test -p booldb-core -- db::tests::test_join

# Check for warnings without building
cargo check --workspace

# Run clippy lints
cargo clippy --workspace
```

## Code Conventions

### Module Organization

- Each module has a `mod.rs` that re-exports submodules.
- Public APIs are documented with `///` doc comments.
- Internal-only functions are private (no `pub`).
- Tests live in the same file as the code they test, in a `#[cfg(test)] mod tests` block.

### Error Handling

- All fallible operations return `Result<T, BoolDBError>` (aliased as `Result<T>`).
- Use `?` for error propagation.
- Add new error variants to `BoolDBError` in `error.rs` when needed.
- Provide descriptive error messages that help the user fix the problem.

### Naming

- Types: `PascalCase` (`PageId`, `HeapFile`, `LogicalPlan`)
- Functions: `snake_case` (`fetch_page`, `execute`, `plan_select`)
- Constants: `SCREAMING_SNAKE_CASE` (`PAGE_SIZE`, `HEADER_SIZE`)
- Test functions: `test_` prefix (`test_insert_and_scan`)

### Testing

- Every module should have tests covering the happy path and edge cases.
- Tests create temp files in `std::env::temp_dir()/booldb_test/` and clean up after themselves.
- Use descriptive test names that explain what's being tested.

## Areas for Contribution

Here are concrete tasks organized by difficulty:

### Beginner

1. **Add `\d TABLE` command to CLI** — Describe a table's columns, types, and constraints. Requires adding a meta-query or a new protocol message.

2. **Add `COUNT(*)` support** — Parse `SELECT COUNT(*) FROM table`, return a single row with the count.

3. **Add `ORDER BY` support** — Sort results by one or more columns (ASC/DESC).

4. **Add `LIMIT` and `OFFSET`** — Restrict the number of returned rows.

5. **Improve error messages** — Add context to SQL errors (line/column number, suggestion).

### Intermediate

6. **Disk-based B-Tree** — Replace the in-memory `BTreeMap` index with a B+Tree stored on pages. This involves:
   - Designing internal and leaf node page layouts
   - Implementing search, insert, and split operations
   - Handling overflow pages for large keys

7. **Hash Join** — Implement a hash-based join algorithm for better performance on large tables:
   - Build phase: Hash the smaller table on the join key
   - Probe phase: Scan the larger table and look up matches

8. **Aggregate functions** — Support `SUM`, `AVG`, `MIN`, `MAX`, `COUNT` with `GROUP BY`.

9. **Disk-based B+Tree pages** — Replace the in-memory `BTreeMap` with a B+Tree stored on disk pages for indexes that exceed memory. Currently indexes are serialized whole to `index_{name}.bin` files; a page-based B+Tree would scale to much larger datasets.

10. **Connection pooling** — Replace `Arc<Mutex<Database>>` with a connection pool that allows concurrent read transactions.

### Advanced

11. **Full WAL integration** — Wire the WAL into the execution path so every page modification is logged. Implement crash recovery on startup.

12. **MVCC-integrated executor** — Modify the executor to use transaction snapshots for visibility checks. Each row would carry version metadata (created_by, deleted_by transaction IDs).

13. **Prepared statements** — Parse once, execute many times with different parameters. Requires a parameter binding protocol.

14. **Subqueries** — Support `WHERE col IN (SELECT ...)` and `FROM (SELECT ...) AS alias`.

15. **Write-optimized storage** — Implement a log-structured merge tree (LSM) as an alternative storage engine.

## Adding a New SQL Command

Here's a walkthrough of adding a new SQL command (e.g., `TRUNCATE TABLE`):

### Step 1: Planner

In `sql/planner.rs`, add a new `LogicalPlan` variant:

```rust
pub enum LogicalPlan {
    // ... existing variants ...
    TruncateTable { table_name: String },
}
```

Add a match arm in `plan_statement()`:

```rust
Statement::Truncate { table_name, .. } => {
    Ok(LogicalPlan::TruncateTable {
        table_name: table_name.to_string(),
    })
}
```

### Step 2: Executor

In `sql/executor.rs`, add a handler:

```rust
LogicalPlan::TruncateTable { table_name } => {
    exec_truncate(table_name, catalog, heaps, pool)
}
```

Implement the function:

```rust
fn exec_truncate(
    table_name: &str,
    catalog: &mut Catalog,
    heaps: &mut HashMap<String, HeapFile>,
    pool: &mut BufferPool,
) -> Result<ExecResult> {
    // Verify table exists
    let _meta = catalog.get_table(table_name)?;

    // Replace heap file with empty one
    heaps.insert(table_name.to_string(), HeapFile::new(table_name));

    // Update catalog
    let meta = catalog.get_table_mut(table_name)?;
    meta.heap_page_ids.clear();

    Ok(ExecResult::Ok {
        message: format!("Table '{}' truncated", table_name),
    })
}
```

### Step 3: Optimizer

In `sql/optimizer.rs`, add EXPLAIN support:

```rust
LogicalPlan::TruncateTable { table_name } => {
    lines.push(format!("TruncateTable: {}", table_name));
}
```

### Step 4: Tests

In `db.rs`, add a test:

```rust
#[test]
fn test_truncate() {
    let dir = tmp_dir("test_truncate");
    let mut db = Database::open(&dir).unwrap();

    db.execute("CREATE TABLE t (id INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    db.execute("INSERT INTO t VALUES (2)").unwrap();
    db.execute("TRUNCATE TABLE t").unwrap();

    match db.execute("SELECT * FROM t").unwrap() {
        ExecResult::Rows { rows, .. } => assert_eq!(rows.len(), 0),
        _ => panic!("Expected Rows"),
    }

    // Table still exists
    assert!(db.table_names().contains(&"t".to_string()));

    std::fs::remove_dir_all(&dir).unwrap();
}
```

### Step 5: Run Tests

```bash
cargo test -p booldb-core -- test_truncate
cargo test --workspace
```

## Debugging Tips

### Enable Logging

Add `eprintln!` statements for debugging. The server already prints session events to stderr.

### Inspect Database Files

```bash
# Check data file size (number of pages = size / 4096)
ls -la booldb_data/data.db

# Hex dump a page (page 0 starts at offset 0)
xxd -l 4096 booldb_data/data.db

# View catalog (binary, but structure is visible)
xxd booldb_data/catalog.bin | head
```

### Run Specific Tests with Output

```bash
cargo test -p booldb-core -- test_name --nocapture
```

### Test with a Fresh Database

```bash
rm -rf booldb_data
cargo run -p booldb-server
```

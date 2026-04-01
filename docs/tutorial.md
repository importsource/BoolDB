# BoolDB Tutorial

This step-by-step tutorial walks you through using BoolDB, from basic operations to advanced features.

## Prerequisites

- Rust toolchain (1.70+)
- Terminal / command line

## 1. Build and Start

```bash
# Clone and build
cd BoolDB
cargo build --release

# Start the server (Terminal 1)
cargo run --release -p booldb-server
```

You should see:
```
Starting BoolDB server...
BoolDB server listening on 127.0.0.1:5433
Data directory: ./booldb_data
```

```bash
# Connect with the CLI (Terminal 2)
cargo run --release -p booldb-cli
```

You should see:
```
BoolDB CLI - Connecting to 127.0.0.1:5433...
Connected. Type SQL statements or \q to quit.
```

## 2. Creating Tables

Let's build a simple application schema — a bookstore.

```sql
CREATE TABLE authors (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT
);

CREATE TABLE books (
    id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    price FLOAT,
    in_stock BOOLEAN
);

CREATE TABLE reviews (
    id INTEGER PRIMARY KEY,
    book_id INTEGER NOT NULL,
    rating INTEGER,
    comment TEXT
);
```

Each `CREATE TABLE` should respond with:
```
Table 'authors' created
```

## 3. Inserting Data

### Authors

```sql
INSERT INTO authors VALUES (1, 'Jane Austen', 'England');
INSERT INTO authors VALUES (2, 'Mark Twain', 'USA');
INSERT INTO authors VALUES (3, 'Leo Tolstoy', 'Russia');
INSERT INTO authors VALUES (4, 'Gabriel Garcia Marquez', 'Colombia');
```

### Books

```sql
INSERT INTO books VALUES (1, 'Pride and Prejudice', 1, 12.99, true);
INSERT INTO books VALUES (2, 'Sense and Sensibility', 1, 11.50, true);
INSERT INTO books VALUES (3, 'Adventures of Huckleberry Finn', 2, 9.99, false);
INSERT INTO books VALUES (4, 'War and Peace', 3, 15.99, true);
INSERT INTO books VALUES (5, 'Anna Karenina', 3, 14.50, true);
INSERT INTO books VALUES (6, 'One Hundred Years of Solitude', 4, 13.99, true);
```

### Reviews

```sql
INSERT INTO reviews VALUES (1, 1, 5, 'A timeless classic');
INSERT INTO reviews VALUES (2, 1, 4, 'Wonderful characters');
INSERT INTO reviews VALUES (3, 4, 5, 'Epic masterpiece');
INSERT INTO reviews VALUES (4, 6, 5, 'Magical realism at its best');
INSERT INTO reviews VALUES (5, 3, 3, 'Good but dated');
INSERT INTO reviews VALUES (6, 5, 4, 'Deeply moving');
```

Each INSERT responds with:
```
1 row(s) affected
```

## 4. Querying Data

### Select All Rows

```sql
SELECT * FROM books;
```

Output:
```
 id | title                          | author_id | price | in_stock
----+--------------------------------+-----------+-------+----------
 1  | Pride and Prejudice            | 1         | 12.99 | true
 2  | Sense and Sensibility          | 1         | 11.5  | true
 3  | Adventures of Huckleberry Finn | 2         | 9.99  | false
 4  | War and Peace                  | 3         | 15.99 | true
 5  | Anna Karenina                  | 3         | 14.5  | true
 6  | One Hundred Years of Solitude  | 4         | 13.99 | true

(6 row(s))
```

### Select Specific Columns

```sql
SELECT title, price FROM books;
```

### Filtering with WHERE

**Comparison operators:**

```sql
-- Books under $13
SELECT title, price FROM books WHERE price < 13;

-- Books in stock
SELECT title FROM books WHERE in_stock = true;

-- Books by a specific author
SELECT title FROM books WHERE author_id = 3;
```

**Compound conditions:**

```sql
-- In stock AND under $14
SELECT title, price FROM books WHERE in_stock = true AND price < 14;

-- By Austen OR Tolstoy
SELECT title, author_id FROM books WHERE author_id = 1 OR author_id = 3;
```

**NULL checks:**

```sql
-- Authors with known country
SELECT name FROM authors WHERE country IS NOT NULL;
```

## 5. Joins

### INNER JOIN — Books with Author Names

```sql
SELECT * FROM books INNER JOIN authors ON author_id = id;
```

This joins every book with its author, showing all columns from both tables.

### LEFT JOIN — All Authors, Even Without Books

Suppose we add an author with no books:

```sql
INSERT INTO authors VALUES (5, 'New Author', NULL);
```

```sql
SELECT * FROM authors LEFT JOIN books ON id = author_id;
```

Authors without books will have NULL values in the book columns.

### RIGHT JOIN — All Books, Even If Author Is Missing

```sql
SELECT * FROM books RIGHT JOIN authors ON author_id = id;
```

### Multi-Table Analysis

To see books with their reviews:

```sql
SELECT * FROM books INNER JOIN reviews ON id = book_id;
```

## 6. Updating Data

### Update a Single Row

```sql
UPDATE books SET price = 10.99 WHERE id = 3;
```

Verify:
```sql
SELECT title, price FROM books WHERE id = 3;
```

### Update Multiple Rows

```sql
-- Mark all books under $12 as not in stock
UPDATE books SET in_stock = false WHERE price < 12;
```

### Update Multiple Columns

```sql
UPDATE authors SET name = 'Samuel Clemens', country = 'United States' WHERE id = 2;
```

## 7. Deleting Data

### Delete Specific Rows

```sql
DELETE FROM reviews WHERE rating < 4;
```

### Delete with Compound Conditions

```sql
DELETE FROM books WHERE in_stock = false AND price < 11;
```

### Verify After Delete

```sql
SELECT * FROM reviews;
SELECT * FROM books;
```

## 8. Using EXPLAIN

See how BoolDB plans to execute a query:

```sql
EXPLAIN SELECT * FROM books WHERE price > 12;
```

Output:
```
 plan
---------------------------------
 SeqScan: books
   Filter: price > 12
   Projection: *

(3 row(s))
```

```sql
EXPLAIN SELECT title FROM books WHERE author_id = 1;
```

```sql
EXPLAIN SELECT * FROM books INNER JOIN authors ON author_id = id;
```

## 9. Creating and Using Indexes

Indexes speed up queries that filter on a specific column. BoolDB automatically creates indexes for PRIMARY KEY columns, but you can also create your own.

### Automatic Primary Key Indexes

When you created the `books` table with `id INTEGER PRIMARY KEY`, BoolDB automatically created an index named `pk_books_id`. You can verify this with EXPLAIN:

```sql
EXPLAIN SELECT * FROM books WHERE id = 1;
```

If the optimizer detects a matching index, EXPLAIN will show `IndexScan` instead of `SeqScan`.

### Creating Manual Indexes

Create an index on a frequently filtered column:

```sql
CREATE INDEX idx_books_author ON books (author_id);
```

Output:
```
Index 'idx_books_author' created on books.author_id (6 entries)
```

Now queries filtering on `author_id` can use this index:

```sql
EXPLAIN SELECT * FROM books WHERE author_id = 3;
```

### Index Persistence

Indexes survive server restarts. B+Tree index pages are stored in `data.db` alongside table data — there are no separate index files. The catalog remembers each index's root page ID, which is all BoolDB needs to restore the full tree on startup.

```
booldb_data/
├── data.db       ← contains both table data AND B+Tree index pages
└── catalog.bin   ← stores index root page IDs
```

If an index's root page ID is missing (e.g., after a migration), BoolDB automatically rebuilds it by scanning the table on startup.

### Dropping Indexes

Remove an index you no longer need:

```sql
DROP INDEX idx_books_author;
```

This removes the index from memory and the catalog. The underlying table data is not affected.

**Note:** Indexes are automatically maintained when you INSERT, UPDATE, or DELETE rows — you don't need to do anything manually.

## 10. Dropping Tables

When you're done with a table:

```sql
DROP TABLE reviews;
DROP TABLE books;
DROP TABLE authors;
```

## 11. Data Persistence

BoolDB persists data to disk. To verify:

1. Insert some data.
2. Stop the server (Ctrl+C).
3. Restart the server.
4. Reconnect with the CLI.
5. Query — your data is still there.

```bash
# Terminal 1: Stop server with Ctrl+C
# Terminal 1: Restart
cargo run --release -p booldb-server

# Terminal 2: Reconnect
cargo run --release -p booldb-cli
booldb> SELECT * FROM books;  -- data persists!
```

## 12. Using BoolDB as a Library

You can embed BoolDB directly in your Rust application without the TCP server:

```rust
use booldb_core::db::Database;
use booldb_core::sql::executor::ExecResult;

fn main() {
    let mut db = Database::open("./my_app_data").unwrap();

    // Create schema
    db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")
        .unwrap();

    // Insert data
    db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)").unwrap();
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 19.99)").unwrap();

    // Query
    match db.execute("SELECT * FROM products WHERE price > 10").unwrap() {
        ExecResult::Rows { columns, rows } => {
            println!("Columns: {:?}", columns);
            for row in &rows {
                println!("  {:?}", row);
            }
        }
        _ => unreachable!(),
    }

    // Clean up
    db.execute("DROP TABLE products").unwrap();
}
```

Add to your `Cargo.toml`:
```toml
[dependencies]
booldb-core = { path = "path/to/booldb-core" }
```

## 13. Writing a Custom TCP Client

Any language can connect to BoolDB. Here's a Node.js example:

```javascript
const net = require('net');

const client = net.createConnection({ port: 5433 }, () => {
    sendQuery('CREATE TABLE test (id INTEGER, val TEXT)');
});

function sendQuery(sql) {
    const payload = Buffer.from(JSON.stringify({ sql }));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length);
    client.write(Buffer.concat([header, payload]));
}

let buffer = Buffer.alloc(0);
client.on('data', (data) => {
    buffer = Buffer.concat([buffer, data]);
    while (buffer.length >= 4) {
        const len = buffer.readUInt32BE(0);
        if (buffer.length < 4 + len) break;
        const payload = buffer.slice(4, 4 + len);
        buffer = buffer.slice(4 + len);
        const response = JSON.parse(payload.toString());
        console.log(response);

        // Send next query or close
    }
});
```

## Tips and Best Practices

1. **Always use PRIMARY KEY** on your ID columns for data integrity.
2. **Use NOT NULL** constraints where appropriate to prevent missing data.
3. **Use EXPLAIN** to understand query execution before optimizing.
4. **Use specific column names** in SELECT instead of `*` for better performance.
5. **Filter early** — put the most selective conditions first in WHERE clauses.
6. **Stop the server cleanly** (Ctrl+C) to ensure all data is flushed to disk.

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `Table 'x' not found` | Table doesn't exist | Check spelling, create the table first |
| `Table 'x' already exists` | Duplicate CREATE TABLE | Drop the table first, or use a different name |
| `Expected N values, got M` | Wrong number of INSERT values | Match the number of values to the number of columns |
| `Column 'x' not found` | Typo in column name | Check the table schema |
| `Failed to connect` | Server not running | Start the server first |
| `Lost connection to server` | Server crashed or stopped | Restart the server |

## Next Steps

- Read the [Architecture Guide](architecture.md) to understand how BoolDB works internally.
- Look at the test suite (`cargo test --workspace`) for examples of all features.
- Try writing a client in your favorite programming language using the [wire protocol](../README.md#client-server-protocol).

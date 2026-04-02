# JSON Support

BoolDB supports storing, querying, and indexing JSON data. This guide covers everything you need to know to use JSON columns effectively.

## Table of Contents

- [Overview](#overview)
- [Creating Tables with JSON Columns](#creating-tables-with-json-columns)
- [Inserting JSON Data](#inserting-json-data)
- [Extracting Values with json_extract()](#extracting-values-with-json_extract)
- [Filtering with WHERE](#filtering-with-where)
- [Multiple Filter Criteria](#multiple-filter-criteria)
- [JSONPath Syntax](#jsonpath-syntax)
- [Expression Indexes](#expression-indexes)
- [Type Mapping](#type-mapping)
- [Limitations](#limitations)
- [Examples](#examples)

## Overview

BoolDB's JSON support follows the same model as MySQL and SQLite:

1. A `JSON` column type stores validated JSON strings.
2. The `json_extract(column, '$.path')` function extracts scalar values from JSON at query time.
3. Expression indexes on `json_extract()` accelerate filtered queries on specific JSON paths.

This allows you to store flexible, schema-less data alongside structured columns while still querying it with standard SQL.

## Creating Tables with JSON Columns

Use the `JSON` data type for columns that store JSON:

```sql
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    event_type TEXT,
    data JSON
);

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    profile JSON
);
```

You can mix JSON columns with any other column types.

## Inserting JSON Data

Insert JSON as a single-quoted string. BoolDB validates that the value is valid JSON on insert:

```sql
-- Simple object
INSERT INTO events VALUES (1, 'click', '{"page": "/home", "duration": 3.5}');

-- Nested objects
INSERT INTO users VALUES (1, '{"name": "Alice", "age": 30, "address": {"city": "NYC", "zip": "10001"}}');

-- Arrays
INSERT INTO events VALUES (2, 'purchase', '{"items": ["widget", "gadget"], "total": 29.99}');

-- Mixed types
INSERT INTO users VALUES (2, '{"name": "Bob", "age": 25, "active": true, "notes": null}');
```

Invalid JSON will produce an error:

```sql
INSERT INTO events VALUES (3, 'err', 'not valid json');
-- ERROR: Invalid JSON: expected value at line 1 column 1
```

## Extracting Values with json_extract()

The `json_extract(column, '$.path')` function extracts a value from a JSON column at the specified path.

### In SELECT

```sql
-- Extract top-level fields
SELECT json_extract(data, '$.page'), json_extract(data, '$.duration') FROM events;

-- Mix with regular columns
SELECT id, event_type, json_extract(data, '$.total') FROM events;

-- Extract from nested objects
SELECT json_extract(profile, '$.address.city') FROM users;

-- Extract array elements
SELECT json_extract(data, '$.items[0]') FROM events;
```

**Output column names** are automatically generated as `json_extract(column, 'path')`:

```
booldb> SELECT json_extract(profile, '$.name'), json_extract(profile, '$.age') FROM users;
 json_extract(profile, '$.name') | json_extract(profile, '$.age')
---------------------------------+--------------------------------
 Alice                           | 30
 Bob                             | 25

(2 row(s))
```

### In WHERE

```sql
-- Filter by extracted value
SELECT * FROM users WHERE json_extract(profile, '$.age') > 25;

-- String equality
SELECT * FROM events WHERE json_extract(data, '$.page') = '/home';

-- Boolean values
SELECT * FROM users WHERE json_extract(profile, '$.active') = true;
```

## Multiple Filter Criteria

You can combine multiple `json_extract()` conditions with `AND` and `OR`, just like regular WHERE clauses:

### AND — All conditions must match

```sql
-- Age over 25 AND lives in NYC
SELECT * FROM users
WHERE json_extract(profile, '$.age') > 25
  AND json_extract(profile, '$.address.city') = 'NYC';
```

### OR — Any condition matches

```sql
-- Name is Alice OR name is Bob
SELECT * FROM users
WHERE json_extract(profile, '$.name') = 'Alice'
   OR json_extract(profile, '$.name') = 'Bob';
```

### Mixed with regular columns

```sql
-- Regular column + JSON filter
SELECT * FROM events
WHERE event_type = 'purchase'
  AND json_extract(data, '$.total') > 20;

-- Multiple mixed conditions
SELECT * FROM events
WHERE id >= 10
  AND json_extract(data, '$.page') = '/checkout'
  AND json_extract(data, '$.duration') < 5;
```

### Complex nesting

```sql
-- (A AND B) OR C
SELECT * FROM users
WHERE (json_extract(profile, '$.age') > 30 AND json_extract(profile, '$.address.city') = 'NYC')
   OR json_extract(profile, '$.name') = 'Admin';
```

## JSONPath Syntax

BoolDB supports the following JSONPath expressions:

| Path | Description | Example JSON | Result |
|------|-------------|-------------|--------|
| `$.field` | Top-level field | `{"name": "Alice"}` | `"Alice"` |
| `$.a.b` | Nested field | `{"a": {"b": 1}}` | `1` |
| `$.a.b.c` | Deep nesting | `{"a": {"b": {"c": true}}}` | `true` |
| `$.arr[0]` | Array element | `{"arr": ["x", "y"]}` | `"x"` |
| `$.arr[1].name` | Array + field | `{"arr": [{"name": "A"}]}` | (index 1 not found → NULL) |
| `$` | Root element | `{"x": 1}` | `{"x": 1}` (as text) |

**Missing paths return NULL:**

```sql
-- If the path doesn't exist, json_extract returns NULL
SELECT json_extract(profile, '$.nonexistent') FROM users;
-- Returns: NULL
```

**Objects and arrays are returned as JSON text:**

```sql
-- Extracting a nested object returns its JSON representation
SELECT json_extract(profile, '$.address') FROM users;
-- Returns: '{"city":"NYC","zip":"10001"}'
```

## Expression Indexes

Expression indexes accelerate queries that filter on specific JSON paths. Without an index, every `json_extract()` in a WHERE clause requires a full table scan. With an expression index, BoolDB can look up matching rows directly.

### Creating an Expression Index

```sql
CREATE INDEX index_name ON table_name (json_extract(column, '$.path'));
```

**Examples:**

```sql
-- Index on user names
CREATE INDEX idx_user_name ON users (json_extract(profile, '$.name'));

-- Index on nested city
CREATE INDEX idx_user_city ON users (json_extract(profile, '$.address.city'));

-- Index on event pages
CREATE INDEX idx_event_page ON events (json_extract(data, '$.page'));
```

### How It Works

When you create an expression index:

1. BoolDB scans all existing rows in the table.
2. For each row, it extracts the value at the specified JSON path.
3. The extracted scalar value is inserted into a B+Tree index.
4. The root page ID is stored in the catalog.

When you INSERT, UPDATE, or DELETE rows, the index is automatically rebuilt to stay consistent.

### What Gets Indexed

The extracted value is stored in the B+Tree as a regular scalar:

| JSON value at path | Indexed as |
|-------------------|------------|
| `"Alice"` | `Value::Text("Alice")` |
| `30` | `Value::Integer(30)` |
| `3.14` | `Value::Float(3.14)` |
| `true` | `Value::Boolean(true)` |
| `null` | `Value::Null` |
| `{"a": 1}` (object) | `Value::Text("{\"a\":1}")` |
| `["x"]` (array) | `Value::Text("[\"x\"]")` |
| (path missing) | `Value::Null` |

### Viewing Expression Indexes

```sql
SHOW INDEXES ON users;
```

Output:
```
 table | index_name    | column  | entries | depth
-------+---------------+---------+---------+-------
 users | idx_user_name | profile | 100     | 1
 users | pk_users_id   | id      | 100     | 1

(2 row(s))
```

### Dropping Expression Indexes

```sql
DROP INDEX idx_user_name;
```

### Index Persistence

Expression indexes are stored as B+Tree pages in `data.db` — the same file as table data. The catalog stores the JSON path along with the index metadata. On restart, the index is immediately available without rebuilding.

### Query Planning with EXPLAIN

Use EXPLAIN to see if a query would benefit from an index:

```sql
EXPLAIN SELECT * FROM users WHERE json_extract(profile, '$.name') = 'Alice';
```

Output (without index):
```
SeqScan: users
  Filter: json_extract(profile, '$.name') = Alice
  Projection: *
```

After `CREATE INDEX idx_name ON users (json_extract(profile, '$.name'))`, the optimizer can detect the matching index.

## Type Mapping

When `json_extract()` pulls a value from JSON, it maps to BoolDB types:

| JSON Type | BoolDB Type | Comparison behavior |
|-----------|-------------|-------------------|
| String | `TEXT` | Lexicographic |
| Integer | `INTEGER` | Numeric (i64) |
| Float | `FLOAT` | Numeric (f64) |
| Boolean | `BOOLEAN` | false < true |
| Null | `NULL` | NULL is less than any non-NULL |
| Object | `TEXT` | JSON string representation |
| Array | `TEXT` | JSON string representation |

This means comparisons work naturally:

```sql
-- Numeric comparison (age is stored as JSON number)
WHERE json_extract(data, '$.age') > 25

-- String comparison
WHERE json_extract(data, '$.name') = 'Alice'

-- Boolean comparison
WHERE json_extract(data, '$.active') = true
```

## Limitations

1. **No JSON modification** — There is no `json_set()`, `json_insert()`, or `json_remove()`. To modify JSON data, UPDATE the entire column with a new JSON string.

2. **No wildcard paths** — Paths like `$.users[*].name` or `$..name` (recursive descent) are not supported. Each path must point to a specific location.

3. **No JSON operators** — PostgreSQL-style operators (`->`, `->>`, `@>`, `?`) are not supported. Use `json_extract()` instead.

4. **No schema validation** — The `JSON` type only validates that the value is syntactically valid JSON. There is no JSON Schema enforcement.

5. **Index on single path** — Each expression index covers one JSON path. To index multiple paths, create multiple indexes.

6. **Full rebuild on mutation** — When rows are inserted, updated, or deleted, affected expression indexes are rebuilt by scanning the table. For very large tables with frequent writes, this can be a bottleneck.

## Examples

### Example 1: User Profiles

```sql
CREATE TABLE users (id INTEGER PRIMARY KEY, profile JSON);

INSERT INTO users VALUES (1, '{"name": "Alice", "age": 30, "role": "admin", "settings": {"theme": "dark", "lang": "en"}}');
INSERT INTO users VALUES (2, '{"name": "Bob", "age": 25, "role": "user", "settings": {"theme": "light", "lang": "fr"}}');
INSERT INTO users VALUES (3, '{"name": "Charlie", "age": 35, "role": "admin", "settings": {"theme": "dark", "lang": "en"}}');

-- Find all admins
SELECT id, json_extract(profile, '$.name') FROM users
WHERE json_extract(profile, '$.role') = 'admin';

-- Find users with dark theme who are over 30
SELECT json_extract(profile, '$.name'), json_extract(profile, '$.age') FROM users
WHERE json_extract(profile, '$.settings.theme') = 'dark'
  AND json_extract(profile, '$.age') > 30;

-- Create index for frequent role lookups
CREATE INDEX idx_role ON users (json_extract(profile, '$.role'));
```

### Example 2: Event Tracking

```sql
CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, data JSON);

INSERT INTO events VALUES (1, 1000, '{"type": "pageview", "url": "/home", "user_id": 42}');
INSERT INTO events VALUES (2, 1001, '{"type": "click", "element": "buy_btn", "user_id": 42}');
INSERT INTO events VALUES (3, 1002, '{"type": "pageview", "url": "/checkout", "user_id": 42}');
INSERT INTO events VALUES (4, 1003, '{"type": "purchase", "amount": 99.99, "user_id": 42}');
INSERT INTO events VALUES (5, 1004, '{"type": "pageview", "url": "/home", "user_id": 99}');

-- Find all pageviews
SELECT id, json_extract(data, '$.url') FROM events
WHERE json_extract(data, '$.type') = 'pageview';

-- Find expensive purchases
SELECT * FROM events
WHERE json_extract(data, '$.type') = 'purchase'
  AND json_extract(data, '$.amount') > 50;

-- Find events for a specific user after a timestamp
SELECT * FROM events
WHERE ts > 1001 AND json_extract(data, '$.user_id') = 42;

-- Index on event type for fast filtering
CREATE INDEX idx_event_type ON events (json_extract(data, '$.type'));
```

### Example 3: Product Catalog

```sql
CREATE TABLE products (id INTEGER PRIMARY KEY, info JSON);

INSERT INTO products VALUES (1, '{"name": "Widget", "price": 9.99, "tags": ["sale", "popular"], "specs": {"weight": 0.5, "color": "red"}}');
INSERT INTO products VALUES (2, '{"name": "Gadget", "price": 19.99, "tags": ["new"], "specs": {"weight": 1.2, "color": "blue"}}');
INSERT INTO products VALUES (3, '{"name": "Doohickey", "price": 4.99, "tags": ["sale", "clearance"], "specs": {"weight": 0.1, "color": "red"}}');

-- Find products under $10
SELECT json_extract(info, '$.name'), json_extract(info, '$.price') FROM products
WHERE json_extract(info, '$.price') < 10;

-- Find red products
SELECT json_extract(info, '$.name') FROM products
WHERE json_extract(info, '$.specs.color') = 'red';

-- Find first tag of each product
SELECT json_extract(info, '$.name'), json_extract(info, '$.tags[0]') FROM products;

-- Index on price for range queries
CREATE INDEX idx_price ON products (json_extract(info, '$.price'));
```

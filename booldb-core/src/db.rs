use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::catalog::schema::{Catalog, IndexMeta};
use crate::error::{BoolDBError, Result};
use crate::index::disk_btree::DiskBTree;
use crate::sql::executor::{self, ExecResult};
use crate::sql::planner;
use crate::storage::buffer::BufferPool;
use crate::storage::disk::DiskManager;
use crate::storage::heap::HeapFile;
use crate::types::Value;

const DEFAULT_BUFFER_POOL_SIZE: usize = 256;
const CATALOG_FILE: &str = "catalog.bin";
const DATA_FILE: &str = "data.db";

/// The main database engine.
pub struct Database {
    data_dir: PathBuf,
    catalog: Catalog,
    heaps: HashMap<String, HeapFile>,
    /// Disk-based B+Tree indexes, keyed by index name.
    indexes: HashMap<String, DiskBTree>,
    pool: BufferPool,
}

impl Database {
    /// Open or create a database at the given directory.
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;

        let data_path = data_dir.join(DATA_FILE);
        let disk = DiskManager::open(&data_path)?;
        let pool = BufferPool::new(disk, DEFAULT_BUFFER_POOL_SIZE);

        // Load catalog if it exists.
        let catalog_path = data_dir.join(CATALOG_FILE);
        let catalog = if catalog_path.exists() {
            let data = std::fs::read(&catalog_path)?;
            Catalog::from_bytes(&data)?
        } else {
            Catalog::new()
        };

        // Rebuild heap files from catalog metadata.
        let mut heaps = HashMap::new();
        for name in catalog.table_names() {
            let meta = catalog.get_table(&name)?;
            heaps.insert(
                name.clone(),
                HeapFile::from_pages(&name, meta.heap_page_ids.clone()),
            );
        }

        // Restore disk B+Tree indexes from catalog (root page IDs are in data.db).
        let mut indexes = HashMap::new();
        for table_name in catalog.table_names() {
            let meta = catalog.get_table(&table_name)?;
            for (idx_name, idx_meta) in &meta.indexes {
                if idx_meta.root_page_id != 0 {
                    indexes.insert(
                        idx_name.clone(),
                        DiskBTree::from_root(
                            idx_name,
                            &table_name,
                            idx_meta.column_index,
                            idx_meta.root_page_id,
                        ),
                    );
                } else {
                    // root_page_id 0 means the index was never populated or
                    // was created with the old serialization format. Rebuild it.
                    indexes.insert(
                        idx_name.clone(),
                        DiskBTree::new(idx_name, &table_name, idx_meta.column_index),
                    );
                }
            }
        }

        let mut db = Database {
            data_dir,
            catalog,
            heaps,
            indexes,
            pool,
        };

        // Rebuild any indexes that have no root (empty or migrated from old format).
        db.rebuild_empty_indexes()?;

        // Clean up legacy index files from old serialization format.
        db.cleanup_legacy_index_files();

        Ok(db)
    }

    /// Rebuild indexes that exist in the catalog but have no data (root_page_id is None).
    fn rebuild_empty_indexes(&mut self) -> Result<()> {
        let table_names = self.catalog.table_names();
        for table_name in &table_names {
            let meta = self.catalog.get_table(table_name)?;
            let to_rebuild: Vec<(String, usize)> = meta
                .indexes
                .iter()
                .filter(|(name, _)| {
                    self.indexes
                        .get(*name)
                        .map(|idx| idx.root_page_id().is_none())
                        .unwrap_or(true)
                })
                .map(|(name, m)| (name.clone(), m.column_index))
                .collect();

            for (idx_name, col_idx) in to_rebuild {
                let heap = self
                    .heaps
                    .get(table_name)
                    .ok_or_else(|| BoolDBError::TableNotFound(table_name.clone()))?;
                let tuples = heap.scan(&mut self.pool)?;

                if tuples.is_empty() {
                    continue;
                }

                let idx = self.indexes.get_mut(&idx_name).unwrap();
                for tuple in &tuples {
                    if col_idx < tuple.values.len() {
                        idx.insert(&mut self.pool, &tuple.values[col_idx], tuple.row_id)?;
                    }
                }

                // Update root_page_id in catalog.
                if let Some(root_id) = idx.root_page_id() {
                    let meta = self.catalog.get_table_mut(table_name)?;
                    if let Some(im) = meta.indexes.get_mut(&idx_name) {
                        im.root_page_id = root_id;
                    }
                }

                eprintln!(
                    "[info] Rebuilt index '{}' ({} entries, depth {})",
                    idx_name,
                    idx.len(&mut self.pool)?,
                    idx.depth(&mut self.pool)?
                );
            }
        }
        Ok(())
    }

    /// Remove old-format `index_*.bin` files if they exist.
    fn cleanup_legacy_index_files(&self) {
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("index_") && name.ends_with(".bin") {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
    }

    /// Execute a SQL statement.
    pub fn execute(&mut self, sql: &str) -> Result<ExecResult> {
        // Handle EXPLAIN
        let trimmed = sql.trim();
        if trimmed.to_uppercase().starts_with("EXPLAIN ") {
            let inner_sql = &trimmed[8..];
            let plan = planner::plan(inner_sql)?;
            let explanation = crate::sql::optimizer::explain(&plan, &self.catalog);
            return Ok(ExecResult::Rows {
                columns: vec!["plan".to_string()],
                rows: explanation
                    .lines()
                    .map(|l| vec![Value::Text(l.to_string())])
                    .collect(),
            });
        }

        // Handle SHOW TABLES
        let upper = trimmed.to_uppercase();
        if upper == "SHOW TABLES" || upper == "SHOW TABLES;" {
            return self.execute_show_tables();
        }

        // Handle SHOW INDEXES / SHOW INDEXES ON table
        if upper.starts_with("SHOW INDEX") {
            return self.execute_show_indexes(trimmed);
        }

        // Handle DESCRIBE table / DESC table
        if upper.starts_with("DESCRIBE ") || upper.starts_with("DESC ") {
            let table_name = if upper.starts_with("DESCRIBE ") {
                trimmed[9..].trim().trim_end_matches(';').trim()
            } else {
                trimmed[5..].trim().trim_end_matches(';').trim()
            };
            return self.execute_describe(table_name);
        }

        // Handle CREATE INDEX
        if upper.starts_with("CREATE INDEX ") {
            return self.execute_create_index(trimmed);
        }

        // Handle DROP INDEX
        if upper.starts_with("DROP INDEX ") {
            return self.execute_drop_index(trimmed);
        }

        let plan = planner::plan(sql)?;
        let result =
            executor::execute(&plan, &mut self.catalog, &mut self.heaps, &mut self.pool)?;

        // Post-execution: maintain indexes + persist.
        match &plan {
            planner::LogicalPlan::CreateTable { schema } => {
                // Auto-create indexes for PRIMARY KEY columns.
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.primary_key {
                        let idx_name = format!("pk_{}_{}", schema.table_name, col.name);
                        let idx = DiskBTree::new(&idx_name, &schema.table_name, i);
                        self.catalog.add_index(
                            &schema.table_name,
                            IndexMeta {
                                name: idx_name.clone(),
                                table_name: schema.table_name.clone(),
                                column_index: i,
                                root_page_id: 0,
                            },
                        )?;
                        self.indexes.insert(idx_name, idx);
                    }
                }
                self.save_catalog()?;
            }
            planner::LogicalPlan::DropTable { table_name } => {
                self.remove_indexes_for_table(table_name);
                self.save_catalog()?;
            }
            planner::LogicalPlan::Insert { table_name, .. } => {
                self.rebuild_indexes_for_table(table_name)?;
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            planner::LogicalPlan::Update { table_name, .. } => {
                self.rebuild_indexes_for_table(table_name)?;
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            planner::LogicalPlan::Delete { table_name, .. } => {
                self.rebuild_indexes_for_table(table_name)?;
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            _ => {}
        }

        Ok(result)
    }

    /// CREATE INDEX idx_name ON table_name (column_name)
    fn execute_create_index(&mut self, sql: &str) -> Result<ExecResult> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 6 {
            return Err(BoolDBError::Parse(
                "Syntax: CREATE INDEX name ON table (column)".to_string(),
            ));
        }

        let idx_name = parts[2].to_string();
        if parts[3].to_uppercase() != "ON" {
            return Err(BoolDBError::Parse(
                "Expected ON after index name".to_string(),
            ));
        }
        let table_name = parts[4].to_string();
        let rest = sql[sql.find('(').ok_or_else(|| {
            BoolDBError::Parse("Expected (column) after table name".to_string())
        })? + 1..]
            .trim();
        let col_name = rest
            .trim_end_matches(')')
            .trim_end_matches(';')
            .trim()
            .to_string();

        if col_name.is_empty() {
            return Err(BoolDBError::Parse("Column name is empty".to_string()));
        }

        let schema = self.catalog.get_table(&table_name)?.schema.clone();
        let col_idx = schema
            .column_index(&col_name)
            .ok_or_else(|| BoolDBError::ColumnNotFound(col_name.clone()))?;

        if self.indexes.contains_key(&idx_name) {
            return Err(BoolDBError::Sql(format!(
                "Index '{}' already exists",
                idx_name
            )));
        }

        // Build the index by scanning existing data.
        let mut idx = DiskBTree::new(&idx_name, &table_name, col_idx);
        let heap = self
            .heaps
            .get(&table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.clone()))?;
        let tuples = heap.scan(&mut self.pool)?;

        for tuple in &tuples {
            if col_idx < tuple.values.len() {
                idx.insert(&mut self.pool, &tuple.values[col_idx], tuple.row_id)?;
            }
        }

        let root_page_id = idx.root_page_id().unwrap_or(0);
        let entry_count = idx.len(&mut self.pool)?;
        let depth = idx.depth(&mut self.pool)?;

        self.catalog.add_index(
            &table_name,
            IndexMeta {
                name: idx_name.clone(),
                table_name: table_name.clone(),
                column_index: col_idx,
                root_page_id,
            },
        )?;
        self.save_catalog()?;
        self.pool.flush_all()?;
        self.indexes.insert(idx_name.clone(), idx);

        Ok(ExecResult::Ok {
            message: format!(
                "Index '{}' created on {}.{} ({} entries, depth {})",
                idx_name, table_name, col_name, entry_count, depth
            ),
        })
    }

    /// DROP INDEX idx_name
    fn execute_drop_index(&mut self, sql: &str) -> Result<ExecResult> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(BoolDBError::Parse("Syntax: DROP INDEX name".to_string()));
        }
        let idx_name = parts[2].trim_end_matches(';').to_string();

        let idx = self
            .indexes
            .get(&idx_name)
            .ok_or_else(|| BoolDBError::IndexNotFound(idx_name.clone()))?;
        let table_name = idx.table_name.clone();

        let table_meta = self.catalog.get_table_mut(&table_name)?;
        table_meta.indexes.remove(&idx_name);

        self.indexes.remove(&idx_name);
        self.save_catalog()?;

        Ok(ExecResult::Ok {
            message: format!("Index '{}' dropped", idx_name),
        })
    }

    /// SHOW TABLES
    fn execute_show_tables(&self) -> Result<ExecResult> {
        let mut names = self.catalog.table_names();
        names.sort();
        let rows: Vec<crate::types::Row> = names
            .into_iter()
            .map(|n| vec![Value::Text(n)])
            .collect();
        Ok(ExecResult::Rows {
            columns: vec!["table_name".to_string()],
            rows,
        })
    }

    /// SHOW INDEXES [ON table]
    fn execute_show_indexes(&mut self, sql: &str) -> Result<ExecResult> {
        let upper = sql.to_uppercase();
        let table_filter = if upper.contains(" ON ") {
            let pos = sql.to_uppercase().find(" ON ").unwrap();
            Some(sql[pos + 4..].trim().trim_end_matches(';').trim().to_string())
        } else {
            None
        };

        let mut rows: Vec<crate::types::Row> = Vec::new();

        for table_name in self.catalog.table_names() {
            if let Some(ref filter) = table_filter {
                if table_name != *filter {
                    continue;
                }
            }
            let meta = self.catalog.get_table(&table_name)?;
            for (idx_name, idx_meta) in &meta.indexes {
                let col_name = meta
                    .schema
                    .columns
                    .get(idx_meta.column_index)
                    .map(|c| c.name.as_str())
                    .unwrap_or("?");
                let entry_count = self
                    .indexes
                    .get(idx_name)
                    .map(|idx| idx.len(&mut self.pool).unwrap_or(0))
                    .unwrap_or(0);
                let depth = self
                    .indexes
                    .get(idx_name)
                    .map(|idx| idx.depth(&mut self.pool).unwrap_or(0))
                    .unwrap_or(0);

                rows.push(vec![
                    Value::Text(table_name.clone()),
                    Value::Text(idx_name.clone()),
                    Value::Text(col_name.to_string()),
                    Value::Integer(entry_count as i64),
                    Value::Integer(depth as i64),
                ]);
            }
        }

        rows.sort_by(|a, b| {
            let ta = if let Value::Text(s) = &a[0] { s.as_str() } else { "" };
            let tb = if let Value::Text(s) = &b[0] { s.as_str() } else { "" };
            ta.cmp(tb).then_with(|| {
                let ia = if let Value::Text(s) = &a[1] { s.as_str() } else { "" };
                let ib = if let Value::Text(s) = &b[1] { s.as_str() } else { "" };
                ia.cmp(ib)
            })
        });

        Ok(ExecResult::Rows {
            columns: vec![
                "table".to_string(),
                "index_name".to_string(),
                "column".to_string(),
                "entries".to_string(),
                "depth".to_string(),
            ],
            rows,
        })
    }

    /// DESCRIBE table / DESC table
    fn execute_describe(&self, table_name: &str) -> Result<ExecResult> {
        let meta = self.catalog.get_table(table_name)?;
        let rows: Vec<crate::types::Row> = meta
            .schema
            .columns
            .iter()
            .map(|col| {
                vec![
                    Value::Text(col.name.clone()),
                    Value::Text(col.data_type.to_string()),
                    Value::Boolean(col.nullable),
                    Value::Boolean(col.primary_key),
                ]
            })
            .collect();

        Ok(ExecResult::Rows {
            columns: vec![
                "column".to_string(),
                "type".to_string(),
                "nullable".to_string(),
                "primary_key".to_string(),
            ],
            rows,
        })
    }

    /// Rebuild all disk B+Tree indexes for a given table from heap data.
    fn rebuild_indexes_for_table(&mut self, table_name: &str) -> Result<()> {
        let index_info: Vec<(String, usize)> = match self.catalog.get_table(table_name) {
            Ok(meta) => meta
                .indexes
                .iter()
                .map(|(name, m)| (name.clone(), m.column_index))
                .collect(),
            Err(_) => return Ok(()),
        };

        if index_info.is_empty() {
            return Ok(());
        }

        let heap = self
            .heaps
            .get(table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;
        let tuples = heap.scan(&mut self.pool)?;

        for (idx_name, col_idx) in &index_info {
            // Create a fresh B+Tree and rebuild from scratch.
            let mut new_idx = DiskBTree::new(idx_name, table_name, *col_idx);
            for tuple in &tuples {
                if *col_idx < tuple.values.len() {
                    new_idx.insert(&mut self.pool, &tuple.values[*col_idx], tuple.row_id)?;
                }
            }

            // Update catalog with the new root page.
            let root_id = new_idx.root_page_id().unwrap_or(0);
            let meta = self.catalog.get_table_mut(table_name)?;
            if let Some(im) = meta.indexes.get_mut(idx_name) {
                im.root_page_id = root_id;
            }

            self.indexes.insert(idx_name.clone(), new_idx);
        }

        Ok(())
    }

    /// Remove all indexes for a table.
    fn remove_indexes_for_table(&mut self, table_name: &str) {
        let to_remove: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();
        for name in to_remove {
            self.indexes.remove(&name);
        }
    }

    /// Save the catalog to disk.
    fn save_catalog(&self) -> Result<()> {
        let path = self.data_dir.join(CATALOG_FILE);
        let data = self.catalog.to_bytes();
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Get the list of table names.
    pub fn table_names(&self) -> Vec<String> {
        self.catalog.table_names()
    }

    /// Get a table's schema.
    pub fn table_schema(&self, name: &str) -> Result<&crate::types::Schema> {
        Ok(&self.catalog.get_table(name)?.schema)
    }

    /// Get the names of all indexes.
    pub fn index_names(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Get an index by name.
    pub fn get_index(&self, name: &str) -> Option<&DiskBTree> {
        self.indexes.get(name)
    }

    /// Get the depth of an index (requires mutable pool access).
    pub fn index_depth(&mut self, name: &str) -> Result<usize> {
        let idx = self
            .indexes
            .get(name)
            .ok_or_else(|| BoolDBError::IndexNotFound(name.to_string()))?;
        idx.depth(&mut self.pool)
    }

    /// Get the entry count of an index.
    pub fn index_len(&mut self, name: &str) -> Result<usize> {
        let idx = self
            .indexes
            .get(name)
            .ok_or_else(|| BoolDBError::IndexNotFound(name.to_string()))?;
        idx.len(&mut self.pool)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.pool.flush_all();
        let _ = self.save_catalog();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("booldb_test").join(name);
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn test_create_insert_select() {
        let dir = tmp_dir("test_basic_sql2");
        let mut db = Database::open(&dir).unwrap();

        match db
            .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
            .unwrap()
        {
            ExecResult::Ok { message } => assert!(message.contains("users")),
            _ => panic!("Expected Ok"),
        }

        // PRIMARY KEY should auto-create an index.
        assert!(db.get_index("pk_users_id").is_some());

        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap();

        match db.execute("SELECT * FROM users").unwrap() {
            ExecResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name", "age"]);
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }

        // Verify the index has 2 entries.
        assert_eq!(db.index_len("pk_users_id").unwrap(), 2);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_where_clause() {
        let dir = tmp_dir("test_where2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        match db.execute("SELECT * FROM t WHERE val > 15").unwrap() {
            ExecResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Rows"),
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_update_maintains_index() {
        let dir = tmp_dir("test_update_idx2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
        db.execute("UPDATE t SET name = 'Alicia' WHERE id = 1")
            .unwrap();

        assert_eq!(db.index_len("pk_t_id").unwrap(), 2);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_delete_maintains_index() {
        let dir = tmp_dir("test_delete_idx2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();

        assert_eq!(db.index_len("pk_t_id").unwrap(), 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_drop_table_removes_indexes() {
        let dir = tmp_dir("test_drop2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        assert!(db.get_index("pk_t_id").is_some());

        db.execute("DROP TABLE t").unwrap();
        assert!(db.get_index("pk_t_id").is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_index_persistence_across_restart() {
        let dir = tmp_dir("test_idx_persist2");

        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'Alice')")
                .unwrap();
            db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
            db.execute("CREATE INDEX idx_name ON users (name)")
                .unwrap();

            assert_eq!(db.index_len("pk_users_id").unwrap(), 2);
            assert_eq!(db.index_len("idx_name").unwrap(), 2);
            assert!(db.index_depth("pk_users_id").unwrap() >= 1);
        }

        // Reopen — B+Tree pages are in data.db, root IDs in catalog.
        {
            let mut db = Database::open(&dir).unwrap();
            assert_eq!(db.index_len("pk_users_id").unwrap(), 2);
            assert_eq!(db.index_len("idx_name").unwrap(), 2);

            // Verify search works.
            match db
                .execute("SELECT * FROM users WHERE id = 1")
                .unwrap()
            {
                ExecResult::Rows { rows, .. } => assert_eq!(rows.len(), 1),
                _ => panic!("Expected Rows"),
            }
        }

        // No legacy index_*.bin files should exist.
        let files: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("index_")
            })
            .collect();
        assert!(files.is_empty(), "Legacy index files should not exist");

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_index_depth_with_many_rows() {
        let dir = tmp_dir("test_idx_depth2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE big (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        // Insert enough rows to force B+Tree splits.
        for i in 0..500 {
            db.execute(&format!("INSERT INTO big VALUES ({}, 'row_{}')", i, i))
                .unwrap();
        }

        let depth = db.index_depth("pk_big_id").unwrap();
        let len = db.index_len("pk_big_id").unwrap();

        assert_eq!(len, 500);
        assert!(
            depth >= 2,
            "B+Tree with 500 entries should have depth >= 2, got {}",
            depth
        );

        eprintln!("B+Tree: {} entries, depth {}", len, depth);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_create_drop_index() {
        let dir = tmp_dir("test_crt_drp_idx2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

        match db.execute("CREATE INDEX idx_name ON t (name)").unwrap() {
            ExecResult::Ok { message } => {
                assert!(message.contains("idx_name"));
                assert!(message.contains("2 entries"));
            }
            _ => panic!("Expected Ok"),
        }

        assert!(db.get_index("idx_name").is_some());
        assert_eq!(db.index_len("idx_name").unwrap(), 2);

        db.execute("DROP INDEX idx_name").unwrap();
        assert!(db.get_index("idx_name").is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_persistence() {
        let dir = tmp_dir("test_persist2");

        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
        }

        {
            let mut db = Database::open(&dir).unwrap();
            assert!(db.table_names().contains(&"t".to_string()));
            match db.execute("SELECT * FROM t").unwrap() {
                ExecResult::Rows { rows, .. } => assert_eq!(rows.len(), 2),
                _ => panic!("Expected Rows"),
            }
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_join() {
        let dir = tmp_dir("test_join2");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE orders (id INTEGER, user_id INTEGER, product TEXT)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();
        db.execute("INSERT INTO orders VALUES (1, 1, 'Widget')")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (2, 1, 'Gadget')")
            .unwrap();
        db.execute("INSERT INTO orders VALUES (3, 2, 'Doohickey')")
            .unwrap();

        match db
            .execute("SELECT * FROM users INNER JOIN orders ON id = user_id")
            .unwrap()
        {
            ExecResult::Rows { rows, columns } => {
                assert_eq!(columns.len(), 5);
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

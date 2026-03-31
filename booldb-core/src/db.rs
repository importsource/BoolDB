use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::catalog::schema::{Catalog, IndexMeta};
use crate::error::{BoolDBError, Result};
use crate::index::btree::BTreeIndex;
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
    /// All indexes, keyed by index name.
    indexes: HashMap<String, BTreeIndex>,
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

        // Load indexes from their persistent files.
        let mut indexes = HashMap::new();
        for table_name in catalog.table_names() {
            let meta = catalog.get_table(&table_name)?;
            for (idx_name, _idx_meta) in &meta.indexes {
                let idx_path = data_dir.join(format!("index_{}.bin", idx_name));
                if idx_path.exists() {
                    let data = std::fs::read(&idx_path)?;
                    match BTreeIndex::from_bytes(&data) {
                        Ok(idx) => {
                            indexes.insert(idx_name.clone(), idx);
                        }
                        Err(e) => {
                            eprintln!(
                                "[warn] Failed to load index '{}': {}, will rebuild",
                                idx_name, e
                            );
                        }
                    }
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

        // Rebuild any indexes that failed to load from file.
        db.rebuild_missing_indexes()?;

        Ok(db)
    }

    /// Rebuild indexes that exist in the catalog but are not loaded in memory.
    fn rebuild_missing_indexes(&mut self) -> Result<()> {
        let table_names = self.catalog.table_names();
        for table_name in &table_names {
            let meta = self.catalog.get_table(table_name)?;
            let index_metas: Vec<(String, usize)> = meta
                .indexes
                .iter()
                .map(|(name, m)| (name.clone(), m.column_index))
                .collect();

            for (idx_name, col_idx) in index_metas {
                if self.indexes.contains_key(&idx_name) {
                    continue;
                }

                // Rebuild by scanning the heap.
                let mut idx = BTreeIndex::new(&idx_name, table_name, col_idx);
                let heap = self
                    .heaps
                    .get(table_name)
                    .ok_or_else(|| BoolDBError::TableNotFound(table_name.clone()))?;
                let tuples = heap.scan(&mut self.pool)?;

                for tuple in &tuples {
                    if col_idx < tuple.values.len() {
                        idx.insert(&tuple.values[col_idx], tuple.row_id);
                    }
                }

                eprintln!(
                    "[info] Rebuilt index '{}' on {}.{} ({} entries)",
                    idx_name,
                    table_name,
                    col_idx,
                    idx.len()
                );

                // Persist the rebuilt index.
                self.save_index(&idx)?;
                self.indexes.insert(idx_name, idx);
            }
        }
        Ok(())
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

        // Handle CREATE INDEX
        if trimmed.to_uppercase().starts_with("CREATE INDEX ") {
            return self.execute_create_index(trimmed);
        }

        // Handle DROP INDEX
        if trimmed.to_uppercase().starts_with("DROP INDEX ") {
            return self.execute_drop_index(trimmed);
        }

        let plan = planner::plan(sql)?;

        // Execute the plan.
        let result =
            executor::execute(&plan, &mut self.catalog, &mut self.heaps, &mut self.pool)?;

        // Post-execution: maintain indexes + persist.
        match &plan {
            planner::LogicalPlan::CreateTable { schema } => {
                // Auto-create indexes for PRIMARY KEY columns.
                for (i, col) in schema.columns.iter().enumerate() {
                    if col.primary_key {
                        let idx_name = format!("pk_{}_{}", schema.table_name, col.name);
                        let idx =
                            BTreeIndex::new(&idx_name, &schema.table_name, i);
                        self.catalog.add_index(
                            &schema.table_name,
                            IndexMeta {
                                name: idx_name.clone(),
                                table_name: schema.table_name.clone(),
                                column_index: i,
                                root_page_id: 0,
                            },
                        )?;
                        self.save_index(&idx)?;
                        self.indexes.insert(idx_name, idx);
                    }
                }
                self.save_catalog()?;
            }
            planner::LogicalPlan::DropTable { table_name } => {
                // Remove all index files for this table.
                self.remove_indexes_for_table(table_name);
                self.save_catalog()?;
            }
            planner::LogicalPlan::Insert {
                table_name,
                columns,
                rows,
            } => {
                // Update indexes with newly inserted rows.
                self.update_indexes_after_insert(table_name, columns, rows)?;
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            planner::LogicalPlan::Update {
                table_name,
                assignments: _,
                filter: _,
            } => {
                // Rebuild affected indexes (update may change indexed columns).
                self.rebuild_indexes_for_table(table_name)?;
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            planner::LogicalPlan::Delete { table_name, .. } => {
                // Rebuild affected indexes (rows were removed).
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
        // Simple parser: CREATE INDEX <name> ON <table> (<column>)
        let parts: Vec<&str> = sql.split_whitespace().collect();
        // Expected: CREATE INDEX name ON table (col)
        if parts.len() < 6 {
            return Err(BoolDBError::Parse(
                "Syntax: CREATE INDEX name ON table (column)".to_string(),
            ));
        }

        let idx_name = parts[2].to_string();
        // parts[3] should be "ON"
        if parts[3].to_uppercase() != "ON" {
            return Err(BoolDBError::Parse(
                "Expected ON after index name".to_string(),
            ));
        }
        let table_name = parts[4].to_string();
        // Extract column name from parentheses.
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

        // Validate table and column exist.
        let schema = self.catalog.get_table(&table_name)?.schema.clone();
        let col_idx = schema
            .column_index(&col_name)
            .ok_or_else(|| BoolDBError::ColumnNotFound(col_name.clone()))?;

        // Check if index already exists.
        if self.indexes.contains_key(&idx_name) {
            return Err(BoolDBError::Sql(format!(
                "Index '{}' already exists",
                idx_name
            )));
        }

        // Build the index by scanning existing data.
        let mut idx = BTreeIndex::new(&idx_name, &table_name, col_idx);
        let heap = self
            .heaps
            .get(&table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.clone()))?;
        let tuples = heap.scan(&mut self.pool)?;

        for tuple in &tuples {
            if col_idx < tuple.values.len() {
                idx.insert(&tuple.values[col_idx], tuple.row_id);
            }
        }

        // Register in catalog and persist.
        self.catalog.add_index(
            &table_name,
            IndexMeta {
                name: idx_name.clone(),
                table_name: table_name.clone(),
                column_index: col_idx,
                root_page_id: 0,
            },
        )?;
        self.save_index(&idx)?;
        self.save_catalog()?;
        self.indexes.insert(idx_name.clone(), idx);

        let count = tuples.len();
        Ok(ExecResult::Ok {
            message: format!(
                "Index '{}' created on {}.{} ({} entries)",
                idx_name, table_name, col_name, count
            ),
        })
    }

    /// DROP INDEX idx_name
    fn execute_drop_index(&mut self, sql: &str) -> Result<ExecResult> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(BoolDBError::Parse(
                "Syntax: DROP INDEX name".to_string(),
            ));
        }
        let idx_name = parts[2].trim_end_matches(';').to_string();

        // Find which table owns this index.
        let idx = self
            .indexes
            .get(&idx_name)
            .ok_or_else(|| BoolDBError::IndexNotFound(idx_name.clone()))?;
        let table_name = idx.table_name.clone();

        // Remove from catalog.
        let table_meta = self.catalog.get_table_mut(&table_name)?;
        table_meta.indexes.remove(&idx_name);

        // Remove index file.
        let idx_path = self.data_dir.join(format!("index_{}.bin", idx_name));
        let _ = std::fs::remove_file(&idx_path);

        // Remove from memory.
        self.indexes.remove(&idx_name);
        self.save_catalog()?;

        Ok(ExecResult::Ok {
            message: format!("Index '{}' dropped", idx_name),
        })
    }

    /// Update indexes after INSERT.
    fn update_indexes_after_insert(
        &mut self,
        table_name: &str,
        _columns: &Option<Vec<String>>,
        _rows: &[crate::types::Row],
    ) -> Result<()> {
        let index_names: Vec<String> = self
            .catalog
            .get_table(table_name)?
            .indexes
            .keys()
            .cloned()
            .collect();

        if index_names.is_empty() {
            return Ok(());
        }

        // We need the RowIds of the just-inserted rows.
        // Since the executor already inserted them, scan the heap to find them.
        // For efficiency, we rebuild the index from scratch for this table's indexes.
        // (A more optimal approach would track RowIds from the executor, but this is correct.)
        for idx_name in &index_names {
            self.rebuild_single_index(table_name, idx_name)?;
        }

        Ok(())
    }

    /// Rebuild a single index by scanning the heap.
    fn rebuild_single_index(&mut self, table_name: &str, idx_name: &str) -> Result<()> {
        let idx_meta = self
            .catalog
            .get_table(table_name)?
            .indexes
            .get(idx_name)
            .ok_or_else(|| BoolDBError::IndexNotFound(idx_name.to_string()))?
            .clone();

        let heap = self
            .heaps
            .get(table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))?;
        let tuples = heap.scan(&mut self.pool)?;

        let mut idx = BTreeIndex::new(idx_name, table_name, idx_meta.column_index);
        for tuple in &tuples {
            if idx_meta.column_index < tuple.values.len() {
                idx.insert(&tuple.values[idx_meta.column_index], tuple.row_id);
            }
        }

        self.save_index(&idx)?;
        self.indexes.insert(idx_name.to_string(), idx);
        Ok(())
    }

    /// Rebuild all indexes for a given table.
    fn rebuild_indexes_for_table(&mut self, table_name: &str) -> Result<()> {
        let index_names: Vec<String> = match self.catalog.get_table(table_name) {
            Ok(meta) => meta.indexes.keys().cloned().collect(),
            Err(_) => return Ok(()), // Table was dropped, nothing to rebuild
        };

        for idx_name in &index_names {
            self.rebuild_single_index(table_name, idx_name)?;
        }
        Ok(())
    }

    /// Remove all index files and in-memory indexes for a table.
    fn remove_indexes_for_table(&mut self, table_name: &str) {
        let to_remove: Vec<String> = self
            .indexes
            .iter()
            .filter(|(_, idx)| idx.table_name == table_name)
            .map(|(name, _)| name.clone())
            .collect();

        for idx_name in to_remove {
            let idx_path = self.data_dir.join(format!("index_{}.bin", idx_name));
            let _ = std::fs::remove_file(&idx_path);
            self.indexes.remove(&idx_name);
        }
    }

    /// Persist a single index to its file.
    fn save_index(&self, idx: &BTreeIndex) -> Result<()> {
        let path = self.data_dir.join(idx.file_name());
        let data = idx.to_bytes();
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Persist all indexes to their files.
    fn save_all_indexes(&self) -> Result<()> {
        for idx in self.indexes.values() {
            self.save_index(idx)?;
        }
        Ok(())
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
    pub fn get_index(&self, name: &str) -> Option<&BTreeIndex> {
        self.indexes.get(name)
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        let _ = self.pool.flush_all();
        let _ = self.save_catalog();
        let _ = self.save_all_indexes();
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
        let dir = tmp_dir("test_basic_sql");
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

        match db
            .execute("INSERT INTO users VALUES (1, 'Alice', 30)")
            .unwrap()
        {
            ExecResult::RowsAffected { count } => assert_eq!(count, 1),
            _ => panic!("Expected RowsAffected"),
        }

        match db
            .execute("INSERT INTO users VALUES (2, 'Bob', 25)")
            .unwrap()
        {
            ExecResult::RowsAffected { count } => assert_eq!(count, 1),
            _ => panic!("Expected RowsAffected"),
        }

        match db.execute("SELECT * FROM users").unwrap() {
            ExecResult::Rows { columns, rows } => {
                assert_eq!(columns, vec!["id", "name", "age"]);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][1], Value::Text("Alice".to_string()));
                assert_eq!(rows[1][1], Value::Text("Bob".to_string()));
            }
            _ => panic!("Expected Rows"),
        }

        // Verify the index has 2 entries.
        assert_eq!(db.get_index("pk_users_id").unwrap().len(), 2);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_where_clause() {
        let dir = tmp_dir("test_where");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

        match db.execute("SELECT * FROM t WHERE val > 15").unwrap() {
            ExecResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Integer(2));
                assert_eq!(rows[1][0], Value::Integer(3));
            }
            _ => panic!("Expected Rows"),
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_update() {
        let dir = tmp_dir("test_update");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

        match db
            .execute("UPDATE t SET name = 'Alicia' WHERE id = 1")
            .unwrap()
        {
            ExecResult::RowsAffected { count } => assert_eq!(count, 1),
            _ => panic!("Expected RowsAffected"),
        }

        match db.execute("SELECT * FROM t WHERE id = 1").unwrap() {
            ExecResult::Rows { rows, .. } => {
                assert_eq!(rows[0][1], Value::Text("Alicia".to_string()));
            }
            _ => panic!("Expected Rows"),
        }

        // Index should still have 2 entries after update.
        assert_eq!(db.get_index("pk_t_id").unwrap().len(), 2);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_delete() {
        let dir = tmp_dir("test_delete");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

        match db.execute("DELETE FROM t WHERE id = 1").unwrap() {
            ExecResult::RowsAffected { count } => assert_eq!(count, 1),
            _ => panic!("Expected RowsAffected"),
        }

        match db.execute("SELECT * FROM t").unwrap() {
            ExecResult::Rows { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Integer(2));
            }
            _ => panic!("Expected Rows"),
        }

        // Index should have 1 entry after delete.
        assert_eq!(db.get_index("pk_t_id").unwrap().len(), 1);

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_drop_table() {
        let dir = tmp_dir("test_drop");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        assert!(db.table_names().contains(&"t".to_string()));
        assert!(db.get_index("pk_t_id").is_some());

        db.execute("DROP TABLE t").unwrap();
        assert!(!db.table_names().contains(&"t".to_string()));
        // Index should be gone too.
        assert!(db.get_index("pk_t_id").is_none());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_persistence() {
        let dir = tmp_dir("test_persist");

        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();
        }

        {
            let mut db = Database::open(&dir).unwrap();
            assert!(db.table_names().contains(&"t".to_string()));
            match db.execute("SELECT * FROM t").unwrap() {
                ExecResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 2);
                }
                _ => panic!("Expected Rows"),
            }
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_index_persistence() {
        let dir = tmp_dir("test_index_persist");

        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
                .unwrap();
            db.execute("INSERT INTO users VALUES (1, 'Alice')")
                .unwrap();
            db.execute("INSERT INTO users VALUES (2, 'Bob')").unwrap();

            // Create a manual index on name.
            db.execute("CREATE INDEX idx_name ON users (name)")
                .unwrap();

            // Verify both indexes exist.
            assert!(db.get_index("pk_users_id").is_some());
            assert!(db.get_index("idx_name").is_some());
            assert_eq!(db.get_index("pk_users_id").unwrap().len(), 2);
            assert_eq!(db.get_index("idx_name").unwrap().len(), 2);
        }

        // Reopen — indexes should be restored from files.
        {
            let db = Database::open(&dir).unwrap();
            let pk_idx = db.get_index("pk_users_id").unwrap();
            assert_eq!(pk_idx.len(), 2);
            assert_eq!(
                pk_idx.search(&Value::Integer(1)).len(),
                1
            );

            let name_idx = db.get_index("idx_name").unwrap();
            assert_eq!(name_idx.len(), 2);
            assert_eq!(
                name_idx.search(&Value::Text("Alice".to_string())).len(),
                1
            );
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_index_survives_mutations() {
        let dir = tmp_dir("test_index_mutations");

        {
            let mut db = Database::open(&dir).unwrap();
            db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
                .unwrap();
            db.execute("CREATE INDEX idx_val ON t (val)").unwrap();

            db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
            db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();
            db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
            assert_eq!(db.get_index("idx_val").unwrap().len(), 3);

            db.execute("DELETE FROM t WHERE id = 2").unwrap();
            assert_eq!(db.get_index("idx_val").unwrap().len(), 2);

            db.execute("UPDATE t SET val = 'x' WHERE id = 1").unwrap();
            assert_eq!(db.get_index("idx_val").unwrap().len(), 2);
            assert_eq!(
                db.get_index("idx_val")
                    .unwrap()
                    .search(&Value::Text("x".to_string()))
                    .len(),
                1
            );
            // Old value should be gone.
            assert_eq!(
                db.get_index("idx_val")
                    .unwrap()
                    .search(&Value::Text("a".to_string()))
                    .len(),
                0
            );
        }

        // Reopen and verify index state persisted correctly.
        {
            let db = Database::open(&dir).unwrap();
            let idx = db.get_index("idx_val").unwrap();
            assert_eq!(idx.len(), 2);
            assert_eq!(
                idx.search(&Value::Text("x".to_string())).len(),
                1
            );
            assert_eq!(
                idx.search(&Value::Text("c".to_string())).len(),
                1
            );
            assert_eq!(
                idx.search(&Value::Text("a".to_string())).len(),
                0
            );
            assert_eq!(
                idx.search(&Value::Text("b".to_string())).len(),
                0
            );
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_create_drop_index() {
        let dir = tmp_dir("test_create_drop_idx");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'Alice')").unwrap();
        db.execute("INSERT INTO t VALUES (2, 'Bob')").unwrap();

        // Create index.
        match db.execute("CREATE INDEX idx_name ON t (name)").unwrap() {
            ExecResult::Ok { message } => {
                assert!(message.contains("idx_name"));
                assert!(message.contains("2 entries"));
            }
            _ => panic!("Expected Ok"),
        }

        assert!(db.get_index("idx_name").is_some());

        // Drop index.
        match db.execute("DROP INDEX idx_name").unwrap() {
            ExecResult::Ok { message } => assert!(message.contains("idx_name")),
            _ => panic!("Expected Ok"),
        }

        assert!(db.get_index("idx_name").is_none());

        // Verify index file is removed.
        let idx_path = dir.join("index_idx_name.bin");
        assert!(!idx_path.exists());

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_join() {
        let dir = tmp_dir("test_join");
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
                assert_eq!(columns.len(), 5); // 2 + 3
                assert_eq!(rows.len(), 3);
            }
            _ => panic!("Expected Rows"),
        }

        std::fs::remove_dir_all(&dir).unwrap();
    }
}

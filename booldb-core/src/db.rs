use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::catalog::schema::Catalog;
use crate::error::Result;
use crate::sql::executor::{self, ExecResult};
use crate::sql::planner;
use crate::storage::buffer::BufferPool;
use crate::storage::disk::DiskManager;
use crate::storage::heap::HeapFile;

const DEFAULT_BUFFER_POOL_SIZE: usize = 256;
const CATALOG_FILE: &str = "catalog.bin";
const DATA_FILE: &str = "data.db";

/// The main database engine.
pub struct Database {
    data_dir: PathBuf,
    catalog: Catalog,
    heaps: HashMap<String, HeapFile>,
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

        Ok(Database {
            data_dir,
            catalog,
            heaps,
            pool,
        })
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
                    .map(|l| vec![crate::types::Value::Text(l.to_string())])
                    .collect(),
            });
        }

        let plan = planner::plan(sql)?;
        let result = executor::execute(&plan, &mut self.catalog, &mut self.heaps, &mut self.pool)?;

        // Persist catalog after mutations.
        match &plan {
            planner::LogicalPlan::CreateTable { .. }
            | planner::LogicalPlan::DropTable { .. }
            | planner::LogicalPlan::Insert { .. }
            | planner::LogicalPlan::Update { .. }
            | planner::LogicalPlan::Delete { .. } => {
                self.save_catalog()?;
                self.pool.flush_all()?;
            }
            _ => {}
        }

        Ok(result)
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
    use crate::types::Value;

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

        db.execute("CREATE TABLE t (id INTEGER, name TEXT)")
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

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_delete() {
        let dir = tmp_dir("test_delete");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER, val INTEGER)")
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

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_drop_table() {
        let dir = tmp_dir("test_drop");
        let mut db = Database::open(&dir).unwrap();

        db.execute("CREATE TABLE t (id INTEGER)").unwrap();
        assert!(db.table_names().contains(&"t".to_string()));

        db.execute("DROP TABLE t").unwrap();
        assert!(!db.table_names().contains(&"t".to_string()));

        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn test_persistence() {
        let dir = tmp_dir("test_persist");

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
                ExecResult::Rows { rows, .. } => {
                    assert_eq!(rows.len(), 2);
                }
                _ => panic!("Expected Rows"),
            }
        }

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

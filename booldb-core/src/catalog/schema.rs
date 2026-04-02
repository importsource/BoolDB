use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::error::{BoolDBError, Result};
use crate::types::{PageId, Schema};

/// Metadata about a table stored in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMeta {
    pub schema: Schema,
    /// Page IDs that make up this table's heap file.
    pub heap_page_ids: Vec<PageId>,
    /// Named indexes on this table.
    pub indexes: HashMap<String, IndexMeta>,
}

/// Metadata about an index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMeta {
    pub name: String,
    pub table_name: String,
    /// Column index in the table schema.
    pub column_index: usize,
    /// Root page ID of the B+Tree.
    pub root_page_id: PageId,
    /// For expression indexes: the JSON path to extract (e.g., "$.name").
    #[serde(default)]
    pub json_path: Option<String>,
}

/// In-memory catalog of all tables and indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    tables: HashMap<String, TableMeta>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
        }
    }

    /// Create a new table. Returns error if it already exists.
    pub fn create_table(&mut self, schema: Schema) -> Result<()> {
        if self.tables.contains_key(&schema.table_name) {
            return Err(BoolDBError::TableAlreadyExists(schema.table_name.clone()));
        }
        let table_name = schema.table_name.clone();
        self.tables.insert(
            table_name,
            TableMeta {
                schema,
                heap_page_ids: Vec::new(),
                indexes: HashMap::new(),
            },
        );
        Ok(())
    }

    /// Drop a table. Returns error if it doesn't exist.
    pub fn drop_table(&mut self, table_name: &str) -> Result<TableMeta> {
        self.tables
            .remove(table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))
    }

    /// Get table metadata.
    pub fn get_table(&self, table_name: &str) -> Result<&TableMeta> {
        self.tables
            .get(table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))
    }

    /// Get mutable table metadata.
    pub fn get_table_mut(&mut self, table_name: &str) -> Result<&mut TableMeta> {
        self.tables
            .get_mut(table_name)
            .ok_or_else(|| BoolDBError::TableNotFound(table_name.to_string()))
    }

    /// List all table names.
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Check if a table exists.
    pub fn table_exists(&self, table_name: &str) -> bool {
        self.tables.contains_key(table_name)
    }

    /// Add an index to a table.
    pub fn add_index(
        &mut self,
        table_name: &str,
        index_meta: IndexMeta,
    ) -> Result<()> {
        let table = self.get_table_mut(table_name)?;
        table
            .indexes
            .insert(index_meta.name.clone(), index_meta);
        Ok(())
    }

    /// Serialize catalog to bytes (for persistence).
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Catalog serialization should not fail")
    }

    /// Deserialize catalog from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| BoolDBError::Serialization(e.to_string()))
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, DataType};

    fn test_schema(name: &str) -> Schema {
        Schema {
            table_name: name.to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                },
                Column {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                },
            ],
        }
    }

    #[test]
    fn test_create_and_get_table() {
        let mut cat = Catalog::new();
        cat.create_table(test_schema("users")).unwrap();

        let meta = cat.get_table("users").unwrap();
        assert_eq!(meta.schema.table_name, "users");
        assert_eq!(meta.schema.columns.len(), 2);
    }

    #[test]
    fn test_duplicate_table() {
        let mut cat = Catalog::new();
        cat.create_table(test_schema("users")).unwrap();
        assert!(cat.create_table(test_schema("users")).is_err());
    }

    #[test]
    fn test_drop_table() {
        let mut cat = Catalog::new();
        cat.create_table(test_schema("users")).unwrap();
        cat.drop_table("users").unwrap();
        assert!(!cat.table_exists("users"));
        assert!(cat.drop_table("users").is_err());
    }

    #[test]
    fn test_serialization() {
        let mut cat = Catalog::new();
        cat.create_table(test_schema("users")).unwrap();
        cat.create_table(test_schema("orders")).unwrap();

        let bytes = cat.to_bytes();
        let cat2 = Catalog::from_bytes(&bytes).unwrap();

        assert!(cat2.table_exists("users"));
        assert!(cat2.table_exists("orders"));
        assert_eq!(cat2.table_names().len(), 2);
    }

    #[test]
    fn test_add_index() {
        let mut cat = Catalog::new();
        cat.create_table(test_schema("users")).unwrap();
        cat.add_index(
            "users",
            IndexMeta {
                name: "idx_users_id".to_string(),
                table_name: "users".to_string(),
                column_index: 0,
                root_page_id: 42,
                json_path: None,
            },
        )
        .unwrap();

        let meta = cat.get_table("users").unwrap();
        assert!(meta.indexes.contains_key("idx_users_id"));
    }
}

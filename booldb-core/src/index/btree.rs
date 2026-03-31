use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::types::{RowId, Value};

/// An in-memory B+Tree index mapping a column value to row IDs.
///
/// For Phase 2, we keep the index in memory. Disk-based B+Tree pages
/// will be added in a later phase for large indexes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeIndex {
    pub name: String,
    pub table_name: String,
    pub column_index: usize,
    /// Map from serialized key → list of RowIds.
    entries: BTreeMap<Vec<u8>, Vec<RowId>>,
}

impl BTreeIndex {
    pub fn new(name: &str, table_name: &str, column_index: usize) -> Self {
        BTreeIndex {
            name: name.to_string(),
            table_name: table_name.to_string(),
            column_index,
            entries: BTreeMap::new(),
        }
    }

    /// Serialize a Value into bytes for use as a BTree key.
    /// Null sorts first.
    fn key_bytes(value: &Value) -> Vec<u8> {
        match value {
            Value::Null => vec![0x00],
            Value::Boolean(false) => vec![0x01, 0x00],
            Value::Boolean(true) => vec![0x01, 0x01],
            Value::Integer(v) => {
                let mut buf = vec![0x02];
                // Flip sign bit for correct byte ordering of signed integers.
                let ordered = (*v as u64) ^ (1u64 << 63);
                buf.extend_from_slice(&ordered.to_be_bytes());
                buf
            }
            Value::Float(v) => {
                let mut buf = vec![0x03];
                let bits = v.to_bits();
                // IEEE 754 order-preserving transform.
                let ordered = if bits & (1u64 << 63) != 0 {
                    !bits
                } else {
                    bits ^ (1u64 << 63)
                };
                buf.extend_from_slice(&ordered.to_be_bytes());
                buf
            }
            Value::Text(s) => {
                let mut buf = vec![0x04];
                buf.extend_from_slice(s.as_bytes());
                buf
            }
        }
    }

    /// Insert a key-value pair.
    pub fn insert(&mut self, key: &Value, row_id: RowId) {
        let k = Self::key_bytes(key);
        self.entries.entry(k).or_default().push(row_id);
    }

    /// Delete a specific key-rowid pair.
    pub fn delete(&mut self, key: &Value, row_id: &RowId) {
        let k = Self::key_bytes(key);
        if let Some(rids) = self.entries.get_mut(&k) {
            rids.retain(|r| r != row_id);
            if rids.is_empty() {
                self.entries.remove(&k);
            }
        }
    }

    /// Point lookup: find all RowIds with the given key.
    pub fn search(&self, key: &Value) -> Vec<RowId> {
        let k = Self::key_bytes(key);
        self.entries.get(&k).cloned().unwrap_or_default()
    }

    /// Range scan: find all RowIds where key is in [start, end] (inclusive).
    pub fn range_scan(&self, start: &Value, end: &Value) -> Vec<(Vec<u8>, RowId)> {
        let start_key = Self::key_bytes(start);
        let end_key = Self::key_bytes(end);

        let mut results = Vec::new();
        for (k, rids) in self.entries.range(start_key..=end_key) {
            for rid in rids {
                results.push((k.clone(), *rid));
            }
        }
        results
    }

    /// Scan all entries (for full index scan).
    pub fn scan_all(&self) -> Vec<(Vec<u8>, RowId)> {
        let mut results = Vec::new();
        for (k, rids) in &self.entries {
            for rid in rids {
                results.push((k.clone(), *rid));
            }
        }
        results
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Serialize the index to bytes for persistence.
    pub fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("BTreeIndex serialization should not fail")
    }

    /// Deserialize an index from bytes.
    pub fn from_bytes(data: &[u8]) -> std::result::Result<Self, String> {
        bincode::deserialize(data).map_err(|e| e.to_string())
    }

    /// File name for persisting this index.
    pub fn file_name(&self) -> String {
        format!("index_{}.bin", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rid(page: u32, slot: u16) -> RowId {
        RowId {
            page_id: page,
            slot_id: slot,
        }
    }

    #[test]
    fn test_insert_and_search() {
        let mut idx = BTreeIndex::new("idx_id", "users", 0);

        idx.insert(&Value::Integer(1), rid(0, 0));
        idx.insert(&Value::Integer(2), rid(0, 1));
        idx.insert(&Value::Integer(3), rid(0, 2));

        assert_eq!(idx.search(&Value::Integer(2)), vec![rid(0, 1)]);
        assert_eq!(idx.search(&Value::Integer(99)), vec![]);
        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn test_duplicate_keys() {
        let mut idx = BTreeIndex::new("idx_name", "users", 1);

        idx.insert(&Value::Text("Alice".to_string()), rid(0, 0));
        idx.insert(&Value::Text("Alice".to_string()), rid(0, 5));
        idx.insert(&Value::Text("Bob".to_string()), rid(0, 1));

        let results = idx.search(&Value::Text("Alice".to_string()));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_delete() {
        let mut idx = BTreeIndex::new("idx_id", "users", 0);

        idx.insert(&Value::Integer(1), rid(0, 0));
        idx.insert(&Value::Integer(2), rid(0, 1));

        idx.delete(&Value::Integer(1), &rid(0, 0));
        assert_eq!(idx.search(&Value::Integer(1)), vec![]);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn test_range_scan() {
        let mut idx = BTreeIndex::new("idx_age", "users", 2);

        for i in 1..=10 {
            idx.insert(&Value::Integer(i), rid(0, i as u16));
        }

        let results = idx.range_scan(&Value::Integer(3), &Value::Integer(7));
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_integer_ordering() {
        let mut idx = BTreeIndex::new("idx_id", "test", 0);

        // Insert out of order, including negative numbers.
        idx.insert(&Value::Integer(-10), rid(0, 0));
        idx.insert(&Value::Integer(5), rid(0, 1));
        idx.insert(&Value::Integer(-1), rid(0, 2));
        idx.insert(&Value::Integer(100), rid(0, 3));

        let all = idx.scan_all();
        let keys: Vec<_> = all.iter().map(|(k, _)| k.clone()).collect();
        // Should be sorted: -10, -1, 5, 100
        assert!(keys.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn test_text_ordering() {
        let mut idx = BTreeIndex::new("idx_name", "test", 0);

        idx.insert(&Value::Text("Charlie".to_string()), rid(0, 0));
        idx.insert(&Value::Text("Alice".to_string()), rid(0, 1));
        idx.insert(&Value::Text("Bob".to_string()), rid(0, 2));

        let all = idx.scan_all();
        // Should be alphabetical: Alice, Bob, Charlie
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].1, rid(0, 1)); // Alice
        assert_eq!(all[1].1, rid(0, 2)); // Bob
        assert_eq!(all[2].1, rid(0, 0)); // Charlie
    }
}

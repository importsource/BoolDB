use serde::{Deserialize, Serialize};

use crate::error::{BoolDBError, Result};
use crate::storage::buffer::BufferPool;
use crate::storage::page::{Page, PageType, HEADER_SIZE, PAGE_BODY_SIZE};
use crate::types::{PageId, RowId, Value};

/// Maximum serialized node size before we split.
/// Leave some margin below PAGE_BODY_SIZE (4083) for bincode overhead.
const SPLIT_THRESHOLD: usize = 3600;

/// A B+Tree node stored on a single page.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct BTreeNode {
    is_leaf: bool,
    /// Sorted keys (encoded via `encode_key`).
    keys: Vec<Vec<u8>>,
    /// Internal nodes only: child page IDs. len = keys.len() + 1.
    children: Vec<PageId>,
    /// Leaf nodes only: RowIds for each key. len = keys.len().
    values: Vec<Vec<RowId>>,
    /// Leaf nodes only: next leaf page for range scans.
    next_leaf: Option<PageId>,
}

impl BTreeNode {
    fn new_leaf() -> Self {
        BTreeNode {
            is_leaf: true,
            keys: Vec::new(),
            children: Vec::new(),
            values: Vec::new(),
            next_leaf: None,
        }
    }

    fn new_internal(keys: Vec<Vec<u8>>, children: Vec<PageId>) -> Self {
        BTreeNode {
            is_leaf: false,
            keys,
            children,
            values: Vec::new(),
            next_leaf: None,
        }
    }

    fn serialized_size(&self) -> usize {
        bincode::serialized_size(self).unwrap_or(u64::MAX) as usize
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("BTreeNode serialization failed")
    }

    fn from_bytes(data: &[u8]) -> Result<Self> {
        bincode::deserialize(data).map_err(|e| BoolDBError::Serialization(e.to_string()))
    }

    /// Read a node from a page in the buffer pool.
    fn read(pool: &mut BufferPool, page_id: PageId) -> Result<Self> {
        let page = pool.fetch_page(page_id)?;
        // The node data length is stored in the first 4 bytes of the body.
        let len = u32::from_le_bytes(
            page.data[HEADER_SIZE..HEADER_SIZE + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let data = &page.data[HEADER_SIZE + 4..HEADER_SIZE + 4 + len];
        let node = Self::from_bytes(data)?;
        pool.unpin_page(page_id, false);
        Ok(node)
    }

    /// Write a node to a page in the buffer pool.
    fn write(&self, pool: &mut BufferPool, page_id: PageId) -> Result<()> {
        let data = self.to_bytes();
        if data.len() + 4 > PAGE_BODY_SIZE {
            return Err(BoolDBError::Internal(format!(
                "BTree node too large: {} bytes (max {})",
                data.len() + 4,
                PAGE_BODY_SIZE
            )));
        }
        let page = pool.fetch_page_mut(page_id)?;
        let page_type = if self.is_leaf {
            PageType::BTreeLeaf
        } else {
            PageType::BTreeInternal
        };
        page.set_page_type(page_type);
        // Write length prefix + data into body.
        let len = data.len() as u32;
        page.data[HEADER_SIZE..HEADER_SIZE + 4].copy_from_slice(&len.to_le_bytes());
        page.data[HEADER_SIZE + 4..HEADER_SIZE + 4 + data.len()].copy_from_slice(&data);
        page.update_checksum();
        pool.unpin_page(page_id, true);
        Ok(())
    }

    /// Allocate a new page and write this node to it.
    fn create(pool: &mut BufferPool, node: &BTreeNode) -> Result<PageId> {
        let page_type = if node.is_leaf {
            PageType::BTreeLeaf
        } else {
            PageType::BTreeInternal
        };
        let page = Page::new(0, page_type);
        let page_id = pool.new_page(page)?;
        pool.unpin_page(page_id, false);
        node.write(pool, page_id)?;
        Ok(page_id)
    }

    /// Find which child to descend into for a given key (internal nodes).
    fn find_child_index(&self, key: &[u8]) -> usize {
        match self.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(i) => i + 1, // key found: go right
            Err(i) => i,    // key not found: insertion point
        }
    }

    /// Find the position of a key in a leaf node.
    fn find_key_index(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.keys.binary_search_by(|k| k.as_slice().cmp(key))
    }

    /// Check if this node needs splitting.
    fn needs_split(&self) -> bool {
        self.serialized_size() > SPLIT_THRESHOLD
    }
}

/// Result of inserting into a node: optionally a split occurred.
struct InsertResult {
    /// If a split happened, the pushed-up key and the new right page.
    split: Option<(Vec<u8>, PageId)>,
}

/// A disk-based B+Tree index stored on pages in the buffer pool.
///
/// Internal nodes contain keys and child page pointers.
/// Leaf nodes contain keys, RowId lists, and a next-leaf pointer for range scans.
/// All nodes are stored as single pages.
pub struct DiskBTree {
    pub name: String,
    pub table_name: String,
    pub column_index: usize,
    /// Root page ID, or None if tree is empty.
    root_page_id: Option<PageId>,
}

impl DiskBTree {
    /// Create a new empty B+Tree (no pages allocated yet).
    pub fn new(name: &str, table_name: &str, column_index: usize) -> Self {
        DiskBTree {
            name: name.to_string(),
            table_name: table_name.to_string(),
            column_index,
            root_page_id: None,
        }
    }

    /// Restore a B+Tree from a known root page.
    pub fn from_root(
        name: &str,
        table_name: &str,
        column_index: usize,
        root_page_id: PageId,
    ) -> Self {
        DiskBTree {
            name: name.to_string(),
            table_name: table_name.to_string(),
            column_index,
            root_page_id: Some(root_page_id),
        }
    }

    pub fn root_page_id(&self) -> Option<PageId> {
        self.root_page_id
    }

    // --- Key encoding (same order-preserving scheme as the old BTreeIndex) ---

    fn encode_key(value: &Value) -> Vec<u8> {
        match value {
            Value::Null => vec![0x00],
            Value::Boolean(false) => vec![0x01, 0x00],
            Value::Boolean(true) => vec![0x01, 0x01],
            Value::Integer(v) => {
                let mut buf = vec![0x02];
                let ordered = (*v as u64) ^ (1u64 << 63);
                buf.extend_from_slice(&ordered.to_be_bytes());
                buf
            }
            Value::Float(v) => {
                let mut buf = vec![0x03];
                let bits = v.to_bits();
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

    // --- Public API ---

    /// Insert a key-value pair into the B+Tree.
    pub fn insert(&mut self, pool: &mut BufferPool, key: &Value, row_id: RowId) -> Result<()> {
        let encoded = Self::encode_key(key);

        match self.root_page_id {
            None => {
                // Tree is empty: create a leaf root.
                let mut leaf = BTreeNode::new_leaf();
                leaf.keys.push(encoded);
                leaf.values.push(vec![row_id]);
                let page_id = BTreeNode::create(pool, &leaf)?;
                self.root_page_id = Some(page_id);
                Ok(())
            }
            Some(root_id) => {
                let result = self.insert_recursive(pool, root_id, &encoded, row_id)?;
                if let Some((push_key, new_child_id)) = result.split {
                    // Root was split: create a new root.
                    let new_root = BTreeNode::new_internal(
                        vec![push_key],
                        vec![root_id, new_child_id],
                    );
                    let new_root_id = BTreeNode::create(pool, &new_root)?;
                    self.root_page_id = Some(new_root_id);
                }
                Ok(())
            }
        }
    }

    fn insert_recursive(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
        row_id: RowId,
    ) -> Result<InsertResult> {
        let mut node = BTreeNode::read(pool, page_id)?;

        if node.is_leaf {
            // Insert into leaf.
            match node.find_key_index(key) {
                Ok(i) => {
                    // Key exists: append RowId (duplicate key support).
                    if !node.values[i].contains(&row_id) {
                        node.values[i].push(row_id);
                    }
                }
                Err(i) => {
                    // New key: insert at position i.
                    node.keys.insert(i, key.to_vec());
                    node.values.insert(i, vec![row_id]);
                }
            }

            if node.needs_split() {
                let (push_key, new_page_id) = self.split_leaf(pool, page_id, &mut node)?;
                return Ok(InsertResult {
                    split: Some((push_key, new_page_id)),
                });
            }

            node.write(pool, page_id)?;
            Ok(InsertResult { split: None })
        } else {
            // Internal node: descend into the correct child.
            let child_idx = node.find_child_index(key);
            let child_id = node.children[child_idx];
            let result = self.insert_recursive(pool, child_id, key, row_id)?;

            if let Some((push_key, new_child_id)) = result.split {
                // Child was split: insert the pushed-up key into this node.
                node.keys.insert(child_idx, push_key);
                node.children.insert(child_idx + 1, new_child_id);

                if node.needs_split() {
                    let (push_key, new_page_id) =
                        self.split_internal(pool, page_id, &mut node)?;
                    return Ok(InsertResult {
                        split: Some((push_key, new_page_id)),
                    });
                }

                node.write(pool, page_id)?;
            }

            Ok(InsertResult { split: None })
        }
    }

    /// Split a leaf node. Returns (pushed-up key, new right page ID).
    fn split_leaf(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        node: &mut BTreeNode,
    ) -> Result<(Vec<u8>, PageId)> {
        let mid = node.keys.len() / 2;

        // New right leaf gets the upper half.
        let mut right = BTreeNode::new_leaf();
        right.keys = node.keys.split_off(mid);
        right.values = node.values.split_off(mid);
        right.next_leaf = node.next_leaf;

        // Allocate page for right leaf.
        let right_id = BTreeNode::create(pool, &right)?;

        // Left leaf (node) keeps the lower half, points to right.
        node.next_leaf = Some(right_id);
        node.write(pool, page_id)?;

        // Push up the first key of the right leaf.
        let push_key = right.keys[0].clone();
        Ok((push_key, right_id))
    }

    /// Split an internal node. Returns (pushed-up key, new right page ID).
    fn split_internal(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        node: &mut BTreeNode,
    ) -> Result<(Vec<u8>, PageId)> {
        let mid = node.keys.len() / 2;

        // The middle key gets pushed up.
        let push_key = node.keys[mid].clone();

        // Right node gets keys[mid+1..] and children[mid+1..].
        let right_keys = node.keys.split_off(mid + 1);
        let right_children = node.children.split_off(mid + 1);
        // Remove the middle key from the left node.
        node.keys.pop(); // remove keys[mid]

        let right = BTreeNode::new_internal(right_keys, right_children);
        let right_id = BTreeNode::create(pool, &right)?;

        node.write(pool, page_id)?;
        Ok((push_key, right_id))
    }

    /// Point lookup: find all RowIds for a given key.
    pub fn search(&self, pool: &mut BufferPool, key: &Value) -> Result<Vec<RowId>> {
        let encoded = Self::encode_key(key);
        match self.root_page_id {
            None => Ok(Vec::new()),
            Some(root_id) => self.search_recursive(pool, root_id, &encoded),
        }
    }

    fn search_recursive(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
    ) -> Result<Vec<RowId>> {
        let node = BTreeNode::read(pool, page_id)?;

        if node.is_leaf {
            match node.find_key_index(key) {
                Ok(i) => Ok(node.values[i].clone()),
                Err(_) => Ok(Vec::new()),
            }
        } else {
            let child_idx = node.find_child_index(key);
            self.search_recursive(pool, node.children[child_idx], key)
        }
    }

    /// Delete a specific key-RowId pair.
    pub fn delete(&mut self, pool: &mut BufferPool, key: &Value, row_id: &RowId) -> Result<()> {
        let encoded = Self::encode_key(key);
        if let Some(root_id) = self.root_page_id {
            self.delete_recursive(pool, root_id, &encoded, row_id)?;

            // If root is an internal node with no keys, collapse.
            let root = BTreeNode::read(pool, root_id)?;
            if !root.is_leaf && root.keys.is_empty() && root.children.len() == 1 {
                self.root_page_id = Some(root.children[0]);
            }
            // If root is an empty leaf, tree is empty.
            if root.is_leaf && root.keys.is_empty() {
                self.root_page_id = None;
            }
        }
        Ok(())
    }

    fn delete_recursive(
        &self,
        pool: &mut BufferPool,
        page_id: PageId,
        key: &[u8],
        row_id: &RowId,
    ) -> Result<()> {
        let mut node = BTreeNode::read(pool, page_id)?;

        if node.is_leaf {
            if let Ok(i) = node.find_key_index(key) {
                node.values[i].retain(|r| r != row_id);
                if node.values[i].is_empty() {
                    node.keys.remove(i);
                    node.values.remove(i);
                }
                node.write(pool, page_id)?;
            }
        } else {
            let child_idx = node.find_child_index(key);
            self.delete_recursive(pool, node.children[child_idx], key, row_id)?;
        }
        Ok(())
    }

    /// Range scan: return all RowIds where key is in [start, end] (inclusive).
    pub fn range_scan(
        &self,
        pool: &mut BufferPool,
        start: &Value,
        end: &Value,
    ) -> Result<Vec<RowId>> {
        let start_key = Self::encode_key(start);
        let end_key = Self::encode_key(end);

        // Find the leaf containing start_key.
        let leaf_id = match self.root_page_id {
            None => return Ok(Vec::new()),
            Some(root_id) => self.find_leaf(pool, root_id, &start_key)?,
        };

        // Scan leaves using next_leaf pointers.
        let mut results = Vec::new();
        let mut current_leaf_id = Some(leaf_id);

        while let Some(lid) = current_leaf_id {
            let node = BTreeNode::read(pool, lid)?;
            for (i, k) in node.keys.iter().enumerate() {
                if k.as_slice() > end_key.as_slice() {
                    return Ok(results);
                }
                if k.as_slice() >= start_key.as_slice() {
                    results.extend_from_slice(&node.values[i]);
                }
            }
            current_leaf_id = node.next_leaf;
        }

        Ok(results)
    }

    /// Scan all entries in key order.
    pub fn scan_all(&self, pool: &mut BufferPool) -> Result<Vec<(Vec<u8>, RowId)>> {
        let first_leaf = match self.root_page_id {
            None => return Ok(Vec::new()),
            Some(root_id) => self.find_leftmost_leaf(pool, root_id)?,
        };

        let mut results = Vec::new();
        let mut current = Some(first_leaf);

        while let Some(lid) = current {
            let node = BTreeNode::read(pool, lid)?;
            for (i, k) in node.keys.iter().enumerate() {
                for rid in &node.values[i] {
                    results.push((k.clone(), *rid));
                }
            }
            current = node.next_leaf;
        }

        Ok(results)
    }

    /// Count total entries.
    pub fn len(&self, pool: &mut BufferPool) -> Result<usize> {
        let first_leaf = match self.root_page_id {
            None => return Ok(0),
            Some(root_id) => self.find_leftmost_leaf(pool, root_id)?,
        };

        let mut count = 0;
        let mut current = Some(first_leaf);

        while let Some(lid) = current {
            let node = BTreeNode::read(pool, lid)?;
            for vals in &node.values {
                count += vals.len();
            }
            current = node.next_leaf;
        }

        Ok(count)
    }

    pub fn is_empty(&self) -> bool {
        self.root_page_id.is_none()
    }

    /// Return the depth (number of levels) of the tree.
    /// 0 = empty, 1 = just a leaf root, 2 = root + leaves, etc.
    pub fn depth(&self, pool: &mut BufferPool) -> Result<usize> {
        match self.root_page_id {
            None => Ok(0),
            Some(root_id) => {
                let mut d = 1;
                let mut current = root_id;
                loop {
                    let node = BTreeNode::read(pool, current)?;
                    if node.is_leaf {
                        return Ok(d);
                    }
                    d += 1;
                    current = node.children[0];
                }
            }
        }
    }

    // --- Helper methods ---

    /// Find the leaf page that would contain the given key.
    fn find_leaf(&self, pool: &mut BufferPool, page_id: PageId, key: &[u8]) -> Result<PageId> {
        let node = BTreeNode::read(pool, page_id)?;
        if node.is_leaf {
            Ok(page_id)
        } else {
            let child_idx = node.find_child_index(key);
            self.find_leaf(pool, node.children[child_idx], key)
        }
    }

    /// Find the leftmost leaf (for full scans).
    fn find_leftmost_leaf(&self, pool: &mut BufferPool, page_id: PageId) -> Result<PageId> {
        let node = BTreeNode::read(pool, page_id)?;
        if node.is_leaf {
            Ok(page_id)
        } else {
            self.find_leftmost_leaf(pool, node.children[0])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use std::fs;
    use std::path::PathBuf;

    fn setup(name: &str) -> (PathBuf, BufferPool) {
        let dir = std::env::temp_dir().join("booldb_test");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);
        let disk = DiskManager::open(&path).unwrap();
        let pool = BufferPool::new(disk, 64);
        (path, pool)
    }

    #[test]
    fn test_empty_tree() {
        let (path, mut pool) = setup("test_dbtree_empty.db");
        let tree = DiskBTree::new("idx", "t", 0);

        assert!(tree.is_empty());
        assert_eq!(tree.depth(&mut pool).unwrap(), 0);
        assert_eq!(tree.len(&mut pool).unwrap(), 0);
        assert_eq!(tree.search(&mut pool, &Value::Integer(1)).unwrap(), vec![]);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_insert_and_search() {
        let (path, mut pool) = setup("test_dbtree_basic.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        tree.insert(&mut pool, &Value::Integer(5), rid(0)).unwrap();
        tree.insert(&mut pool, &Value::Integer(3), rid(1)).unwrap();
        tree.insert(&mut pool, &Value::Integer(7), rid(2)).unwrap();
        tree.insert(&mut pool, &Value::Integer(1), rid(3)).unwrap();
        tree.insert(&mut pool, &Value::Integer(9), rid(4)).unwrap();

        assert_eq!(tree.len(&mut pool).unwrap(), 5);
        assert_eq!(tree.depth(&mut pool).unwrap(), 1); // all fit in one leaf

        assert_eq!(tree.search(&mut pool, &Value::Integer(3)).unwrap(), vec![rid(1)]);
        assert_eq!(tree.search(&mut pool, &Value::Integer(99)).unwrap(), vec![]);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_duplicate_keys() {
        let (path, mut pool) = setup("test_dbtree_dup.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        tree.insert(&mut pool, &Value::Text("Alice".into()), rid(0)).unwrap();
        tree.insert(&mut pool, &Value::Text("Alice".into()), rid(1)).unwrap();
        tree.insert(&mut pool, &Value::Text("Bob".into()), rid(2)).unwrap();

        let results = tree.search(&mut pool, &Value::Text("Alice".into())).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(tree.len(&mut pool).unwrap(), 3);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_delete() {
        let (path, mut pool) = setup("test_dbtree_delete.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        tree.insert(&mut pool, &Value::Integer(1), rid(0)).unwrap();
        tree.insert(&mut pool, &Value::Integer(2), rid(1)).unwrap();
        tree.insert(&mut pool, &Value::Integer(3), rid(2)).unwrap();

        tree.delete(&mut pool, &Value::Integer(2), &rid(1)).unwrap();
        assert_eq!(tree.search(&mut pool, &Value::Integer(2)).unwrap(), vec![]);
        assert_eq!(tree.len(&mut pool).unwrap(), 2);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_range_scan() {
        let (path, mut pool) = setup("test_dbtree_range.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        for i in 1..=10 {
            tree.insert(&mut pool, &Value::Integer(i), rid(i as u16)).unwrap();
        }

        let results = tree
            .range_scan(&mut pool, &Value::Integer(3), &Value::Integer(7))
            .unwrap();
        assert_eq!(results.len(), 5);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_split_creates_depth() {
        let (path, mut pool) = setup("test_dbtree_split.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        // Insert enough entries to force leaf splits.
        // Each integer key is ~9 bytes + RowId ~6 bytes + overhead.
        // With SPLIT_THRESHOLD=3600, roughly ~200 entries trigger a split.
        for i in 0..500 {
            tree.insert(&mut pool, &Value::Integer(i), rid(i as u16)).unwrap();
        }

        let depth = tree.depth(&mut pool).unwrap();
        assert!(depth >= 2, "Expected depth >= 2, got {}", depth);

        // Verify all entries are still searchable.
        for i in 0..500 {
            let results = tree.search(&mut pool, &Value::Integer(i)).unwrap();
            assert_eq!(results, vec![rid(i as u16)], "Key {} not found", i);
        }

        assert_eq!(tree.len(&mut pool).unwrap(), 500);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_large_tree_depth() {
        let (path, mut pool) = setup("test_dbtree_large.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        // Insert 5000 entries — should create a multi-level tree.
        for i in 0..5000 {
            tree.insert(&mut pool, &Value::Integer(i), rid((i % 65536) as u16))
                .unwrap();
        }

        let depth = tree.depth(&mut pool).unwrap();
        let len = tree.len(&mut pool).unwrap();

        assert_eq!(len, 5000);
        assert!(depth >= 2, "Expected depth >= 2, got {}", depth);

        // Verify range scan across leaves.
        let range = tree
            .range_scan(&mut pool, &Value::Integer(100), &Value::Integer(199))
            .unwrap();
        assert_eq!(range.len(), 100);

        // Verify scan_all returns all entries in sorted order.
        let all = tree.scan_all(&mut pool).unwrap();
        assert_eq!(all.len(), 5000);
        for w in all.windows(2) {
            assert!(w[0].0 <= w[1].0, "Entries not in sorted order");
        }

        // Spot-check a few searches.
        assert_eq!(
            tree.search(&mut pool, &Value::Integer(0)).unwrap(),
            vec![rid(0)]
        );
        assert_eq!(
            tree.search(&mut pool, &Value::Integer(4999)).unwrap(),
            vec![rid(4999)]
        );
        assert_eq!(
            tree.search(&mut pool, &Value::Integer(9999)).unwrap(),
            vec![]
        );

        eprintln!("Large tree: {} entries, depth {}", len, depth);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_integer_ordering() {
        let (path, mut pool) = setup("test_dbtree_order.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        // Insert negative and positive integers.
        tree.insert(&mut pool, &Value::Integer(-10), rid(0)).unwrap();
        tree.insert(&mut pool, &Value::Integer(5), rid(1)).unwrap();
        tree.insert(&mut pool, &Value::Integer(-1), rid(2)).unwrap();
        tree.insert(&mut pool, &Value::Integer(100), rid(3)).unwrap();

        let all = tree.scan_all(&mut pool).unwrap();
        // Should be sorted: -10, -1, 5, 100
        assert_eq!(all.len(), 4);
        assert_eq!(all[0].1, rid(0)); // -10
        assert_eq!(all[1].1, rid(2)); // -1
        assert_eq!(all[2].1, rid(1)); // 5
        assert_eq!(all[3].1, rid(3)); // 100

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_delete_until_empty() {
        let (path, mut pool) = setup("test_dbtree_del_empty.db");
        let mut tree = DiskBTree::new("idx", "t", 0);

        let rid = |s: u16| RowId { page_id: 0, slot_id: s };

        tree.insert(&mut pool, &Value::Integer(1), rid(0)).unwrap();
        tree.insert(&mut pool, &Value::Integer(2), rid(1)).unwrap();

        tree.delete(&mut pool, &Value::Integer(1), &rid(0)).unwrap();
        tree.delete(&mut pool, &Value::Integer(2), &rid(1)).unwrap();

        assert_eq!(tree.len(&mut pool).unwrap(), 0);
        assert!(tree.is_empty());

        fs::remove_file(&path).unwrap();
    }
}

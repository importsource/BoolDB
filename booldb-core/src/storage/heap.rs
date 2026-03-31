use crate::error::{BoolDBError, Result};
use crate::storage::buffer::BufferPool;
use crate::storage::page::{Page, PageType};
use crate::types::{
    deserialize_row, serialize_row, PageId, Row, RowId, Tuple,
};

/// A heap file stores rows across multiple slotted pages.
/// It maintains a list of page IDs belonging to a table.
pub struct HeapFile {
    /// Page IDs belonging to this heap file.
    page_ids: Vec<PageId>,
    /// Table identifier for error messages.
    #[allow(dead_code)]
    table_name: String,
}

impl HeapFile {
    /// Create a new, empty heap file.
    pub fn new(table_name: &str) -> Self {
        HeapFile {
            page_ids: Vec::new(),
            table_name: table_name.to_string(),
        }
    }

    /// Create a heap file from existing page IDs (e.g., loaded from catalog).
    pub fn from_pages(table_name: &str, page_ids: Vec<PageId>) -> Self {
        HeapFile {
            page_ids,
            table_name: table_name.to_string(),
        }
    }

    /// Get the page IDs.
    pub fn page_ids(&self) -> &[PageId] {
        &self.page_ids
    }

    /// Insert a row. Returns the RowId where it was stored.
    pub fn insert(&mut self, pool: &mut BufferPool, row: &Row) -> Result<RowId> {
        let data = serialize_row(row);

        // Try to insert into an existing page with space.
        for &page_id in &self.page_ids {
            let page = pool.fetch_page_mut(page_id)?;
            if let Some(slot_id) = page.insert_tuple(&data) {
                pool.unpin_page(page_id, true);
                return Ok(RowId { page_id, slot_id });
            }
            pool.unpin_page(page_id, false);
        }

        // No page has space — allocate a new one.
        let new_page = Page::new(0, PageType::Heap); // page_id will be set by buffer pool
        let page_id = pool.new_page(new_page)?;
        self.page_ids.push(page_id);

        let page = pool.fetch_page_mut(page_id)?;
        // Re-initialize with correct page_id
        page.set_page_id(page_id);
        page.set_page_type(PageType::Heap);
        let slot_id = page.insert_tuple(&data).ok_or_else(|| BoolDBError::PageFull {
            page_id,
            tuple_size: data.len(),
        })?;
        pool.unpin_page(page_id, true);

        Ok(RowId { page_id, slot_id })
    }

    /// Get a row by its RowId.
    pub fn get(&self, pool: &mut BufferPool, row_id: RowId) -> Result<Row> {
        let page = pool.fetch_page(row_id.page_id)?;
        let data = page
            .get_tuple(row_id.slot_id)
            .ok_or(BoolDBError::TupleNotFound {
                page_id: row_id.page_id,
                slot_id: row_id.slot_id,
            })?;
        let row = deserialize_row(data);
        pool.unpin_page(row_id.page_id, false);
        Ok(row)
    }

    /// Delete a row by its RowId.
    pub fn delete(&self, pool: &mut BufferPool, row_id: RowId) -> Result<()> {
        let page = pool.fetch_page_mut(row_id.page_id)?;
        if !page.delete_tuple(row_id.slot_id) {
            pool.unpin_page(row_id.page_id, false);
            return Err(BoolDBError::TupleNotFound {
                page_id: row_id.page_id,
                slot_id: row_id.slot_id,
            });
        }
        pool.unpin_page(row_id.page_id, true);
        Ok(())
    }

    /// Update a row. Deletes the old one and inserts the new one.
    /// Returns the new RowId (may differ if the row moved to a different page).
    pub fn update(
        &mut self,
        pool: &mut BufferPool,
        row_id: RowId,
        new_row: &Row,
    ) -> Result<RowId> {
        self.delete(pool, row_id)?;
        self.insert(pool, new_row)
    }

    /// Scan all rows in the heap file. Returns tuples with their RowIds.
    pub fn scan(&self, pool: &mut BufferPool) -> Result<Vec<Tuple>> {
        let mut tuples = Vec::new();
        for &page_id in &self.page_ids {
            let page = pool.fetch_page(page_id)?;
            for (slot_id, data) in page.iter_tuples() {
                let values = deserialize_row(&data);
                tuples.push(Tuple {
                    row_id: RowId { page_id, slot_id },
                    values,
                });
            }
            pool.unpin_page(page_id, false);
        }
        Ok(tuples)
    }

    /// Count the number of live rows.
    pub fn count(&self, pool: &mut BufferPool) -> Result<usize> {
        let mut count = 0;
        for &page_id in &self.page_ids {
            let page = pool.fetch_page(page_id)?;
            count += page.iter_tuples().len();
            pool.unpin_page(page_id, false);
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::disk::DiskManager;
    use crate::types::Value;
    use std::fs;
    use std::path::PathBuf;

    fn setup(name: &str) -> (PathBuf, BufferPool) {
        let dir = std::env::temp_dir().join("booldb_test");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);
        let disk = DiskManager::open(&path).unwrap();
        let pool = BufferPool::new(disk, 16);
        (path, pool)
    }

    fn make_row(id: i64, name: &str) -> Row {
        vec![Value::Integer(id), Value::Text(name.to_string())]
    }

    #[test]
    fn test_insert_and_scan() {
        let (path, mut pool) = setup("test_heap_scan.db");
        let mut heap = HeapFile::new("test_table");

        heap.insert(&mut pool, &make_row(1, "Alice")).unwrap();
        heap.insert(&mut pool, &make_row(2, "Bob")).unwrap();
        heap.insert(&mut pool, &make_row(3, "Charlie")).unwrap();

        let tuples = heap.scan(&mut pool).unwrap();
        assert_eq!(tuples.len(), 3);
        assert_eq!(tuples[0].values, make_row(1, "Alice"));
        assert_eq!(tuples[1].values, make_row(2, "Bob"));
        assert_eq!(tuples[2].values, make_row(3, "Charlie"));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_delete() {
        let (path, mut pool) = setup("test_heap_delete.db");
        let mut heap = HeapFile::new("test_table");

        let rid1 = heap.insert(&mut pool, &make_row(1, "Alice")).unwrap();
        let _rid2 = heap.insert(&mut pool, &make_row(2, "Bob")).unwrap();

        heap.delete(&mut pool, rid1).unwrap();

        let tuples = heap.scan(&mut pool).unwrap();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].values, make_row(2, "Bob"));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_update() {
        let (path, mut pool) = setup("test_heap_update.db");
        let mut heap = HeapFile::new("test_table");

        let rid = heap.insert(&mut pool, &make_row(1, "Alice")).unwrap();
        heap.update(&mut pool, rid, &make_row(1, "Alicia")).unwrap();

        let tuples = heap.scan(&mut pool).unwrap();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].values, make_row(1, "Alicia"));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_count() {
        let (path, mut pool) = setup("test_heap_count.db");
        let mut heap = HeapFile::new("test_table");

        for i in 0..100 {
            heap.insert(&mut pool, &make_row(i, &format!("name_{}", i)))
                .unwrap();
        }

        assert_eq!(heap.count(&mut pool).unwrap(), 100);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_multi_page() {
        let (path, mut pool) = setup("test_heap_multipage.db");
        let mut heap = HeapFile::new("test_table");

        // Insert enough rows to span multiple pages
        let big_name = "x".repeat(200);
        for i in 0..50 {
            heap.insert(&mut pool, &make_row(i, &big_name)).unwrap();
        }

        assert!(heap.page_ids().len() > 1, "Should span multiple pages");
        assert_eq!(heap.count(&mut pool).unwrap(), 50);

        let tuples = heap.scan(&mut pool).unwrap();
        assert_eq!(tuples.len(), 50);

        fs::remove_file(&path).unwrap();
    }
}

use std::collections::HashMap;

use crate::error::{BoolDBError, Result};
use crate::storage::disk::DiskManager;
use crate::storage::page::Page;
use crate::types::PageId;

/// A frame in the buffer pool holding a page.
struct BufferFrame {
    page: Page,
    page_id: PageId,
    dirty: bool,
    pin_count: u32,
    /// Clock hand bit for clock replacement algorithm.
    reference: bool,
}

/// Buffer pool: an LRU/Clock page cache over the disk manager.
pub struct BufferPool {
    frames: Vec<BufferFrame>,
    page_table: HashMap<PageId, usize>, // page_id → frame index
    capacity: usize,
    clock_hand: usize,
    disk: DiskManager,
}

impl BufferPool {
    /// Create a new buffer pool with the given capacity (number of pages).
    pub fn new(disk: DiskManager, capacity: usize) -> Self {
        BufferPool {
            frames: Vec::with_capacity(capacity),
            page_table: HashMap::new(),
            capacity,
            clock_hand: 0,
            disk,
        }
    }

    /// Fetch a page. Pins it in the pool (increments pin count).
    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&Page> {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.frames[frame_idx].pin_count += 1;
            self.frames[frame_idx].reference = true;
            return Ok(&self.frames[frame_idx].page);
        }

        let page = self.disk.read_page(page_id)?;
        let frame_idx = self.find_or_evict()?;
        self.install_page(frame_idx, page_id, page, false);
        Ok(&self.frames[frame_idx].page)
    }

    /// Fetch a mutable reference to a page. Marks it dirty.
    pub fn fetch_page_mut(&mut self, page_id: PageId) -> Result<&mut Page> {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            self.frames[frame_idx].pin_count += 1;
            self.frames[frame_idx].reference = true;
            self.frames[frame_idx].dirty = true;
            return Ok(&mut self.frames[frame_idx].page);
        }

        let page = self.disk.read_page(page_id)?;
        let frame_idx = self.find_or_evict()?;
        self.install_page(frame_idx, page_id, page, true);
        Ok(&mut self.frames[frame_idx].page)
    }

    /// Allocate a new page on disk and bring it into the pool.
    pub fn new_page(&mut self, page: Page) -> Result<PageId> {
        let page_id = self.disk.allocate_page()?;
        let frame_idx = self.find_or_evict()?;
        self.install_page(frame_idx, page_id, page, true);
        Ok(page_id)
    }

    /// Unpin a page (decrement pin count).
    pub fn unpin_page(&mut self, page_id: PageId, dirty: bool) {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_idx];
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
            if dirty {
                frame.dirty = true;
            }
        }
    }

    /// Flush a specific page to disk.
    pub fn flush_page(&mut self, page_id: PageId) -> Result<()> {
        if let Some(&frame_idx) = self.page_table.get(&page_id) {
            let frame = &mut self.frames[frame_idx];
            if frame.dirty {
                frame.page.update_checksum();
                self.disk.write_page(page_id, &frame.page)?;
                frame.dirty = false;
            }
        }
        Ok(())
    }

    /// Flush all dirty pages to disk.
    pub fn flush_all(&mut self) -> Result<()> {
        for i in 0..self.frames.len() {
            if self.frames[i].dirty {
                let page_id = self.frames[i].page_id;
                self.frames[i].page.update_checksum();
                self.disk.write_page(page_id, &self.frames[i].page)?;
                self.frames[i].dirty = false;
            }
        }
        Ok(())
    }

    /// Get the underlying disk manager (for direct operations like num_pages).
    pub fn disk(&self) -> &DiskManager {
        &self.disk
    }

    pub fn disk_mut(&mut self) -> &mut DiskManager {
        &mut self.disk
    }

    /// Find a free frame or evict one using the clock algorithm.
    fn find_or_evict(&mut self) -> Result<usize> {
        // If there's still capacity, just add a new frame.
        if self.frames.len() < self.capacity {
            let idx = self.frames.len();
            self.frames.push(BufferFrame {
                page: Page::default(),
                page_id: 0,
                dirty: false,
                pin_count: 0,
                reference: false,
            });
            return Ok(idx);
        }

        // Clock replacement: sweep for an unpinned, unreferenced page.
        let n = self.frames.len();
        for _ in 0..2 * n {
            let frame = &mut self.frames[self.clock_hand];
            if frame.pin_count == 0 {
                if frame.reference {
                    frame.reference = false;
                } else {
                    // Evict this frame.
                    if frame.dirty {
                        let pid = frame.page_id;
                        frame.page.update_checksum();
                        self.disk.write_page(pid, &frame.page)?;
                        frame.dirty = false;
                    }
                    let old_page_id = frame.page_id;
                    self.page_table.remove(&old_page_id);
                    let idx = self.clock_hand;
                    self.clock_hand = (self.clock_hand + 1) % n;
                    return Ok(idx);
                }
            }
            self.clock_hand = (self.clock_hand + 1) % n;
        }

        Err(BoolDBError::BufferPoolFull)
    }

    fn install_page(
        &mut self,
        frame_idx: usize,
        page_id: PageId,
        page: Page,
        dirty: bool,
    ) {
        self.frames[frame_idx] = BufferFrame {
            page,
            page_id,
            dirty,
            pin_count: 1,
            reference: true,
        };
        self.page_table.insert(page_id, frame_idx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageType;
    use std::fs;
    use std::path::PathBuf;

    fn setup(name: &str) -> (PathBuf, BufferPool) {
        let dir = std::env::temp_dir().join("booldb_test");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(name);
        let _ = fs::remove_file(&path);

        let disk = DiskManager::open(&path).unwrap();
        let pool = BufferPool::new(disk, 4);
        (path, pool)
    }

    #[test]
    fn test_new_and_fetch_page() {
        let (path, mut pool) = setup("test_buffer_basic.db");

        let mut page = Page::new(0, PageType::Heap);
        page.insert_tuple(b"buffer test");
        let page_id = pool.new_page(page).unwrap();

        pool.unpin_page(page_id, true);

        let fetched = pool.fetch_page(page_id).unwrap();
        assert_eq!(fetched.get_tuple(0).unwrap(), b"buffer test");
        pool.unpin_page(page_id, false);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_flush_and_reread() {
        let (path, mut pool) = setup("test_buffer_flush.db");

        let mut page = Page::new(0, PageType::Heap);
        page.insert_tuple(b"flush me");
        let page_id = pool.new_page(page).unwrap();
        pool.unpin_page(page_id, true);

        pool.flush_all().unwrap();

        // Re-open from disk
        drop(pool);
        let disk = DiskManager::open(&path).unwrap();
        let mut pool2 = BufferPool::new(disk, 4);
        let fetched = pool2.fetch_page(page_id).unwrap();
        assert_eq!(fetched.get_tuple(0).unwrap(), b"flush me");
        pool2.unpin_page(page_id, false);

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_eviction() {
        let (path, mut pool) = setup("test_buffer_evict.db");

        // Pool capacity is 4, insert 6 pages
        let mut page_ids = Vec::new();
        for i in 0..6u32 {
            let mut page = Page::new(i, PageType::Heap);
            page.insert_tuple(format!("page{}", i).as_bytes());
            let pid = pool.new_page(page).unwrap();
            pool.unpin_page(pid, true);
            page_ids.push(pid);
        }

        // All 6 pages should still be readable (some via eviction + re-fetch)
        for pid in &page_ids {
            let p = pool.fetch_page(*pid).unwrap();
            assert!(p.get_tuple(0).is_some());
            pool.unpin_page(*pid, false);
        }

        fs::remove_file(&path).unwrap();
    }
}

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{BoolDBError, Result};
use crate::storage::page::{Page, PAGE_SIZE};
use crate::types::PageId;

/// Manages reading and writing pages to a single database file.
pub struct DiskManager {
    file_path: PathBuf,
    file: File,
    num_pages: u32,
}

impl DiskManager {
    /// Open or create a database file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file_path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&file_path)?;

        let metadata = file.metadata()?;
        let file_size = metadata.len();
        let num_pages = (file_size / PAGE_SIZE as u64) as u32;

        Ok(DiskManager {
            file_path,
            file,
            num_pages,
        })
    }

    /// Allocate a new page and return its ID.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        let page_id = self.num_pages;
        self.num_pages += 1;

        // Extend the file
        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&[0u8; PAGE_SIZE])?;
        self.file.flush()?;

        Ok(page_id)
    }

    /// Read a page from disk.
    pub fn read_page(&mut self, page_id: PageId) -> Result<Page> {
        if page_id >= self.num_pages {
            return Err(BoolDBError::PageNotFound { page_id });
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;

        let mut page = Page::default();
        self.file.read_exact(&mut page.data)?;
        Ok(page)
    }

    /// Write a page to disk.
    pub fn write_page(&mut self, page_id: PageId, page: &Page) -> Result<()> {
        if page_id >= self.num_pages {
            return Err(BoolDBError::PageNotFound { page_id });
        }

        let offset = page_id as u64 * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page.data)?;
        self.file.flush()?;
        Ok(())
    }

    /// Total number of pages in the file.
    pub fn num_pages(&self) -> u32 {
        self.num_pages
    }

    /// Path to the database file.
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::PageType;
    use std::fs;

    fn tmp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("booldb_test");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn test_allocate_and_read_write() {
        let path = tmp_path("test_disk_basic.db");
        let _ = fs::remove_file(&path);

        let mut dm = DiskManager::open(&path).unwrap();
        assert_eq!(dm.num_pages(), 0);

        let pid = dm.allocate_page().unwrap();
        assert_eq!(pid, 0);
        assert_eq!(dm.num_pages(), 1);

        let mut page = Page::new(pid, PageType::Heap);
        page.insert_tuple(b"hello disk").unwrap();
        dm.write_page(pid, &page).unwrap();

        let read_back = dm.read_page(pid).unwrap();
        assert_eq!(read_back.get_tuple(0).unwrap(), b"hello disk");

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_persistence_across_reopen() {
        let path = tmp_path("test_disk_reopen.db");
        let _ = fs::remove_file(&path);

        {
            let mut dm = DiskManager::open(&path).unwrap();
            let pid = dm.allocate_page().unwrap();
            let mut page = Page::new(pid, PageType::Heap);
            page.insert_tuple(b"persistent data").unwrap();
            dm.write_page(pid, &page).unwrap();
        }

        {
            let mut dm = DiskManager::open(&path).unwrap();
            assert_eq!(dm.num_pages(), 1);
            let page = dm.read_page(0).unwrap();
            assert_eq!(page.get_tuple(0).unwrap(), b"persistent data");
        }

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_read_nonexistent_page() {
        let path = tmp_path("test_disk_nopage.db");
        let _ = fs::remove_file(&path);

        let mut dm = DiskManager::open(&path).unwrap();
        let result = dm.read_page(99);
        assert!(result.is_err());

        fs::remove_file(&path).unwrap();
    }
}

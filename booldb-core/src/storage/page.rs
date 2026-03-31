use crate::types::PageId;

/// Page size in bytes (4 KB).
pub const PAGE_SIZE: usize = 4096;

/// Page header size in bytes.
/// Layout: page_id(4) + page_type(1) + num_slots(2) + free_space_offset(2) + checksum(4) = 13 bytes
pub const HEADER_SIZE: usize = 13;

/// Usable body size per page (PAGE_SIZE - HEADER_SIZE).
pub const PAGE_BODY_SIZE: usize = PAGE_SIZE - HEADER_SIZE;

/// Each slot entry: offset(2) + length(2) = 4 bytes
const SLOT_ENTRY_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    Invalid = 0,
    Heap = 1,
    BTreeInternal = 2,
    BTreeLeaf = 3,
    Catalog = 4,
}

impl From<u8> for PageType {
    fn from(v: u8) -> Self {
        match v {
            1 => PageType::Heap,
            2 => PageType::BTreeInternal,
            3 => PageType::BTreeLeaf,
            4 => PageType::Catalog,
            _ => PageType::Invalid,
        }
    }
}

/// A slotted page with a fixed-size byte buffer.
///
/// Layout:
/// ```text
/// [Header (13 bytes)] [Slot array →  ...  ← Tuple data] [Free space]
/// ```
///
/// - Slot array grows forward from the header.
/// - Tuple data grows backward from the end of the page.
/// - Free space is in between.
#[derive(Clone)]
pub struct Page {
    pub data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new empty page.
    pub fn new(page_id: PageId, page_type: PageType) -> Self {
        let mut page = Page {
            data: [0u8; PAGE_SIZE],
        };
        page.set_page_id(page_id);
        page.set_page_type(page_type);
        page.set_num_slots(0);
        page.set_free_space_offset(PAGE_SIZE as u16);
        page.update_checksum();
        page
    }

    // --- Header accessors ---

    pub fn page_id(&self) -> PageId {
        u32::from_le_bytes(self.data[0..4].try_into().unwrap())
    }

    pub fn set_page_id(&mut self, id: PageId) {
        self.data[0..4].copy_from_slice(&id.to_le_bytes());
    }

    pub fn page_type(&self) -> PageType {
        PageType::from(self.data[4])
    }

    pub fn set_page_type(&mut self, pt: PageType) {
        self.data[4] = pt as u8;
    }

    pub fn num_slots(&self) -> u16 {
        u16::from_le_bytes(self.data[5..7].try_into().unwrap())
    }

    pub fn set_num_slots(&mut self, n: u16) {
        self.data[5..7].copy_from_slice(&n.to_le_bytes());
    }

    /// Offset where free space ends (tuple data starts growing backward from here).
    pub fn free_space_offset(&self) -> u16 {
        u16::from_le_bytes(self.data[7..9].try_into().unwrap())
    }

    pub fn set_free_space_offset(&mut self, offset: u16) {
        self.data[7..9].copy_from_slice(&offset.to_le_bytes());
    }

    pub fn checksum(&self) -> u32 {
        u32::from_le_bytes(self.data[9..13].try_into().unwrap())
    }

    fn set_checksum(&mut self, crc: u32) {
        self.data[9..13].copy_from_slice(&crc.to_le_bytes());
    }

    pub fn update_checksum(&mut self) {
        // Zero out checksum field before computing.
        self.data[9..13].copy_from_slice(&[0u8; 4]);
        let crc = crc32fast::hash(&self.data);
        self.set_checksum(crc);
    }

    pub fn verify_checksum(&self) -> bool {
        let stored = self.checksum();
        let mut tmp = self.data;
        tmp[9..13].copy_from_slice(&[0u8; 4]);
        let computed = crc32fast::hash(&tmp);
        stored == computed
    }

    // --- Slot array ---

    fn slot_offset(slot_index: u16) -> usize {
        HEADER_SIZE + (slot_index as usize) * SLOT_ENTRY_SIZE
    }

    /// Get the (offset, length) for a slot. Returns (0, 0) for deleted slots.
    pub fn get_slot(&self, slot_index: u16) -> (u16, u16) {
        let base = Self::slot_offset(slot_index);
        let offset = u16::from_le_bytes(self.data[base..base + 2].try_into().unwrap());
        let length = u16::from_le_bytes(self.data[base + 2..base + 4].try_into().unwrap());
        (offset, length)
    }

    fn set_slot(&mut self, slot_index: u16, offset: u16, length: u16) {
        let base = Self::slot_offset(slot_index);
        self.data[base..base + 2].copy_from_slice(&offset.to_le_bytes());
        self.data[base + 2..base + 4].copy_from_slice(&length.to_le_bytes());
    }

    /// Available free space in the page.
    pub fn free_space(&self) -> usize {
        let slot_array_end = HEADER_SIZE + (self.num_slots() as usize) * SLOT_ENTRY_SIZE;
        let tuple_data_start = self.free_space_offset() as usize;
        if tuple_data_start > slot_array_end {
            tuple_data_start - slot_array_end
        } else {
            0
        }
    }

    /// Insert a tuple into the page. Returns the slot index, or None if not enough space.
    pub fn insert_tuple(&mut self, data: &[u8]) -> Option<u16> {
        let needed = data.len() + SLOT_ENTRY_SIZE;
        if self.free_space() < needed {
            return None;
        }

        // Allocate space from the end.
        let new_offset = self.free_space_offset() - data.len() as u16;
        self.data[new_offset as usize..new_offset as usize + data.len()].copy_from_slice(data);
        self.set_free_space_offset(new_offset);

        // Find a deleted slot to reuse, or append a new one.
        let num_slots = self.num_slots();
        let mut slot_index = num_slots;
        for i in 0..num_slots {
            let (off, len) = self.get_slot(i);
            if off == 0 && len == 0 {
                slot_index = i;
                break;
            }
        }

        self.set_slot(slot_index, new_offset, data.len() as u16);
        if slot_index == num_slots {
            self.set_num_slots(num_slots + 1);
        }

        self.update_checksum();
        Some(slot_index)
    }

    /// Read tuple data at the given slot.
    pub fn get_tuple(&self, slot_index: u16) -> Option<&[u8]> {
        if slot_index >= self.num_slots() {
            return None;
        }
        let (offset, length) = self.get_slot(slot_index);
        if offset == 0 && length == 0 {
            return None; // Deleted
        }
        Some(&self.data[offset as usize..(offset + length) as usize])
    }

    /// Delete a tuple by zeroing its slot entry.
    pub fn delete_tuple(&mut self, slot_index: u16) -> bool {
        if slot_index >= self.num_slots() {
            return false;
        }
        let (offset, length) = self.get_slot(slot_index);
        if offset == 0 && length == 0 {
            return false; // Already deleted
        }
        self.set_slot(slot_index, 0, 0);
        self.update_checksum();
        true
    }

    /// Iterate over all live tuples: yields (slot_index, data).
    pub fn iter_tuples(&self) -> Vec<(u16, Vec<u8>)> {
        let mut result = Vec::new();
        for i in 0..self.num_slots() {
            if let Some(data) = self.get_tuple(i) {
                result.push((i, data.to_vec()));
            }
        }
        result
    }
}

impl Default for Page {
    fn default() -> Self {
        Page {
            data: [0u8; PAGE_SIZE],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_page() {
        let page = Page::new(42, PageType::Heap);
        assert_eq!(page.page_id(), 42);
        assert_eq!(page.page_type(), PageType::Heap);
        assert_eq!(page.num_slots(), 0);
        assert_eq!(page.free_space_offset(), PAGE_SIZE as u16);
        assert!(page.verify_checksum());
    }

    #[test]
    fn test_insert_and_read_tuple() {
        let mut page = Page::new(1, PageType::Heap);
        let data = b"hello world";
        let slot = page.insert_tuple(data).unwrap();
        assert_eq!(slot, 0);
        assert_eq!(page.num_slots(), 1);
        assert_eq!(page.get_tuple(0).unwrap(), data);
        assert!(page.verify_checksum());
    }

    #[test]
    fn test_delete_tuple() {
        let mut page = Page::new(1, PageType::Heap);
        page.insert_tuple(b"row1").unwrap();
        page.insert_tuple(b"row2").unwrap();

        assert!(page.delete_tuple(0));
        assert!(page.get_tuple(0).is_none());
        assert_eq!(page.get_tuple(1).unwrap(), b"row2");
    }

    #[test]
    fn test_slot_reuse() {
        let mut page = Page::new(1, PageType::Heap);
        page.insert_tuple(b"row1").unwrap();
        page.insert_tuple(b"row2").unwrap();
        page.delete_tuple(0);

        let slot = page.insert_tuple(b"row3").unwrap();
        assert_eq!(slot, 0); // Reused deleted slot
        assert_eq!(page.get_tuple(0).unwrap(), b"row3");
    }

    #[test]
    fn test_page_full() {
        let mut page = Page::new(1, PageType::Heap);
        // Fill the page with large tuples
        let big = vec![0xABu8; 1000];
        let mut count = 0;
        while page.insert_tuple(&big).is_some() {
            count += 1;
        }
        assert!(count > 0);
        assert!(count < 5); // ~4KB page, ~1KB tuples
    }

    #[test]
    fn test_iter_tuples() {
        let mut page = Page::new(1, PageType::Heap);
        page.insert_tuple(b"a").unwrap();
        page.insert_tuple(b"b").unwrap();
        page.insert_tuple(b"c").unwrap();
        page.delete_tuple(1);

        let tuples = page.iter_tuples();
        assert_eq!(tuples.len(), 2);
        assert_eq!(tuples[0], (0, b"a".to_vec()));
        assert_eq!(tuples[1], (2, b"c".to_vec()));
    }
}

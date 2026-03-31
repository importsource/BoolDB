use std::collections::HashMap;

use crate::error::{BoolDBError, Result};
use crate::types::RowId;

/// Lock modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// A lock held on a row.
#[derive(Debug, Clone)]
struct LockEntry {
    mode: LockMode,
    holders: Vec<u64>, // tx_ids holding this lock
}

/// Row-level lock manager.
pub struct LockManager {
    locks: HashMap<RowId, LockEntry>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            locks: HashMap::new(),
        }
    }

    /// Acquire a lock. Returns Ok if granted, Err if conflicting.
    pub fn acquire(&mut self, row_id: RowId, tx_id: u64, mode: LockMode) -> Result<()> {
        if let Some(entry) = self.locks.get_mut(&row_id) {
            match (entry.mode, mode) {
                // Multiple shared locks are compatible.
                (LockMode::Shared, LockMode::Shared) => {
                    if !entry.holders.contains(&tx_id) {
                        entry.holders.push(tx_id);
                    }
                    return Ok(());
                }
                // Same tx can upgrade or re-acquire.
                _ if entry.holders == vec![tx_id] => {
                    entry.mode = mode;
                    return Ok(());
                }
                // Conflict.
                _ => {
                    return Err(BoolDBError::LockConflict(format!(
                        "Row {:?} locked by tx {:?} in {:?} mode, cannot acquire {:?}",
                        row_id, entry.holders, entry.mode, mode
                    )));
                }
            }
        }

        self.locks.insert(
            row_id,
            LockEntry {
                mode,
                holders: vec![tx_id],
            },
        );
        Ok(())
    }

    /// Release all locks held by a transaction.
    pub fn release_all(&mut self, tx_id: u64) {
        let mut to_remove = Vec::new();
        for (row_id, entry) in self.locks.iter_mut() {
            entry.holders.retain(|&id| id != tx_id);
            if entry.holders.is_empty() {
                to_remove.push(*row_id);
            }
        }
        for row_id in to_remove {
            self.locks.remove(&row_id);
        }
    }

    /// Release a specific lock.
    pub fn release(&mut self, row_id: &RowId, tx_id: u64) {
        if let Some(entry) = self.locks.get_mut(row_id) {
            entry.holders.retain(|&id| id != tx_id);
            if entry.holders.is_empty() {
                self.locks.remove(row_id);
            }
        }
    }

    /// Check if a tx holds a lock on a row.
    pub fn is_locked_by(&self, row_id: &RowId, tx_id: u64) -> bool {
        self.locks
            .get(row_id)
            .map(|e| e.holders.contains(&tx_id))
            .unwrap_or(false)
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
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
    fn test_shared_locks_compatible() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Shared).unwrap();
        lm.acquire(rid(0, 0), 2, LockMode::Shared).unwrap();
        assert!(lm.is_locked_by(&rid(0, 0), 1));
        assert!(lm.is_locked_by(&rid(0, 0), 2));
    }

    #[test]
    fn test_exclusive_lock_conflict() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Exclusive).unwrap();
        assert!(lm.acquire(rid(0, 0), 2, LockMode::Exclusive).is_err());
        assert!(lm.acquire(rid(0, 0), 2, LockMode::Shared).is_err());
    }

    #[test]
    fn test_shared_exclusive_conflict() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Shared).unwrap();
        assert!(lm.acquire(rid(0, 0), 2, LockMode::Exclusive).is_err());
    }

    #[test]
    fn test_lock_upgrade() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Shared).unwrap();
        // Same tx can upgrade.
        lm.acquire(rid(0, 0), 1, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_release_all() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Exclusive).unwrap();
        lm.acquire(rid(0, 1), 1, LockMode::Exclusive).unwrap();

        lm.release_all(1);
        assert!(!lm.is_locked_by(&rid(0, 0), 1));
        assert!(!lm.is_locked_by(&rid(0, 1), 1));

        // Now another tx can acquire.
        lm.acquire(rid(0, 0), 2, LockMode::Exclusive).unwrap();
    }

    #[test]
    fn test_release_specific() {
        let mut lm = LockManager::new();
        lm.acquire(rid(0, 0), 1, LockMode::Exclusive).unwrap();
        lm.release(&rid(0, 0), 1);
        assert!(!lm.is_locked_by(&rid(0, 0), 1));
    }
}

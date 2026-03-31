use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::{BoolDBError, Result};

/// Transaction state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxState {
    Active,
    Committed,
    Aborted,
}

/// A transaction handle.
#[derive(Debug, Clone)]
pub struct Transaction {
    pub tx_id: u64,
    pub state: TxState,
    /// Snapshot: set of tx_ids that were active when this tx began.
    pub active_at_start: Vec<u64>,
    /// Timestamp when this transaction started (monotonic counter).
    pub start_ts: u64,
}

impl Transaction {
    /// Check if a given tx_id is visible to this transaction under snapshot isolation.
    /// A row written by `writer_tx_id` is visible if:
    /// - writer_tx_id == self.tx_id (our own writes), OR
    /// - writer_tx_id was committed before we started AND was NOT in our active snapshot
    pub fn is_visible(&self, writer_tx_id: u64, committed_txs: &HashMap<u64, u64>) -> bool {
        if writer_tx_id == self.tx_id {
            return true;
        }
        // Was the writer committed?
        if let Some(&commit_ts) = committed_txs.get(&writer_tx_id) {
            // Committed before our start and not in our active snapshot
            commit_ts < self.start_ts && !self.active_at_start.contains(&writer_tx_id)
        } else {
            false
        }
    }
}

/// MVCC Transaction Manager.
pub struct TransactionManager {
    next_tx_id: AtomicU64,
    next_ts: AtomicU64,
    /// Active transactions.
    active: HashMap<u64, Transaction>,
    /// Committed transactions: tx_id → commit timestamp.
    committed: HashMap<u64, u64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            next_tx_id: AtomicU64::new(1),
            next_ts: AtomicU64::new(1),
            active: HashMap::new(),
            committed: HashMap::new(),
        }
    }

    /// Begin a new transaction.
    pub fn begin(&mut self) -> Transaction {
        let tx_id = self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        let start_ts = self.next_ts.fetch_add(1, Ordering::SeqCst);

        let active_at_start: Vec<u64> = self.active.keys().copied().collect();

        let tx = Transaction {
            tx_id,
            state: TxState::Active,
            active_at_start,
            start_ts,
        };

        self.active.insert(tx_id, tx.clone());
        tx
    }

    /// Commit a transaction.
    pub fn commit(&mut self, tx_id: u64) -> Result<()> {
        let tx = self
            .active
            .get_mut(&tx_id)
            .ok_or_else(|| BoolDBError::Transaction(format!("Transaction {} not found", tx_id)))?;

        if tx.state != TxState::Active {
            return Err(BoolDBError::Transaction(format!(
                "Transaction {} is not active (state: {:?})",
                tx_id, tx.state
            )));
        }

        tx.state = TxState::Committed;
        let commit_ts = self.next_ts.fetch_add(1, Ordering::SeqCst);

        self.active.remove(&tx_id);
        self.committed.insert(tx_id, commit_ts);

        Ok(())
    }

    /// Abort a transaction.
    pub fn abort(&mut self, tx_id: u64) -> Result<()> {
        let tx = self
            .active
            .get_mut(&tx_id)
            .ok_or_else(|| BoolDBError::Transaction(format!("Transaction {} not found", tx_id)))?;

        tx.state = TxState::Aborted;
        self.active.remove(&tx_id);
        Ok(())
    }

    /// Get a transaction by ID (active only).
    pub fn get(&self, tx_id: u64) -> Option<&Transaction> {
        self.active.get(&tx_id)
    }

    /// Check if a transaction is committed.
    pub fn is_committed(&self, tx_id: u64) -> bool {
        self.committed.contains_key(&tx_id)
    }

    /// Get all active transaction IDs.
    pub fn active_tx_ids(&self) -> Vec<u64> {
        self.active.keys().copied().collect()
    }

    /// Get the committed transactions map (for visibility checks).
    pub fn committed_txs(&self) -> &HashMap<u64, u64> {
        &self.committed
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_begin_commit() {
        let mut tm = TransactionManager::new();

        let tx1 = tm.begin();
        assert_eq!(tx1.state, TxState::Active);
        assert!(tm.get(tx1.tx_id).is_some());

        tm.commit(tx1.tx_id).unwrap();
        assert!(tm.get(tx1.tx_id).is_none());
        assert!(tm.is_committed(tx1.tx_id));
    }

    #[test]
    fn test_abort() {
        let mut tm = TransactionManager::new();
        let tx = tm.begin();
        tm.abort(tx.tx_id).unwrap();
        assert!(tm.get(tx.tx_id).is_none());
        assert!(!tm.is_committed(tx.tx_id));
    }

    #[test]
    fn test_snapshot_isolation() {
        let mut tm = TransactionManager::new();

        let tx1 = tm.begin();
        let tx2 = tm.begin();

        // tx2's active snapshot should include tx1
        assert!(tx2.active_at_start.contains(&tx1.tx_id));

        // tx1 commits
        tm.commit(tx1.tx_id).unwrap();

        // tx1's writes should NOT be visible to tx2 (was active when tx2 started)
        assert!(!tx2.is_visible(tx1.tx_id, tm.committed_txs()));

        // New tx3 should see tx1's writes
        let tx3 = tm.begin();
        assert!(tx3.is_visible(tx1.tx_id, tm.committed_txs()));

        tm.commit(tx2.tx_id).unwrap();
        tm.commit(tx3.tx_id).unwrap();
    }

    #[test]
    fn test_own_writes_visible() {
        let mut tm = TransactionManager::new();
        let tx = tm.begin();
        // Own writes are always visible.
        assert!(tx.is_visible(tx.tx_id, tm.committed_txs()));
    }

    #[test]
    fn test_concurrent_transactions() {
        let mut tm = TransactionManager::new();

        let tx1 = tm.begin();
        let tx2 = tm.begin();
        let tx3 = tm.begin();

        assert_eq!(tm.active_tx_ids().len(), 3);

        tm.commit(tx1.tx_id).unwrap();
        tm.abort(tx2.tx_id).unwrap();

        assert_eq!(tm.active_tx_ids().len(), 1);
        assert!(tm.active_tx_ids().contains(&tx3.tx_id));

        tm.commit(tx3.tx_id).unwrap();
        assert!(tm.active_tx_ids().is_empty());
    }
}

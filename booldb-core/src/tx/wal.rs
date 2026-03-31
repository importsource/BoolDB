use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{BoolDBError, Result};
use crate::types::PageId;

/// A log record in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogRecord {
    /// Transaction started.
    Begin { tx_id: u64 },
    /// Transaction committed.
    Commit { tx_id: u64 },
    /// Transaction aborted.
    Abort { tx_id: u64 },
    /// A page was modified. Stores the before and after images.
    PageWrite {
        tx_id: u64,
        page_id: PageId,
        before_image: Vec<u8>,
        after_image: Vec<u8>,
    },
    /// Checkpoint: all dirty pages have been flushed.
    Checkpoint { active_tx_ids: Vec<u64> },
}

/// Write-Ahead Log for crash recovery.
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
    /// Monotonically increasing LSN (Log Sequence Number).
    next_lsn: u64,
}

impl Wal {
    /// Open or create a WAL file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let _file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        // Count existing records to set the next LSN.
        let next_lsn = Self::count_records(&path)?;

        let file = OpenOptions::new().append(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Wal {
            path,
            writer,
            next_lsn,
        })
    }

    /// Append a log record. Returns the LSN.
    pub fn append(&mut self, record: &LogRecord) -> Result<u64> {
        let lsn = self.next_lsn;
        let data = bincode::serialize(record)
            .map_err(|e| BoolDBError::Serialization(e.to_string()))?;

        // Write: [length: u32][data: bytes]
        let len = data.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&data)?;

        self.next_lsn += 1;
        Ok(lsn)
    }

    /// Flush the WAL to disk.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    /// Read all log records for recovery.
    pub fn read_all(path: &Path) -> Result<Vec<(u64, LogRecord)>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let mut file = File::open(path)?;
        let mut records = Vec::new();
        let mut lsn = 0u64;

        loop {
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut data = vec![0u8; len];
            file.read_exact(&mut data)?;

            let record: LogRecord = bincode::deserialize(&data)
                .map_err(|e| BoolDBError::Serialization(e.to_string()))?;
            records.push((lsn, record));
            lsn += 1;
        }

        Ok(records)
    }

    fn count_records(path: &Path) -> Result<u64> {
        if !path.exists() || std::fs::metadata(path)?.len() == 0 {
            return Ok(0);
        }
        let records = Self::read_all(path)?;
        Ok(records.len() as u64)
    }

    /// Truncate the WAL (after a successful checkpoint).
    pub fn truncate(&mut self) -> Result<()> {
        // Close and reopen as truncated.
        self.writer.flush()?;
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        self.next_lsn = 0;
        Ok(())
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }
}

/// Recover committed transactions from WAL records.
/// Returns a list of (page_id, after_image) for pages that should be restored.
pub fn recover(records: &[(u64, LogRecord)]) -> Vec<(PageId, Vec<u8>)> {
    use std::collections::HashSet;

    let mut committed = HashSet::new();
    let mut aborted = HashSet::new();

    // First pass: find committed and aborted transactions.
    for (_, record) in records {
        match record {
            LogRecord::Commit { tx_id } => {
                committed.insert(*tx_id);
            }
            LogRecord::Abort { tx_id } => {
                aborted.insert(*tx_id);
            }
            _ => {}
        }
    }

    // Second pass: collect page writes from committed transactions.
    let mut page_writes: Vec<(PageId, Vec<u8>)> = Vec::new();
    for (_, record) in records {
        if let LogRecord::PageWrite {
            tx_id,
            page_id,
            after_image,
            ..
        } = record
        {
            if committed.contains(tx_id) {
                page_writes.push((*page_id, after_image.clone()));
            }
        }
    }

    page_writes
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn tmp_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join("booldb_test");
        fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    #[test]
    fn test_wal_write_and_read() {
        let path = tmp_path("test_wal.log");
        let _ = fs::remove_file(&path);

        {
            let mut wal = Wal::open(&path).unwrap();
            wal.append(&LogRecord::Begin { tx_id: 1 }).unwrap();
            wal.append(&LogRecord::PageWrite {
                tx_id: 1,
                page_id: 0,
                before_image: vec![0; 10],
                after_image: vec![1; 10],
            })
            .unwrap();
            wal.append(&LogRecord::Commit { tx_id: 1 }).unwrap();
            wal.flush().unwrap();
        }

        let records = Wal::read_all(&path).unwrap();
        assert_eq!(records.len(), 3);
        assert!(matches!(records[0].1, LogRecord::Begin { tx_id: 1 }));
        assert!(matches!(records[2].1, LogRecord::Commit { tx_id: 1 }));

        fs::remove_file(&path).unwrap();
    }

    #[test]
    fn test_recovery() {
        let records = vec![
            (0, LogRecord::Begin { tx_id: 1 }),
            (1, LogRecord::Begin { tx_id: 2 }),
            (
                2,
                LogRecord::PageWrite {
                    tx_id: 1,
                    page_id: 5,
                    before_image: vec![0],
                    after_image: vec![1],
                },
            ),
            (
                3,
                LogRecord::PageWrite {
                    tx_id: 2,
                    page_id: 10,
                    before_image: vec![0],
                    after_image: vec![2],
                },
            ),
            (4, LogRecord::Commit { tx_id: 1 }),
            (5, LogRecord::Abort { tx_id: 2 }),
        ];

        let writes = recover(&records);
        // Only tx_id 1's write should be recovered.
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, 5);
        assert_eq!(writes[0].1, vec![1]);
    }

    #[test]
    fn test_wal_truncate() {
        let path = tmp_path("test_wal_truncate.log");
        let _ = fs::remove_file(&path);

        let mut wal = Wal::open(&path).unwrap();
        wal.append(&LogRecord::Begin { tx_id: 1 }).unwrap();
        wal.flush().unwrap();
        assert_eq!(wal.next_lsn(), 1);

        wal.truncate().unwrap();
        assert_eq!(wal.next_lsn(), 0);

        let records = Wal::read_all(&path).unwrap();
        assert!(records.is_empty());

        fs::remove_file(&path).unwrap();
    }
}

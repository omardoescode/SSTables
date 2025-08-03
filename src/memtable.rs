use std::fs::{File, OpenOptions};
use std::io::{BufReader, Error, ErrorKind, Read, Result as IOResult, Write};

use rbtree::RBTree;

pub trait Serializable {
    fn serialize(&self) -> Vec<u8>;
    fn deserialize(data: &[u8]) -> Self
    where
        Self: Sized;
}

pub struct MemTable<T: Serializable> {
    tree: RBTree<String, T>,
    log: MemTableLog,
}

impl<T: Serializable> MemTable<T> {
    fn new() -> IOResult<MemTable<T>> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/log.txt")?;
        Ok(MemTable {
            tree: RBTree::new(),
            log: MemTableLog::new(file),
        })
    }

    fn build_from(path: &str) -> IOResult<MemTable<T>> {
        let mut reader = MemTableLogReader::open(path)?;
        let mut memtable = MemTable::new()?;

        while let Some(op) = reader.next_op()? {
            match op {
                LogOperation::Insert { key, value } => {
                    let value = T::deserialize(&value);
                    memtable.insert(key, value);
                }
                LogOperation::Delete { key } => {
                    memtable.delete(key);
                }
            }
        }

        Ok(memtable)
    }

    fn insert(&mut self, key: String, value: T) -> IOResult<()> {
        self.log.append(LogOperation::Insert {
            key: key.clone(),
            value: value.serialize(),
        })?;
        self.tree.insert(key, value);
        Ok(())
    }

    fn delete(&mut self, key: String) -> IOResult<bool> {
        let result = self.tree.remove(&key).is_some();
        if result {
            self.log.append(LogOperation::Delete { key })?;
        }
        Ok(result)
    }

    fn get(&self, key: &String) -> Option<&T> {
        self.tree.get(key)
    }

    fn len(&self) -> usize {
        self.tree.len()
    }

    fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
    pub fn iter(&self) -> rbtree::Iter<String, T> {
        self.tree.iter()
    }
    pub fn iter_mut(&mut self) -> rbtree::IterMut<String, T> {
        self.tree.iter_mut()
    }
}

enum LogOperation {
    Insert { key: String, value: Vec<u8> },
    Delete { key: String },
}

struct MemTableLog {
    file: File,
}

impl MemTableLog {
    fn new(file: File) -> MemTableLog {
        MemTableLog { file }
    }

    fn append(&mut self, opt: LogOperation) -> IOResult<()> {
        match opt {
            LogOperation::Insert { key, value } => {
                self.file.write_all(&[1])?;
                self.file.write_all(&(key.len() as u32).to_le_bytes())?;
                self.file.write_all(key.as_bytes())?;
                self.file.write_all(&(value.len() as u32).to_le_bytes())?;
                self.file.write_all(&value)?;
            }
            LogOperation::Delete { key } => {
                self.file.write_all(&[2])?;
                self.file.write_all(&(key.len() as u32).to_le_bytes())?;
                self.file.write_all(key.as_bytes())?;
            }
        }
        self.file.flush()?;
        Ok(())
    }
}

struct MemTableLogReader<R: Read> {
    reader: BufReader<R>,
}

impl MemTableLogReader<File> {
    fn open(path: &str) -> IOResult<Self> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl<R: Read> MemTableLogReader<R> {
    fn next_op(&mut self) -> IOResult<Option<LogOperation>> {
        let mut op = [0u8; 1];
        if self.reader.read(&mut op)? == 0 {
            return Ok(None);
        }

        let mut key_len = [0u8; 4];
        self.reader.read_exact(&mut key_len)?;
        let key_len = u32::from_le_bytes(key_len) as usize;

        let mut key = vec![0u8; key_len];
        self.reader.read_exact(&mut key)?;
        let key = String::from_utf8_lossy(&key).to_string();

        if op[0] == 1 {
            let mut value_len = [0u8; 4];
            self.reader.read_exact(&mut value_len)?;
            let value_len = u32::from_le_bytes(value_len) as usize;

            let mut value = vec![0u8; value_len];
            self.reader.read_exact(&mut value)?;

            Ok(Some(LogOperation::Insert { key, value }))
        } else if op[0] == 2 {
            Ok(Some(LogOperation::Delete { key }))
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                format!("Invalid log opcode: {}", op[0]),
            ))
        }
    }
}

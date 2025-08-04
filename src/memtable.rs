use bincode::{Decode, Encode, config};
use std::fs::{File, OpenOptions, create_dir_all};
use std::io::{BufReader, Error, ErrorKind, Read, Result as IOResult, Write};

use rbtree::RBTree;

pub struct MemTable<T: Decode<()> + Encode + Clone> {
    tree: RBTree<String, T>,
    log: MemTableLog,
}

impl<T: Decode<()> + Encode + Clone> MemTable<T> {
    pub fn new() -> IOResult<MemTable<T>> {
        create_dir_all("logs")?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open("logs/log.txt")?;
        Ok(MemTable {
            tree: RBTree::new(),
            log: MemTableLog::new(file),
        })
    }

    pub fn build_from(path: &str) -> IOResult<MemTable<T>> {
        let mut reader = MemTableLogReader::open(path)?;
        let mut memtable = MemTable::new()?;

        while let Some(op) = reader.next_op()? {
            match op {
                LogOperation::Insert { key, value } => {
                    memtable.insert(key, value);
                }
                LogOperation::Delete { key } => {
                    memtable.delete(key);
                }
            }
        }

        Ok(memtable)
    }

    pub fn insert(&mut self, key: String, value: T) -> IOResult<()> {
        self.log.append(LogOperation::Insert {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.tree.insert(key, value);
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> IOResult<bool> {
        let result = self.tree.remove(&key).is_some();
        if result {
            self.log.append(LogOperation::<T>::Delete { key })?;
        }
        Ok(result)
    }

    pub fn get(&self, key: &String) -> Option<&T> {
        self.tree.get(key)
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }
    pub fn iter(&self) -> rbtree::Iter<String, T> {
        self.tree.iter()
    }
    pub fn iter_mut(&mut self) -> rbtree::IterMut<String, T> {
        self.tree.iter_mut()
    }
}

#[derive(Encode, Decode, PartialEq, Debug)]
enum LogOperation<T: Encode + Decode<()>> {
    Insert { key: String, value: T },
    Delete { key: String },
}

struct MemTableLog {
    file: File,
}

impl MemTableLog {
    fn new(file: File) -> MemTableLog {
        MemTableLog { file }
    }

    fn append<T: Encode + Decode<()>>(&mut self, opt: LogOperation<T>) -> IOResult<()> {
        let Ok(decoded) = bincode::encode_to_vec(opt, config::standard()) else {
            return Err(Error::new(ErrorKind::InvalidInput, "Failed to encode data"));
        };
        self.file.write_all(&decoded)?;
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
    fn next_op<T: Decode<()> + Encode>(&mut self) -> IOResult<Option<LogOperation<T>>> {
        let result = bincode::decode_from_std_read(&mut self.reader, config::standard());
        match result {
            Ok(op) => Ok(Some(op)),
            Err(bincode::error::DecodeError::UnexpectedEnd { .. }) => Ok(None),
            Err(e) => Err(Error::new(ErrorKind::Other, e)),
        }
    }
}

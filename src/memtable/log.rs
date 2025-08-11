use crate::memtable::MemTableRecord;
use crate::serialization::SerializationEngine;

use super::operation::LogOperation;
use std::fs::File;
use std::io::{Error, ErrorKind, Result as IOResult, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

pub struct MemTableLog {
    pub file: Arc<Mutex<File>>,
}

impl MemTableLog {
    pub fn new(file: File) -> Self {
        MemTableLog {
            file: Arc::new(Mutex::new(file)),
        }
    }

    pub fn append<T, S>(&self, opt: LogOperation<T>, serializer: &S) -> IOResult<()>
    where
        T: MemTableRecord,
        S: SerializationEngine<LogOperation<T>>,
    {
        let Ok(decoded) = serializer.serialize(opt) else {
            return Err(Error::new(ErrorKind::InvalidInput, "Failed to encode data"));
        };
        let mut file = self.file.lock().unwrap();
        file.write_all(&decoded)?;
        file.flush()?;
        Ok(())
    }

    pub fn clear(&self) -> IOResult<()> {
        let mut file = self.file.lock().unwrap();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.flush()?;
        Ok(())
    }
}

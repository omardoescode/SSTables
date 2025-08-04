use crate::memtable::MemTableRecord;
use crate::serialization::SerializationEngine;

use super::operation::LogOperation;
use std::fs::File;
use std::io::{Error, ErrorKind, Result as IOResult, Write};

pub struct MemTableLog {
    pub(crate) file: File,
}

impl MemTableLog {
    pub fn new(file: File) -> Self {
        MemTableLog { file }
    }

    pub fn append<T, S>(&mut self, opt: LogOperation<T>, serializer: &S) -> IOResult<()>
    where
        T: MemTableRecord,
        S: SerializationEngine<LogOperation<T>>,
    {
        let Ok(decoded) = serializer.serialize(opt) else {
            return Err(Error::new(ErrorKind::InvalidInput, "Failed to encode data"));
        };
        self.file.write_all(&decoded)?;
        self.file.flush()?;
        Ok(())
    }
}

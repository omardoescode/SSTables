use crate::memtable::MemTableRecord;

use super::operation::LogOperation;
use bincode::{Decode, Encode, config};
use std::fs::File;
use std::io::{Error, ErrorKind, Result as IOResult, Write};

pub struct MemTableLog {
    pub(crate) file: File,
}

impl MemTableLog {
    pub fn new(file: File) -> Self {
        MemTableLog { file }
    }

    pub fn append<T: MemTableRecord>(&mut self, opt: LogOperation<T>) -> IOResult<()> {
        let Ok(decoded) = bincode::encode_to_vec(opt, config::standard()) else {
            return Err(Error::new(ErrorKind::InvalidInput, "Failed to encode data"));
        };
        self.file.write_all(&decoded)?;
        self.file.flush()?;
        Ok(())
    }
}

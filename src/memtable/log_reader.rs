use crate::memtable::MemTableRecord;
use crate::serialization::{SerializationEngine, SerializationError};

use super::operation::LogOperation;
use std::fs::File;
use std::io::{BufReader, Error, Read, Result as IOResult};

pub struct MemTableLogReader<R: Read> {
    pub(crate) reader: BufReader<R>,
}

impl MemTableLogReader<File> {
    pub fn open(file: File) -> IOResult<Self> {
        Ok(Self {
            reader: BufReader::new(file),
        })
    }
}

impl<R: Read> MemTableLogReader<R> {
    pub fn next_op<T, S>(&mut self, serializer: &S) -> IOResult<Option<LogOperation<T>>>
    where
        T: MemTableRecord,
        S: SerializationEngine<LogOperation<T>>,
    {
        let result = serializer.deserialize(&mut self.reader);
        match result {
            Ok(op) => Ok(Some(op)),
            Err(SerializationError::UnexpectedEOF) => Ok(None),
            Err(_) => Err(Error::other("Failed to read next record")),
        }
    }
}

use super::operation::LogOperation;
use bincode::{Decode, Encode, config};
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read, Result as IOResult};

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
    pub fn next_op<T: Decode<()> + Encode>(&mut self) -> IOResult<Option<LogOperation<T>>> {
        let result = bincode::decode_from_std_read(&mut self.reader, config::standard());
        match result {
            Ok(op) => Ok(Some(op)),
            Err(bincode::error::DecodeError::UnexpectedEnd { .. }) => Ok(None),
            Err(bincode::error::DecodeError::Io { inner, .. })
                if inner.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                Ok(None)
            }

            Err(e) => Err(Error::other(e)),
        }
    }
}

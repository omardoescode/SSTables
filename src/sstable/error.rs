use std::{fmt::Debug, io};

#[derive(Debug)]
pub enum SSTableError {
    LogFileAlreadyExistsError,
    FileCreationError,
    EncodingError,
    LogWriteError { err: io::Error },
}

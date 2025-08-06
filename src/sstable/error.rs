use std::{fmt::Debug, io};

#[derive(Debug)]
pub enum SSTableError {
    LogFileAlreadyExistsError,
    IndexFileAlreadyExistsError,
    FileCreationError,
    EncodingError,
    LogWriteError { err: io::Error },
}

use std::{fmt::Debug, fs::File, io};

#[derive(Debug)]
pub enum SSTableError {
    LogFileAlreadyExistsError,
    IndexFileAlreadyExistsError,
    FileCreationError,
    EncodingError,
    LogWriteError { err: io::Error },
    EmptyMemtableError,
    DBFileDeleted { file: String },
    DBFilePermissionsChanged { file: String },
    DBFileCorrupted { file: String },
}

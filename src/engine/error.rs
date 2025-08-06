use std::io;

#[derive(Debug)]
pub enum EngineError {
    DBDoesntExist,
    MemtableInitialization { err: io::Error },
    Insertion { err: io::Error },
    Deletion { err: io::Error },
    DBFileDeleted { file: String },
    DBCorrupted { file: String },
}

use std::io;

#[derive(Debug)]
pub enum EngineError {
    DBDoesntExist,
    MemtableInitialization { err: io::Error },
    Insertion { err: io::Error },
}

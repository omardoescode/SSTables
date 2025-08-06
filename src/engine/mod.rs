mod error;
use std::{fs::create_dir_all, path::Path, vec};

use crate::{
    config::Config,
    memtable::{LogOperation, MemTable, MemTableRecord},
    serialization::SerializationEngine,
};
use error::EngineError;

pub struct Engine<'a, T, S>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
{
    memtable: Box<MemTable<'a, T, S>>,
    sstables: Vec<String>,
    config: &'a Config,
}

impl<'a, T, S> Engine<'a, T, S>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
{
    pub fn new(
        db_path: &str,
        serializer: &'a S,
        config: &'a Config,
    ) -> Result<Engine<'a, T, S>, EngineError> {
        let db_path = Path::new(db_path);
        if !db_path.exists() {
            return Err(EngineError::DBDoesntExist);
        }
        let _ = create_dir_all(db_path.join(Path::new("metadata")));
        let _ = create_dir_all(db_path.join(Path::new("indices")));
        let _ = create_dir_all(db_path.join(Path::new("storage")));
        let _ = create_dir_all(db_path.join(Path::new("logs")));

        let memtable = MemTable::<T, S>::open_or_build(
            &db_path
                .join(format!("logs/{}.txt", T::TYPE_NAME))
                .display()
                .to_string(),
            serializer,
        )
        .map_err(|err| EngineError::MemtableInitialization { err })?;

        let memtable = Box::new(memtable);

        Ok(Engine {
            memtable,
            sstables: vec![],
            config,
        })
    }
    pub fn insert(&mut self, record: T) -> Result<(), EngineError> {
        self.memtable
            .insert(record)
            .map_err(|err| EngineError::Insertion { err })?;
        Ok(())
    }
}

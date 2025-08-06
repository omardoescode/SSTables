mod error;
use std::{fs::create_dir_all, path::Path, vec};

use crate::{
    config::Config,
    memtable::{self, LogOperation, MemTable, MemTableRecord},
    serialization::SerializationEngine,
    sstable::SSTable,
};
use error::EngineError;

pub struct Engine<'a, T, S, SS>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
    SS: SerializationEngine<T>,
{
    db_path: &'a str,
    memtable: Box<MemTable<'a, T, S>>,
    sstables: Vec<String>,
    config: &'a Config,
    serializer: &'a SS,
}

impl<'a, T, S, SS> Engine<'a, T, S, SS>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
    SS: SerializationEngine<T>,
{
    pub fn new(
        db: &'a str,
        memtable_serializer: &'a S,
        storage_serializer: &'a SS,
        config: &'a Config,
    ) -> Result<Engine<'a, T, S, SS>, EngineError> {
        let db_path = Path::new(db);
        if !db_path.exists() {
            return Err(EngineError::DBDoesntExist);
        }
        let _ = create_dir_all(db_path.join(Path::new("metadata")));
        let _ = create_dir_all(db_path.join(Path::new("indices")));
        let _ = create_dir_all(db_path.join(Path::new("storage")));
        let _ = create_dir_all(db_path.join(Path::new("logs")));

        let memtable = MemTable::<T, S>::open_or_build(
            &db_path
                .join(format!("logs/{}.log", T::TYPE_NAME))
                .display()
                .to_string(),
            memtable_serializer,
        )
        .map_err(|err| EngineError::MemtableInitialization { err })?;

        let memtable = Box::new(memtable);

        Ok(Engine {
            memtable,
            sstables: vec![],
            config,
            serializer: storage_serializer,
            db_path: db,
        })
    }

    pub fn insert(&mut self, record: T) -> Result<(), EngineError> {
        self.memtable
            .insert(record)
            .map_err(|err| EngineError::Insertion { err })?;
        self.flush_if_ready();
        Ok(())
    }
    fn flush_if_ready(&mut self) {
        let Config {
            index_offset_size,
            index_key_string_size,
            memtable_threshold,
        } = self.config;
        let pair_size = index_key_string_size + index_offset_size;
        if pair_size * self.memtable.len() < *memtable_threshold {
            return;
        }

        let [storage_path, index_path] = ["storage", "indices"].map(|dir| {
            Path::new(&self.db_path)
                .join(format!("{}/{}.log", dir, T::TYPE_NAME))
                .display()
                .to_string()
        });

        SSTable::create::<T, S, SS>(
            &storage_path,
            &index_path,
            &self.memtable.tree,
            self.serializer,
        );

        self.memtable.clear();
    }
}

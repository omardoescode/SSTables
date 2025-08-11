mod error;

use std::{
    fs::{self, File, OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Result as IOResult, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use crate::{
    compaction::compact,
    config::Config,
    memtable::{LogOperation, MemTable, MemTableRecord},
    serialization::SerializationEngine,
    sstable::SSTable,
};
use error::EngineError;
use tempfile::NamedTempFile;

pub struct Engine<'a, T, S, SS>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
    SS: SerializationEngine<Option<T>>,
{
    metadata: File,
    memtable: Box<MemTable<'a, T, S>>,
    sstables: Vec<SSTable>,
    config: &'a Config,
    serializer: &'a SS,
}

impl<'a, T, S, SS> Engine<'a, T, S, SS>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
    SS: SerializationEngine<Option<T>>,
{
    pub fn new(
        memtable_serializer: &'a S,
        storage_serializer: &'a SS,
        config: &'a Config,
    ) -> Result<Engine<'a, T, S, SS>, EngineError> {
        let db_path = Path::new(&config.db_path);
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

        // Load all sstables
        let metadata_path = Self::get_metadata_path(&config.db_path);
        let metadata = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&metadata_path)
            .unwrap(); // TODO: Fix this unwrap later

        let sstables = Self::read_sstables(&metadata);

        Ok(Engine {
            metadata,
            memtable,
            sstables,
            config,
            serializer: storage_serializer,
        })
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }
    pub fn sstable_len(&self) -> usize {
        self.sstables.len()
    }

    pub fn insert(&mut self, record: T) -> Result<(), EngineError> {
        self.memtable
            .insert(record)
            .map_err(|err| EngineError::Insertion { err })?;
        self.flush_if_ready();
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> Result<(), EngineError> {
        self.memtable
            .delete(key)
            .map_err(|err| EngineError::Deletion { err })?;
        self.flush_if_ready();
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<T>, EngineError> {
        let memlookup = self.memtable.get(&key);
        if let Some(value) = memlookup {
            return Ok(value.clone());
        }

        // Lookup in SSTables
        for table in self.sstables.iter().rev() {
            let lookup = table.get(&key, self.config, self.serializer).unwrap(); // TODO: Handle These errors
            if let Some(value) = lookup {
                return Ok(value);
            }
        }

        // File Not Found
        Ok(None)
    }

    pub fn compact(&mut self) {
        let compact_file_count = self
            .config
            .parallel_merging_file_count
            .min(self.sstables.len());
        println!("Compact File Count: {}", compact_file_count);

        let to_compact = &self.sstables[0..compact_file_count];
        let (index_file, storage_file) = self.get_next_index_storage_logs_name();
        println!("{} {}", index_file, storage_file);

        let table = compact(
            to_compact,
            self.serializer,
            self.config,
            storage_file,
            index_file,
        )
        .unwrap();

        let mut remaining = self.sstables.split_off(compact_file_count);

        self.sstables.clear();
        self.sstables.push(table);
        self.sstables.append(&mut remaining);

        self.recreate_metadata().unwrap();
    }

    fn flush_if_ready(&mut self) {
        let Config {
            index_offset_size,
            index_key_string_size,
            initial_index_file_threshold: memtable_threshold,
            ..
        } = self.config;

        let pair_size = index_key_string_size + index_offset_size;
        if pair_size * self.memtable.len() < *memtable_threshold {
            return;
        }

        let (index_path, storage_path) = self.get_next_index_storage_logs_name();

        let table = SSTable::create::<T, S, SS>(
            &storage_path,
            &index_path,
            &self.memtable.tree,
            self.serializer,
            self.config,
        )
        .unwrap();

        self.add_sstable_to_metadata(&table);
        self.memtable.clear().unwrap();
        self.sstables.push(table);
    }

    fn read_sstables(metadata_file: &File) -> Vec<SSTable> {
        let reader = BufReader::new(metadata_file);
        reader
            .lines()
            .map(|line| {
                let line = line.unwrap();

                let values: Vec<&str> = line.split(" ").collect();

                if values.len() != 6 {
                    panic!("Invalid metadata");
                }

                SSTable {
                    storage_path: values[0].to_string(),
                    index_path: values[1].to_string(),
                    min: values[2].to_string(),
                    max: values[3].to_string(),
                    count: values[4].parse().unwrap(),
                    size: values[5].parse().unwrap(),
                }
            })
            .collect()
    }

    fn add_sstable_to_metadata(&mut self, table: &SSTable) {
        self.metadata.seek(SeekFrom::End(0));
        self.metadata.write_all(
            format!(
                "{} {} {} {} {} {}\n",
                table.storage_path.clone(),
                table.index_path.clone(),
                table.min.clone(),
                table.max.clone(),
                table.count,
                table.size
            )
            .as_bytes(),
        );
    }

    fn get_next_index_storage_logs_name(&self) -> (String, String) {
        let count = fs::read_dir(Path::new(&self.config.db_path).join("storage"))
            .unwrap()
            .count();
        let [storage_path, index_path] = ["storage", "indices"].map(|dir| {
            Path::new(&self.config.db_path)
                .join(format!("{}/{}-{}.log", dir, T::TYPE_NAME, count))
                .display()
                .to_string()
        });
        (index_path, storage_path)
    }

    fn recreate_metadata(&self) -> IOResult<()> {
        let mut temp_file = NamedTempFile::new_in(&self.config.db_path)?;
        for table in self.sstables.iter() {
            temp_file.write_all(
                format!(
                    "{} {} {} {} {} {}\n",
                    table.storage_path.clone(),
                    table.index_path.clone(),
                    table.min.clone(),
                    table.max.clone(),
                    table.count,
                    table.size
                )
                .as_bytes(),
            )?;
        }

        temp_file.persist(Self::get_metadata_path(&self.config.db_path))?;
        Ok(())
    }

    fn get_metadata_path(db_path: &str) -> PathBuf {
        Path::new(db_path).join(format!("metadata/{}.meta", T::TYPE_NAME))
    }
}

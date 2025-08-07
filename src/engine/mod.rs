mod error;

use std::{
    fs::{File, OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{
    config::Config,
    memtable::{LogOperation, MemTable, MemTableRecord},
    serialization::SerializationEngine,
    sstable::SSTable,
};
use error::EngineError;

pub struct Engine<'a, T, S, SS>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
    SS: SerializationEngine<Option<T>>,
{
    metadata: File,
    db_path: &'a str,
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

        // Load all sstables
        let metadata_path = db_path.join(format!("metadata/{}.meta", T::TYPE_NAME));
        let metadata = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(metadata_path)
            .unwrap(); // TODO: Fix this unwrap later

        let sstables = Self::read_sstables(&metadata);

        Ok(Engine {
            metadata,
            memtable,
            sstables,
            config,
            serializer: storage_serializer,
            db_path: db,
        })
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
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
        Ok(())
    }

    pub fn get(&mut self, key: String) -> Result<Option<T>, EngineError> {
        let memlookup = self.memtable.get(&key);
        if let Some(value) = memlookup {
            return Ok(value.clone());
        }

        // Lookup in SSTables
        for table in self.sstables.iter().rev() {
            if key > table.max || key < table.min {
                continue;
            }

            let index_file = OpenOptions::new()
                .read(true)
                .open(table.index_path.clone())
                .map_err(|err| EngineError::DBFileDeleted {
                    file: table.index_path.clone(),
                })?;

            // binary search
            let unit = self.config.index_key_string_size + self.config.index_offset_size;
            if table.size % unit != 0 {
                return Err(EngineError::DBCorrupted {
                    file: table.index_path.clone(),
                });
            }
            let count = table.size;
            let mut lo = 0;
            let mut hi = count;
            let mut reader = BufReader::new(index_file);

            while lo < hi {
                let mid = (lo + hi) / 2;
                let offset = (mid * unit) as u64;

                reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(|_| EngineError::DBCorrupted {
                        file: table.index_path.clone(),
                    })?;

                let mut key_buf = vec![0u8; self.config.index_key_string_size];
                reader
                    .read_exact(&mut key_buf)
                    .map_err(|_| EngineError::DBCorrupted {
                        file: table.index_path.clone(),
                    })?;

                let current_key = String::from_utf8_lossy(&key_buf)
                    .trim_end_matches('\0')
                    .to_string();

                if current_key < key {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            // After binary search, lo is the position where key should be
            // Check if we found the exact key
            if lo < count {
                let offset = (lo * unit) as u64;
                reader
                    .seek(SeekFrom::Start(offset))
                    .map_err(|_| EngineError::DBCorrupted {
                        file: table.index_path.clone(),
                    })?;

                let mut key_buf = vec![0u8; self.config.index_key_string_size];
                reader
                    .read_exact(&mut key_buf)
                    .map_err(|_| EngineError::DBCorrupted {
                        file: table.index_path.clone(),
                    })?;

                let found_key = String::from_utf8_lossy(&key_buf)
                    .trim_end_matches('\0')
                    .to_string();

                if found_key == key {
                    // Found the key, now read the offset
                    let mut offset_buf = vec![0u8; self.config.index_offset_size];
                    reader
                        .read_exact(&mut offset_buf)
                        .map_err(|_| EngineError::DBCorrupted {
                            file: table.index_path.clone(),
                        })?;

                    let file_offset =
                        u64::from_le_bytes(offset_buf.try_into().expect("offset size mismatch"));

                    return Ok(self.load_record(&table.storage_path, file_offset));
                }
            }
        }

        // File Not Found
        Ok(None)
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
                .join(format!(
                    "{}/{}-{}.log",
                    dir,
                    T::TYPE_NAME,
                    self.sstables.len()
                ))
                .display()
                .to_string()
        });

        let table = SSTable::create::<T, S, SS>(
            &storage_path,
            &index_path,
            &self.memtable.tree,
            self.serializer,
            self.config,
        )
        .unwrap();

        self.add_sstable_to_metadata(&table);
        self.sstables.push(table);

        self.memtable.clear();
    }

    fn read_sstables(metadata_file: &File) -> Vec<SSTable> {
        let reader = BufReader::new(metadata_file);
        reader
            .lines()
            .map(|line| {
                let line = line.unwrap();

                let values: Vec<&str> = line.split(" ").collect();

                if values.len() != 5 {
                    panic!("Invalid metadata");
                }

                SSTable {
                    storage_path: values[0].to_string(),
                    index_path: values[1].to_string(),
                    min: values[2].to_string(),
                    max: values[3].to_string(),
                    size: values[4].parse().unwrap(),
                }
            })
            .collect()
    }

    fn add_sstable_to_metadata(&mut self, table: &SSTable) {
        self.metadata.seek(SeekFrom::End(0));
        self.metadata.write_all(
            format!(
                "{} {} {} {} {}\n",
                table.storage_path.clone(),
                table.index_path.clone(),
                table.min.clone(),
                table.max.clone(),
                table.size
            )
            .as_bytes(),
        );
        self.metadata.flush();
    }

    fn load_record(&self, storage: &str, offset: u64) -> Option<T> {
        let file = OpenOptions::new().read(true).open(storage).unwrap();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset));
        self.serializer.deserialize(&mut reader).unwrap()
    }
}

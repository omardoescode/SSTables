use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read, Result as IOResult, Seek, SeekFrom, Write},
    path::Path,
};

use rbtree::RBTree;

use crate::{
    config::Config,
    memtable::{LogOperation, MemTableRecord},
    serialization::SerializationEngine,
    sstable::error::SSTableError,
};
/// @definition: An implementation of sorted string tables. This struct is a reference to an
/// immutable file on disk that has sorted records of the same schema
/// @field index_path: A file that has an index on the primary keys in the immutable file
/// @field storage_path: The path of the storage file #TODO: make relative to the database path
/// @field min: The minimum key in this file. used for faster lookup
/// @field max: The maximum key in this file. used for faster lookup
/// @field size: The actual storage_file size. used for compaction
/// @field count: the number of records in the sstable
pub struct SSTable {
    pub storage_path: String,
    pub index_path: String,
    pub min: String,
    pub max: String,
    pub size: usize,
    pub count: usize,
}
impl SSTable {
    pub fn create<'a, T, S, SS>(
        storage_path: &'a str,
        index_path: &'a str,
        tree: &RBTree<String, Option<T>>,
        serializer: &SS,
        config: &Config,
    ) -> Result<SSTable, SSTableError>
    where
        T: MemTableRecord,
        S: SerializationEngine<LogOperation<T>>,
        SS: SerializationEngine<Option<T>>,
    {
        if tree.is_empty() {
            return Err(SSTableError::EmptyMemtableError);
        }
        if Path::new(storage_path).exists() {
            return Err(SSTableError::LogFileAlreadyExistsError);
        }
        if Path::new(index_path).exists() {
            return Err(SSTableError::IndexFileAlreadyExistsError);
        }

        let file = File::create(storage_path).map_err(|_| SSTableError::FileCreationError)?;
        let mut indices: Vec<(String, u64)> = vec![];

        let min = tree.get_first().unwrap().0.clone();
        let max = tree.get_last().unwrap().0.clone();

        let mut writer = BufWriter::new(file);
        for (key, value) in tree.iter() {
            indices.push((key.clone(), writer.stream_position().unwrap()));
            let encoded = serializer
                .serialize(value.clone())
                .map_err(|_| SSTableError::EncodingError)?;

            writer
                .write_all(&encoded)
                .map_err(|err| SSTableError::LogWriteError { err })?;
        }
        // writer.flush();
        let size = writer.stream_position().unwrap() as usize;

        let index_file = File::create(index_path).map_err(|_| SSTableError::FileCreationError)?;
        let mut index_writer = BufWriter::new(index_file);

        for (key, offset) in indices.iter() {
            let mut key_bytes = vec![0u8; config.index_key_string_size];
            let truncated = key.as_bytes();
            let len = truncated.len().min(config.index_key_string_size);
            key_bytes[..len].copy_from_slice(&truncated[..len]);

            index_writer
                .write_all(&key_bytes)
                .map_err(|err| SSTableError::LogWriteError { err })?;
            index_writer
                .write_all(&offset.to_le_bytes())
                .map_err(|err| SSTableError::LogWriteError { err })?;
        }

        Ok(SSTable {
            storage_path: storage_path.to_string(),
            index_path: index_path.to_string(),
            min,
            max,
            size,
            count: tree.len(),
        })
    }

    pub fn get<T, SS>(
        &self,
        key: &str,
        config: &Config,
        serializer: &SS,
    ) -> Result<Option<Option<T>>, SSTableError>
    where
        T: MemTableRecord,
        SS: SerializationEngine<Option<T>>,
    {
        if key > self.max.as_str() || key < self.min.as_str() {
            return Ok(None);
        }

        let index_file = OpenOptions::new()
            .read(true)
            .open(self.index_path.clone())
            .map_err(|err| SSTableError::DBFileDeleted {
                file: self.index_path.clone(),
            })?;

        // binary search
        let unit = config.index_key_string_size + config.index_offset_size;

        if self.count % unit != 0 {
            return Err(SSTableError::DBFileCorrupted {
                file: self.index_path.clone(),
            });
        }
        let mut lo = 0;
        let mut hi = self.count;
        let mut reader = BufReader::new(index_file);

        while lo < hi {
            let mid = (lo + hi) / 2;
            let offset = (mid * unit) as u64;

            reader
                .seek(SeekFrom::Start(offset))
                .map_err(|_| SSTableError::DBFileCorrupted {
                    file: self.index_path.clone(),
                })?;

            let mut key_buf = vec![0u8; config.index_key_string_size];
            reader
                .read_exact(&mut key_buf)
                .map_err(|_| SSTableError::DBFileCorrupted {
                    file: self.index_path.clone(),
                })?;

            let current_key = String::from_utf8_lossy(&key_buf)
                .trim_end_matches('\0')
                .to_string();

            if current_key.as_str() < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // After binary search, lo is the position where key should be
        // Check if we found the exact key
        if lo < self.size {
            let offset = (lo * unit) as u64;
            reader
                .seek(SeekFrom::Start(offset))
                .map_err(|_| SSTableError::DBFileCorrupted {
                    file: self.index_path.clone(),
                })?;

            let mut key_buf = vec![0u8; config.index_key_string_size];
            reader
                .read_exact(&mut key_buf)
                .map_err(|_| SSTableError::DBFileCorrupted {
                    file: self.index_path.clone(),
                })?;

            let found_key = String::from_utf8_lossy(&key_buf)
                .trim_end_matches('\0')
                .to_string();

            if found_key == key {
                // Found the key, now read the offset
                let mut offset_buf = vec![0u8; config.index_offset_size];
                reader
                    .read_exact(&mut offset_buf)
                    .map_err(|_| SSTableError::DBFileCorrupted {
                        file: self.index_path.clone(),
                    })?;

                let file_offset =
                    u64::from_le_bytes(offset_buf.try_into().expect("offset size mismatch"));

                return Ok(Some(self.load_record(
                    &self.storage_path,
                    file_offset,
                    serializer,
                )));
            }
        }
        Ok(None)
    }

    fn load_record<T, SS>(&self, storage: &str, offset: u64, serializer: &SS) -> Option<T>
    where
        T: MemTableRecord,
        SS: SerializationEngine<Option<T>>,
    {
        let file = OpenOptions::new().read(true).open(storage).unwrap();
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset));
        serializer.deserialize(&mut reader).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };
    use tempfile::TempDir;
    use uuid::Uuid;

    use crate::{
        config::Config,
        memtable::{MemTable, MemTableRecord},
        serialization::BinarySerializationEngine,
        sstable::SSTable,
    };

    use bincode::{Decode, Encode};

    #[derive(Encode, Decode, Clone)]
    struct Photo {
        id: i32,
        url: String,
        thumbnail_url: String,
    }

    impl MemTableRecord for Photo {
        const TYPE_NAME: &'static str = "Photo";
        fn get_key(&self) -> String {
            self.id.to_string()
        }
    }

    #[test]
    fn create_ss_table() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");

        let file = File::open("resources/photos.txt").expect("Missing photos.txt");
        let reader = BufReader::new(file);

        let log_path = temp_dir.path().join(format!("{}.log", Uuid::new_v4()));
        let serializer = BinarySerializationEngine;

        let mut memtable = MemTable::<Photo, BinarySerializationEngine>::open_or_build(
            log_path.to_str().unwrap(),
            &serializer,
        )
        .expect("Failed to open or build MemTable");

        for line in reader.lines() {
            let line = line.unwrap();
            let values: Vec<&str> = line.split_whitespace().collect();
            assert_eq!(values.len(), 3, "Invalid line in photos.txt");

            memtable
                .insert(Photo {
                    id: values[0].parse().unwrap(),
                    url: values[1].to_string(),
                    thumbnail_url: values[2].to_string(),
                })
                .unwrap();
        }

        let storage_path = temp_dir.path().join("sstable_data.txt");
        let index_path = temp_dir.path().join("sstable_index.txt");

        SSTable::create::<Photo, BinarySerializationEngine, BinarySerializationEngine>(
            storage_path.to_str().unwrap(),
            index_path.to_str().unwrap(),
            &memtable.tree,
            &serializer,
            &Config {
                same_size_before_compaction_threshold: 3,
                index_key_string_size: 24,
                index_offset_size: 8,
                initial_index_file_threshold: 1024,
                parallel_merging_file_count: 2,
                db_path: "temp/db".to_string(),
            },
        )
        .expect("Failed to create SSTable");

        assert!(storage_path.exists(), "SSTable data file was not created");
        assert!(index_path.exists(), "SSTable index file was not created");
    }
}

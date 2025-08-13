use core::panic;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::{File, OpenOptions},
    io::{BufReader, Read, Result as IOResult, Seek, SeekFrom, Write},
};

use crate::{
    config::Config, memtable::MemTableRecord, serialization::SerializationEngine, sstable::SSTable,
};
use tempfile::NamedTempFile;

#[derive(Debug)]
struct Entry<T> {
    key: String,
    reader: usize,
    value: Option<T>,
}

impl<T> Eq for Entry<T> {}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.reader == other.reader
    }
}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key
            .cmp(&other.key)
            .then(self.reader.cmp(&other.reader))
    }
}

impl<T> Entry<T> {
    fn new(key: String, reader: usize, value: Option<T>) -> Entry<T> {
        Entry { key, reader, value }
    }
}

/// The order of SSTables is given such that an older index indicate the newest SSTable. This will
/// be used for conflicting keys where the newer will be used
pub fn compact<T, SS>(
    tables: &[SSTable],
    serializer: &SS,
    config: &Config,
    new_storage_path: String,
    new_index_path: String,
) -> IOResult<SSTable>
where
    T: MemTableRecord,
    SS: SerializationEngine<Option<T>>,
{
    if tables.is_empty() {
        panic!("There must be a number of tables");
    }

    let mut readers: Vec<(BufReader<_>, BufReader<_>)> = tables
        .iter()
        .map(|table| {
            let index_file = OpenOptions::new()
                .read(true)
                .open(&table.index_path)
                .unwrap();
            let storage_file = OpenOptions::new()
                .read(true)
                .open(&table.storage_path)
                .unwrap();
            (BufReader::new(index_file), BufReader::new(storage_file))
        })
        .collect();

    let mut heap = BinaryHeap::<Reverse<Entry<T>>>::new();
    let mut index_file = NamedTempFile::new_in(&config.db_path)?;
    let mut storage_file = NamedTempFile::new_in(&config.db_path)?;
    let mut indices: Vec<(String, u64)> = vec![];

    // Read the first elements in each key
    for (i, (index_reader, storage_reader)) in readers.iter_mut().enumerate() {
        if let Some((key, value)) =
            read_next_key(index_reader, storage_reader, config, serializer).unwrap()
        {
            heap.push(Reverse(Entry::new(key, i, value)));
        }
    }

    // Get initial values for min, max, and count
    let mut max = {
        let Reverse(entry): &Reverse<Entry<_>> = heap.peek().unwrap();
        entry.key.clone()
    };
    let mut min = {
        let Reverse(entry): &Reverse<Entry<_>> = heap.peek().unwrap();
        entry.key.clone()
    };
    let mut count = 0;

    // Main Loop
    while !heap.is_empty() {
        let current_key = {
            let Reverse(peek) = heap.peek().unwrap();
            peek.key.clone()
        };

        // Read from all the files so that at least all occurrences of this key are in the heap
        for (i, (index_reader, storage_reader)) in readers.iter_mut().enumerate() {
            if let Some((key, value)) =
                read_next_key(index_reader, storage_reader, config, serializer).unwrap()
            {
                heap.push(Reverse(Entry::new(key, i, value)));
            }
        }

        let mut versions = vec![];
        while let Some(Reverse(entry)) = heap.pop() {
            if entry.key != current_key {
                heap.push(Reverse(Entry::new(entry.key, entry.reader, entry.value)));
                break;
            }
            versions.push(entry);
        }

        let entry = versions
            .into_iter()
            .max_by_key(|entry| entry.reader)
            .unwrap();

        min = min.min(entry.key.clone());
        max = max.max(entry.key.clone());
        count += 1;

        indices.push((entry.key, storage_file.stream_position().unwrap()));
        let encoded = serializer.serialize(entry.value).unwrap();
        storage_file.write_all(&encoded).unwrap();
    }

    for (key, offset) in indices.iter() {
        let mut key_bytes = vec![0u8; config.index_key_string_size];
        let truncated = key.as_bytes();
        let len = truncated.len().min(config.index_key_string_size);
        key_bytes[..len].copy_from_slice(&truncated[..len]);

        index_file.write_all(&key_bytes).unwrap();
        index_file.write_all(&offset.to_le_bytes()).unwrap();
    }

    let size = storage_file.stream_position().unwrap() as usize;

    index_file.persist(&new_index_path).unwrap();
    storage_file.persist(&new_storage_path).unwrap();

    Ok(SSTable {
        storage_path: new_storage_path,
        index_path: new_index_path,
        size,
        min,
        max,
        count,
    })
}
fn read_next_key<T, SS>(
    index_reader: &mut BufReader<File>,
    storage_reader: &mut BufReader<File>,
    config: &Config,
    serializer: &SS,
) -> IOResult<Option<(String, Option<T>)>>
where
    T: MemTableRecord,
    SS: SerializationEngine<Option<T>>,
{
    let mut key = vec![0u8; config.index_key_string_size];
    if index_reader.read_exact(&mut key).is_err() {
        return Ok(None);
    }
    let key = String::from_utf8_lossy(&key)
        .trim_end_matches('\0')
        .to_string();

    let mut offset = vec![0u8; config.index_offset_size];
    index_reader.read_exact(&mut offset).unwrap();

    let offset = u64::from_le_bytes(offset.try_into().unwrap());

    storage_reader.seek(SeekFrom::Start(offset)).unwrap();
    let value = serializer.deserialize(storage_reader).unwrap(); // TODO: Fix if possible

    Ok(Some((key, value)))
}

#[cfg(test)]
mod tests {
    use crate::{
        config::Config, engine::Engine, memtable::MemTableRecord,
        serialization::BinarySerializationEngine,
    };
    use bincode::{Decode, Encode};
    use tempfile::TempDir;

    #[derive(Encode, Decode, Clone, Debug, PartialEq)]
    struct Photo {
        id: String,
        url: String,
        thumbnail_url: String,
    }

    impl MemTableRecord for Photo {
        const TYPE_NAME: &'static str = "Photo";
        fn get_key(&self) -> String {
            self.id.clone()
        }
    }

    #[test]
    fn test_engine_compaction_with_tempdir() {
        // Create a temp directory, will be deleted after test
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create config manually (adjust fields and types if needed)
        let config = Config {
            db_path: temp_dir.path().to_str().unwrap().to_string(),
            index_key_string_size: 24,
            index_offset_size: 8,
            initial_index_file_threshold: 1024,
            parallel_merging_file_count: 2,
            same_size_before_compaction_threshold: 2,
        };

        let serializer = BinarySerializationEngine;

        let engine = Engine::<Photo, BinarySerializationEngine, BinarySerializationEngine>::new(
            &serializer,
            &serializer,
            &config,
        )
        .expect("Engine creation failed");

        // Inline small sample dataset instead of file
        let all_range = 1..1000;
        let all_range: Vec<_> = all_range.collect();
        let sample_photos = all_range.iter().map(|i| Photo {
            id: format!("id_{}", i),
            url: format!("url_{}", i),
            thumbnail_url: format!("thumb_{}", i),
        });

        for photo in sample_photos {
            engine.insert(photo).expect("Insert failed");
        }

        // Delete specific keys
        let mapper = |i: &i32| format!("id_{}", i);
        let deleted: Vec<_> = [1, 2, 3, 4].iter().map(mapper).collect();
        let present: Vec<_> = all_range
            .iter()
            .map(mapper)
            .filter(|key| !deleted.contains(key))
            .collect();

        for key in deleted.iter() {
            engine.delete(key.clone()).expect("Deletion failed");
        }

        // Run compaction multiple times to test stability
        for _ in 0..10 {
            engine.compact();
        }

        // Verify data integrity after compaction
        for key in present {
            let photo = engine.get(key.to_string()).expect("Get failed");
            assert!(photo.is_some(), "Expected key {} to exist", key);
        }

        for key in deleted {
            let photo = engine.get(key.to_string()).expect("Get failed");
            assert!(photo.is_none(), "Expected key {} to be deleted", key);
        }
    }
}

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fs::OpenOptions,
    io::{BufReader, Read, Result as IOResult, Seek, Write},
};

use crate::{config::Config, serialization::SerializationEngine, sstable::SSTable};
use tempfile::NamedTempFile;

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct Entry {
    key: String,
    reader: usize,
}

impl Entry {
    fn new(key: String, reader: usize) -> Entry {
        Entry { key, reader }
    }
}

pub fn compact<T, SS>(
    tables: &[SSTable],
    serializer: &SS,
    config: &Config,
    new_storage_path: String,
    new_index_path: String,
) -> IOResult<SSTable>
where
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

    let count = tables.iter().fold(0, |acc, table| acc + table.count);
    let min = tables
        .iter()
        .map(|table| table.min.clone())
        .reduce(|a, b| a.min(b))
        .unwrap();

    let max = tables
        .iter()
        .map(|table| table.max.clone())
        .reduce(|a, b| a.max(b))
        .unwrap();

    let mut heap: BinaryHeap<Reverse<Entry>> = BinaryHeap::new();

    // Read all first entries
    for (i, (index_reader, _)) in readers.iter_mut().enumerate() {
        let mut key = vec![0u8; config.index_key_string_size];
        index_reader.read_exact(&mut key).unwrap();
        let key = String::from_utf8_lossy(&key)
            .trim_end_matches('\0')
            .to_string();

        index_reader
            .seek_relative(config.index_offset_size as i64)
            .unwrap();

        heap.push(Reverse(Entry::new(key, i)));
    }

    let mut index_file = NamedTempFile::new_in(&config.db_path)?;
    let mut storage_file = NamedTempFile::new_in(&config.db_path)?;

    let mut indices: Vec<(String, u64)> = vec![];

    while let Some(Reverse(entry)) = heap.pop() {
        let Entry { key, reader } = entry;
        let (index_reader, storage_reader) = &mut readers.get_mut(reader).unwrap();
        let record = serializer.deserialize(storage_reader).unwrap();
        indices.push((key, storage_file.stream_position().unwrap()));

        let encoded = serializer.serialize(record).unwrap();
        storage_file.write_all(&encoded).unwrap();

        // Push the following item
        let mut key = vec![0u8; config.index_key_string_size];
        match index_reader.read_exact(&mut key) {
            Ok(()) => {
                let key = String::from_utf8_lossy(&key)
                    .trim_end_matches('\0')
                    .to_string();
                heap.push(Reverse(Entry::new(key, reader)));
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {}
            Err(e) => return Err(e),
        }
        // Skip the offset values
        index_reader
            .seek_relative(config.index_offset_size as i64)
            .unwrap();
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
        count,
        min,
        max,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        config::Config, engine::Engine, memtable::MemTableRecord,
        serialization::BinarySerializationEngine, sstable,
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

        let mut engine =
            Engine::<Photo, BinarySerializationEngine, BinarySerializationEngine>::new(
                &serializer,
                &serializer,
                &config,
            )
            .expect("Engine creation failed");

        // Inline small sample dataset instead of file
        let all_range = 1..1000;
        let all_range: Vec<_> = all_range.collect();
        let sample_photos = all_range.iter().map(|i| Photo {
            id: format!("id_{}", i.to_string()),
            url: format!("url_{}", i.to_string()),
            thumbnail_url: format!("thumb_{}", i.to_string()),
        });

        for photo in sample_photos {
            engine.insert(photo).expect("Insert failed");
        }

        println!("There are {} sstables", engine.sstable_len());
        // Delete specific keys
        let mapper = |i: &i32| format!("id_{}", i.to_string());
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
            println!(
                "Compaction is done. There are {} sstables",
                engine.sstable_len()
            );
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

        // TempDir is cleaned automatically here
    }
}

use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    fs::OpenOptions,
    io::{BufReader, Read, Result as IOResult, Seek, SeekFrom, Write},
    path::Path,
};

use crate::{config::Config, serialization::SerializationEngine, sstable::SSTable};
use tempfile::NamedTempFile;

struct Compaction;

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
            Err(e) => return Err(e.into()),
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

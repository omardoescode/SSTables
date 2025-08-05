use std::{
    fmt::Debug,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use crate::{
    memtable::{LogOperation, MemTable, MemTableRecord},
    serialization::SerializationEngine,
    sstable::error::SSTableError,
};

#[derive(Debug)]
pub struct SSTable {
    path: String,
}

impl SSTable {
    fn create<'a, T, S, SS>(
        table: MemTable<'a, T, S>,
        path: &'a str,
        serializer: SS,
    ) -> Result<SSTable, SSTableError>
    where
        T: MemTableRecord,
        S: SerializationEngine<LogOperation<T>>,
        SS: SerializationEngine<T>,
    {
        if Path::new(path).exists() {
            return Err(SSTableError::LogFileAlreadyExistsError);
        }

        let file = File::create(path).map_err(|_| SSTableError::FileCreationError)?;

        // TODO: Do I need to write metadata??

        let mut writer = BufWriter::new(file);
        for (_, value) in table.iter() {
            let encoded = serializer
                .serialize(value.clone())
                .map_err(|_| SSTableError::EncodingError)?;

            writer
                .write_all(&encoded)
                .map_err(|err| SSTableError::LogWriteError { err })?;
        }

        Ok(SSTable {
            path: String::from(path),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{self, BufRead, BufReader},
        panic,
    };
    use uuid::Uuid;

    use crate::{
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
        fn get_key(&self) -> String {
            self.id.to_string()
        }
    }

    #[test]
    fn create_ss_table() {
        let file = File::open("resources/photos.txt").unwrap();
        let reader = BufReader::new(file);
        let path = format!("logs/{}.txt", Uuid::new_v4());
        let mut memtable = MemTable::<Photo, BinarySerializationEngine>::open_or_build(
            &path,
            &BinarySerializationEngine,
        )
        .unwrap();

        for line in reader.lines() {
            let line = line.unwrap();
            let values: Vec<&str> = line.split(" ").collect();

            if values.len() != 3 {
                panic!("Wrong value");
            }

            memtable
                .insert(Photo {
                    id: values[0].to_string().parse().unwrap(),
                    url: values[1].to_string(),
                    thumbnail_url: values[2].to_string(),
                })
                .unwrap();
        }

        SSTable::create(memtable, "logs/sstable.txt", BinarySerializationEngine).unwrap();
    }
}

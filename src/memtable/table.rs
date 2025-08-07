use rbtree::RBTree;
use std::fs::OpenOptions;
use std::io::Result as IOResult;
use std::path::Path;

use crate::{memtable::MemTableRecord, serialization::SerializationEngine};

use super::{LogOperation, MemTableLog, MemTableLogReader};

pub struct MemTable<'a, T, S>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
{
    pub tree: RBTree<String, Option<T>>,
    pub log: MemTableLog,
    pub serializer: &'a S,
}

impl<'a, T, S> MemTable<'a, T, S>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
{
    pub fn open_or_build(path: &str, serializer: &'a S) -> IOResult<Self> {
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        if Path::new(path).exists() {
            Self::build_from(path, &mut options, serializer)
        } else {
            // File does not exist → create new table
            let file = options.open(path)?;
            Ok(Self {
                tree: RBTree::new(),
                log: MemTableLog::new(file),
                serializer,
            })
        }
    }

    fn build_from(path: &str, options: &mut OpenOptions, serializer: &'a S) -> IOResult<Self> {
        let mut reader = MemTableLogReader::open(options.open(path)?)?;
        let mut tree = RBTree::<String, Option<T>>::new();

        while let Some(op) = reader.next_op(serializer)? {
            match op {
                LogOperation::Insert { record } => {
                    let key = record.get_key();
                    tree.remove(&key);
                    tree.insert(key, Some(record));
                }
                LogOperation::Delete { key } => {
                    tree.remove(&key);
                    tree.insert(key, None);
                }
            }
        }

        Ok(MemTable {
            tree,
            log: MemTableLog::new(options.open(path)?),
            serializer,
        })
    }

    pub fn insert(&mut self, record: T) -> IOResult<()> {
        let key = record.get_key();
        self.log.append(
            LogOperation::Insert {
                record: record.clone(),
            },
            self.serializer,
        )?;
        self.tree.remove(&key); // remove any previous values
        self.tree.insert(key, Some(record));
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> IOResult<()> {
        self.tree.remove(&key); // remove any previous values
        self.tree.insert(key.clone(), None);
        println!(
            "{key} is being deleted. New min is {}",
            self.tree.get_first().unwrap().0
        );
        self.log
            .append(LogOperation::<T>::Delete { key }, self.serializer)?;
        Ok(())
    }

    pub fn get(&self, key: &String) -> Option<&Option<T>> {
        self.tree.get(key)
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    pub fn iter(&self) -> rbtree::Iter<String, Option<T>> {
        self.tree.iter()
    }

    pub fn iter_mut(&mut self) -> rbtree::IterMut<String, Option<T>> {
        self.tree.iter_mut()
    }

    pub fn clear(&mut self) -> IOResult<()> {
        self.tree.clear();
        self.log.clear()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;

    use bincode::{Decode, Encode};
    use uuid::Uuid;

    use crate::{
        memtable::{MemTable, MemTableRecord},
        serialization::BinarySerializationEngine,
    };

    #[derive(Encode, Decode, Clone)]
    struct Dummy(String, i32);
    impl MemTableRecord for Dummy {
        const TYPE_NAME: &'static str = "Dummy";
        fn get_key(&self) -> String {
            self.0.clone()
        }
    }

    fn setup() {
        create_dir_all("temp").unwrap();
    }

    #[test]
    #[should_panic]
    fn non_existing_folder() {
        let serializer = BinarySerializationEngine {};
        let _table = MemTable::<Dummy, BinarySerializationEngine>::open_or_build(
            "/folder/that/doesn't/exist/file.txt",
            &serializer,
        )
        .unwrap();
    }

    #[test]
    fn no_repetitive_items() {
        setup();
        let serializer = BinarySerializationEngine {};
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let mut table =
            MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                .expect("Failed to create table");

        table.insert(Dummy("hello".to_string(), 10)).unwrap();
        table.insert(Dummy("hello".to_string(), 20)).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&"hello".to_string()).unwrap().1, 20);
    }

    #[test]
    fn roundtrip() {
        setup();
        let serializer = BinarySerializationEngine {};
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let mut table =
            MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                .expect("Failed to create table");

        table.insert(Dummy("hello".to_string(), 10)).unwrap();
        let value = table.get(&"hello".to_string()).unwrap();
        assert_eq!(value.1, 10);
    }

    #[test]
    fn deletion() {
        setup();
        let serializer = BinarySerializationEngine {};
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let mut table =
            MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                .expect("Failed to create table");

        table.insert(Dummy("hello".to_string(), 1)).unwrap();

        assert_eq!(table.len(), 1);
        table.delete("hello".to_string()).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn iterates_in_order() {
        setup();
        let serializer = BinarySerializationEngine {};
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let mut table =
            MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                .expect("Failed to create table");

        table.insert(Dummy("b".into(), 10)).unwrap();
        table.insert(Dummy("a".into(), 20)).unwrap();
        table.insert(Dummy("c".into(), 30)).unwrap();

        let keys: Vec<_> = table.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn rebuild_from_log() {
        setup();
        let serializer = BinarySerializationEngine {};
        let path = format!("temp/log_{}.txt", Uuid::new_v4());

        {
            let mut table =
                MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                    .expect("Failed to create table");
            table.insert(Dummy("k1".into(), 1)).unwrap();
            table.insert(Dummy("k2".into(), 2)).unwrap();
            table.delete("k1".into()).unwrap();
            assert_eq!(table.len(), 1);
        }

        let table2 =
            MemTable::<Dummy, BinarySerializationEngine>::open_or_build(&path, &serializer)
                .expect("Failed to create table");

        assert_eq!(table2.len(), 1);
        assert!(table2.get(&"k2".into()).is_some());
        assert!(table2.get(&"k1".into()).is_none());
    }
}

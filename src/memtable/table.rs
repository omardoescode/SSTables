use rbtree::RBTree;
use std::io::Result as IOResult;
use std::sync::RwLock;
use std::{fs::OpenOptions, sync::Arc};

use crate::{memtable::MemTableRecord, serialization::SerializationEngine};

use super::{LogOperation, MemTableLog, MemTableLogReader};

pub struct MemTable<'a, T, S>
where
    T: MemTableRecord,
    S: SerializationEngine<LogOperation<T>>,
{
    pub tree: Arc<RwLock<RBTree<String, Option<T>>>>,
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

        let tree = Arc::new(RwLock::new(tree));
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
        let mut tree = self.tree.write().unwrap();
        tree.remove(&key); // remove any previous values
        tree.insert(key, Some(record));
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> IOResult<()> {
        let mut tree = self.tree.write().unwrap();
        tree.remove(&key); // remove any previous values
        tree.insert(key.clone(), None);
        self.log
            .append(LogOperation::<T>::Delete { key }, self.serializer)?;
        Ok(())
    }

    pub fn get(&self, key: &String) -> Option<Option<T>> {
        let tree = self.tree.read().unwrap();
        tree.get(key).cloned()
    }

    pub fn len(&self) -> usize {
        let tree = self.tree.read().unwrap();
        tree.len()
    }

    pub fn is_empty(&self) -> bool {
        let tree = self.tree.read().unwrap();
        tree.is_empty()
    }

    pub fn clear(&mut self) -> IOResult<()> {
        self.log.clear()?;
        self.tree.write().unwrap().clear();
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (String, Option<T>)> {
        let tree = self.tree.read().unwrap();
        // Snapshot into Vec to avoid holding the lock during iteration
        tree.iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use bincode::{Decode, Encode};
    use tempfile::NamedTempFile;

    use crate::{
        memtable::{MemTable, MemTableRecord},
        serialization::BinarySerializationEngine,
    };

    #[derive(Encode, Decode, Clone, Debug, PartialEq)]
    struct Dummy(String, i32);

    impl MemTableRecord for Dummy {
        const TYPE_NAME: &'static str = "Dummy";
        fn get_key(&self) -> String {
            self.0.clone()
        }
    }

    fn new_temp_path() -> String {
        let file = NamedTempFile::new().unwrap();
        file.path().to_str().unwrap().to_string()
    }

    fn create_memtable<'a>(
        path: &str,
        serializer: &'a BinarySerializationEngine,
    ) -> MemTable<'a, Dummy, BinarySerializationEngine> {
        MemTable::<Dummy, BinarySerializationEngine>::open_or_build(path, serializer)
            .expect("Failed to create MemTable")
    }

    #[test]
    fn non_existing_folder_should_fail() {
        let serializer = BinarySerializationEngine {};
        let result = MemTable::<Dummy, BinarySerializationEngine>::open_or_build(
            "/invalid/path/to/file.log",
            &serializer,
        );
        assert!(result.is_err());
    }

    #[test]
    fn no_repetitive_items() {
        let ser = BinarySerializationEngine;
        let path = new_temp_path();
        let mut table = create_memtable(&path, &ser);

        table.insert(Dummy("hello".to_string(), 10)).unwrap();
        table.insert(Dummy("hello".to_string(), 20)).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(
            table.get(&"hello".to_string()).unwrap().as_ref().unwrap().1,
            20
        );
    }

    #[test]
    fn roundtrip_get() {
        let ser = BinarySerializationEngine;
        let path = new_temp_path();
        let mut table = create_memtable(&path, &ser);

        table.insert(Dummy("hello".to_string(), 10)).unwrap();

        let value = table.get(&"hello".to_string());
        assert!(value.is_some());
        assert_eq!(value.unwrap().as_ref().unwrap().1, 10);
    }

    #[test]
    fn deletion_marks_none() {
        let ser = BinarySerializationEngine;
        let path = new_temp_path();
        let mut table = create_memtable(&path, &ser);

        table.insert(Dummy("hello".to_string(), 1)).unwrap();
        assert_eq!(table.len(), 1);

        table.delete("hello".to_string()).unwrap();

        // Still present in tree, but value is None
        assert_eq!(table.len(), 1);
        assert!(table.get(&"hello".to_string()).is_some());
        assert!(table.get(&"hello".to_string()).unwrap().is_none());
    }

    #[test]
    fn iterates_in_order() {
        let ser = BinarySerializationEngine;
        let path = new_temp_path();
        let mut table = create_memtable(&path, &ser);

        table.insert(Dummy("b".into(), 10)).unwrap();
        table.insert(Dummy("a".into(), 20)).unwrap();
        table.insert(Dummy("c".into(), 30)).unwrap();

        let keys: Vec<_> = table.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn rebuild_from_log_preserves_state() {
        let ser = BinarySerializationEngine;
        let path = new_temp_path();

        {
            let mut table = create_memtable(&path, &ser);
            table.insert(Dummy("k1".into(), 1)).unwrap();
            table.insert(Dummy("k2".into(), 2)).unwrap();
            table.delete("k1".into()).unwrap();

            // Expect tombstone for k1
            assert_eq!(table.len(), 2);
        }

        let table = create_memtable(&path, &ser);

        assert_eq!(table.len(), 2);
        assert!(table.get(&"k2".into()).unwrap().is_some());
        assert!(table.get(&"k1".into()).unwrap().is_none());
    }
}

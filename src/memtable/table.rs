use rbtree::RBTree;
use std::fs::OpenOptions;
use std::io::Result as IOResult;
use std::path::Path;

use crate::memtable::MemTableRecord;

use super::{LogOperation, MemTableLog, MemTableLogReader};

pub struct MemTable<T: MemTableRecord> {
    pub tree: RBTree<String, T>,
    pub log: MemTableLog,
}

impl<T: MemTableRecord> MemTable<T> {
    pub fn open_or_build(path: &str) -> IOResult<Self> {
        let mut options = OpenOptions::new();
        options.create(true).append(true).read(true);
        if Path::new(path).exists() {
            Self::build_from(path, &mut options)
        } else {
            // File does not exist → create new table
            let file = options.open(path)?;
            Ok(Self {
                tree: RBTree::new(),
                log: MemTableLog::new(file),
            })
        }
    }

    fn build_from(path: &str, options: &mut OpenOptions) -> IOResult<Self> {
        let mut reader = MemTableLogReader::open(options.open(path)?)?;
        let mut tree = RBTree::<String, T>::new();

        while let Some(op) = reader.next_op::<T>()? {
            match op {
                LogOperation::Insert { record } => {
                    let key = record.get_key();
                    tree.remove(&key);
                    tree.insert(key, record);
                }
                LogOperation::Delete { key } => {
                    tree.remove(&key);
                }
            }
        }

        Ok(MemTable {
            tree,
            log: MemTableLog::new(options.open(path)?),
        })
    }

    pub fn insert(&mut self, record: T) -> IOResult<()> {
        let key = record.get_key();
        self.log.append(LogOperation::Insert {
            record: record.clone(),
        })?;
        self.tree.remove(&key); // remove any previous values
        self.tree.insert(key, record);
        Ok(())
    }

    pub fn delete(&mut self, key: String) -> IOResult<bool> {
        let result = self.tree.remove(&key).is_some();
        if result {
            self.log.append(LogOperation::<T>::Delete { key })?;
        }
        Ok(result)
    }

    pub fn get(&self, key: &String) -> Option<&T> {
        self.tree.get(key)
    }

    pub fn len(&self) -> usize {
        self.tree.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    pub fn iter(&self) -> rbtree::Iter<String, T> {
        self.tree.iter()
    }

    pub fn iter_mut(&mut self) -> rbtree::IterMut<String, T> {
        self.tree.iter_mut()
    }
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::fs::create_dir_all;

    use bincode::{Decode, Encode};
    use uuid::Uuid;

    use crate::memtable::{MemTable, MemTableRecord};

    #[derive(Encode, Decode, Clone)]
    struct Dummy(String, i32);
    impl MemTableRecord for Dummy {
        fn get_key(&self) -> String {
            self.0.clone()
        }
    }

    fn setup() {
        create_dir_all("temp");
    }

    #[test]
    #[should_panic]
    fn non_existing_folder() {
        let Ok(_) = MemTable::<Dummy>::open_or_build("/folder/that/doesn't/exist") else {
            panic!("Folder doesn't exist");
        };
    }

    #[test]
    fn no_repititive_items() {
        setup();
        let Ok(mut table) = MemTable::<Dummy>::open_or_build("temp/log.txt") else {
            panic!("Folder doesn't exist");
        };

        table.insert(Dummy("hello".to_string(), 10)).unwrap();
        table.insert(Dummy("hello".to_string(), 20)).unwrap();

        assert_eq!(table.len(), 1);
        assert_eq!(table.get(&"hello".to_string()).unwrap().1, 20);
    }

    #[test]
    fn roundtrip() {
        setup();
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let Ok(mut table) = MemTable::<Dummy>::open_or_build(&path) else {
            panic!("Folder doesn't exist");
        };

        table.insert(Dummy("hello".to_string(), 10)).unwrap();
        let value = table.get(&"hello".to_string()).unwrap();
        assert_eq!(value.1, 10);
    }

    #[test]
    fn deletion() {
        setup();
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let Ok(mut table) = MemTable::<Dummy>::open_or_build(&path) else {
            panic!("Folder doesn't exist");
        };
        table.insert(Dummy("hello".to_string(), 1)).unwrap();

        assert_eq!(table.len(), 1);
        table.delete("hello".to_string()).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn iterates_in_order() {
        setup();
        let path = format!("temp/log_{}.txt", Uuid::new_v4());
        let mut table = MemTable::<Dummy>::open_or_build(&path).unwrap();

        table.insert(Dummy("b".into(), 10)).unwrap();
        table.insert(Dummy("a".into(), 20)).unwrap();
        table.insert(Dummy("c".into(), 30)).unwrap();

        let keys: Vec<_> = table.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn rebuild_from_log() {
        setup();
        let path = format!("temp/log_{}.txt", Uuid::new_v4());

        {
            let mut table = MemTable::<Dummy>::open_or_build(&path).unwrap();
            table.insert(Dummy("k1".into(), 1)).unwrap();
            table.insert(Dummy("k2".into(), 2)).unwrap();
            table.delete("k1".into()).unwrap();
            assert_eq!(table.len(), 1);
        }

        let table2 = MemTable::<Dummy>::open_or_build(&path).unwrap();

        assert_eq!(table2.len(), 1);
        assert!(table2.get(&"k2".into()).is_some());
        assert!(table2.get(&"k1".into()).is_none());
    }
}

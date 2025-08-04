use bincode::{Decode, Encode};
use rbtree::RBTree;
use std::fs::{File, OpenOptions};
use std::io::Result as IOResult;

use super::{LogOperation, MemTableLog, MemTableLogReader};

pub struct MemTable<T: Decode<()> + Encode + Clone> {
    pub tree: RBTree<String, T>,
    pub log: MemTableLog,
}

impl<T: Decode<()> + Encode + Clone> MemTable<T> {
    pub fn new(path: &str) -> IOResult<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            tree: RBTree::new(),
            log: MemTableLog::new(file),
        })
    }

    pub fn build_from(path: &str) -> IOResult<Self> {
        let mut reader = MemTableLogReader::open(File::open(path)?)?;
        let mut tree = RBTree::<String, T>::new();

        while let Some(op) = reader.next_op()? {
            match op {
                LogOperation::Insert { key, value } => {
                    tree.insert(key, value);
                }
                LogOperation::Delete { key } => {
                    tree.remove(&key);
                }
            }
        }

        Ok(MemTable {
            tree,
            log: MemTableLog::new(File::open(path)?),
        })
    }

    pub fn insert(&mut self, key: String, value: T) -> IOResult<()> {
        self.log.append(LogOperation::Insert {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.tree.remove(&key); // remove any previous values
        self.tree.insert(key, value);
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
    use std::fs::{create_dir, create_dir_all};

    use bincode::{Decode, Encode};

    use crate::memtable::MemTable;

    #[derive(Encode, Decode, Clone)]
    struct Dummy(String);

    fn setup() {
        create_dir_all("temp");
    }

    #[test]
    #[should_panic]
    fn non_existing_folder() {
        let Ok(_) = MemTable::<Dummy>::new("/folder/that/doesn't/exist") else {
            panic!("Folder doesn't exist");
        };
    }

    #[test]
    fn no_repititive_items() {
        setup();
        let Ok(mut table) = MemTable::<Dummy>::new("temp/log.txt") else {
            panic!("Folder doesn't exist");
        };

        table
            .insert("hello".to_string(), Dummy("hello".to_string()))
            .unwrap();
        table
            .insert("hello".to_string(), Dummy("hello".to_string()))
            .unwrap();

        assert_eq!(table.len(), 1);
    }

    #[test]
    fn roundtrip() {
        setup();
        let Ok(mut table) = MemTable::<Dummy>::new("temp/log.txt") else {
            panic!("Folder doesn't exist");
        };

        table
            .insert("hello".to_string(), Dummy("world".to_string()))
            .unwrap();
        let value = table.get(&"hello".to_string()).unwrap();
        assert_eq!(value.0, "world".to_string());
    }

    #[test]
    fn deletion() {
        let Ok(mut table) = MemTable::<Dummy>::new("temp/log.txt") else {
            panic!("Folder doesn't exist");
        };
        table
            .insert("hello".to_string(), Dummy("world".to_string()))
            .unwrap();

        assert_eq!(table.len(), 1);
        table.delete("hello".to_string()).unwrap();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn iterates_in_order() {
        setup();
        let mut table = MemTable::<Dummy>::new("temp/log4.txt").unwrap();

        table.insert("b".into(), Dummy("2".into())).unwrap();
        table.insert("a".into(), Dummy("1".into())).unwrap();
        table.insert("c".into(), Dummy("3".into())).unwrap();

        let keys: Vec<_> = table.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn rebuild_from_log() {
        setup();
        let path = "temp/log3.txt";

        {
            let mut table = MemTable::<Dummy>::new(path).unwrap();
            table.insert("k1".into(), Dummy("v1".into())).unwrap();
            table.insert("k2".into(), Dummy("v2".into())).unwrap();
            table.delete("k1".into()).unwrap();
        }

        let table2 = MemTable::<Dummy>::build_from(path).unwrap();

        assert_eq!(table2.len(), 1);
        assert!(table2.get(&"k2".into()).is_some());
        assert!(table2.get(&"k1".into()).is_none());
    }
}

use bincode::{Decode, Encode};

use crate::memtable::MemTableRecord;
#[derive(Encode, Decode, PartialEq, Debug)]
pub enum LogOperation<T: MemTableRecord> {
    Insert { record: T },
    Delete { key: String },
}

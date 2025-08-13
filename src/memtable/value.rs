use std::fmt::Debug;

use bincode::{Decode, Encode};

// TODO: This means it will use the comparator of strings only. This won't work for numbers. change
// this interface to change the key type as long as comparable, and serializable
pub trait MemTableRecord: Encode + Decode<()> + Clone + Debug {
    const TYPE_NAME: &'static str;
    fn get_key(&self) -> String;
}

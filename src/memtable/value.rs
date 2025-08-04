use bincode::{Decode, Encode};

pub trait MemTableRecord: Encode + Decode<()> + Clone {
    fn get_key(&self) -> String;
}

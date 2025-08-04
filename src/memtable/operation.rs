use bincode::{Decode, Encode};
#[derive(Encode, Decode, PartialEq, Debug)]
pub enum LogOperation<T: Encode + Decode<()>> {
    Insert { key: String, value: T },
    Delete { key: String },
}

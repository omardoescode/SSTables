use std::io::{BufReader, Read};

use super::SerializationError;

pub trait SerializationEngine<T> {
    fn serialize(&self, data: T) -> Result<Vec<u8>, SerializationError>;
    fn deserialize<R: Read>(&self, data: &mut BufReader<R>) -> Result<T, SerializationError>;
}

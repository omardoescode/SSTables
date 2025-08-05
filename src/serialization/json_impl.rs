use std::io::{BufReader, Read};

use super::{SerializationEngine, SerializationError};
use serde::{Serialize, de::DeserializeOwned};
use serde_json;

pub struct JsonSerializationEngine;

impl<T: Serialize + DeserializeOwned> SerializationEngine<T> for JsonSerializationEngine {
    fn serialize(&self, data: T) -> Result<Vec<u8>, SerializationError> {
        serde_json::to_vec(&data).map_err(|err| SerializationError::Unknown {
            message: format!("Failed to encode to JSON: {err:?}"),
        })
    }
    fn deserialize<R: Read>(&self, reader: &mut BufReader<R>) -> Result<T, SerializationError> {
        serde_json::from_reader(reader).map_err(|err| match err.classify() {
            serde_json::error::Category::Eof => SerializationError::UnexpectedEOF,
            _ => SerializationError::Unknown {
                message: format!("Deserialization Error (JSON): {err:?}"),
            },
        })
    }
}

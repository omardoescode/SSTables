use std::io::{BufReader, Read};

use bincode::{Decode, Encode, config, encode_to_vec};

use super::{SerializationEngine, SerializationError};

pub struct BinarySerializationEngine;

impl<T: Encode + Decode<()>> SerializationEngine<T> for BinarySerializationEngine {
    fn serialize(&self, data: T) -> Result<Vec<u8>, SerializationError> {
        encode_to_vec(data, config::standard()).map_err(|err| SerializationError::Unknown {
            message: format!("Failed to encode: {err:?}"),
        })
    }
    fn deserialize<R: Read>(&self, reader: &mut BufReader<R>) -> Result<T, SerializationError> {
        bincode::decode_from_reader(reader, config::standard()).map_err(|err| match err {
            bincode::error::DecodeError::UnexpectedEnd { .. } => SerializationError::UnexpectedEOF,
            bincode::error::DecodeError::Io { inner, .. }
                if inner.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                SerializationError::UnexpectedEOF
            }
            err => SerializationError::Unknown {
                message: format!("Deserilization Error: {err:?}"),
            },
        })
    }
}

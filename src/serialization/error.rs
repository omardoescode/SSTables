#[derive(Debug)]
pub enum SerializationError {
    UnexpectedEOF,
    Unknown { message: String },
}

pub enum SerializationError {
    UnexpectedEOF,
    Unknown { message: String },
}

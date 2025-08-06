use std::io::{self, Write};

use SSTables::{
    config::Config,
    engine::Engine,
    memtable::{MemTable, MemTableRecord},
    serialization::BinarySerializationEngine,
};
use bincode::{Decode, Encode};

#[derive(Encode, Decode, Clone)]
struct User {
    username: String,
    password: String,
}

impl MemTableRecord for User {
    const TYPE_NAME: &'static str = "User";
    fn get_key(&self) -> String {
        self.username.clone()
    }
}

fn main() {
    let serializer = BinarySerializationEngine;
    let config = Config::from_file("config.yaml").unwrap();
    let mut engine =
        Engine::<User, BinarySerializationEngine>::new("temp/db/", &serializer, &config).unwrap();
}

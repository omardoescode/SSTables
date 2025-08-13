use SSTables::{
    config::Config, engine::Engine, memtable::MemTableRecord,
    serialization::BinarySerializationEngine,
};
use bincode::{Decode, Encode};

#[derive(Encode, Decode, Clone, Debug)]
struct User {
    username: String,
    password: String,
    photo_url: Option<String>,
}

impl MemTableRecord for User {
    const TYPE_NAME: &'static str = "Photo";
    fn get_key(&self) -> String {
        self.username.clone()
    }
}

fn main() {
    let serializer = BinarySerializationEngine;
    let config = Config::from_file("config.yaml").unwrap();
    let engine = Engine::<User, BinarySerializationEngine, BinarySerializationEngine>::new(
        &serializer,
        &serializer,
        &config,
    )
    .unwrap();

    let count = config.initial_index_file_threshold
        / (config.index_key_string_size + config.index_offset_size);

    for newer in 0..100 {
        for i in 0..count {
            engine
                .insert(User {
                    username: format!("user_{}", i),
                    password: format!("pass_{}", newer),
                    photo_url: None,
                })
                .unwrap();
        }
    }

    // Make sure all files exist
    for i in 0..count {
        let key = format!("user_{}", i);
        assert!(engine.get(key).unwrap().is_some());
    }

    engine.compact();

    for i in 0..count {
        let key = format!("user_{}", i);
        assert!(engine.get(key).unwrap().is_some());
    }
}

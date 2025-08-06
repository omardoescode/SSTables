use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use SSTables::{
    config::Config, engine::Engine, memtable::MemTableRecord,
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

#[derive(Encode, Decode, Clone)]
struct Photo {
    id: i32,
    url: String,
    thumbnail_url: String,
}

impl MemTableRecord for Photo {
    const TYPE_NAME: &'static str = "Photo";
    fn get_key(&self) -> String {
        self.id.to_string()
    }
}

fn main() {
    let serializer = BinarySerializationEngine;
    let config = Config::from_file("config.yaml").unwrap();
    let mut engine = Engine::<Photo, BinarySerializationEngine, BinarySerializationEngine>::new(
        "temp/db/",
        &serializer,
        &serializer,
        &config,
    )
    .unwrap();

    // Seed if empty
    if engine.memtable_len() == 0 {
        let file = File::open("resources/photos.txt").unwrap();
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();
            let values: Vec<&str> = line.split(" ").collect();

            if values.len() != 3 {
                panic!("Wrong value");
            }

            engine
                .insert(Photo {
                    id: values[0].to_string().parse().unwrap(),
                    url: values[1].to_string(),
                    thumbnail_url: values[2].to_string(),
                })
                .unwrap();
        }
    }

    for i in 1..5001 {
        assert!(
            engine.get(i.to_string()).unwrap().is_some(),
            "loading {i} failed"
        );
    }
}

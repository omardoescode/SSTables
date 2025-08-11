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
        &serializer,
        &serializer,
        &config,
    )
    .unwrap();

    // Seed if empty
    if engine.memtable_len() == 0 && engine.sstable_len() == 0 {
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

        engine.delete("1000".to_string()).unwrap();
        engine.delete("50".to_string()).unwrap();
        engine.delete("5000".to_string()).unwrap();
    }

    println!("Engine has {} in memory", engine.memtable_len());

    while engine.sstable_len() != 1 {
        engine.compact();
        for i in 1..5001 {
            let photo = engine.get(i.to_string()).unwrap();
            if i == 1000 || i == 50 || i == 5000 {
                assert!(photo.is_none(), "{i} still exists");
                continue;
            }
            assert!(photo.is_some(), "loading {i} failed");
            let photo = photo.unwrap();

            // println!("{} - {} - {}", photo.id, photo.url, photo.thumbnail_url);
        }
    }
}

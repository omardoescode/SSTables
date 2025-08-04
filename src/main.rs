use std::io::{self, Write};

use SSTables::memtable::MemTable;
use bincode::{Decode, Encode};

#[derive(Encode, Decode, Clone)]
struct User {
    username: String,
    password: String,
}

fn insertion() {
    let mut memtable = match MemTable::<User>::new("logs/log.txt") {
        Ok(val) => val,
        Err(err) => {
            panic!("Failed to create a new memtable: {err:?}");
        }
    };

    memtable.insert(
        "admin".to_string(),
        User {
            username: "admin".to_string(),
            password: "1234".to_string(),
        },
    );

    memtable.insert(
        "admin".to_string(),
        User {
            username: "admin".to_string(),
            password: "1234".to_string(),
        },
    );
}

fn reading() {
    let mut memtable = match MemTable::<User>::build_from("logs/log.txt") {
        Ok(val) => val,
        Err(err) => {
            panic!("Failed to create a new memtable: {err:?}");
        }
    };

    println!("Len: {}", memtable.len());

    for user in memtable.iter() {
        println!(
            "key: {}, username: {}, password: {}",
            user.0, user.1.username, user.1.password
        );
    }
}

fn main() {
    println!("Enter 0 for insertion, 1 for reading:");

    print!("> ");
    io::stdout().flush().unwrap();

    let mut choice = String::new();
    io::stdin().read_line(&mut choice).unwrap();

    match choice.trim() {
        "0" => insertion(),
        "1" => reading(),
        _ => println!("Invalid choice! Please enter 0 or 1."),
    }
}

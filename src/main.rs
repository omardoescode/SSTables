use SSTables::memtable::MemTable;
use bincode::{Decode, Encode};

#[derive(Encode, Decode, Clone)]
struct User {
    username: String,
    password: String,
}
fn main() {
    let mut memtable = match MemTable::<User>::new() {
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

    memtable.insert(
        "omar".to_string(),
        User {
            username: "omar".to_string(),
            password: "HelloWorld".to_string(),
        },
    );
}

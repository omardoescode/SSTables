use serde::Deserialize;
use serde_yaml;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub index_key_string_size: u8,
    pub index_offset_size: u8,
    pub memtable_threshold: u16,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let yaml_str = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&yaml_str)?;
        Ok(config)
    }
}

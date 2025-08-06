use serde::Deserialize;
use serde_yaml;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub index_key_string_size: usize,
    pub index_offset_size: usize,
    pub memtable_threshold: usize,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let yaml_str = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&yaml_str)?;
        Ok(config)
    }
}

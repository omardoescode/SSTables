use serde::Deserialize;
use serde_yaml;
use std::fs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub db_path: String,
    pub index_key_string_size: usize,
    pub index_offset_size: usize,
    pub initial_index_file_threshold: usize,
    pub compaction_threshold: u32,
    pub compaction_tier_size: usize,
    pub compaction_size_multiplier: u32,
}

impl Config {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let yaml_str = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&yaml_str)?;
        Ok(config)
    }
}

use std::io::{Error, ErrorKind};

use crate::config::{config::Config, config_reader::ConfigReader};

pub struct ConfigFileReader {
    config_path: String,
}

impl ConfigFileReader {
    pub fn new(config_path: &str) -> ConfigFileReader {
        ConfigFileReader {
            config_path: config_path.to_string(),
        }
    }
}

impl ConfigReader for ConfigFileReader {
    fn read(&self) -> Result<Config, Error> {
        match std::fs::read_to_string(self.config_path.as_str()) {
            Ok(config_json) => match serde_json::de::from_str(config_json.as_str()) {
                Ok(config) => return Ok(config),
                Err(error) => {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Failed to deserialize config's JSON: {}", error),
                    ));
                }
            },
            Err(error) => return Err(error),
        }
    }
}

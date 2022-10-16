use super::{config_file_reader::ConfigFileReader, config_reader::ConfigReader};

const DEFAULT_CONFIG_PATH: &str = "./config.json";

pub fn default() -> Box<dyn ConfigReader> {
    Box::new(ConfigFileReader::new(DEFAULT_CONFIG_PATH))
}

use std::io::{Error, ErrorKind};

use cooplan_definition_git_downloader::{
    downloader::Downloader,
    version_detector::{self, VersionDetector},
};
use definition::{file_reader::FileReader, rabbitmq_input::RabbitMQInput};

pub mod config;
pub mod definition;
pub mod error;

#[tokio::main]
async fn main() -> Result<(), Error> {
    match simple_logger::SimpleLogger::new().env().init() {
        Ok(_) => (),
        Err(error) => {
            return Err(Error::new(
                ErrorKind::Interrupted,
                format!("failed to initialize logger: {}", error),
            ));
        }
    }

    let mut input = RabbitMQInput::new(
        "amqp://guest:guest@127.0.0.1:5672".to_string(),
        "definition-provider-output".to_string(),
    );

    let config = match crate::config::config_reader_builder::default().read() {
        Ok(config) => config,
        Err(error) => {
            return Err(error);
        }
    };

    match input.connect().await {
        Ok(_) => loop {
            match input.get().await {
                Ok(definition) => {
                    let downloader = Downloader::new(config.git());

                    match downloader.download() {
                        Ok(_) => {
                            let version_detector =
                                VersionDetector::new(config.git().repository_local_dir);

                            match downloader.set_version(definition.version()) {
                                Ok(_) => {
                                    let file_reader = FileReader::new(
                                        config.git().repository_local_dir,
                                        version_detector,
                                    );

                                    match file_reader.read() {
                                        Ok(read_definition) => {
                                            if definition == read_definition {
                                                log::info!("definitions match");
                                                std::process::exit(0);
                                            }

                                            log::error!("definitions do not match: received {:?} ||| downloaded {:?}", definition, read_definition);
                                            std::process::exit(1);
                                        }
                                        Err(error) => {
                                            log::error!("error: {}", error);
                                            std::process::exit(2);
                                        }
                                    }
                                }
                                Err(error) => {
                                    log::error!("error: {}", error);
                                    std::process::exit(2);
                                }
                            }
                        }
                        Err(error) => {
                            log::error!("error: {}", error);
                            std::process::exit(2);
                        }
                    }
                }
                Err(error) => {
                    log::error!("error: {}", error);
                    std::process::exit(2);
                }
            }
        },
        Err(error) => {
            log::error!("error: {}", error);
            std::process::exit(1);
        }
    }

    Ok(())
}

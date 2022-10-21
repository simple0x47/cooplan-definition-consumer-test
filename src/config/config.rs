use cooplan_definition_git_downloader::git_config::GitConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Config {
    git: GitConfig,
    amqp_channel_name: String,
}

impl Config {
    pub fn git(&self) -> GitConfig {
        self.git.clone()
    }

    pub fn amqp_channel_name(&self) -> String {
        self.amqp_channel_name.clone()
    }
}

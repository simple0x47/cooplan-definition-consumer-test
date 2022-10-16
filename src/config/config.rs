use cooplan_definition_git_downloader::git_config::GitConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Config {
    git: GitConfig,
}

impl Config {
    pub fn git(&self) -> GitConfig {
        self.git.clone()
    }
}

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,

    #[serde(default = "default_ttl_seconds")]
    pub ttl_seconds: u64,

    #[serde(default = "default_max_entries")]
    pub max_entries: u64,

    #[serde(default = "default_op_path")]
    pub op_path: String,

    #[serde(default = "default_op_timeout_seconds")]
    pub op_timeout_seconds: u64,
}

fn default_socket_path() -> PathBuf {
    PathBuf::from("/tmp/op-cache.sock")
}

fn default_ttl_seconds() -> u64 {
    86400 // 24 hours
}

fn default_max_entries() -> u64 {
    1000
}

fn default_op_path() -> String {
    "op".to_string()
}

fn default_op_timeout_seconds() -> u64 {
    30
}

impl Default for Config {
    fn default() -> Self {
        Self {
            socket_path: default_socket_path(),
            ttl_seconds: default_ttl_seconds(),
            max_entries: default_max_entries(),
            op_path: default_op_path(),
            op_timeout_seconds: default_op_timeout_seconds(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = Self::config_path()?;

        if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path)?;
            let config: Config = serde_yaml::from_str(&contents)?;
            Ok(config)
        } else {
            Ok(Config::default())
        }
    }

    pub fn config_path() -> Result<PathBuf> {
        if let Some(proj_dirs) = directories::ProjectDirs::from("", "", "op-cache") {
            Ok(proj_dirs.config_dir().join("config.yaml"))
        } else {
            Ok(PathBuf::from("~/.config/op-cache/config.yaml"))
        }
    }

    pub fn pid_path(&self) -> PathBuf {
        self.socket_path.with_extension("pid")
    }

    pub fn log_path(&self) -> PathBuf {
        self.socket_path.with_extension("log")
    }

    pub fn temp_file_dir(&self) -> PathBuf {
        let tmp_base = std::env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
        PathBuf::from(tmp_base).join("op-cache-fs")
    }
}

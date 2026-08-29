use serde::{Deserialize, Serialize};
use std::fs;
use anyhow::{Context, Result};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub peer: Option<PeerConfig>,
    pub storage: StorageConfig,
    pub wal: WalConfig,
    pub http: HttpConfig,
    pub databases: Vec<DatabaseConfig>,
    #[serde(default)]
    pub memory: MemoryConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ServerConfig {
    pub id: u32,
    pub replication_port: u16,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PeerConfig {
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct StorageConfig {
    pub base_dir: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WalConfig {
    pub flush_interval_secs: u64,
    pub batch_size: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HttpConfig {
    pub port: u16,
    pub trusted_ips: Vec<String>,
    pub secret_key: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    pub id: u32,
    pub name: String,
}

/// Optional memory-pressure controls for the raw key-value store.
/// `max_entries_per_table = 0` (the default) means "unlimited", i.e. this
/// behavior is opt-in via config.toml.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MemoryConfig {
    #[serde(default)]
    pub max_entries_per_table: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        MemoryConfig { max_entries_per_table: 0 }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            server: ServerConfig { id: 1, replication_port: 9999 },
            peer: None,
            storage: StorageConfig { base_dir: "/var/lib/skvs".to_string() },
            wal: WalConfig { flush_interval_secs: 1, batch_size: 1000 },
            http: HttpConfig {
                port: 3000,
                trusted_ips: vec!["127.0.0.1".to_string()],
                secret_key: "change-me".to_string(),
            },
            databases: vec![DatabaseConfig { id: 0, name: "default".to_string() }],
            memory: MemoryConfig::default(),
        }
    }
}

pub fn load_config(path: &str) -> Result<Config> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path))?;
    let config: Config = toml::from_str(&contents)
        .with_context(|| "Invalid TOML format")?;
    Ok(config)
}

pub fn load_config_from_default() -> Result<Config> {
    let path = std::env::var("SKVS_CONFIG")
        .unwrap_or_else(|_| "/etc/skvs/config.toml".to_string());
    load_config(&path)
}
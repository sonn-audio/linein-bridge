use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CONFIG_DIR_SYSTEM: &str = "/etc/lox-linein-bridge";
const CONFIG_DIR_FALLBACK: &str = ".config/lox-linein-bridge";
const CONFIG_FILE: &str = "config.toml";

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub bridge_id: String,
    #[serde(default)]
    pub preferred_server_name: Option<String>,
    #[serde(default)]
    pub preferred_server_mac: Option<String>,
    /// Run for every command the server sends, with the command and its arguments as positional
    /// parameters: `start`, `stop`, `play`, `pause`, `next`, `previous`, and whatever the server
    /// adds later. `start` is what switches on gear that does not power up by itself -- without it
    /// the chain deadlocks, because the VAD waits for audio that only appears once the source is on.
    #[serde(default)]
    pub on_command: Option<String>,
}

pub fn preferred_config_path() -> PathBuf {
    PathBuf::from(CONFIG_DIR_SYSTEM).join(CONFIG_FILE)
}

pub fn fallback_config_path() -> Result<PathBuf> {
    let home = std::env::var("HOME").context("HOME is not set")?;
    Ok(PathBuf::from(home)
        .join(CONFIG_DIR_FALLBACK)
        .join(CONFIG_FILE))
}

pub fn write_config(config: &Config) -> Result<PathBuf> {
    let contents = toml::to_string_pretty(config).context("serialize config")?;
    let preferred = preferred_config_path();
    if try_write(&preferred, &contents).is_ok() {
        return Ok(preferred);
    }

    let fallback = fallback_config_path()?;
    try_write(&fallback, &contents).context("write fallback config")?;
    Ok(fallback)
}

pub fn load_or_create_config() -> Result<(Config, PathBuf)> {
    let preferred = preferred_config_path();
    if preferred.exists() {
        match load_config_file(&preferred) {
            Ok(config) => return Ok((config, preferred)),
            Err(err) => {
                backup_invalid_config(&preferred, &err)?;
            }
        }
    }

    let fallback = fallback_config_path()?;
    if fallback.exists() {
        match load_config_file(&fallback) {
            Ok(config) => return Ok((config, fallback)),
            Err(err) => {
                backup_invalid_config(&fallback, &err)?;
            }
        }
    }

    let config = Config {
        bridge_id: uuid::Uuid::new_v4().to_string(),
        preferred_server_name: None,
        preferred_server_mac: None,
        on_command: None,
    };
    let path = write_config(&config)?;
    Ok((config, path))
}

fn load_config_file(path: &Path) -> Result<Config> {
    let data = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    toml::from_str(&data).with_context(|| format!("parse {}", path.display()))
}

fn backup_invalid_config(path: &Path, err: &anyhow::Error) -> Result<()> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let backup = path.with_extension(format!("invalid.{}", timestamp));
    fs::rename(path, &backup)
        .or_else(|_| {
            fs::copy(path, &backup)
                .map(|_| ())
                .and_then(|_| fs::remove_file(path))
        })
        .with_context(|| format!("backup invalid config {}: {}", path.display(), err))?;
    Ok(())
}

fn try_write(path: &Path, contents: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    fs::write(path, contents).with_context(|| format!("write {}", path.display()))
}

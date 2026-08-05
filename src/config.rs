use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

/// Runtime configuration loaded from the environment (.env file).
#[derive(Debug, Clone)]
pub struct Config {
    pub telegram_bot_token: String,
    pub telegram_api_id: i32,
    pub telegram_api_hash: String,
    pub telegram_session_file: PathBuf,
    pub telegram_chat_id: Option<i64>,
    pub telegram_channel: Option<String>,
    pub cache_directory: PathBuf,
    pub bandcamp_tags: Vec<String>,
    pub bandcamp_tag_blacklist: Vec<String>,
    pub bandcamp_artists: Vec<String>,
    pub poll_interval: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        if let Err(err) = dotenvy::dotenv() {
            log::warn!("failed to parse .env: {err}");
        }
        let telegram_bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
            .context("TELEGRAM_BOT_TOKEN is not set (see .env.example)")?;

        let telegram_api_id = std::env::var("TELEGRAM_API_ID")
            .context("TELEGRAM_API_ID is not set (see .env.example)")?
            .parse::<i32>()
            .context("TELEGRAM_API_ID must be an integer")?;

        let telegram_api_hash = std::env::var("TELEGRAM_API_HASH")
            .context("TELEGRAM_API_HASH is not set (see .env.example)")?;

        let telegram_session_file = PathBuf::from(
            std::env::var("TELEGRAM_SESSION_FILE").unwrap_or_else(|_| "session.bin".into()),
        );

        let telegram_chat_id = std::env::var("TELEGRAM_CHAT_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| value.parse::<i64>().context("TELEGRAM_CHAT_ID must be an integer"))
            .transpose()?;

        let telegram_channel = std::env::var("TELEGRAM_CHANNEL")
            .ok()
            .filter(|value| !value.is_empty());

        let cache_directory = PathBuf::from(
            std::env::var("CACHE_DIRECTORY").unwrap_or_else(|_| "cache".into()),
        );

        let bandcamp_tags = parse_list(&std::env::var("BANDCAMP_TAGS").unwrap_or_default())
            .into_iter()
            .map(|tag| crate::bandcamp::normalize_tag(&tag))
            .collect();
        // Blacklist entries are matched against page tag slugs via `tag_key`,
        // so no slug normalization is applied (it would mangle non-ASCII names
        // like "sascha_müller").
        let bandcamp_tag_blacklist =
            parse_list(&std::env::var("BANDCAMP_TAG_BLACKLIST").unwrap_or_default());
        let bandcamp_artists = parse_list(&std::env::var("BANDCAMP_ARTISTS").unwrap_or_default());

        let poll_interval = std::env::var("POLL_INTERVAL_SECONDS")
            .ok()
            .map(|value| value.parse::<u64>().context("POLL_INTERVAL_SECONDS must be an integer"))
            .transpose()?
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(3600));

        Ok(Self {
            telegram_bot_token,
            telegram_api_id,
            telegram_api_hash,
            telegram_session_file,
            telegram_chat_id,
            telegram_channel,
            cache_directory,
            bandcamp_tags,
            bandcamp_tag_blacklist,
            bandcamp_artists,
            poll_interval,
        })
    }
}

/// Splits a comma-separated env value into trimmed, non-empty entries.
fn parse_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(str::to_string)
        .collect()
}

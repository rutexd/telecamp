use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;

/// Streams Bandcamp releases into a Telegram channel.
///
/// Every option can be set either as a CLI flag or via the matching environment
/// variable (see `--help`). CLI flags take priority; env vars are the fallback.
#[derive(Debug, Clone, Parser)]
#[command(name = "telecamp", version, about)]
pub struct Config {
    /// Telegram bot token (from @BotFather).
    #[arg(long, env = "TELEGRAM_BOT_TOKEN")]
    pub telegram_bot_token: String,

    /// MTProto developer api_id (from https://my.telegram.org).
    #[arg(long, env = "TELEGRAM_API_ID")]
    pub telegram_api_id: i32,

    /// MTProto developer api_hash (from https://my.telegram.org).
    #[arg(long, env = "TELEGRAM_API_HASH")]
    pub telegram_api_hash: String,

    /// Path to the persisted MTProto session (created on first run).
    #[arg(long, env = "TELEGRAM_SESSION_FILE", default_value = "session.bin")]
    pub telegram_session_file: PathBuf,

    /// Numeric chat id to post into (mutually exclusive with `--telegram-channel`).
    #[arg(long, env = "TELEGRAM_CHAT_ID")]
    pub telegram_chat_id: Option<String>,

    /// Channel @username to post into (takes priority over `--telegram-chat-id`).
    #[arg(long, env = "TELEGRAM_CHANNEL")]
    pub telegram_channel: Option<String>,

    /// Directory holding the "already seen" cache and stores.
    #[arg(long, env = "CACHE_DIRECTORY", default_value = "cache")]
    pub cache_directory: PathBuf,

    /// Comma-separated Bandcamp tags to watch for new releases.
    #[arg(long, env = "BANDCAMP_TAGS", value_delimiter = ',')]
    pub bandcamp_tags: Vec<String>,

    /// Comma-separated tags that mark an album as unwanted.
    #[arg(long, env = "BANDCAMP_TAG_BLACKLIST", value_delimiter = ',')]
    pub bandcamp_tag_blacklist: Vec<String>,

    /// Comma-separated artist pages to watch.
    #[arg(long, env = "BANDCAMP_ARTISTS", value_delimiter = ',')]
    pub bandcamp_artists: Vec<String>,

    /// Comma-separated artist sites (full URLs) to never post.
    #[arg(long, env = "BANDCAMP_ARTIST_BLACKLIST", value_delimiter = ',')]
    pub bandcamp_artist_blacklist: Vec<String>,

    /// Skip releases older than this many days; 0 disables the filter.
    #[arg(long, env = "BANDCAMP_MAX_AGE_DAYS", default_value_t = 30)]
    pub bandcamp_max_age_days: u64,

    /// Albums with more tracks than this are posted cover-only; 0 disables the limit.
    #[arg(long, env = "BANDCAMP_MAX_TRACKS", default_value_t = 20)]
    pub bandcamp_max_tracks: usize,

    /// Seconds to wait between album posts (uploads share the MTProto connection
    /// that also carries updates, so pacing posts keeps the server from flushing
    /// the update queue with `updatesTooLong`).
    #[arg(long, env = "POST_ALBUM_DELAY_SECONDS", default_value_t = 5)]
    pub post_album_delay_seconds: u64,

    /// Seconds between scan rounds.
    #[arg(long, env = "POLL_INTERVAL_SECONDS", default_value_t = 3600)]
    pub poll_interval_seconds: u64,
}

impl Config {
    /// Loads the `.env` file (if any), then parses CLI flags with env vars as
    /// fallback, and normalizes the result (empty -> None, tag slugs, ...).
    pub fn from_env() -> Result<Self> {
        if let Err(err) = dotenvy::dotenv() {
            log::warn!("failed to parse .env: {err}");
        }

        let mut config = Self::parse();
        config.finalize()?;
        Ok(config)
    }

    /// Normalizes raw parsed values into their final form: empty channel/chat
    /// become unset, list entries are trimmed and tags are converted to
    /// Bandcamp slugs. Split out so tests can exercise it without a real `.env`.
    fn finalize(&mut self) -> Result<()> {
        self.telegram_channel = self
            .telegram_channel
            .take()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());

        self.telegram_chat_id = self
            .telegram_chat_id
            .take()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        if let Some(value) = &self.telegram_chat_id {
            value
                .parse::<i64>()
                .context("TELEGRAM_CHAT_ID must be an integer")?;
        }

        // Tags are normalized to Bandcamp slugs ("Drum & Bass" -> "drum-and-bass");
        // blacklist entries are matched against page tag slugs, so they are not.
        self.bandcamp_tags = clean_list(std::mem::take(&mut self.bandcamp_tags))
            .into_iter()
            .map(|tag| crate::bandcamp::normalize_tag(&tag))
            .collect();
        self.bandcamp_tag_blacklist = clean_list(std::mem::take(&mut self.bandcamp_tag_blacklist));
        self.bandcamp_artists = clean_list(std::mem::take(&mut self.bandcamp_artists));
        self.bandcamp_artist_blacklist =
            clean_list(std::mem::take(&mut self.bandcamp_artist_blacklist));

        Ok(())
    }

    /// The configured scan interval.
    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_seconds)
    }

    /// The configured pause inserted between album posts.
    pub fn post_album_delay(&self) -> Duration {
        Duration::from_secs(self.post_album_delay_seconds)
    }
}

/// Trims comma-separated list entries and drops empty ones.
fn clean_list(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_ENV_VARS: &[&str] = &[
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        "TELEGRAM_SESSION_FILE",
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_CHANNEL",
        "CACHE_DIRECTORY",
        "BANDCAMP_TAGS",
        "BANDCAMP_TAG_BLACKLIST",
        "BANDCAMP_ARTISTS",
        "BANDCAMP_ARTIST_BLACKLIST",
        "BANDCAMP_MAX_AGE_DAYS",
        "BANDCAMP_MAX_TRACKS",
        "POST_ALBUM_DELAY_SECONDS",
        "POLL_INTERVAL_SECONDS",
    ];

    /// These tests read/write the same process env vars, so they must not run
    /// concurrently (cargo test threads) or they would race each other.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn clear_env() {
        for var in ALL_ENV_VARS {
            unsafe { std::env::remove_var(var) };
        }
    }

    fn set_var(var: &str, value: &str) {
        unsafe { std::env::set_var(var, value) };
    }

    fn parse_from<I, T>(args: I) -> Config
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let mut config = Config::try_parse_from(args).unwrap();
        config.finalize().unwrap();
        config
    }

    #[test]
    fn reads_required_from_env_with_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("TELEGRAM_BOT_TOKEN", "123:token");
        set_var("TELEGRAM_API_ID", "25096665");
        set_var("TELEGRAM_API_HASH", "hash");

        let config = parse_from(std::iter::empty::<&str>());
        assert_eq!(config.telegram_bot_token, "123:token");
        assert_eq!(config.telegram_api_id, 25096665);
        assert_eq!(config.telegram_api_hash, "hash");
        assert_eq!(config.telegram_session_file, PathBuf::from("session.bin"));
        assert_eq!(config.cache_directory, PathBuf::from("cache"));
        assert_eq!(config.bandcamp_max_age_days, 30);
        assert_eq!(config.bandcamp_max_tracks, 20);
        assert_eq!(config.post_album_delay_seconds, 5);
        assert_eq!(config.poll_interval_seconds, 3600);
        assert!(config.telegram_chat_id.is_none());
        assert!(config.telegram_channel.is_none());
    }

    #[test]
    fn cli_flags_override_env() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("TELEGRAM_BOT_TOKEN", "env-token");
        set_var("TELEGRAM_API_ID", "1");
        set_var("TELEGRAM_API_HASH", "env-hash");

        let config = parse_from([
            "telecamp",
            "--telegram-bot-token", "cli-token",
            "--telegram-api-id", "42",
            "--telegram-api-hash", "cli-hash",
            "--bandcamp-max-tracks", "5",
            "--poll-interval-seconds", "60",
            "--cache-directory", "C:/tmp/cache",
        ]);
        assert_eq!(config.telegram_bot_token, "cli-token");
        assert_eq!(config.telegram_api_id, 42);
        assert_eq!(config.telegram_api_hash, "cli-hash");
        assert_eq!(config.bandcamp_max_tracks, 5);
        assert_eq!(config.poll_interval_seconds, 60);
        assert_eq!(config.cache_directory, PathBuf::from("C:/tmp/cache"));
    }

    #[test]
    fn empty_env_values_are_treated_as_unset() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("TELEGRAM_BOT_TOKEN", "t");
        set_var("TELEGRAM_API_ID", "1");
        set_var("TELEGRAM_API_HASH", "h");
        set_var("TELEGRAM_CHANNEL", "");
        set_var("TELEGRAM_CHAT_ID", "");

        let config = parse_from(std::iter::empty::<&str>());
        assert!(config.telegram_channel.is_none());
        assert!(config.telegram_chat_id.is_none());
    }

    #[test]
    fn lists_split_on_commas_and_normalize_tags() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_env();
        set_var("TELEGRAM_BOT_TOKEN", "t");
        set_var("TELEGRAM_API_ID", "1");
        set_var("TELEGRAM_API_HASH", "h");
        set_var("BANDCAMP_TAGS", "noise, black-metal, Drum & Bass");
        set_var("BANDCAMP_TAG_BLACKLIST", "demo,  mixtape ");

        let config = parse_from(std::iter::empty::<&str>());
        assert_eq!(
            config.bandcamp_tags,
            vec!["noise", "black-metal", "drum-and-bass"]
        );
        assert_eq!(config.bandcamp_tag_blacklist, vec!["demo", "mixtape"]);
    }
}

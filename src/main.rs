use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use tokio::signal;
use tokio::time::sleep;
use telecamp::{bandcamp, bot, cache, config, downloader, scanner, store, tg, uploader};

/// Delay before reconnecting to Telegram after the connection drops.
const RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let config = config::Config::from_env()?;
    log::info!(
        "telecamp starting: {} tag(s), {} artist(s), {} blacklisted tag(s), poll every {}s, cache at {}",
        config.bandcamp_tags.len(),
        config.bandcamp_artists.len(),
        config.bandcamp_tag_blacklist.len(),
        config.poll_interval_seconds,
        config.cache_directory.display(),
    );

    let state = Arc::new(bot::AppState {
        bandcamp: bandcamp::BandcampClient::new(),
        cache: Mutex::new(cache::SeenCache::open(
            config.cache_directory.join("seen.json"),
        )?),
        downloader: Box::new(downloader::HttpDownloader::new()),
        uploader: Box::new(uploader::TelegramUploader::default()),
        tag_blacklist: config.bandcamp_tag_blacklist.clone(),
        liked: Mutex::new(store::LikedStore::open(
            config.cache_directory.join("favorites.json"),
        )?),
        posts: Mutex::new(store::PostRegistry::open(
            config.cache_directory.join("posts.json"),
        )?),
        votes: Mutex::new(store::VoteLog::open(
            config.cache_directory.join("votes.json"),
        )?),
        menu_id: Mutex::new(None),
        menu_page: Mutex::new(0),
        max_tracks: config.bandcamp_max_tracks,
        uploading: Mutex::new(std::collections::HashSet::new()),
        posting: tokio::sync::Mutex::new(()),
    });

    let result = supervise(config, state.clone()).await;

    // Persist the "already seen" cache and votes before exiting.
    if let Err(err) = state.cache.lock().unwrap().save() {
        log::error!("failed to save cache on exit: {err:#}");
    }
    if let Err(err) = state.liked.lock().unwrap().save() {
        log::error!("failed to save favorites on exit: {err:#}");
    }
    if let Err(err) = state.posts.lock().unwrap().save() {
        log::error!("failed to save posts registry on exit: {err:#}");
    }
    if let Err(err) = state.votes.lock().unwrap().save() {
        log::error!("failed to save votes on exit: {err:#}");
    }

    result
}

/// Runs the scanner and the interactive bot, reconnecting to Telegram
/// whenever either task dies, until Ctrl+C is pressed.
async fn supervise(config: config::Config, state: Arc<bot::AppState>) -> Result<()> {
    loop {
        let client = match tg::TgClient::connect(&config).await {
            Ok(client) => client,
            Err(err) => {
                log::error!(
                    "telegram connect failed: {err:#}; retrying in {}s",
                    RECONNECT_DELAY.as_secs()
                );
                sleep(RECONNECT_DELAY).await;
                continue;
            }
        };

        log::info!("telegram connected");

        let mut scan_task = tokio::spawn(scanner::run(config.clone(), state.clone(), client.clone()));
        let mut bot_task = tokio::spawn(bot::run(client, state.clone()));

        tokio::select! {
            result = signal::ctrl_c() => {
                if let Err(err) = result {
                    log::error!("failed to listen for Ctrl+C: {err}");
                }
                log::info!("shutting down");
                scan_task.abort();
                bot_task.abort();
                let _ = scan_task.await;
                let _ = bot_task.await;
                return Ok(());
            }
            result = &mut scan_task => {
                log::error!("scanner exited unexpectedly: {result:?}");
                // scan_task already completed; only wait for the aborted bot.
                bot_task.abort();
                let _ = bot_task.await;
            }
            result = &mut bot_task => {
                log::error!("bot exited unexpectedly: {result:?}");
                // bot_task already completed; only wait for the aborted scanner.
                scan_task.abort();
                let _ = scan_task.await;
            }
        }

        log::warn!("restarting in {}s", RECONNECT_DELAY.as_secs());
        sleep(RECONNECT_DELAY).await;
    }
}

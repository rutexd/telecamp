use std::sync::Arc;

use anyhow::Result;
use tokio::time::sleep;

use crate::config::Config;
use crate::progress::NoopProgressSink;
use crate::tg::{Peer, TgClient};
use crate::AppState;

/// Summary of one scan round across all subscriptions.
#[derive(Debug, Default)]
pub struct ScanReport {
    pub found: usize,
    pub new: usize,
    pub uploaded: usize,
    pub skipped: usize,
}

/// What happened to a single release during processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseAction {
    /// Already seen; nothing to do.
    Seen,
    /// New but blacklisted; marked as seen, nothing posted.
    Blacklisted,
    /// New and posted to Telegram.
    Posted,
}

/// Runs the polling scanner forever: scans all tags and artists, then sleeps.
pub async fn run(config: Config, state: Arc<AppState>, client: TgClient) -> Result<()> {
    let peer = client.resolve_chat(&config).await?;

    if peer.is_none() {
        log::warn!("neither TELEGRAM_CHANNEL nor TELEGRAM_CHAT_ID is set; new releases will be marked seen but not sent anywhere");
    } else {
        if let Err(err) = client
            .send_message(peer.as_ref().unwrap(), startup_summary(&config))
            .await
        {
            log::error!("failed to post startup message: {err:#}");
        }
    }

    loop {
        match scan_round(&client, &state, &config, peer.as_ref()).await {
            Ok(report) => {
                log::info!(
                    "scan round done: {} found, {} new, {} uploaded, {} blacklisted",
                    report.found,
                    report.new,
                    report.uploaded,
                    report.skipped,
                );
            }
            Err(err) => log::error!("scan round failed: {err:#}"),
        }

        sleep(config.poll_interval).await;
    }
}

/// Text posted to the channel on startup describing the current configuration.
fn startup_summary(config: &Config) -> String {
    let tags = if config.bandcamp_tags.is_empty() {
        "—".to_string()
    } else {
        config.bandcamp_tags.join(", ")
    };
    let blacklist = if config.bandcamp_tag_blacklist.is_empty() {
        "—".to_string()
    } else {
        config.bandcamp_tag_blacklist.join(", ")
    };
    let artists = if config.bandcamp_artists.is_empty() {
        "—".to_string()
    } else {
        config.bandcamp_artists.join(", ")
    };

    format!(
        "Бот запущен.\nТеги: {tags}\nАртисты: {artists}\nБлэклист: {blacklist}\nОпрос каждые {} с",
        config.poll_interval.as_secs()
    )
}

/// One pass over every configured tag and artist, streaming new releases to Telegram.
pub async fn scan_round(
    client: &TgClient,
    state: &AppState,
    config: &Config,
    peer: Option<&Peer>,
) -> Result<ScanReport> {
    let mut report = ScanReport::default();

    for tag in &config.bandcamp_tags {
        log::info!("scanning tag '{tag}'");
        let releases = match state.bandcamp.discover(&[tag.clone()], "new").await {
            Ok(releases) => releases,
            Err(err) => {
                log::error!("failed to scan tag '{tag}': {err:#}");
                continue;
            }
        };
        for release in releases {
            report.found += 1;
            match process_release(client, state, config, &release.url, peer).await {
                Ok(ReleaseAction::Seen) => {}
                Ok(ReleaseAction::Blacklisted) => {
                    report.new += 1;
                    report.skipped += 1;
                }
                Ok(ReleaseAction::Posted) => {
                    report.new += 1;
                    report.uploaded += 1;
                }
                Err(err) => {
                    log::error!("failed to process {}: {err:#}", release.url);
                }
            }
        }
    }

    for artist in &config.bandcamp_artists {
        log::info!("scanning artist '{artist}'");
        let releases = match state.bandcamp.artist_releases(artist).await {
            Ok(releases) => releases,
            Err(err) => {
                log::error!("failed to scan artist '{artist}': {err:#}");
                continue;
            }
        };
        for release in releases {
            report.found += 1;
            match process_release(client, state, config, &release.url, peer).await {
                Ok(ReleaseAction::Seen) => {}
                Ok(ReleaseAction::Blacklisted) => {
                    report.new += 1;
                    report.skipped += 1;
                }
                Ok(ReleaseAction::Posted) => {
                    report.new += 1;
                    report.uploaded += 1;
                }
                Err(err) => {
                    log::error!("failed to process {}: {err:#}", release.url);
                }
            }
        }
    }

    Ok(report)
}

/// Processes a previously unseen release: skips blacklisted albums, otherwise
/// streams it into Telegram and marks it in the cache.
async fn process_release(
    client: &TgClient,
    state: &AppState,
    config: &Config,
    url: &str,
    peer: Option<&Peer>,
) -> Result<ReleaseAction> {
    if is_seen(state, url) {
        return Ok(ReleaseAction::Seen);
    }

    let album = state.bandcamp.get_album(url).await?;

    // Re-check inside the lock holder after the slow fetch.
    if is_seen(state, url) {
        return Ok(ReleaseAction::Seen);
    }

    // Blacklisted tag: skip the album but never look at it again.
    if album.has_any_tag(&config.bandcamp_tag_blacklist) {
        log::info!(
            "skipping '{} - {}' ({url}): blacklisted tag",
            album.artist,
            album.title
        );
        mark_seen(state, url);
        return Ok(ReleaseAction::Blacklisted);
    }

    log::info!("new release: {} - {} ({url})", album.artist, album.title);

    let Some(peer) = peer else {
        // No target peer configured, just mark as seen
        mark_seen(state, url);
        return Ok(ReleaseAction::Seen);
    };

    // Progress is intentionally disabled: per-event edits spam the chat and
    // trip Telegram's flood limits (FLOOD_WAIT on editMessage).
    let progress: Arc<dyn crate::progress::ProgressSink> = Arc::new(NoopProgressSink);

    // Producer/consumer: the downloader streams tracks into a bounded channel
    // while the uploader consumes and posts them one at a time.
    let stream = state.downloader.stream(&album, progress.clone()).await?;

    state
        .uploader
        .upload(client, peer, stream, progress.clone())
        .await?;

    // Mark as seen
    mark_seen(state, url);

    Ok(ReleaseAction::Posted)
}

fn is_seen(state: &AppState, url: &str) -> bool {
    state.cache.lock().unwrap().contains(url)
}

fn mark_seen(state: &AppState, url: &str) {
    let mut cache = state.cache.lock().unwrap();
    cache.insert(url.to_string());
    if let Err(err) = cache.save() {
        log::error!("failed to persist cache: {err:#}");
    }
}

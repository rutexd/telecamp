use std::sync::Arc;

use anyhow::Result;
use tokio::time::sleep;

use crate::config::Config;
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
    /// New but older than the configured age limit; marked as seen, nothing posted.
    TooOld,
    /// New and posted to Telegram.
    Posted,
}

/// Runs the polling scanner forever: scans all tags and artists, then sleeps.
pub async fn run(config: Config, state: Arc<AppState>, client: TgClient) -> Result<()> {
    let peer = client.resolve_chat(&config).await?;

    if let Some(chat) = &peer {
        if let Err(err) = client.send_message(chat, startup_summary(&config)).await {
            log::error!("failed to post startup message: {err:#}");
        }
        if let Err(err) = crate::menu::ensure_pinned_menu(&client, chat, &state).await {
            log::error!("failed to ensure pinned menu: {err:#}");
        }
    } else {
        log::warn!("neither TELEGRAM_CHANNEL nor TELEGRAM_CHAT_ID is set; new releases will be marked seen but not sent anywhere");
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

        sleep(config.poll_interval()).await;
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
    let artist_blacklist = if config.bandcamp_artist_blacklist.is_empty() {
        "—".to_string()
    } else {
        config.bandcamp_artist_blacklist.join(", ")
    };
    let max_age = if config.bandcamp_max_age_days == 0 {
        "без ограничения".to_string()
    } else {
        format!("{} дн.", config.bandcamp_max_age_days)
    };
    let max_tracks = if config.bandcamp_max_tracks == 0 {
        "без ограничения".to_string()
    } else {
        format!("до {} треков", config.bandcamp_max_tracks)
    };

    format!(
        "Бот запущен.\nТеги: {tags}\nАртисты: {artists}\nБлэклист тегов: {blacklist}\nБлэклист артистов: {artist_blacklist}\nВозраст релиза: до {max_age}\nЛимит альбома: {max_tracks}\nОпрос каждые {} с",
        config.poll_interval_seconds
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
            let posted = match process_release(client, state, config, &release.url, peer).await {
                Ok(ReleaseAction::Seen) => false,
                Ok(ReleaseAction::Blacklisted) => {
                    report.new += 1;
                    report.skipped += 1;
                    false
                }
                Ok(ReleaseAction::TooOld) => {
                    report.skipped += 1;
                    false
                }
                Ok(ReleaseAction::Posted) => {
                    report.new += 1;
                    report.uploaded += 1;
                    true
                }
                Err(err) => {
                    log::error!("failed to process {}: {err:#}", release.url);
                    false
                }
            };
            if posted {
                // Pace album posts: uploads share the MTProto connection that
                // carries updates, so back-to-back albums can make the server
                // flush the update queue with updatesTooLong (a gap the update
                // stream cannot always resolve).
                sleep(config.post_album_delay()).await;
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
            let posted = match process_release(client, state, config, &release.url, peer).await {
                Ok(ReleaseAction::Seen) => false,
                Ok(ReleaseAction::Blacklisted) => {
                    report.new += 1;
                    report.skipped += 1;
                    false
                }
                Ok(ReleaseAction::TooOld) => {
                    report.skipped += 1;
                    false
                }
                Ok(ReleaseAction::Posted) => {
                    report.new += 1;
                    report.uploaded += 1;
                    true
                }
                Err(err) => {
                    log::error!("failed to process {}: {err:#}", release.url);
                    false
                }
            };
            if posted {
                sleep(config.post_album_delay()).await;
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

    let album = match state.bandcamp.get_album(url).await {
        Ok(album) => album,
        Err(err) => {
            // A 404 means the release was removed; it will never come back, so
            // mark it seen instead of retrying it every round.
            if err.to_string().contains("status 404") {
                log::info!("marking {url} as seen: album no longer exists ({err:#})");
                mark_seen(state, url);
                return Ok(ReleaseAction::Seen);
            }
            return Err(err);
        }
    };

    // Re-check inside the lock holder after the slow fetch.
    if is_seen(state, url) {
        return Ok(ReleaseAction::Seen);
    }

    // Blacklisted artist: skip the album but never look at it again.
    if crate::bandcamp::is_artist_blacklisted(&album.url, &config.bandcamp_artist_blacklist) {
        log::info!(
            "skipping '{} - {}' ({url}): artist is blacklisted",
            album.artist,
            album.title
        );
        mark_seen(state, url);
        return Ok(ReleaseAction::Blacklisted);
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

    // Age limit: releases older than the cutoff never get younger, so mark
    // them seen and skip.
    if config.bandcamp_max_age_days > 0 {
        let too_old = crate::bandcamp::release_date_unix(&album.release_date).map_or(false, |released| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or(0);
            now.saturating_sub(released) > config.bandcamp_max_age_days * 86_400
        });

        if too_old {
            log::info!(
                "skipping '{} - {}' ({url}): released more than {} day(s) ago",
                album.artist,
                album.title,
                config.bandcamp_max_age_days
            );
            mark_seen(state, url);
            return Ok(ReleaseAction::TooOld);
        }
    }

    log::info!("new release: {} - {} ({url})", album.artist, album.title);

    let Some(peer) = peer else {
        // No target peer configured, just mark as seen
        mark_seen(state, url);
        return Ok(ReleaseAction::Seen);
    };

    crate::poster::post_album(client, peer, state, &album).await?;

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

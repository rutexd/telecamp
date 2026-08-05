use std::sync::{Arc, Mutex};

use anyhow::Result;
use grammers_client::update::Update;

use crate::bandcamp::BandcampClient;
use crate::cache::SeenCache;
use crate::downloader::Downloader;
use crate::progress::NoopProgressSink;
use crate::tg::{Message, Peer, TgClient};
use crate::uploader::Uploader;

/// Shared dependencies injected into both the update handlers and the scanner.
pub struct AppState {
    pub bandcamp: BandcampClient,
    pub cache: Mutex<SeenCache>,
    pub downloader: Box<dyn Downloader>,
    pub uploader: Box<dyn Uploader>,
    /// Tag slugs that mark an album as unwanted (skipped, marked seen).
    pub tag_blacklist: Vec<String>,
}

/// Runs the interactive Telegram bot (manual album lookups by URL).
pub async fn run(client: TgClient, state: Arc<AppState>) -> Result<()> {
    log::info!("starting telecamp bot");

    let mut updates = client.updates().await?;

    loop {
        match updates.next().await {
            Ok(update) => {
                if let Update::NewMessage(message) = update {
                    let client = client.clone();
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_message(client, state, Message::from_raw(message.into_inner()))
                            .await
                        {
                            log::error!("message handler error: {e:#}");
                        }
                    });
                }
            }
            Err(err) => {
                log::error!("update error: {err}");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

async fn handle_message(client: TgClient, state: Arc<AppState>, msg: Message) -> Result<()> {
    // Ignore messages the bot itself sent (its own progress/status posts would
    // otherwise be re-processed and cause feedback loops).
    if msg.outgoing() {
        return Ok(());
    }

    let text = msg.text();
    let text = text.trim();

    if text.is_empty() {
        return Ok(());
    }

    let Some(peer) = msg.peer() else {
        return Ok(());
    };

    if text.starts_with("/start") || text.starts_with("/help") {
        client.send_message(&peer, help_text()).await?;
        return Ok(());
    }

    if let Some(url) = extract_bandcamp_url(text) {
        return handle_album(client, state, &peer, url).await;
    }

    Ok(())
}

fn help_text() -> &'static str {
    "I watch Bandcamp tags and artists for new releases and stream them here.\n\n\
     You can also send me any Bandcamp album URL and I will process it manually."
}

/// Pulls the first Bandcamp URL (a token containing "bandcamp.com") out of a message.
fn extract_bandcamp_url(text: &str) -> Option<String> {
    let lower = text.to_ascii_lowercase();
    let idx = lower.find("bandcamp.com")?;

    let start = text[..idx]
        .rfind(char::is_whitespace)
        .map(|i| i + 1)
        .unwrap_or(0);
    let end = text[idx..]
        .find(char::is_whitespace)
        .map(|i| idx + i)
        .unwrap_or(text.len());

    let token = text[start..end].trim();
    if token.is_empty() {
        return None;
    }

    let url = if token.starts_with("http://") || token.starts_with("https://") {
        token.to_string()
    } else {
        format!("https://{token}")
    };

    Some(url)
}

async fn handle_album(
    client: TgClient,
    state: Arc<AppState>,
    peer: &Peer,
    url: String,
) -> Result<()> {
    if is_seen(&state, &url) {
        client
            .send_message(peer, "This album was already processed.")
            .await?;
        return Ok(());
    }

    let album = match state.bandcamp.get_album(&url).await {
        Ok(album) => album,
        Err(err) => {
            log::error!("failed to fetch bandcamp album {url}: {err:#}");
            client
                .send_message(peer, format!("Failed to fetch the album: {err}"))
                .await?;
            return Ok(());
        }
    };

    if album.has_any_tag(&state.tag_blacklist) {
        client
            .send_message(peer, "Skipped (blacklisted tag).")
            .await?;
        mark_seen(&state, &url);
        return Ok(());
    }

    client
        .send_message(
            peer,
            format!(
                "Found: {} - {}\n{} track(s), tags: {}",
                album.artist,
                album.title,
                album.tracks.len(),
                album.tags.join(", "),
            ),
        )
        .await?;

    // Use noop progress for interactive mode (no progress message)
    let progress: Arc<dyn crate::progress::ProgressSink> = Arc::new(NoopProgressSink);

    match state.downloader.stream(&album, progress.clone()).await {
        Ok(stream) => {
            match state
                .uploader
                .upload(&client, peer, stream, progress.clone())
                .await
            {
                Ok(result) => {
                    mark_seen(&state, &url);
                    client
                        .send_message(peer, format!("Done: uploaded {} file(s).", result.sent_files))
                        .await?;
                }
                Err(err) => {
                    log::error!("failed to upload album: {err:#}");
                    client
                        .send_message(peer, format!("Failed to upload the album: {err}"))
                        .await?;
                }
            }
        }
        Err(err) => {
            log::error!("failed to download album: {err:#}");
            client
                .send_message(peer, format!("Failed to download the album: {err}"))
                .await?;
        }
    }

    Ok(())
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

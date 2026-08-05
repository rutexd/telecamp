use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use tokio::task::spawn_blocking;

use crate::bandcamp::Album;
use crate::progress::{ProgressEvent, ProgressSink, Stage};

/// An in-memory artifact of a downloaded album, ready to be uploaded to Telegram.
///
/// Nothing touches the disk: track bytes are piped straight from Bandcamp's
/// stream servers into Telegram's upload.
#[derive(Debug)]
pub struct DownloadedAlbum {
    pub album: Album,
    pub cover: Option<Vec<u8>>,
    pub tracks: Vec<DownloadedTrack>,
}

/// A single track with its in-memory audio payload.
#[derive(Debug)]
pub struct DownloadedTrack {
    pub num: u32,
    pub title: String,
    pub data: Vec<u8>,
}

/// Streams a Bandcamp album into memory.
#[async_trait]
pub trait Downloader: Send + Sync {
    async fn download(&self, album: &Album, progress: &dyn ProgressSink) -> Result<DownloadedAlbum>;
}

/// Carcase-only implementation: wires the pipeline together, does no real download.
#[derive(Debug, Default)]
pub struct NoopDownloader;

#[async_trait]
impl Downloader for NoopDownloader {
    async fn download(&self, album: &Album, _progress: &dyn ProgressSink) -> Result<DownloadedAlbum> {
        log::info!(
            "[noop] would stream {} track(s) of '{} - {}' straight to telegram",
            album.tracks.len(),
            album.artist,
            album.title
        );

        Ok(DownloadedAlbum {
            album: album.clone(),
            cover: None,
            tracks: Vec::new(),
        })
    }
}

/// Real downloader: pipes each track's mp3-128 stream and the cover art into memory.
#[derive(Debug)]
pub struct HttpDownloader {
    http: impersonate_rs::Client,
}

impl Default for HttpDownloader {
    fn default() -> Self {
        let http = impersonate_rs::Client::builder()
            .impersonate(impersonate_rs::Browser::Chrome142)
            .timeout(Duration::from_secs(180))
            .build();

        Self { http }
    }
}

impl HttpDownloader {
    pub fn new() -> Self {
        Self::default()
    }

    async fn get_bytes(&self, url: &str) -> Result<Vec<u8>> {
        let url = url.to_string();
        let http = self.http.clone();

        spawn_blocking(move || {
            let response = http
                .get(&url)
                .send()
                .map_err(|err| anyhow!("bandcamp stream request failed: {err}"))?;

            if response.status() != 200 {
                bail!(
                    "bandcamp stream returned status {} for {url}",
                    response.status()
                );
            }

            Ok(response.bytes().to_vec())
        })
        .await
        .context("stream fetch task panicked")?
    }

    async fn get_bytes_with_progress(
        &self,
        url: &str,
        track_num: usize,
        total_tracks: usize,
        progress: &dyn ProgressSink,
    ) -> Result<Vec<u8>> {
        let url = url.to_string();
        let http = self.http.clone();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

        let task = spawn_blocking(move || {
            // Probe the expected size with a HEAD request so the progress bar
            // can show a meaningful percentage while the body streams in.
            let total_bytes = http
                .request("HEAD", &url)
                .send()
                .ok()
                .and_then(|response| {
                    header_value(response.headers(), "content-length").map(str::to_owned)
                })
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);

            let mut buffer = Vec::new();
            let mut bytes_read = 0u64;

            let response = http
                .get(&url)
                .send_with_callback(|chunk| {
                    buffer.extend_from_slice(chunk);
                    bytes_read += chunk.len() as u64;

                    let _ = tx.send(ProgressEvent {
                        stage: Stage::Download,
                        track_num,
                        total_tracks,
                        bytes: bytes_read,
                        total_bytes,
                    });

                    Ok(())
                })
                .map_err(|err| anyhow!("bandcamp stream request failed: {err}"))?;

            if response.status() != 200 {
                bail!(
                    "bandcamp stream returned status {} for {url}",
                    response.status()
                );
            }

            Ok(buffer)
        });

        // Forward progress events on this task while the blocking request streams.
        while let Some(event) = rx.recv().await {
            progress.report(event);
        }

        task.await.context("stream fetch task panicked")?
    }
}

/// Case-insensitive header lookup.
fn header_value<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

#[async_trait]
impl Downloader for HttpDownloader {
    async fn download(&self, album: &Album, progress: &dyn ProgressSink) -> Result<DownloadedAlbum> {
        let total_tracks = album.tracks.len();

        let cover = match album.art_id {
            Some(art_id) => {
                let url = format!("https://f4.bcbits.com/img/a{art_id}_10.jpg");
                match self.get_bytes(&url).await {
                    Ok(bytes) => Some(bytes),
                    Err(err) => {
                        log::warn!("failed to fetch cover art {url}: {err:#}");
                        None
                    }
                }
            }
            None => None,
        };

        let mut tracks = Vec::new();
        for (idx, track) in album.tracks.iter().enumerate() {
            let track_num = idx + 1;
            
            let Some(url) = &track.mp3_128 else {
                log::warn!("track {} has no stream url, skipping", track.num);
                continue;
            };

            match self.get_bytes_with_progress(url, track_num, total_tracks, progress).await {
                Ok(data) => {
                    log::info!(
                        "streamed track {} of '{} - {}' ({} bytes)",
                        track.num,
                        album.artist,
                        album.title,
                        data.len(),
                    );
                    tracks.push(DownloadedTrack {
                        num: track.num,
                        title: track.title.clone(),
                        data,
                    });
                }
                Err(err) => log::error!("failed to fetch track {}: {err:#}", track.num),
            }
        }

        Ok(DownloadedAlbum {
            album: album.clone(),
            cover,
            tracks,
        })
    }
}

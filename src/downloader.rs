use std::collections::HashMap;
use std::io::{self, Read};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use async_trait::async_trait;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::spawn_blocking;

use crate::bandcamp::Album;
use crate::progress::{ProgressEvent, ProgressSink, Stage};

/// How many chunks may be buffered in flight between the blocking HTTP
/// download and the async Telegram upload. Bounds peak memory per track to
/// `CHUNK_QUEUE` chunks regardless of how long the track is.
const CHUNK_QUEUE: usize = 4;

/// Capacity of the album channel: how many track streams the producer may hand
/// off ahead of the consumer. Kept at 1 so at most one track stream is pending
/// while the consumer reads the current one.
const TRACK_QUEUE: usize = 1;

/// A track whose audio is streamed straight from Bandcamp into the upload:
/// chunks arrive over a bounded channel and are served as an `AsyncRead`, so
/// the whole track is never materialized in memory (long mixes stay cheap).
#[derive(Debug)]
pub struct TrackReader {
    rx: mpsc::Receiver<Vec<u8>>,
    current: Option<std::io::Cursor<Vec<u8>>>,
}

impl TrackReader {
    fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self { rx, current: None }
    }
}

impl AsyncRead for TrackReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if let Some(mut cursor) = self.current.take() {
                let n = cursor.read(buf.initialize_unfilled())?;
                if n > 0 {
                    buf.advance(n);
                    self.current = Some(cursor);
                    return Poll::Ready(Ok(()));
                }
                // Chunk fully consumed; pull the next one.
            }

            match self.rx.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())), // EOF
                Poll::Ready(Some(chunk)) => self.current = Some(std::io::Cursor::new(chunk)),
            }
        }
    }
}

/// A single streamed track handed from the download producer to the upload
/// consumer. The audio is read chunk-by-chunk from [`DownloadedTrack::reader`].
#[derive(Debug)]
pub struct DownloadedTrack {
    pub num: u32,
    pub title: String,
    /// Expected total size in bytes (from the stream's content-length).
    pub size: u64,
    pub reader: TrackReader,
}

/// A streaming album download: metadata and cover arrive upfront, the tracks
/// themselves are pulled one at a time from a channel.
#[derive(Debug)]
pub struct AlbumStream {
    pub album: Album,
    pub cover: Option<Vec<u8>>,
    pub tracks: mpsc::Receiver<DownloadedTrack>,
}

/// Streams a Bandcamp album track-by-track (producer side of the pipeline).
#[async_trait]
pub trait Downloader: Send + Sync {
    /// Starts streaming an album: fetches the cover, then spawns a producer
    /// that feeds each track's audio chunks into a bounded channel. Returns
    /// immediately; the consumer drains [`AlbumStream::tracks`] at its own pace.
    async fn stream(&self, album: &Album, progress: Arc<dyn ProgressSink>) -> Result<AlbumStream>;
}

/// Carcase-only implementation: wires the pipeline together, does no real download.
#[derive(Debug, Default)]
pub struct NoopDownloader;

#[async_trait]
impl Downloader for NoopDownloader {
    async fn stream(&self, album: &Album, _progress: Arc<dyn ProgressSink>) -> Result<AlbumStream> {
        log::info!(
            "[noop] would stream {} track(s) of '{} - {}' straight to telegram",
            album.tracks.len(),
            album.artist,
            album.title
        );

        // Closed channel: the consumer sees it as "no tracks".
        let (_tx, rx) = mpsc::channel::<DownloadedTrack>(TRACK_QUEUE);

        Ok(AlbumStream {
            album: album.clone(),
            cover: None,
            tracks: rx,
        })
    }
}

/// Real downloader: streams each track's mp3-128 data chunk-by-chunk into a
/// bounded channel (producer), which the uploader drains straight into
/// Telegram (consumer). Only a few chunks are ever in memory at a time.
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

    /// Probes the stream's expected size with a HEAD request. The upload needs
    /// the total size upfront, so a track without a content-length is skipped.
    async fn probe_size(http: &impersonate_rs::Client, url: &str) -> Option<u64> {
        let http = http.clone();
        let url = url.to_string();

        spawn_blocking(move || {
            http.request("HEAD", &url)
                .send()
                .ok()
                .and_then(|response| {
                    header_value(response.headers(), "content-length").map(str::to_owned)
                })
                .and_then(|value| value.parse::<u64>().ok())
        })
        .await
        .ok()
        .flatten()
    }

    /// Downloads a single track's stream on the blocking pool, pushing each
    /// chunk into the bounded channel and reporting progress. Blocks on the
    /// channel when the consumer is slower, applying backpressure without ever
    /// buffering the whole track. Returns when the download finishes (or the
    /// consumer goes away).
    fn stream_track(
        http: &impersonate_rs::Client,
        url: &str,
        size: u64,
        track_num: usize,
        total_tracks: usize,
        progress: Arc<dyn ProgressSink>,
        chunk_tx: mpsc::Sender<Vec<u8>>,
    ) -> tokio::task::JoinHandle<()> {
        let http = http.clone();
        let url = url.to_string();

        spawn_blocking(move || {
            let mut bytes_read = 0u64;
            let aborted = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

            let callback_aborted = aborted.clone();
            let result = http
                .get(&url)
                .send_with_callback(|chunk| {
                    let chunk = chunk.to_vec();

                    bytes_read += chunk.len() as u64;
                    progress.report(ProgressEvent {
                        stage: Stage::Download,
                        track_num,
                        total_tracks,
                        bytes: bytes_read,
                        total_bytes: size,
                    });

                    // Backpressure: block until the consumer drains a slot.
                    // An error means the consumer dropped the stream; abort
                    // the curl transfer so we stop pulling bytes.
                    if chunk_tx.blocking_send(chunk).is_err() {
                        callback_aborted.store(true, std::sync::atomic::Ordering::Relaxed);
                        return Err(impersonate_rs::Error::Impersonate(
                            "upload consumer went away, aborting track stream".into(),
                        ));
                    }
                    Ok(())
                });

            match result {
                // The consumer went away (e.g. an upload error): the abort is
                // expected cleanup, not a download failure.
                Err(_) if aborted.load(std::sync::atomic::Ordering::Relaxed) => {
                    log::debug!("track stream aborted: upload consumer went away");
                }
                Err(err) => log::error!("failed to stream track: {err:#}"),
                Ok(_) => {}
            }
            // Dropping chunk_tx signals EOF to the upload reader.
        })
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
    async fn stream(
        &self,
        album: &Album,
        progress: Arc<dyn ProgressSink>,
    ) -> Result<AlbumStream> {
        // The cover is needed for the summary photo and the per-track
        // thumbnails before any audio uploads, so fetch it up front.
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

        let (tx, rx) = mpsc::channel::<DownloadedTrack>(TRACK_QUEUE);

        let http = self.http.clone();
        let artist = album.artist.clone();
        let title = album.title.clone();
        let tracks = album.tracks.clone();

        // Producer: probes each track's size, starts its stream download, and
        // hands the reader over the channel. The download runs on the blocking
        // pool while the consumer uploads on the async runtime, so the chunks
        // flow Bandcamp -> Telegram with minimal memory.
        tokio::spawn(async move {
            let total_tracks = tracks.len();

            for (idx, track) in tracks.into_iter().enumerate() {
                let track_num = idx + 1;

                let Some(url) = track.mp3_128 else {
                    log::warn!("track {} has no stream url, skipping", track.num);
                    continue;
                };

                let Some(size) = HttpDownloader::probe_size(&http, &url).await else {
                    log::error!(
                        "track {} of '{} - {}' has no content-length, skipping",
                        track.num,
                        artist,
                        title
                    );
                    continue;
                };

                let (chunk_tx, chunk_rx) = mpsc::channel::<Vec<u8>>(CHUNK_QUEUE);

                HttpDownloader::stream_track(
                    &http,
                    &url,
                    size,
                    track_num,
                    total_tracks,
                    progress.clone(),
                    chunk_tx,
                );

                log::info!(
                    "streaming track {} of '{} - {}' ({} bytes)",
                    track.num,
                    artist,
                    title,
                    size,
                );

                if tx
                    .send(DownloadedTrack {
                        num: track.num,
                        title: track.title.clone(),
                        size,
                        reader: TrackReader::new(chunk_rx),
                    })
                    .await
                    .is_err()
                {
                    // Consumer went away; stop producing.
                    break;
                }
            }
        });

        Ok(AlbumStream {
            album: album.clone(),
            cover,
            tracks: rx,
        })
    }
}

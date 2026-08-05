use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use anyhow::{Context as AnyhowContext, Result};
use async_trait::async_trait;
use tokio::io::{AsyncRead, ReadBuf};

use crate::bandcamp::Album;
use crate::downloader::AlbumStream;
use crate::progress::{ProgressEvent, ProgressSink, Stage};
use crate::tg::{InputMedia, Peer, TgClient};

/// Telegram media groups accept at most 10 items.
const MEDIA_GROUP_MAX: usize = 10;

/// Result of uploading a downloaded album to Telegram.
#[derive(Debug, Default)]
pub struct UploadResult {
    pub sent_files: usize,
}

/// Uploads an album streamed from the downloader (consumer side of the pipeline).
#[async_trait]
pub trait Uploader: Send + Sync {
    async fn upload(
        &self,
        client: &TgClient,
        peer: &Peer,
        stream: AlbumStream,
        progress: Arc<dyn ProgressSink>,
    ) -> Result<UploadResult>;
}

/// Carcase-only implementation: wires the pipeline together, does no real upload.
#[derive(Debug, Default)]
pub struct NoopUploader;

#[async_trait]
impl Uploader for NoopUploader {
    async fn upload(
        &self,
        _client: &TgClient,
        peer: &Peer,
        stream: AlbumStream,
        _progress: Arc<dyn ProgressSink>,
    ) -> Result<UploadResult> {
        log::info!(
            "[noop] would upload {} track(s) of '{} - {}' to chat {}",
            stream.album.tracks.len(),
            stream.album.artist,
            stream.album.title,
            peer.id()
        );

        // Drain the channel so the producer does not block forever.
        let mut tracks = stream.tracks;
        while tracks.recv().await.is_some() {}

        Ok(UploadResult { sent_files: 0 })
    }
}

/// Real uploader: posts the cover photo with the album summary as its caption,
/// then every track as audio media groups (one Telegram message per group of
/// up to 10 tracks). The cover art is also downscaled and attached as a
/// thumbnail to each track. Each track's audio is read chunk-by-chunk from the
/// producer's channel and fed straight into Telegram, so only a few chunks are
/// in memory regardless of track length.
#[derive(Debug, Default)]
pub struct TelegramUploader;

#[async_trait]
impl Uploader for TelegramUploader {
    async fn upload(
        &self,
        client: &TgClient,
        peer: &Peer,
        stream: AlbumStream,
        progress: Arc<dyn ProgressSink>,
    ) -> Result<UploadResult> {
        let album = &stream.album;
        let summary = album_summary(album);
        let mut sent_files = 0;

        // Send cover photo with caption
        match &stream.cover {
            Some(cover) => {
                client.send_photo(peer, cover.clone(), summary).await?;
                sent_files += 1;
                log::info!(
                    "posted cover of '{} - {}' to chat {}",
                    album.artist,
                    album.title,
                    peer.id()
                );
            }
            None => {
                client.send_message(peer, summary).await?;
            }
        }

        // Upload thumbnail once
        let thumbnail = match stream.cover.as_deref().and_then(cover_thumbnail) {
            Some(thumb_bytes) => {
                let uploaded = client
                    .inner()
                    .upload_stream(
                        &mut std::io::Cursor::new(thumb_bytes.clone()),
                        thumb_bytes.len(),
                        "thumb.jpg".to_string(),
                    )
                    .await?;
                Some(uploaded)
            }
            None => {
                if stream.cover.is_some() {
                    log::warn!(
                        "could not downscale cover for '{} - {}', posting without thumbnails",
                        album.artist,
                        album.title
                    );
                }
                None
            }
        };

        // Process tracks in groups of 10 as they arrive from the producer.
        let mut tracks = stream.tracks;
        let mut media: Vec<InputMedia> = Vec::new();
        let mut track_index = 0usize;
        let total_tracks = album.tracks.len();

        while let Some(track) = tracks.recv().await {
            track_index += 1;
            let track_size = track.size;

            // A counting reader drives an atomic counter; a small reporter
            // task forwards progress events over a channel, and this task
            // forwards them while the upload runs (raw pointers are not Send).
            let bytes_uploaded = Arc::new(AtomicU64::new(0));
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();

            let reporter = {
                let counter = bytes_uploaded.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        let bytes = counter.load(Ordering::Relaxed);

                        if tx
                            .send(ProgressEvent {
                                stage: Stage::Upload,
                                track_num: track_index,
                                total_tracks,
                                bytes,
                                total_bytes: track_size,
                            })
                            .is_err()
                        {
                            break;
                        }

                        if bytes >= track_size {
                            break;
                        }
                    }
                })
            };

            let mut counting_reader = CountingReader {
                inner: track.reader,
                counter: bytes_uploaded.clone(),
            };

            let filename = format!("{:02}. {}.mp3", track.num, track.title);
            let mut upload_fut = Box::pin(client.inner().upload_stream(
                &mut counting_reader,
                track_size as usize,
                filename.clone(),
            ));

            let uploaded = loop {
                tokio::select! {
                    event = rx.recv() => match event {
                        Some(event) => progress.report(event),
                        None => break upload_fut.await,
                    },
                    result = &mut upload_fut => break result,
                }
            };
            let uploaded = uploaded.context("failed to upload track")?;

            reporter.abort();

            // Create InputMedia with audio attributes
            let input = InputMedia::audio(
                uploaded,
                format!("{}. {}", track.num, track.title),
                album.artist.clone(),
                thumbnail.clone(),
            );

            media.push(input);
            sent_files += 1;

            log::info!(
                "uploaded track {} of '{} - {}' ({} bytes)",
                track.num,
                album.artist,
                album.title,
                track_size,
            );

            if media.len() == MEDIA_GROUP_MAX {
                client.send_album(peer, std::mem::take(&mut media)).await?;
                log::info!(
                    "posted {} track(s) of '{} - {}' to chat {}",
                    MEDIA_GROUP_MAX,
                    album.artist,
                    album.title,
                    peer.id()
                );

                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        }

        if !media.is_empty() {
            let count = media.len();
            client.send_album(peer, media).await?;
            log::info!(
                "posted {} track(s) of '{} - {}' to chat {}",
                count,
                album.artist,
                album.title,
                peer.id()
            );
        }

        Ok(UploadResult { sent_files })
    }
}

/// A reader that counts bytes as they're read (for upload progress).
struct CountingReader<R> {
    inner: R,
    counter: Arc<AtomicU64>,
}

impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                self.counter.fetch_add(read as u64, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

/// Formats the album header message posted above the tracks.
fn album_summary(album: &Album) -> String {
    let mut text = format!("{} - {}\n", album.artist, album.title);

    if !album.release_date.is_empty() {
        text.push_str(&format!("Released {}\n", album.release_date));
    }
    text.push_str(&format!("{} track(s)\n", album.tracks.len()));
    if !album.tags.is_empty() {
        let tags: Vec<String> = album.tags.iter().map(|tag| hashtag(tag)).collect();
        text.push_str(&format!("Tags: {}\n", tags.join(" ")));
    }
    if !album.url.is_empty() {
        text.push_str(&album.url);
    }

    text
}

/// Converts a Bandcamp tag slug into a clickable Telegram hashtag.
/// Hyphens are illegal in hashtags, so they become underscores.
fn hashtag(tag: &str) -> String {
    format!("#{}", tag.replace('-', "_"))
}

/// Downscales the album cover into a Telegram-compatible thumbnail
/// (JPEG, at most 320x320). Returns `None` if the bytes cannot be decoded.
fn cover_thumbnail(cover: &[u8]) -> Option<Vec<u8>> {
    let img = image::load_from_memory(cover).ok()?;
    let img = if img.width() > 320 || img.height() > 320 {
        img.resize(320, 320, image::imageops::FilterType::Lanczos3)
    } else {
        img
    };

    let mut out = Vec::new();
    img.to_rgb8()
        .write_to(&mut std::io::Cursor::new(&mut out), image::ImageFormat::Jpeg)
        .ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashtag_replaces_hyphens() {
        assert_eq!(hashtag("electronic"), "#electronic");
        assert_eq!(hashtag("drum-and-bass"), "#drum_and_bass");
        assert_eq!(hashtag("post-rock"), "#post_rock");
        assert_eq!(hashtag("90s"), "#90s");
        assert_eq!(hashtag("rnb"), "#rnb");
    }

    #[test]
    fn summary_uses_hashtags() {
        let mut album = Album::default();
        album.artist = "Artist".into();
        album.title = "Album".into();
        album.tracks.push(crate::bandcamp::Track::default());
        album.tags = vec!["electronic".into(), "post-rock".into()];
        album.url = "https://x.bandcamp.com/album/y".into();

        let text = album_summary(&album);
        assert!(text.contains("#electronic"));
        assert!(text.contains("#post_rock"));
        assert!(!text.contains("#post-rock"));
        assert!(text.contains("https://x.bandcamp.com/album/y"));
    }
}

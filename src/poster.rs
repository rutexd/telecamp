use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use grammers_client::message::{Button, ReplyMarkup};

use crate::bandcamp::Album;
use crate::progress::NoopProgressSink;
use crate::store;
use crate::tg::{Peer, TgClient};
use crate::AppState;

/// Marks an album as in-progress in `state.uploading` for the duration of a
/// post, so like/dislike presses wait until every message exists. Removal is
/// guaranteed by `Drop` even when the post fails.
struct UploadTracker<'a> {
    set: &'a Mutex<HashSet<String>>,
    token: String,
}

impl Drop for UploadTracker<'_> {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.token);
    }
}

/// How long to wait after acquiring the posting lock before starting a post,
/// so updates accumulated on the (now idle) connection get consumed before it
/// is saturated with upload traffic again.
const POST_DRAIN_DELAY: std::time::Duration = std::time::Duration::from_millis(500);

/// Streams an album into Telegram with like/dislike buttons on the preview
/// message, and records it in the post registry (so a later dislike can delete
/// every message of the album, and likes/dislikes can be tied to its tags).
///
/// Albums with more tracks than `state.max_tracks` are not streamed at all:
/// only the cover is posted (with the buttons) plus a short note linking to
/// the album, so the track limit does not change how likes, dislikes and the
/// favorites menu work.
///
/// Returns the album's callback token.
pub async fn post_album(
    client: &TgClient,
    peer: &Peer,
    state: &AppState,
    album: &Album,
) -> Result<String> {
    // Only one album may be posted at a time: uploads and updates share the
    // same MTProto connection, and overlapping media bursts push the server
    // into flushing the update queue with `updatesTooLong` (which then fails
    // to resolve as a difference and kills the update stream).
    let _posting_guard = state.posting.lock().await;

    // Give the update reader a beat to drain anything queued on the now-idle
    // connection before it is saturated with upload traffic again.
    tokio::time::sleep(POST_DRAIN_DELAY).await;

    let token = store::token(&album.url);
    let _tracker = UploadTracker {
        set: &state.uploading,
        token: token.clone(),
    };

    if should_skip_upload(album.tracks.len(), state.max_tracks) {
        post_too_big_album(client, peer, state, album, &token).await
    } else {
        post_full_album(client, peer, state, album, &token).await
    }
}

/// True when an album has more tracks than the configured limit (no limit when
/// `max_tracks` is 0). The limit is strict: exactly `max_tracks` tracks still
/// uploads normally.
fn should_skip_upload(track_count: usize, max_tracks: usize) -> bool {
    max_tracks > 0 && track_count > max_tracks
}

/// Like/dislike inline keyboard for the album preview.
fn like_dislike_markup(token: &str) -> ReplyMarkup {
    ReplyMarkup::from_buttons_row(&[
        Button::data("👍", format!("like:{token}")),
        Button::data("👎", format!("dislike:{token}")),
    ])
}

/// Uploads an album's tracks (full pipeline: cover + every track media group).
async fn post_full_album(
    client: &TgClient,
    peer: &Peer,
    state: &AppState,
    album: &Album,
    token: &str,
) -> Result<String> {
    let markup = like_dislike_markup(token);

    // Progress is intentionally disabled: per-event edits spam the chat and
    // trip Telegram's flood limits (FLOOD_WAIT on editMessage).
    let progress: Arc<dyn crate::progress::ProgressSink> = Arc::new(NoopProgressSink);

    let stream = state.downloader.stream(album, progress.clone()).await?;
    let result = state
        .uploader
        .upload(client, peer, stream, progress.clone(), Some(markup))
        .await?;

    let record = store::PostRecord {
        token: token.to_string(),
        url: album.url.clone(),
        artist: album.artist.clone(),
        title: album.title.clone(),
        tags: album.tags.clone(),
        preview_msg_id: result.preview_message_id.unwrap_or(0),
        album_msg_ids: result.album_message_ids,
        posted_at_unix: store::now_unix(),
    };

    let mut posts = state.posts.lock().unwrap();
    posts.insert(record);
    if let Err(err) = posts.save() {
        log::error!("failed to persist posts registry: {err:#}");
    }

    log::info!(
        "recorded post {token} for '{} - {}'",
        album.artist,
        album.title
    );

    Ok(token.to_string())
}

/// Posts an album that is too big to upload: the cover only (with the usual
/// like/dislike buttons) and a trailing note pointing at the album, so likes,
/// dislikes and the favorites menu keep working exactly like for a full album.
async fn post_too_big_album(
    client: &TgClient,
    peer: &Peer,
    state: &AppState,
    album: &Album,
    token: &str,
) -> Result<String> {
    let markup = like_dislike_markup(token);

    // No track audio is streamed from Bandcamp; only the cover is needed.
    let cover = state.downloader.fetch_cover(album).await?;

    let mut caption =
        format!("⚠️ Альбом слишком большой ({} треков) — треки не залиты\n\n", album.tracks.len());
    caption.push_str(&crate::uploader::album_summary(album));

    let preview_msg_id = match cover {
        Some(cover) => client.send_photo(peer, cover, caption, Some(markup)).await?.id(),
        None => client.send_message_markup(peer, caption, Some(markup)).await?.id(),
    };

    let note_id = client.send_message(peer, too_big_note(album)).await?.id();

    let record = store::PostRecord {
        token: token.to_string(),
        url: album.url.clone(),
        artist: album.artist.clone(),
        title: album.title.clone(),
        tags: album.tags.clone(),
        preview_msg_id,
        album_msg_ids: vec![note_id],
        posted_at_unix: store::now_unix(),
    };

    let mut posts = state.posts.lock().unwrap();
    posts.insert(record);
    if let Err(err) = posts.save() {
        log::error!("failed to persist posts registry: {err:#}");
    }

    log::info!(
        "recorded too-big post {token} for '{} - {}' ({} tracks)",
        album.artist,
        album.title,
        album.tracks.len()
    );

    Ok(token.to_string())
}

/// Trailing message posted after the cover of a too-big album.
fn too_big_note(album: &Album) -> String {
    format!(
        "Альбом слишком большой ({} треков) — послушать можно на Bandcamp: {}",
        album.tracks.len(),
        album.url
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_big_limit_is_strictly_greater() {
        assert!(should_skip_upload(21, 20));
        assert!(!should_skip_upload(20, 20));
        assert!(!should_skip_upload(3, 20));
        // 0 disables the limit.
        assert!(!should_skip_upload(1000, 0));
    }

    #[test]
    fn too_big_note_mentions_tracks_and_link() {
        let mut album = Album::default();
        album.artist = "Artist".into();
        album.title = "Album".into();
        album.url = "https://x.bandcamp.com/album/y".into();
        album.tracks = vec![crate::bandcamp::Track::default(); 21];

        let note = too_big_note(&album);
        assert!(note.contains("21"));
        assert!(note.contains("https://x.bandcamp.com/album/y"));
    }
}

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use grammers_client::update::{CallbackQuery, Update};

use crate::bandcamp::BandcampClient;
use crate::cache::SeenCache;
use crate::downloader::Downloader;
use crate::store::{LikedEntry, LikedStore, PostRecord, PostRegistry, VoteEntry, VoteLog};
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
    /// Liked albums shown in the pinned menu.
    pub liked: Mutex<LikedStore>,
    /// Posted-album records keyed by callback token (for dislike/cleanup).
    pub posts: Mutex<PostRegistry>,
    /// Append-only like/dislike vote history for tag analysis.
    pub votes: Mutex<VoteLog>,
    /// Message id of the pinned favorites menu, if known.
    pub menu_id: Mutex<Option<i32>>,
    /// Current page of the pinned favorites menu (0-based).
    pub menu_page: Mutex<usize>,
    /// Albums with more tracks than this are posted cover-only (0 = no limit).
    pub max_tracks: usize,
    /// Callback tokens of albums currently being uploaded. Like/dislike on
    /// their buttons must wait, because the post record is only saved after
    /// the whole upload finishes (a partial record cannot be deleted yet).
    pub uploading: Mutex<HashSet<String>>,
    /// Serializes album posts (scanner + manual URL posts). Uploads share the
    /// MTProto connection that also carries updates, so only one heavy upload
    /// burst may run at a time to avoid flooding the server's update queue.
    pub posting: tokio::sync::Mutex<()>,
}

/// Runs the interactive Telegram bot (manual album lookups by URL, and
/// like/dislike/forward button presses).
pub async fn run(client: TgClient, state: Arc<AppState>) -> Result<()> {
    log::info!("starting telecamp bot");

    let mut updates = client.updates().await?;
    let mut consecutive_errors = 0u32;

    loop {
        match updates.next().await {
            Ok(update) => {
                consecutive_errors = 0;
                match update {
                    Update::NewMessage(message) => {
                        let client = client.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_message(
                                client,
                                state,
                                Message::from_raw(message.into_inner()),
                            )
                            .await
                            {
                                log::error!("message handler error: {e:#}");
                            }
                        });
                    }
                    Update::CallbackQuery(query) => {
                        let client = client.clone();
                        let state = state.clone();
                        tokio::spawn(async move {
                            if let Err(e) = handle_callback(client, state, query).await {
                                log::error!("callback handler error: {e:#}");
                            }
                        });
                    }
                    _ => {}
                }
            }
            Err(err) => {
                consecutive_errors += 1;
                log::error!("update error (consecutive: {consecutive_errors}): {err}");
                // grammers 0.10's UpdateStream has no in-process recovery after a
                // failed account-level updates.getDifference: the message box stays
                // in "getting difference" and re-issues the same failing request on
                // every call, so next() would error forever. Exit so the supervisor
                // reconnects, which resyncs the update state via updates.getState.
                let fatal = err.is("PERSISTENT_TIMESTAMP_*") || consecutive_errors >= 5;
                if fatal {
                    return Err(anyhow!(
                        "unrecoverable update error after {consecutive_errors} consecutive failure(s); restarting: {err}"
                    ));
                }
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

    if let Err(err) = crate::poster::post_album(&client, peer, &state, &album).await {
        log::error!("failed to upload album: {err:#}");
        client
            .send_message(peer, format!("Failed to upload the album: {err}"))
            .await?;
        return Ok(());
    }

    mark_seen(&state, &url);
    client.send_message(peer, "Done: uploaded.").await?;

    Ok(())
}

/// Handles an inline button press: like / dislike on an album message, or a
/// forward button in the pinned menu. Every query is answered (otherwise the
/// client shows a spinner until timeout).
async fn handle_callback(
    client: TgClient,
    state: Arc<AppState>,
    query: CallbackQuery,
) -> Result<()> {
    let data = String::from_utf8_lossy(query.data()).to_string();
    let Some((action, token)) = parse_callback_data(&data) else {
        query.answer().text("Неизвестная кнопка").send().await?;
        return Ok(());
    };

    let Some(peer) = query.peer().cloned() else {
        query.answer().text("Не удалось определить чат").send().await?;
        return Ok(());
    };

    match action {
        "like" => handle_like(&client, &state, &query, &peer, token).await,
        "dislike" => handle_dislike(&client, &state, &query, &peer, token).await,
        "fwd" => handle_forward(&client, &state, &query, &peer, token).await,
        "del" => handle_delete_favorite(&client, &state, &query, &peer, token).await,
        "nav" => handle_nav(&client, &state, &query, &peer, token).await,
        _ => {
            query.answer().text("Неизвестная кнопка").send().await?;
            Ok(())
        }
    }
}

/// Splits a callback payload into `(action, token)`, e.g. `"like:abc"`.
fn parse_callback_data(data: &str) -> Option<(&str, &str)> {
    let (action, token) = data.split_once(':')?;
    if action.is_empty() || token.is_empty() {
        return None;
    }
    Some((action, token))
}

async fn handle_like(
    client: &TgClient,
    state: &AppState,
    query: &CallbackQuery,
    peer: &Peer,
    token: &str,
) -> Result<()> {
    let post = state.posts.lock().unwrap().get(token).cloned();
    let Some(post) = post else {
        if state.uploading.lock().unwrap().contains(token) {
            query
                .answer()
                .text("⏳ Альбом ещё загружается, попробуйте позже")
                .send()
                .await?;
        } else {
            query.answer().text("Запись не найдена").send().await?;
        }
        return Ok(());
    };

    record_vote(state, query.sender_id().bare_id().unwrap_or(0), "like", &post);

    let entry = LikedEntry {
        token: post.token.clone(),
        url: post.url.clone(),
        artist: post.artist.clone(),
        title: post.title.clone(),
        preview_msg_id: post.preview_msg_id,
        album_msg_ids: post.album_msg_ids.clone(),
        liked_at_unix: crate::store::now_unix(),
    };

    let added = state.liked.lock().unwrap().insert(entry);
    let message = if added {
        "❤️ Добавлено в избранное"
    } else {
        "❤️ Уже в избранном"
    };
    query.answer().text(message).send().await?;

    if let Err(err) = crate::menu::update_menu(client, peer, state).await {
        log::error!("failed to update menu after like: {err:#}");
    }

    Ok(())
}

async fn handle_dislike(
    client: &TgClient,
    state: &AppState,
    query: &CallbackQuery,
    peer: &Peer,
    token: &str,
) -> Result<()> {
    // The record is kept until every message is confirmed deleted: removing it
    // up front would leave orphaned posts and make the next press report
    // "Запись не найдена" even though the messages are still there.
    let post = state.posts.lock().unwrap().get(token).cloned();
    let liked = state.liked.lock().unwrap().get(token).cloned();

    if state.uploading.lock().unwrap().contains(token) {
        // The album is still being posted: its record (and message ids) only
        // exist after the upload finishes, so deleting now would either fail
        // or only remove the messages posted so far. Ask to wait instead.
        query
            .answer()
            .text("⏳ Альбом ещё загружается, попробуйте позже")
            .send()
            .await?;
        return Ok(());
    }

    // Collect every message of the album (preview + all track groups). The
    // liked entry also carries the ids and survives the 30-day registry prune,
    // so union both sources to cover every posted message.
    let ids = collect_album_message_ids(post.as_ref(), liked.as_ref());

    if ids.is_empty() {
        query.answer().text("Запись не найдена").send().await?;
        return Ok(());
    }

    // The vote history wants a post record; rebuild one from the liked entry
    // when the registry record has already been pruned.
    let vote_post = if let Some(post) = &post {
        post.clone()
    } else if let Some(entry) = &liked {
        PostRecord {
            token: entry.token.clone(),
            url: entry.url.clone(),
            artist: entry.artist.clone(),
            title: entry.title.clone(),
            tags: Vec::new(),
            preview_msg_id: entry.preview_msg_id,
            album_msg_ids: entry.album_msg_ids.clone(),
            posted_at_unix: crate::store::now_unix(),
        }
    } else {
        unreachable!("a non-empty id list implies a matching record")
    };
    record_vote(
        state,
        query.sender_id().bare_id().unwrap_or(0),
        "dislike",
        &vote_post,
    );

    // Delete with retries and verification: a big album produces many messages
    // and a dislike often lands right after the upload, so the first call may
    // hit a flood limit or only remove part of the set.
    let remaining = delete_album_messages(client, peer, &ids).await;

    if remaining.is_empty() {
        // Everything is gone; now it is safe to drop the records for good.
        {
            let mut posts = state.posts.lock().unwrap();
            posts.remove(token);
            if let Err(err) = posts.save() {
                log::error!("failed to persist posts registry after dislike: {err:#}");
            }
        }
        let removed = {
            let mut liked_store = state.liked.lock().unwrap();
            let removed = liked_store.remove(token).is_some();
            if let Err(err) = liked_store.save() {
                log::error!("failed to persist favorites after dislike: {err:#}");
            }
            removed
        };
        if removed {
            if let Err(err) = crate::menu::update_menu(client, peer, state).await {
                log::error!("failed to update menu after dislike: {err:#}");
            }
        }
        query.answer().text("🗑 Удалено").send().await?;
    } else {
        log::error!(
            "dislike: {}/{} message(s) could not be deleted for token {token}",
            remaining.len(),
            ids.len()
        );
        query
            .answer()
            .text("Не удалось удалить все сообщения, попробуйте ещё раз")
            .send()
            .await?;
    }

    Ok(())
}

/// How many retry rounds a dislike deletion is allowed before giving up.
const DELETE_ATTEMPTS: usize = 3;
/// Batch size for channel deletions (well under the API limit; kept small to
/// avoid tripping flood limits on very large albums).
const DELETE_BATCH: usize = 50;

/// Collects every posted message id (preview + all track groups) of an album
/// from the post registry and the liked store, deduplicated and in posting
/// order. Both sources are used because the registry prunes after 30 days
/// while the liked entry keeps the ids, and either may have partial data.
fn collect_album_message_ids(
    post: Option<&PostRecord>,
    liked: Option<&LikedEntry>,
) -> Vec<i32> {
    let mut ids: Vec<i32> = Vec::new();

    if let Some(post) = post {
        if post.preview_msg_id > 0 {
            ids.push(post.preview_msg_id);
        }
        ids.extend(post.album_msg_ids.iter().copied().filter(|id| *id > 0));
    }

    if let Some(liked) = liked {
        if liked.preview_msg_id > 0 && !ids.contains(&liked.preview_msg_id) {
            ids.push(liked.preview_msg_id);
        }
        for id in liked.album_msg_ids.iter().copied().filter(|id| *id > 0) {
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
    }

    ids
}

/// Deletes every message in `ids`, retrying transient failures and re-checking
/// what actually got deleted, so a partial or flood-limited delete is completed.
/// Returns the ids that could not be deleted.
async fn delete_album_messages(client: &TgClient, peer: &Peer, ids: &[i32]) -> Vec<i32> {
    let mut pending: Vec<i32> = ids.iter().copied().filter(|id| *id > 0).collect();

    for attempt in 0..DELETE_ATTEMPTS {
        if pending.is_empty() {
            break;
        }
        if attempt > 0 {
            // Dislikes often land right after a large upload, so give Telegram
            // a moment to cool down before retrying.
            tokio::time::sleep(std::time::Duration::from_secs(1 + attempt as u64)).await;
        }

        for batch in pending.chunks(DELETE_BATCH) {
            if let Err(err) = client.delete_messages(peer, batch).await {
                log::warn!(
                    "dislike: delete attempt {}/{} failed for {} message(s): {err:#}",
                    attempt + 1,
                    DELETE_ATTEMPTS,
                    batch.len()
                );
            }
        }

        // Let the deletions propagate, then see what is still there and retry
        // only those ids on the next round.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        match client.get_messages_by_id(peer, &pending).await {
            Ok(existing) => {
                // Channels report deleted messages as `MessageEmpty`
                // placeholders, which grammers wraps as ordinary messages; those
                // must count as gone, or a successful delete would keep showing
                // up as "still alive".
                let alive: Vec<i32> = existing
                    .iter()
                    .flatten()
                    .filter(|m| !m.is_deleted())
                    .map(|m| m.id())
                    .collect();
                pending.retain(|id| alive.contains(id));
            }
            Err(err) => {
                log::warn!("dislike: verification failed: {err:#}");
            }
        }
    }

    pending
}

async fn handle_forward(
    client: &TgClient,
    state: &AppState,
    query: &CallbackQuery,
    peer: &Peer,
    token: &str,
) -> Result<()> {
    let entry = state.liked.lock().unwrap().get(token).cloned();
    let Some(entry) = entry else {
        query.answer().text("Не найдено в избранном").send().await?;
        return Ok(());
    };

    // Forward the whole album: the preview (cover) plus every track media
    // group, so the repost carries the tracks and not just the artwork.
    let post = state.posts.lock().unwrap().get(token).cloned();
    let mut ids: Vec<i32> = Vec::new();
    if entry.preview_msg_id > 0 {
        ids.push(entry.preview_msg_id);
    }

    // Older favorites (saved before the track ids were persisted) fall back
    // to the post registry; when found there, backfill the favorite so it
    // stays self-contained after the registry is pruned.
    if !entry.album_msg_ids.is_empty() {
        ids.extend(entry.album_msg_ids.iter().copied().filter(|id| *id > 0));
    } else if let Some(post) = &post {
        ids.extend(post.album_msg_ids.iter().copied().filter(|id| *id > 0));
        {
            let mut liked = state.liked.lock().unwrap();
            if let Some(stored) = liked.get_mut(token) {
                stored.album_msg_ids = post.album_msg_ids.clone();
            }
        }
    }

    if ids.is_empty() {
        query.answer().text("Нет сообщений для пересылки").send().await?;
        return Ok(());
    }

    match client.forward_messages(peer, &ids, peer).await {
        Ok(_) => {
            query.answer().text("Переслано в канал").send().await?;
        }
        Err(err) => {
            log::error!("failed to forward album messages: {err:#}");
            query.answer().text("Не удалось переслать").send().await?;
        }
    }

    Ok(())
}

/// Removes a single album from the favorites list via the menu's delete
/// button. The channel posts are left untouched (use the dislike button on the
/// album for that).
async fn handle_delete_favorite(
    client: &TgClient,
    state: &AppState,
    query: &CallbackQuery,
    peer: &Peer,
    token: &str,
) -> Result<()> {
    let removed = {
        let mut liked = state.liked.lock().unwrap();
        let removed = liked.remove(token).is_some();
        if let Err(err) = liked.save() {
            log::error!("failed to persist favorites after delete: {err:#}");
        }
        removed
    };

    if !removed {
        query.answer().text("Не найдено в избранном").send().await?;
        return Ok(());
    }

    if let Err(err) = crate::menu::update_menu(client, peer, state).await {
        log::error!("failed to update menu after delete: {err:#}");
    }

    query.answer().text("🗑 Удалено из избранного").send().await?;

    Ok(())
}

/// Handles the pagination bar of the favorites menu. `sub` is `prev`, `next`
/// or `cur`; the middle button is passive and only reports the current page.
async fn handle_nav(
    client: &TgClient,
    state: &AppState,
    query: &CallbackQuery,
    peer: &Peer,
    sub: &str,
) -> Result<()> {
    let (mut page, pages) = {
        let liked = state.liked.lock().unwrap();
        let stored = *state.menu_page.lock().unwrap();
        (stored, crate::menu::page_count(&liked))
    };

    match sub {
        "prev" => page = page.saturating_sub(1),
        "next" => page = (page + 1).min(pages - 1),
        _ => {}
    }

    if sub == "prev" || sub == "next" {
        {
            let mut stored = state.menu_page.lock().unwrap();
            *stored = page;
        }
        if let Err(err) = crate::menu::update_menu(client, peer, state).await {
            log::error!("failed to update menu after page change: {err:#}");
        }
    }

    query
        .answer()
        .text(format!("Страница {} из {pages}", page + 1))
        .send()
        .await?;

    Ok(())
}

/// Appends one entry to the vote history (user -> album tags) and persists it.
fn record_vote(state: &AppState, user_id: i64, action: &str, post: &PostRecord) {
    let mut votes = state.votes.lock().unwrap();
    votes.push(VoteEntry {
        user_id,
        action: action.to_string(),
        tags: post.tags.clone(),
        url: post.url.clone(),
        artist: post.artist.clone(),
        title: post.title.clone(),
        at_unix: crate::store::now_unix(),
    });
    if let Err(err) = votes.save() {
        log::error!("failed to persist vote log: {err:#}");
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_callback_data() {
        assert_eq!(parse_callback_data("like:abc123"), Some(("like", "abc123")));
        assert_eq!(
            parse_callback_data("dislike:deadbeef"),
            Some(("dislike", "deadbeef"))
        );
        assert_eq!(parse_callback_data("fwd:abc123"), Some(("fwd", "abc123")));
        assert_eq!(parse_callback_data("del:abc123"), Some(("del", "abc123")));
        assert_eq!(parse_callback_data("like:"), None);
        assert_eq!(parse_callback_data(":abc"), None);
        assert_eq!(parse_callback_data("garbage"), None);
    }

    fn post_record(token: &str, preview: i32, tracks: &[i32]) -> PostRecord {
        PostRecord {
            token: token.into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            tags: vec![],
            preview_msg_id: preview,
            album_msg_ids: tracks.to_vec(),
            posted_at_unix: crate::store::now_unix(),
        }
    }

    fn liked_entry(token: &str, preview: i32, tracks: &[i32]) -> LikedEntry {
        LikedEntry {
            token: token.into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            preview_msg_id: preview,
            album_msg_ids: tracks.to_vec(),
            liked_at_unix: crate::store::now_unix(),
        }
    }

    #[test]
    fn collect_album_message_ids_unions_both_sources() {
        // Large album: preview + several track groups.
        let post = post_record("abc", 10, &[11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]);
        let ids = collect_album_message_ids(Some(&post), None);
        assert_eq!(ids.len(), 12);
        assert_eq!(ids[0], 10);

        // Registry pruned: the liked entry still carries the full id set.
        let liked = liked_entry("abc", 10, &[11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]);
        let ids = collect_album_message_ids(None, Some(&liked));
        assert_eq!(ids.len(), 12);

        // Both present: unioned without duplicates.
        let partial_post = post_record("abc", 10, &[11, 12, 13]);
        let fuller_liked = liked_entry("abc", 10, &[11, 12, 13, 14, 15]);
        let ids = collect_album_message_ids(Some(&partial_post), Some(&fuller_liked));
        assert_eq!(ids, vec![10, 11, 12, 13, 14, 15]);

        // Zero/negative ids are skipped, but a missing preview is tolerated.
        let odd = post_record("abc", 0, &[-1, 30, 31]);
        let ids = collect_album_message_ids(Some(&odd), None);
        assert_eq!(ids, vec![30, 31]);

        // Nothing known: empty.
        assert!(collect_album_message_ids(None, None).is_empty());
    }
}

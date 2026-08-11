use anyhow::Result;
use grammers_client::message::{Button, ReplyMarkup};

use crate::store::LikedStore;
use crate::tg::{Peer, TgClient};
use crate::AppState;

/// Marker text of the pinned favorites menu message. Used to recognize an
/// existing menu when scanning the channel's pinned message on startup.
pub const MENU_MARKER: &str = "🎧 Избранное";

/// How many liked albums a single menu page lists. Keeps the message and its
/// buttons comfortably within Telegram's limits and lets the user page through.
const MENU_PAGE_SIZE: usize = 5;

/// Returns `Some` markup only when there are rows to show; an empty inline
/// keyboard would make Telegram reject the message.
fn markup(rows: Vec<Vec<Button>>) -> Option<ReplyMarkup> {
    if rows.is_empty() {
        None
    } else {
        Some(ReplyMarkup::from_buttons(&rows))
    }
}

/// Number of pages the favorites menu spans (never less than 1).
pub fn page_count(liked: &LikedStore) -> usize {
    let len = liked.entries().len();
    if len == 0 {
        1
    } else {
        (len + MENU_PAGE_SIZE - 1) / MENU_PAGE_SIZE
    }
}

/// Builds the menu message text and, per listed album, a forward and a delete
/// button. Entries are numbered by their position in the whole favorites list.
/// The last row is a pagination bar (`◀`, `N/M`, `▶`) shown only when there is
/// more than one page; the middle button is passive and just reports the page.
/// Returns `(text, markup, page_count)`, clamping `page` into a valid range.
pub fn render_menu(liked: &LikedStore, page: usize) -> (String, Option<ReplyMarkup>, usize) {
    let entries = liked.entries();
    let total = entries.len();
    let pages = page_count(liked);
    let page = page.min(pages - 1);

    let mut text = format!("{MENU_MARKER} ({total})\n\n");
    if total == 0 {
        text.push_str("Избранное пока пустое.");
        return (text, None, pages);
    }

    let start = page * MENU_PAGE_SIZE;
    let end = start + MENU_PAGE_SIZE.min(total - start);
    let mut rows = Vec::new();
    for (i, entry) in entries[start..end].iter().enumerate() {
        let n = start + i + 1;
        text.push_str(&format!("{n}. {} — {}\n", entry.artist, entry.title));

        // Button text is capped at 64 bytes, so truncate long titles.
        let label: String = format!("▶ {n}. {}", entry.title).chars().take(40).collect();
        rows.push(vec![
            Button::data(label, format!("fwd:{}", entry.token)),
            Button::data(format!("🗑 {n}"), format!("del:{}", entry.token)),
        ]);
    }

    if pages > 1 {
        rows.push(vec![
            Button::data("◀", "nav:prev"),
            Button::data(format!("{}/{}", page + 1, pages), "nav:cur"),
            Button::data("▶", "nav:next"),
        ]);
    }

    (text, markup(rows), pages)
}

/// Finds the pinned menu message (reusing the cached id, then looking at the
/// channel's pinned message, then creating a fresh one) and returns its id.
pub async fn ensure_pinned_menu(client: &TgClient, peer: &Peer, state: &AppState) -> Result<i32> {
    let cached = *state.menu_id.lock().unwrap();
    if let Some(id) = cached {
        if let Ok(Some(_)) = client.get_message(peer, id).await {
            return Ok(id);
        }
        *state.menu_id.lock().unwrap() = None;
    }

    if let Ok(Some(msg)) = client.get_pinned_message(peer).await {
        if msg.text().starts_with(MENU_MARKER) {
            let id = msg.id();
            *state.menu_id.lock().unwrap() = Some(id);
            return Ok(id);
        }
    }

    let (text, markup, _) = {
        let liked = state.liked.lock().unwrap();
        let mut stored = state.menu_page.lock().unwrap();
        *stored = (*stored).min(page_count(&liked) - 1);
        render_menu(&liked, *stored)
    };
    let msg = client.send_message_markup(peer, text, markup).await?;
    client.pin_message(peer, msg.id()).await?;
    *state.menu_id.lock().unwrap() = Some(msg.id());
    Ok(msg.id())
}

/// Re-renders and edits the pinned menu in place. If the menu is gone it is
/// recreated (and re-pinned) on the next call.
pub async fn update_menu(client: &TgClient, peer: &Peer, state: &AppState) -> Result<()> {
    let id = match ensure_pinned_menu(client, peer, state).await {
        Ok(id) => id,
        Err(err) => {
            log::error!("failed to ensure pinned menu: {err:#}");
            return Err(err);
        }
    };

    let (text, markup, _) = {
        let liked = state.liked.lock().unwrap();
        let mut stored = state.menu_page.lock().unwrap();
        *stored = (*stored).min(page_count(&liked) - 1);
        render_menu(&liked, *stored)
    };
    match client.edit_message_markup(peer, id, text, markup).await {
        Ok(()) => Ok(()),
        Err(err) => {
            log::warn!("failed to edit pinned menu ({err:#}); recreating on next change");
            *state.menu_id.lock().unwrap() = None;
            Err(err)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{LikedEntry, LikedStore};

    #[test]
    fn empty_menu_has_no_buttons() {
        let store = LikedStore::open(std::env::temp_dir().join("telecamp-menu-test-empty.json")).unwrap();
        let (text, markup, pages) = render_menu(&store, 0);
        assert!(text.contains(MENU_MARKER));
        assert!(markup.is_none());
        assert_eq!(pages, 1);
    }

    #[test]
    fn menu_lists_entries_with_forward_and_delete_buttons() {
        let dir = std::env::temp_dir().join(format!("telecamp-menu-test-{}", std::process::id()));
        let path = dir.join("favorites.json");
        let mut store = LikedStore::open(&path).unwrap();
        store.insert(LikedEntry {
            token: "abc123".into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            preview_msg_id: 42,
            album_msg_ids: vec![],
            liked_at_unix: 1,
        });

        let (text, markup, pages) = render_menu(&store, 0);
        assert!(text.contains("Artist — Album"));
        assert_eq!(pages, 1);
        let markup = markup.expect("non-empty menu has buttons");
        match markup.raw {
            grammers_client::tl::enums::ReplyMarkup::ReplyInlineMarkup(kb) => {
                assert_eq!(kb.rows.len(), 1);
                let buttons = match &kb.rows[0] {
                    grammers_client::tl::enums::KeyboardButtonRow::Row(row) => &row.buttons,
                };
                assert_eq!(buttons.len(), 2);
                match &buttons[0] {
                    grammers_client::tl::enums::KeyboardButton::Callback(btn) => {
                        assert_eq!(btn.text, "▶ 1. Album");
                        assert_eq!(btn.data, b"fwd:abc123");
                    }
                    _ => panic!("expected callback button"),
                }
                match &buttons[1] {
                    grammers_client::tl::enums::KeyboardButton::Callback(btn) => {
                        assert_eq!(btn.text, "🗑 1");
                        assert_eq!(btn.data, b"del:abc123");
                    }
                    _ => panic!("expected callback button"),
                }
            }
            _ => panic!("expected inline markup"),
        }

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn menu_paginates_with_nav_bar() {
        let dir = std::env::temp_dir().join(format!("telecamp-menu-page-{}", std::process::id()));
        let path = dir.join("favorites.json");
        let mut store = LikedStore::open(&path).unwrap();
        for i in 0..7 {
            store.insert(LikedEntry {
                token: format!("token-{i}"),
                url: format!("https://x.bandcamp.com/album/{i}"),
                artist: "Artist".into(),
                title: format!("Album {i}"),
                preview_msg_id: i as i32,
                album_msg_ids: vec![],
                liked_at_unix: i as u64,
            });
        }

        // First page: 5 entries, a nav bar with the 3 expected buttons.
        let (text, markup, pages) = render_menu(&store, 0);
        assert_eq!(pages, 2);
        assert!(text.contains("1. Artist — Album 0"));
        assert!(text.contains("5. Artist — Album 4"));
        assert!(!text.contains("Album 6"));
        let markup = markup.expect("menu has buttons");
        let kb = match markup.raw {
            grammers_client::tl::enums::ReplyMarkup::ReplyInlineMarkup(kb) => kb,
            _ => panic!("expected inline markup"),
        };
        assert_eq!(kb.rows.len(), 6);
        let nav = match &kb.rows[5] {
            grammers_client::tl::enums::KeyboardButtonRow::Row(row) => &row.buttons,
        };
        assert_eq!(nav.len(), 3);
        match &nav[0] {
            grammers_client::tl::enums::KeyboardButton::Callback(btn) => {
                assert_eq!(btn.data, b"nav:prev");
            }
            _ => panic!("expected callback button"),
        }
        match &nav[1] {
            grammers_client::tl::enums::KeyboardButton::Callback(btn) => {
                assert_eq!(btn.text, "1/2");
                assert_eq!(btn.data, b"nav:cur");
            }
            _ => panic!("expected callback button"),
        }
        match &nav[2] {
            grammers_client::tl::enums::KeyboardButton::Callback(btn) => {
                assert_eq!(btn.data, b"nav:next");
            }
            _ => panic!("expected callback button"),
        }

        // Second page: the remaining 2 entries, numbered globally.
        let (text, markup, _) = render_menu(&store, 1);
        assert!(text.contains("6. Artist — Album 5"));
        assert!(text.contains("7. Artist — Album 6"));
        assert!(!text.contains("Album 4"));
        let markup = markup.expect("menu has buttons");
        let kb = match markup.raw {
            grammers_client::tl::enums::ReplyMarkup::ReplyInlineMarkup(kb) => kb,
            _ => panic!("expected inline markup"),
        };
        assert_eq!(kb.rows.len(), 3);

        // Out-of-range page clamps to the last page.
        let (_, _, pages) = render_menu(&store, 99);
        assert_eq!(pages, 2);

        std::fs::remove_dir_all(dir).ok();
    }
}

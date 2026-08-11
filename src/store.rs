use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// FNV-1a 64-bit hash of an album URL, hex-encoded. Used as the callback data
/// payload for like/dislike/forward buttons (well under Telegram's 64-byte
/// limit) and as the key joining posts, favorites and vote history.
pub fn token(url: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in url.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// How long a posted-album registry entry is kept before being pruned.
const POST_MAX_AGE_DAYS: u64 = 30;
/// Upper bound on kept registry entries (drop oldest beyond this).
const POST_MAX_ENTRIES: usize = 2000;

pub fn now_unix() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

/// A liked album shown in the pinned menu.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LikedEntry {
    pub token: String,
    pub url: String,
    pub artist: String,
    pub title: String,
    /// Message id of the posted preview (forwarded from the menu).
    pub preview_msg_id: i32,
    /// Message ids of the track media groups (forwarded alongside the preview).
    #[serde(default)]
    pub album_msg_ids: Vec<i32>,
    pub liked_at_unix: u64,
}

/// Everything the bot knows about a posted album, so a dislike can delete all
/// of its messages and a like/dislike can be attributed to the album's tags.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostRecord {
    pub token: String,
    pub url: String,
    pub artist: String,
    pub title: String,
    pub tags: Vec<String>,
    /// Message id of the preview (cover photo) message.
    pub preview_msg_id: i32,
    /// Message ids of the track media groups.
    pub album_msg_ids: Vec<i32>,
    pub posted_at_unix: u64,
}

/// One vote: which user pressed which button on which album, with the album's
/// tags attached, for later analysis.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VoteEntry {
    pub user_id: i64,
    /// "like" or "dislike".
    pub action: String,
    pub tags: Vec<String>,
    pub url: String,
    pub artist: String,
    pub title: String,
    pub at_unix: u64,
}

/// Ordered, deduplicated list of liked albums (insertion order).
#[derive(Debug)]
pub struct LikedStore {
    path: PathBuf,
    liked: Vec<LikedEntry>,
}

/// Map token -> posted album record, pruned by age and size.
#[derive(Debug)]
pub struct PostRegistry {
    path: PathBuf,
    posts: HashMap<String, PostRecord>,
}

/// Append-only vote history for tag analysis.
#[derive(Debug)]
pub struct VoteLog {
    path: PathBuf,
    votes: Vec<VoteEntry>,
}

impl LikedStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let liked = load_json::<LikedFile>(&path)?.map(|file| file.liked).unwrap_or_default();
        Ok(Self { path, liked })
    }

    pub fn insert(&mut self, entry: LikedEntry) -> bool {
        if let Some(existing) = self.liked.iter_mut().find(|e| e.token == entry.token) {
            *existing = entry;
            false
        } else {
            self.liked.push(entry);
            true
        }
    }

    pub fn get(&self, token: &str) -> Option<&LikedEntry> {
        self.liked.iter().find(|entry| entry.token == token)
    }

    /// Returns a mutable reference to the entry for the given token, if present.
    pub fn get_mut(&mut self, token: &str) -> Option<&mut LikedEntry> {
        self.liked.iter_mut().find(|entry| entry.token == token)
    }

    /// Removes and returns the entry with the given token, if present.
    pub fn remove(&mut self, token: &str) -> Option<LikedEntry> {
        let idx = self.liked.iter().position(|entry| entry.token == token)?;
        Some(self.liked.remove(idx))
    }

    pub fn entries(&self) -> &[LikedEntry] {
        &self.liked
    }

    pub fn save(&self) -> Result<()> {
        persist_json(&self.path, &LikedFile { liked: self.liked.clone() })
    }
}

#[derive(Serialize, Deserialize)]
struct LikedFile {
    liked: Vec<LikedEntry>,
}

impl PostRegistry {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut posts = load_json::<PostsFile>(&path)?
            .map(|file| file.posts)
            .unwrap_or_default();
        prune(&mut posts);
        Ok(Self { path, posts })
    }

    pub fn insert(&mut self, record: PostRecord) {
        self.posts.insert(record.token.clone(), record);
        prune(&mut self.posts);
    }

    pub fn get(&self, token: &str) -> Option<&PostRecord> {
        self.posts.get(token)
    }

    pub fn remove(&mut self, token: &str) -> Option<PostRecord> {
        self.posts.remove(token)
    }

    pub fn save(&self) -> Result<()> {
        persist_json(&self.path, &PostsFile { posts: self.posts.clone() })
    }
}

#[derive(Serialize, Deserialize)]
struct PostsFile {
    posts: HashMap<String, PostRecord>,
}

impl VoteLog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let votes = load_json::<VotesFile>(&path)?.map(|file| file.votes).unwrap_or_default();
        Ok(Self { path, votes })
    }

    pub fn push(&mut self, entry: VoteEntry) {
        self.votes.push(entry);
    }

    pub fn votes(&self) -> &[VoteEntry] {
        &self.votes
    }

    pub fn save(&self) -> Result<()> {
        persist_json(&self.path, &VotesFile { votes: self.votes.clone() })
    }
}

#[derive(Serialize, Deserialize)]
struct VotesFile {
    votes: Vec<VoteEntry>,
}

/// Drops registry entries older than [`POST_MAX_AGE_DAYS`], then trims the map
/// down to [`POST_MAX_ENTRIES`] newest entries when it is too large.
fn prune(posts: &mut HashMap<String, PostRecord>) {
    let cutoff = now_unix().saturating_sub(POST_MAX_AGE_DAYS * 86_400);
    posts.retain(|_, record| record.posted_at_unix >= cutoff);

    if posts.len() > POST_MAX_ENTRIES {
        let mut sorted: Vec<PostRecord> = posts.drain().map(|(_, record)| record).collect();
        sorted.sort_by_key(|record| std::cmp::Reverse(record.posted_at_unix));
        sorted.truncate(POST_MAX_ENTRIES);
        posts.extend(sorted.into_iter().map(|record| (record.token.clone(), record)));
    }
}

fn load_json<T: DeserializeOwned>(path: &PathBuf) -> Result<Option<T>> {
    match std::fs::read_to_string(path) {
        Ok(contents) => serde_json::from_str(&contents)
            .context("failed to parse cache file")
            .map(Some),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).context("failed to read cache file"),
    }
}

/// Writes JSON to a temp file, then renames it over the target.
fn persist_json<T: Serialize>(path: &PathBuf, value: &T) -> Result<()> {
    let contents = serde_json::to_string_pretty(value).context("failed to serialize cache")?;

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).context("failed to create cache directory")?;
    }

    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, contents).context("failed to write cache file")?;

    if path.exists() {
        std::fs::remove_file(path).context("failed to replace cache file")?;
    }
    std::fs::rename(&tmp, path).context("failed to persist cache file")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("telecamp-store-test-{name}-{}", std::process::id()))
    }

    #[test]
    fn token_is_deterministic_and_compact() {
        let a = token("https://foo.bandcamp.com/album/bar");
        let b = token("https://foo.bandcamp.com/album/bar");
        let c = token("https://foo.bandcamp.com/album/baz");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.len() == 16 && a.len() + 7 <= 64);
    }

    #[test]
    fn liked_store_roundtrip() {
        let dir = temp_dir("liked");
        let path = dir.join("favorites.json");

        let mut store = LikedStore::open(&path).unwrap();
        assert!(store.insert(LikedEntry {
            token: "abc".into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            preview_msg_id: 42,
            album_msg_ids: vec![43, 44],
            liked_at_unix: 1,
        }));
        assert!(!store.insert(LikedEntry {
            token: "abc".into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            preview_msg_id: 42,
            album_msg_ids: vec![43, 44],
            liked_at_unix: 2,
        }));
        assert_eq!(store.entries().len(), 1);
        store.save().unwrap();

        let reloaded = LikedStore::open(&path).unwrap();
        assert_eq!(reloaded.entries().len(), 1);
        assert_eq!(reloaded.entries()[0].album_msg_ids, vec![43, 44]);
        let mut reloaded = reloaded;
        assert!(reloaded.remove("abc").is_some());
        assert!(reloaded.entries().is_empty());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn liked_entry_without_track_ids_defaults_to_empty() {
        // Favorites persisted before the track ids were added to LikedEntry
        // lack the `album_msg_ids` field; it must load as empty.
        let dir = temp_dir("liked-migrate");
        let path = dir.join("favorites.json");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            &path,
            r#"{"liked":[{"token":"abc","url":"u","artist":"A","title":"T","preview_msg_id":1,"liked_at_unix":1}]}"#,
        )
        .unwrap();

        let store = LikedStore::open(&path).unwrap();
        assert_eq!(store.entries()[0].album_msg_ids, Vec::<i32>::new());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn post_registry_roundtrip() {
        let dir = temp_dir("posts");
        let path = dir.join("posts.json");

        let mut registry = PostRegistry::open(&path).unwrap();
        registry.insert(PostRecord {
            token: "abc".into(),
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            tags: vec!["noise".into()],
            preview_msg_id: 42,
            album_msg_ids: vec![43, 44],
            posted_at_unix: now_unix(),
        });
        assert_eq!(registry.get("abc").unwrap().tags, vec!["noise"]);
        registry.save().unwrap();

        let reloaded = PostRegistry::open(&path).unwrap();
        assert_eq!(reloaded.get("abc").unwrap().album_msg_ids, vec![43, 44]);
        let mut reloaded = reloaded;
        assert!(reloaded.remove("abc").is_some());
        assert!(reloaded.get("abc").is_none());

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn post_registry_prunes_old_entries() {
        let mut registry = PostRegistry::open(temp_dir("prune").join("posts.json")).unwrap();
        registry.insert(PostRecord {
            token: "old".into(),
            url: "https://x.bandcamp.com/album/old".into(),
            artist: "Artist".into(),
            title: "Old".into(),
            tags: vec![],
            preview_msg_id: 1,
            album_msg_ids: vec![],
            posted_at_unix: now_unix() - POST_MAX_AGE_DAYS * 86_400 - 1,
        });
        assert!(registry.get("old").is_none());
    }

    #[test]
    fn vote_log_roundtrip() {
        let dir = temp_dir("votes");
        let path = dir.join("votes.json");

        let mut log = VoteLog::open(&path).unwrap();
        log.push(VoteEntry {
            user_id: 123,
            action: "dislike".into(),
            tags: vec!["noise".into(), "black-metal".into()],
            url: "https://x.bandcamp.com/album/y".into(),
            artist: "Artist".into(),
            title: "Album".into(),
            at_unix: 1,
        });
        log.save().unwrap();

        let reloaded = VoteLog::open(&path).unwrap();
        assert_eq!(reloaded.votes().len(), 1);
        assert_eq!(reloaded.votes()[0].user_id, 123);
        assert_eq!(reloaded.votes()[0].action, "dislike");
        assert_eq!(reloaded.votes()[0].tags.len(), 2);

        std::fs::remove_dir_all(dir).ok();
    }
}

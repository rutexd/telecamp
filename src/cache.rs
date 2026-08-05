use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// Persisted set of album URLs that were already processed, so a re-scan
/// does not download the same release twice.
#[derive(Debug)]
pub struct SeenCache {
    path: PathBuf,
    seen: HashSet<String>,
}

#[derive(Serialize, Deserialize)]
struct CacheFile {
    seen: Vec<String>,
}

impl SeenCache {
    /// Opens (creating if needed) the cache stored at `path`.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        let seen = match std::fs::read_to_string(&path) {
            Ok(contents) => serde_json::from_str::<CacheFile>(&contents)
                .context("failed to parse cache file")?
                .seen
                .into_iter()
                .collect(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).context("failed to create cache directory")?;
                }
                HashSet::new()
            }
            Err(err) => return Err(err).context("failed to read cache file"),
        };

        Ok(Self { path, seen })
    }

    /// Returns true if the album URL was already seen.
    pub fn contains(&self, url: &str) -> bool {
        self.seen.contains(url)
    }

    /// Marks the album URL as seen. Returns true if it was newly inserted.
    pub fn insert(&mut self, url: impl Into<String>) -> bool {
        self.seen.insert(url.into())
    }

    /// Number of cached album URLs.
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Persists the cache to disk (write to a temp file, then rename).
    pub fn save(&self) -> Result<()> {
        let mut entries: Vec<&String> = self.seen.iter().collect();
        entries.sort();

        let mut contents = String::from("{\"seen\": [");
        for (idx, url) in entries.iter().enumerate() {
            if idx > 0 {
                contents.push(',');
            }
            contents.push('"');
            contents.push_str(&url.replace('"', "\\\""));
            contents.push('"');
        }
        contents.push_str("]}");

        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).context("failed to create cache directory")?;
        }

        let tmp = self.path.with_extension("tmp");
        std::fs::write(&tmp, contents).context("failed to write cache file")?;

        if self.path.exists() {
            std::fs::remove_file(&self.path).context("failed to replace cache file")?;
        }
        std::fs::rename(&tmp, &self.path).context("failed to persist cache file")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_roundtrip() {
        let dir =
            std::env::temp_dir().join(format!("telecamp-cache-test-roundtrip-{}", std::process::id()));
        let path = dir.join("seen.json");

        let mut cache = SeenCache::open(&path).unwrap();
        assert!(cache.insert("https://a.bandcamp.com/album/one"));
        assert!(!cache.insert("https://a.bandcamp.com/album/one"));
        assert!(cache.contains("https://a.bandcamp.com/album/one"));
        cache.save().unwrap();

        let reloaded = SeenCache::open(&path).unwrap();
        assert!(reloaded.contains("https://a.bandcamp.com/album/one"));
        assert_eq!(reloaded.len(), 1);

        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn cache_missing_file_starts_empty() {
        let dir =
            std::env::temp_dir().join(format!("telecamp-cache-test-empty-{}", std::process::id()));
        let cache = SeenCache::open(dir.join("nope.json")).unwrap();
        assert!(cache.is_empty());
        std::fs::remove_dir_all(dir).ok();
    }
}

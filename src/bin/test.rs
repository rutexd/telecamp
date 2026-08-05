use anyhow::{anyhow, Result};
use telecamp::bandcamp::BandcampClient;
use telecamp::cache::SeenCache;
use telecamp::downloader::{Downloader, HttpDownloader};
use telecamp::progress::NoopProgressSink;
use telecamp::tg::TgClient;
use telecamp::uploader::{TelegramUploader, Uploader};

fn check(condition: bool, label: &str) -> Result<()> {
    if condition {
        println!("  [PASS] {label}");
        Ok(())
    } else {
        Err(anyhow!("check failed: {label}"))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();

    let client = BandcampClient::new();

    println!("== album fetch ==");
    test_album(&client).await?;

    println!("== discover (tag) ==");
    test_discover(&client).await?;

    println!("== search ==");
    test_search(&client).await?;

    println!("== artist releases ==");
    test_artist(&client).await?;

    println!("== cache ==");
    test_cache()?;

    println!("== download -> upload pipeline (noop) ==");
    test_pipeline(&client).await?;

    println!();
    println!("ALL TESTS PASSED");
    Ok(())
}

async fn test_album(client: &BandcampClient) -> Result<()> {
    let url = std::env::var("TEST_ALBUM_URL")
        .unwrap_or_else(|_| "https://startrail.bandcamp.com/album/structure".into());

    let album = client.get_album(&url).await?;
    println!(
        "  artist={:?} title={:?} release={:?} tracks={} tags={:?}",
        album.artist,
        album.title,
        album.release_date,
        album.tracks.len(),
        album.tags,
    );

    check(!album.artist.is_empty(), "album has an artist")?;
    check(!album.title.is_empty(), "album has a title")?;
    check(!album.tracks.is_empty(), "album has at least one track")?;
    check(
        album.tracks.iter().all(|track| track.id > 0),
        "every track has an id",
    )?;
    check(
        album.tracks.iter().all(|track| track.num > 0),
        "every track has a number",
    )?;

    let streamable = album
        .tracks
        .iter()
        .filter(|track| track.is_downloadable)
        .count();
    println!("  {streamable} track(s) marked downloadable, sample mp3-128:");
    if let Some(url) = album
        .tracks
        .iter()
        .find(|track| track.mp3_128.is_some())
        .and_then(|track| track.mp3_128.as_deref())
    {
        println!("    {url}");
    }

    Ok(())
}

async fn test_discover(client: &BandcampClient) -> Result<()> {
    let tag = std::env::var("TEST_TAG").unwrap_or_else(|_| "electronic".into());
    let releases = client.discover(&[tag], "new").await?;

    check(!releases.is_empty(), "discover returned releases")?;
    println!("  found {} release(s)", releases.len());

    for release in releases.iter().take(3) {
        println!(
            "    {} - {} ({}) tracks={:?} date={:?}",
            release.band_name, release.title, release.url, release.track_count, release.release_date,
        );
        check(
            release.url.starts_with("https://"),
            "release url is absolute",
        )?;
    }

    Ok(())
}

async fn test_search(client: &BandcampClient) -> Result<()> {
    let query = std::env::var("TEST_SEARCH").unwrap_or_else(|_| "startrail".into());
    let results = client.search(&query, "").await?;

    check(!results.is_empty(), "search returned hits")?;
    println!("  found {} hit(s)", results.len());

    for result in results.iter().take(5) {
        println!(
            "    [{}] {} - {} ({})",
            result.item_type,
            result.name,
            result.band_name.as_deref().unwrap_or("-"),
            result.url,
        );
    }

    Ok(())
}

async fn test_artist(client: &BandcampClient) -> Result<()> {
    let artist = std::env::var("TEST_ARTIST_URL")
        .unwrap_or_else(|_| "https://startrail.bandcamp.com".into());
    let releases = client.artist_releases(&artist).await?;

    check(!releases.is_empty(), "artist has releases")?;
    println!("  found {} release(s)", releases.len());

    for release in releases.iter().take(5) {
        println!("    [{}] {} ({})", release.item_type, release.title, release.url);
        check(
            release.url.starts_with("https://"),
            "release url is absolute",
        )?;
    }

    Ok(())
}

fn test_cache() -> Result<()> {
    let dir = std::env::temp_dir().join(format!("telecamp-test-{}", std::process::id()));
    let path = dir.join("seen.json");
    let mut cache = SeenCache::open(&path)?;

    check(cache.is_empty(), "fresh cache is empty")?;
    check(
        cache.insert("https://x.bandcamp.com/album/one"),
        "insert returns true for a new url",
    )?;
    check(
        !cache.insert("https://x.bandcamp.com/album/one"),
        "insert returns false for a duplicate",
    )?;
    cache.save()?;

    let reloaded = SeenCache::open(&path)?;
    check(
        reloaded.contains("https://x.bandcamp.com/album/one"),
        "cache survives a reload",
    )?;

    std::fs::remove_dir_all(&dir).ok();
    Ok(())
}

async fn test_pipeline(client: &BandcampClient) -> Result<()> {
    let url = std::env::var("TEST_ALBUM_URL")
        .unwrap_or_else(|_| "https://startrail.bandcamp.com/album/structure".into());

    let album = client.get_album(&url).await?;

    let downloader = HttpDownloader::new();
    let uploader = TelegramUploader::default();
    let progress: std::sync::Arc<dyn telecamp::progress::ProgressSink> =
        std::sync::Arc::new(NoopProgressSink);

    let stream = downloader.stream(&album, progress.clone()).await?;

    // Drain each track's stream reader to verify it actually yields payload.
    let mut tracks = Vec::new();
    let mut total: usize = 0;
    let mut stream = stream;
    while let Some(mut track) = stream.tracks.recv().await {
        use tokio::io::AsyncReadExt;
        let mut data = Vec::new();
        track.reader.read_to_end(&mut data).await?;
        total += data.len();
        tracks.push((track.num, track.title.clone(), data.len()));
    }

    check(!tracks.is_empty(), "http downloader fetched at least one track")?;
    check(
        tracks.iter().all(|(_, _, len)| *len > 0),
        "every fetched track has payload bytes",
    )?;
    println!(
        "  streamed {} track(s), {} bytes total, cover={}",
        tracks.len(),
        total,
        stream.cover.as_ref().map_or(0, Vec::len),
    );

    let config = match telecamp::config::Config::from_env() {
        Ok(config) => config,
        Err(_) => {
            println!("  skipping live upload: TELEGRAM_BOT_TOKEN not set");
            return Ok(());
        }
    };

    let tg_client = match TgClient::connect(&config).await {
        Ok(client) => client,
        Err(err) => {
            println!("  skipping live upload: failed to connect: {err:#}");
            return Ok(());
        }
    };
    match tg_client.resolve_chat(&config).await? {
        Some(peer) => {
            let stream = downloader.stream(&album, progress.clone()).await?;
            let result = uploader
                .upload(&tg_client, &peer, stream, progress.clone())
                .await?;
            check(result.sent_files > 0, "uploader sent at least one file")?;
            println!("  uploaded {} file(s) to chat {}", result.sent_files, peer.id());
        }
        None => println!(
            "  skipping live upload: neither TELEGRAM_CHANNEL nor TELEGRAM_CHAT_ID set"
        ),
    }

    Ok(())
}

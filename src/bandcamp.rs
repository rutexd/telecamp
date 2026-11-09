use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use log::info;
use scraper::{Html, Selector};
use serde::{de::DeserializeOwned, Deserialize};
use tokio::task::spawn_blocking;

const DISCOVER_URL: &str = "https://bandcamp.com/api/discover/1/discover_web";
const SEARCH_URL: &str = "https://bandcamp.com/api/bcsearch_public_api/1/autocomplete_elastic";

/// An album page fetched from Bandcamp (mirrors the `data-tralbum` JSON plus scraped tags).
#[derive(Debug, Clone, Default)]
pub struct Album {
    pub id: u64,
    pub url: String,
    pub artist: String,
    pub title: String,
    pub release_date: String,
    pub art_id: Option<u64>,
    pub tracks: Vec<Track>,
    pub tags: Vec<String>,
}

impl Album {
    /// Returns true if any album tag matches an entry in `blacklist`.
    ///
    /// Comparison uses [`tag_key`], so "sascha-müller" in the config matches
    /// the page slug "sascha_müller", and "acid-techno" matches "acid_techno".
    pub fn has_any_tag(&self, blacklist: &[String]) -> bool {
        self.tags
            .iter()
            .map(|tag| tag_key(tag))
            .any(|tag| blacklist.iter().any(|blocked| tag_key(blocked) == tag))
    }
}

/// Canonical comparison key for a tag slug: lowercased with `_` and `-`
/// treated as equal, so Bandcamp page slugs match config entries.
fn tag_key(tag: &str) -> String {
    tag.to_lowercase().replace('_', "-")
}

/// A single track with its (streamable) MP3-128 URL.
#[derive(Debug, Clone, Default)]
pub struct Track {
    pub id: u64,
    pub num: u32,
    pub title: String,
    pub duration: Option<f64>,
    pub is_downloadable: bool,
    pub mp3_128: Option<String>,
}

/// A release found via the discover endpoint (summary data, no track list).
#[derive(Debug, Clone)]
pub struct ReleaseRef {
    pub album_id: u64,
    pub band_id: u64,
    pub band_name: String,
    pub title: String,
    pub url: String,
    pub release_date: Option<String>,
    pub track_count: Option<u64>,
    pub art_id: Option<u64>,
}

/// A single hit from the search endpoint.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// "b" (band), "a" (album) or "t" (track).
    pub item_type: String,
    pub id: u64,
    pub name: String,
    pub band_name: Option<String>,
    pub url: String,
}

/// A release listed on an artist's discography page.
#[derive(Debug, Clone)]
pub struct ArtistRelease {
    pub album_id: u64,
    pub band_id: u64,
    pub title: String,
    /// "album" or "track".
    pub item_type: String,
    pub url: String,
    pub art_id: Option<u64>,
}

/// HTTP client impersonating a real browser to get past Bandcamp's anti-bot protections.
pub struct BandcampClient {
    http: impersonate_rs::Client,
}

impl BandcampClient {
    pub fn new() -> Self {
        let http = impersonate_rs::Client::builder()
            .impersonate(impersonate_rs::Browser::Chrome142)
            .timeout(Duration::from_secs(60))
            .build();

        Self { http }
    }

    /// Fetches and parses a Bandcamp album page.
    pub async fn get_album(&self, url: &str) -> Result<Album> {
        info!("fetching bandcamp album page: {url}");
        let html = self.get_html(url).await?;
        parse_album_page(&html, url)
    }

    /// Discovers the newest releases for the given tag slugs (e.g. "electronic").
    ///
    /// `slice` is "new", "top" or "rand"; only the first page is returned.
    pub async fn discover(&self, tags: &[String], slice: &str) -> Result<Vec<ReleaseRef>> {
        let payload = discover_payload(tags, slice);

        info!("discovering newest releases for tags {tags:?} ({slice})");
        let response: DiscoverResponse = self.post_json(DISCOVER_URL, payload).await?;

        Ok(response
            .results
            .into_iter()
            .map(|result| ReleaseRef {
                album_id: result.item_id,
                band_id: result.band_id,
                band_name: result.band_name,
                title: result.title,
                url: strip_tracker(&result.item_url),
                release_date: result.release_date,
                track_count: result.track_count,
                art_id: result.primary_image.map(|image| image.image_id),
            })
            .collect())
    }

    /// Searches Bandcamp. `item_type` is "" (all), "b", "a" or "t".
    pub async fn search(&self, query: &str, item_type: &str) -> Result<Vec<SearchResult>> {
        let payload = serde_json::json!({
            "search_text": query,
            "search_filter": item_type,
            "full_page": true,
            "fan_id": null,
        });

        info!("searching bandcamp for '{query}' ({item_type:?})");
        let response: SearchResponse = self.post_json(SEARCH_URL, payload).await?;

        Ok(response
            .auto
            .map(|auto| {
                auto.results
                    .into_iter()
                    .map(|result| SearchResult {
                        item_type: result.item_type,
                        id: result.id,
                        name: result.name,
                        band_name: result.band_name,
                        url: result
                            .item_url_path
                            .or(result.item_url_root)
                            .unwrap_or_default(),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Lists the releases (albums and tracks) of an artist page.
    pub async fn artist_releases(&self, artist_url: &str) -> Result<Vec<ArtistRelease>> {
        let base = artist_url.trim_end_matches('/');
        let music_url = if base.ends_with("/music") {
            base.to_string()
        } else {
            format!("{base}/music")
        };

        info!("fetching artist discography: {music_url}");
        let html = self.get_html(&music_url).await?;
        parse_artist_releases(&html, base)
    }

    async fn get_html(&self, url: &str) -> Result<String> {
        let url = url.to_string();
        let http = self.http.clone();

        spawn_blocking(move || {
            let response = http
                .get(&url)
                .send()
                .map_err(|err| anyhow!("bandcamp request failed: {err}"))?;

            if response.status() != 200 {
                bail!("bandcamp returned status {} for {url}", response.status());
            }

            response
                .text()
                .map_err(|err| anyhow!("bandcamp response is not UTF-8: {err}"))
        })
        .await
        .context("bandcamp fetch task panicked")?
    }

    async fn post_json<T: DeserializeOwned + Send + 'static>(
        &self,
        url: &str,
        payload: serde_json::Value,
    ) -> Result<T> {
        let url = url.to_string();
        let http = self.http.clone();

        spawn_blocking(move || {
            let response = http
                .post(&url)
                .json(&payload)
                .map_err(|err| anyhow!("failed to build bandcamp request: {err}"))?
                .send()
                .map_err(|err| anyhow!("bandcamp request failed: {err}"))?;

            if response.status() != 200 {
                bail!("bandcamp returned status {} for {url}", response.status());
            }

            serde_json::from_slice(response.bytes()).context("failed to parse bandcamp JSON")
        })
        .await
        .context("bandcamp fetch task panicked")?
    }
}

fn discover_payload(tags: &[String], slice: &str) -> serde_json::Value {
    serde_json::json!({
        "category_id": 0,
        "tag_norm_names": tags,
        "geoname_id": 0,
        "slice": slice,
        "time_facet_id": null,
        "cursor": null,
        "size": 40,
        "include_result_types": ["a"],
    })
}

/// Extracts the album data embedded into a Bandcamp album page.
pub fn parse_album_page(html: &str, _url: &str) -> Result<Album> {
    let document = Html::parse_document(html);

    let script = document
        .select(&Selector::parse("script[data-tralbum]").unwrap())
        .next()
        .context("page does not contain script[data-tralbum]")?;

    let raw = script
        .value()
        .attr("data-tralbum")
        .context("script[data-tralbum] has no data-tralbum attribute")?;

    let raw = decode_html_entities(raw);
    let data: TralbumData = serde_json::from_str(&raw).context("failed to parse data-tralbum JSON")?;

    Ok(Album {
        id: data.id,
        url: data.url,
        artist: data.artist,
        title: data.current.title,
        release_date: data.album_release_date,
        art_id: data.art_id,
        tracks: data
            .trackinfo
            .into_iter()
            .map(|track| Track {
                id: track.id.unwrap_or_default(),
                num: track.track_num,
                title: track.title,
                duration: track.duration,
                is_downloadable: track.is_downloadable.unwrap_or_default(),
                mp3_128: track.file.map(|file| file.mp3_128),
            })
            .collect(),
        tags: scrape_tags(&document),
    })
}

/// Extracts the releases listed on an artist's `/music` page.
pub fn parse_artist_releases(html: &str, base_url: &str) -> Result<Vec<ArtistRelease>> {
    let document = Html::parse_document(html);

    let grid = document
        .select(&Selector::parse("ol#music-grid").unwrap())
        .next()
        .context("artist page does not contain ol#music-grid")?;

    let raw = grid
        .value()
        .attr("data-client-items")
        .context("music grid has no data-client-items attribute")?;

    let raw = decode_html_entities(raw);
    let items: Vec<ClientItem> =
        serde_json::from_str(&raw).context("failed to parse data-client-items JSON")?;

    Ok(items
        .into_iter()
        .map(|item| ArtistRelease {
            album_id: item.id,
            band_id: item.band_id,
            title: item.title,
            item_type: item.item_type,
            url: resolve_url(base_url, &item.page_url),
            art_id: item.art_id,
        })
        .collect())
}

/// Tags listed under `.tralbum-tags` as canonical Bandcamp slugs.
///
/// The canonical slug lives in the `href` (`/discover/<slug>` or `/tag/<slug>`),
/// while the anchor text is a display name that may be capitalized
/// ("Italy", "R&B"). Slugs are URL-encoded on the page (e.g. `sascha_m%C3%BCller`),
/// so they are percent-decoded, lowercased and deduplicated.
fn scrape_tags(document: &Html) -> Vec<String> {
    use percent_encoding::percent_decode_str;

    let mut seen = std::collections::HashSet::new();
    let mut tags = Vec::new();

    for node in document.select(&Selector::parse(".tralbum-tags a.tag").unwrap()) {
        let Some(href) = node.value().attr("href") else {
            continue;
        };

        let raw_slug = href
            .split('?')
            .next()
            .unwrap_or(href)
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("");

        if raw_slug.is_empty() {
            continue;
        }

        let slug = percent_decode_str(raw_slug)
            .decode_utf8_lossy()
            .to_lowercase();

        if slug.is_empty() || seen.contains(&slug) {
            continue;
        }
        seen.insert(slug.clone());
        tags.push(slug);
    }

    tags
}

/// Converts a human-written tag into the Bandcamp tag slug format expected by
/// the discover API: lowercase, spaces and other separators become hyphens,
/// "&" becomes "and" ("Drum & Bass" -> "drum-and-bass").
///
/// Canonical slugs pass through unchanged ("rnb", "drum-and-bass"). For
/// abbreviated tags whose canonical slug drops the ampersand (like "rnb"),
/// prefer configuring the canonical slug directly.
pub fn normalize_tag(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.trim().to_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if ch == '&' {
            out.push_str(" and ");
        } else {
            out.push('-');
        }
    }

    let mut slug = String::new();
    let mut prev_hyphen = false;
    for ch in out.chars() {
        if ch == '-' || ch == ' ' {
            if !slug.is_empty() && !prev_hyphen {
                slug.push('-');
            }
            prev_hyphen = true;
        } else {
            prev_hyphen = false;
            slug.push(ch);
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }

    slug
}

/// Joins a relative page path onto an artist base URL.
fn resolve_url(base: &str, path: &str) -> String {
    if path.starts_with("http://") || path.starts_with("https://") {
        return path.to_string();
    }

    let scheme_end = base.find("://").map(|i| i + 3).unwrap_or(0);
    let authority_end = base[scheme_end..]
        .find('/')
        .map(|i| scheme_end + i)
        .unwrap_or(base.len());

    format!("{}{}", &base[..authority_end], path)
}

/// Drops the `?from=...` tracker Bandcamp appends to item URLs.
fn strip_tracker(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_string()
}

/// Parses the two release-date formats Bandcamp uses into Unix seconds at
/// 00:00 UTC of that date:
/// - album page: `"11 Dec 2025 00:00:00 GMT"`
/// - discover API: `"2026-08-05 00:00:00 UTC"`
pub fn release_date_unix(release_date: &str) -> Option<u64> {
    let parts: Vec<&str> = release_date.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }

    let (year, month, day) = if parts[0].contains('-') {
        let mut it = parts[0].split('-');
        let year: i64 = it.next()?.parse().ok()?;
        let month: u32 = it.next()?.parse().ok()?;
        let day: u32 = it.next()?.parse().ok()?;
        (year, month, day)
    } else if parts.len() >= 3 {
        let day: u32 = parts[0].parse().ok()?;
        let month = month_from_abbr(parts[1])?;
        let year: i64 = parts[2].parse().ok()?;
        (year, month, day)
    } else {
        return None;
    };

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    days_from_civil(year, month, day)
        .checked_mul(86_400)?
        .try_into()
        .ok()
}

fn month_from_abbr(raw: &str) -> Option<u32> {
    match raw.to_ascii_lowercase().as_str() {
        "jan" => Some(1),
        "feb" => Some(2),
        "mar" => Some(3),
        "apr" => Some(4),
        "may" => Some(5),
        "jun" => Some(6),
        "jul" => Some(7),
        "aug" => Some(8),
        "sep" => Some(9),
        "oct" => Some(10),
        "nov" => Some(11),
        "dec" => Some(12),
        _ => None,
    }
}

/// Days since 1970-01-01 for a proleptic Gregorian civil date
/// (Howard Hinnant's `days_from_civil` algorithm).
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let z = year - i64::from(month <= 2);
    let era = z.div_euclid(400);
    let yoe = z - era * 400;
    let offset = if month > 2 { -3 } else { 9 };
    let doy = (153 * (i64::from(month) + offset) + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The normalized host of an URL: `"https://foo.bandcamp.com/album/x"` ->
/// `"foo.bandcamp.com"` (lowercase, `www.` stripped).
pub fn url_host(url: &str) -> Option<String> {
    let host = url
        .trim()
        .split("://")
        .last()?
        .split('/')
        .next()?
        .trim()
        .to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    Some(host.to_string())
}

/// True if the album belongs to an artist whose site appears in `blacklist`.
pub fn is_artist_blacklisted(album_url: &str, blacklist: &[String]) -> bool {
    let Some(host) = url_host(album_url) else {
        return false;
    };
    blacklist
        .iter()
        .filter_map(|entry| url_host(entry))
        .any(|blocked| blocked == host)
}

/// Decodes the handful of HTML entities Bandcamp uses inside JSON attributes.
///
/// html5ever may or may not decode attribute values depending on the parser;
/// decoding again is idempotent for the entities we care about.
fn decode_html_entities(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        if chars[i] == '&' {
            if let Some(offset) = chars[i + 1..].iter().position(|&c| c == ';') {
                let end = i + 1 + offset;
                let name: String = chars[i + 1..end].iter().collect();

                let decoded = match name.as_str() {
                    "quot" => Some('"'),
                    "apos" | "#39" => Some('\''),
                    "amp" => Some('&'),
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    _ => name
                        .strip_prefix('#')
                        .and_then(|n| {
                            let (radix, digits) = n
                                .strip_prefix('x')
                                .map_or((10, n), |hex| (16, hex));
                            u32::from_str_radix(digits, radix).ok()
                        })
                        .and_then(char::from_u32),
                };

                if let Some(c) = decoded {
                    out.push(c);
                    i = end + 1;
                    continue;
                }
            }
        }
        out.push(chars[i]);
        i += 1;
    }

    out
}

/// Raw shape of the `data-tralbum` attribute.
#[derive(Deserialize)]
struct TralbumData {
    id: u64,
    url: String,
    artist: String,
    current: TralbumCurrent,
    album_release_date: String,
    art_id: Option<u64>,
    trackinfo: Vec<TrackData>,
}

#[derive(Deserialize)]
struct TralbumCurrent {
    title: String,
}

#[derive(Deserialize)]
struct TrackData {
    id: Option<u64>,
    track_num: u32,
    title: String,
    duration: Option<f64>,
    is_downloadable: Option<bool>,
    file: Option<TrackFile>,
}

#[derive(Deserialize)]
struct TrackFile {
    #[serde(rename = "mp3-128")]
    mp3_128: String,
}

#[derive(Deserialize)]
struct DiscoverResponse {
    results: Vec<DiscoverResult>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoverResult {
    item_id: u64,
    band_id: u64,
    band_name: String,
    title: String,
    item_url: String,
    release_date: Option<String>,
    track_count: Option<u64>,
    primary_image: Option<PrimaryImage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PrimaryImage {
    image_id: u64,
}

#[derive(Deserialize)]
struct SearchResponse {
    auto: Option<SearchAuto>,
}

#[derive(Deserialize)]
struct SearchAuto {
    results: Vec<SearchResultRaw>,
}

#[derive(Deserialize)]
struct SearchResultRaw {
    #[serde(rename = "type")]
    item_type: String,
    id: u64,
    name: String,
    band_name: Option<String>,
    item_url_path: Option<String>,
    item_url_root: Option<String>,
}

#[derive(Deserialize)]
struct ClientItem {
    id: u64,
    band_id: u64,
    page_url: String,
    title: String,
    #[serde(rename = "type")]
    item_type: String,
    art_id: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALBUM_HTML: &str = r#"<html>
        <head><script type="text/javascript" data-tralbum="{&quot;id&quot;:42,&quot;url&quot;:&quot;https://x.bandcamp.com/album/foo&quot;,&quot;artist&quot;:&quot;Artist&quot;,&quot;current&quot;:{&quot;title&quot;:&quot;Album&quot;},&quot;album_release_date&quot;:&quot;11 Dec 2025 00:00:00 GMT&quot;,&quot;art_id&quot;:7,&quot;trackinfo&quot;:[{&quot;id&quot;:1,&quot;track_num&quot;:1,&quot;title&quot;:&quot;Song&quot;,&quot;duration&quot;:100.5,&quot;is_downloadable&quot;:true,&quot;file&quot;:{&quot;mp3-128&quot;:&quot;https://t4.bcbits.com/stream/x&amp;y&quot;}},{&quot;id&quot;:null,&quot;track_num&quot;:2,&quot;title&quot;:&quot;Hidden&quot;,&quot;duration&quot;:null,&quot;is_downloadable&quot;:null,&quot;file&quot;:null}]}"></script></head>
        <body><div class="tralbum-tags"><a class="tag" href="https://bandcamp.com/discover/ambient?from=tralbum&artist=1">ambient</a><a class="tag" href="https://bandcamp.com/discover/italy?from=tralbum&artist=1">Italy</a></div></body>
    </html>"#;

    #[test]
    fn parses_album_page() {
        let album = parse_album_page(ALBUM_HTML, "https://x.bandcamp.com/album/foo").unwrap();

        assert_eq!(album.id, 42);
        assert_eq!(album.artist, "Artist");
        assert_eq!(album.title, "Album");
        assert_eq!(album.tracks.len(), 2);
        assert_eq!(album.tracks[0].mp3_128.as_deref(), Some("https://t4.bcbits.com/stream/x&y"));
        assert_eq!(album.tracks[1].id, 0);
        assert_eq!(album.tracks[1].is_downloadable, false);
        assert_eq!(album.tags, vec!["ambient", "italy"]);
    }

    #[test]
    fn decodes_url_encoded_tags() {
        let html = r#"<html>
            <body><div class="tralbum-tags"><a class="tag" href="https://bandcamp.com/tag/sascha_m%C3%BCller">Sascha Müller</a><a class="tag" href="https://bandcamp.com/discover/acid_techno">Acid Techno</a></div></body>
        </html>"#;

        let document = Html::parse_document(html);
        assert_eq!(scrape_tags(&document), vec!["sascha_müller", "acid_techno"]);
    }

    #[test]
    fn has_any_tag_matches_slug_variants() {
        let mut album = Album::default();
        album.tags = vec!["sascha_müller".into(), "acid_techno".into()];

        assert!(album.has_any_tag(&["sascha-müller".into()]));
        assert!(album.has_any_tag(&["ACID_TECHNO".into()]));
        assert!(!album.has_any_tag(&["house".into(), "minimal".into()]));
    }

    #[test]
    fn parses_release_dates() {
        assert_eq!(release_date_unix("11 Dec 2025 00:00:00 GMT"), Some(1765411200));
        assert_eq!(release_date_unix("13 Mar 2013 00:00:00 GMT"), Some(1363132800));
        assert_eq!(release_date_unix("2026-08-05 00:00:00 UTC"), Some(1785888000));
        assert_eq!(release_date_unix("1970-01-01 00:00:00 UTC"), Some(0));
        assert_eq!(release_date_unix("garbage"), None);
        assert_eq!(release_date_unix(""), None);
    }

    #[test]
    fn artist_blacklist_matches_host() {
        let album_url = "https://derekchronicbaranekofficial.bandcamp.com/album/foo";

        assert!(is_artist_blacklisted(
            album_url,
            &["https://derekchronicbaranekofficial.bandcamp.com".into()]
        ));
        assert!(is_artist_blacklisted(
            album_url,
            &["derekchronicbaranekofficial.bandcamp.com/".into()]
        ));
        assert!(!is_artist_blacklisted(album_url, &["other.bandcamp.com".into()]));
    }

    #[test]
    fn normalize_tag_slugs() {
        assert_eq!(normalize_tag("electronic"), "electronic");
        assert_eq!(normalize_tag("  Electronic "), "electronic");
        assert_eq!(normalize_tag("Drum & Bass"), "drum-and-bass");
        assert_eq!(normalize_tag("electronic music"), "electronic-music");
        assert_eq!(normalize_tag("Electronic  Music"), "electronic-music");
        assert_eq!(normalize_tag("R&B"), "r-and-b");
        assert_eq!(normalize_tag("rnb"), "rnb");
        assert_eq!(normalize_tag("post-rock"), "post-rock");
        assert_eq!(normalize_tag("dungeon synth"), "dungeon-synth");
        assert_eq!(normalize_tag("90s"), "90s");
        assert_eq!(normalize_tag("--drone--"), "drone");
    }

    #[test]
    fn parses_artist_releases() {
        let html = r#"<html><body>
            <ol id="music-grid" data-client-items="[{&quot;id&quot;:11,&quot;band_id&quot;:5,&quot;page_url&quot;:&quot;/album/one&quot;,&quot;title&quot;:&quot;One&quot;,&quot;type&quot;:&quot;album&quot;},{&quot;id&quot;:12,&quot;band_id&quot;:5,&quot;page_url&quot;:&quot;/track/two&quot;,&quot;title&quot;:&quot;Two&quot;,&quot;type&quot;:&quot;track&quot;}]"></ol>
        </body></html>"#;

        let releases = parse_artist_releases(html, "https://startrail.bandcamp.com").unwrap();

        assert_eq!(releases.len(), 2);
        assert_eq!(releases[0].url, "https://startrail.bandcamp.com/album/one");
        assert_eq!(releases[1].item_type, "track");
        assert_eq!(releases[1].url, "https://startrail.bandcamp.com/track/two");
    }

    #[test]
    fn resolves_urls() {
        assert_eq!(
            resolve_url("https://a.bandcamp.com", "/album/foo"),
            "https://a.bandcamp.com/album/foo"
        );
        assert_eq!(
            resolve_url("https://a.bandcamp.com/music", "/album/foo"),
            "https://a.bandcamp.com/album/foo"
        );
        assert_eq!(
            resolve_url("https://a.bandcamp.com", "https://other.bandcamp.com/album/x"),
            "https://other.bandcamp.com/album/x"
        );
    }

    #[test]
    fn parses_discover_api_response() {
        // A minimal snippet of the real /api/discover/1/discover_web response
        // for the `noise` tag. Bandcamp switched the discover API from
        // snake_case field names (item_id) to camelCase (itemId, releaseDate,
        // trackCount, primaryImage.imageId), which is why the old struct
        // raised "missing field `item_id`".
        let json = r#"{
            "results": [{
                "itemId": 2708466368,
                "itemType": "a",
                "resultType": "a",
                "title": "Test Album",
                "itemUrl": "https://example.bandcamp.com/album/test?from=discover_page",
                "primaryImage": {"imageId": 3998952672, "isArt": true},
                "bandId": 1206223482,
                "bandName": "Test Band",
                "releaseDate": "2026-11-07 00:00:00 UTC",
                "trackCount": 2
            }]
        }"#;

        let response: DiscoverResponse = serde_json::from_str(json).unwrap();
        let result = &response.results[0];

        assert_eq!(result.item_id, 2708466368);
        assert_eq!(result.band_id, 1206223482);
        assert_eq!(result.band_name, "Test Band");
        assert_eq!(result.title, "Test Album");
        assert_eq!(
            result.item_url,
            "https://example.bandcamp.com/album/test?from=discover_page"
        );
        assert_eq!(result.release_date.as_deref(), Some("2026-11-07 00:00:00 UTC"));
        assert_eq!(result.track_count, Some(2));
        assert_eq!(
            result.primary_image.as_ref().map(|image| image.image_id),
            Some(3998952672)
        );
    }

    #[test]
    fn builds_discover_request_and_parses_hardcoded_album() {
        let request = discover_payload(&["noise".to_string()], "new");

        assert_eq!(request["category_id"], 0);
        assert_eq!(request["tag_norm_names"], serde_json::json!(["noise"]));
        assert_eq!(request["slice"], "new");
        assert_eq!(request["size"], 40);
        assert_eq!(request["include_result_types"], serde_json::json!(["a"]));

        let response = serde_json::from_str::<DiscoverResponse>(
            r#"{
                "results": [{
                    "itemId": 2708466368,
                    "itemType": "a",
                    "resultType": "a",
                    "title": "Test Album",
                    "itemUrl": "https://example.bandcamp.com/album/test?from=discover_page",
                    "primaryImage": {"imageId": 3998952672, "isArt": true},
                    "bandId": 1206223482,
                    "bandName": "Test Band",
                    "releaseDate": "2026-11-07 00:00:00 UTC",
                    "trackCount": 2
                }]
            }"#,
        )
        .unwrap();

        let album = response.results.into_iter().next().unwrap();
        assert_eq!(album.item_id, 2708466368);
        assert_eq!(album.title, "Test Album");
        assert_eq!(album.band_name, "Test Band");
        assert_eq!(album.item_url, "https://example.bandcamp.com/album/test?from=discover_page");
        assert_eq!(album.track_count, Some(2));
    }
}

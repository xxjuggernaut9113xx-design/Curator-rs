//! Provider-normalized discovery search.
//!
//! Curator never downloads search results itself. Providers return source or
//! gallery URLs and the selected compatible URLs are handed to the established
//! gallery-dl source queue in `routes::sources`.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::routes::media::db_err;
use crate::AppState;

pub use crate::services::discovery::{ProviderDescriptor, ProviderRegistry};

fn descriptor(
    id: &str,
    name: &str,
    capabilities: &[&str],
    authentication_required: bool,
    availability: &str,
    result_types: &[&str],
    search_template: Option<&str>,
) -> ProviderDescriptor {
    let direct_url_only = id != "local" && !capabilities.contains(&"search");
    ProviderDescriptor {
        id: id.into(),
        name: name.into(),
        capabilities: capabilities.iter().map(|value| (*value).into()).collect(),
        authentication_required,
        availability: if direct_url_only {
            "direct_url_only".into()
        } else {
            availability.into()
        },
        generated: false,
        curated: true,
        result_types: result_types.iter().map(|value| (*value).into()).collect(),
        search_template: (!direct_url_only)
            .then(|| search_template.map(str::to_owned))
            .flatten(),
    }
}

/// A curated baseline intentionally exceeds forty adapters.  Many are
/// download-only until gallery-dl exposes a query/tag extractor for that
/// version, but they remain visible and their capability status is explicit.
fn curated_providers() -> Vec<ProviderDescriptor> {
    let page = &["page", "download"];
    // Balbums, Kemono, and Coomer have real query adapters in this build.
    // Every other advertised extractor remains discoverable, but is explicitly
    // direct URL-only rather than pretending a free-text search exists.
    let searchable = &["page", "download", "direct_url"];
    vec![
        descriptor(
            "local",
            "Curator library",
            &["catalog"],
            false,
            "available",
            &["creator", "album", "post"],
            None,
        ),
        descriptor(
            "balbums",
            "Balbums / Bunkr",
            &["search", "page", "download"],
            false,
            "available",
            &["album", "collection"],
            Some("https://balbums.st/?search={QUERY}"),
        ),
        descriptor("bunkr", "Bunkr", page, false, "available", &["album"], None),
        descriptor(
            "kemono",
            "Kemono",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://kemono.su/{QUERY}"),
        ),
        descriptor(
            "erome",
            "Erome",
            searchable,
            false,
            "available",
            &["album", "creator"],
            Some("https://www.erome.com/search?q={QUERY}"),
        ),
        descriptor(
            "redgifs",
            "Redgifs",
            searchable,
            false,
            "available",
            &["post", "creator"],
            Some("https://www.redgifs.com/browse/{QUERY}"),
        ),
        descriptor(
            "deviantart",
            "DeviantArt",
            searchable,
            true,
            "authentication_optional",
            &["creator", "album", "post"],
            Some("https://www.deviantart.com/search?q={QUERY}"),
        ),
        descriptor(
            "pixiv",
            "Pixiv",
            searchable,
            true,
            "authentication_optional",
            &["creator", "tag", "post"],
            Some("https://www.pixiv.net/en/tags/{QUERY}/artworks"),
        ),
        descriptor(
            "twitter",
            "X / Twitter",
            searchable,
            true,
            "authentication_optional",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "bluesky",
            "Bluesky",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://bsky.app/search?q={QUERY}"),
        ),
        descriptor(
            "mastodon",
            "Mastodon",
            searchable,
            false,
            "experimental",
            &["creator", "post", "tag"],
            None,
        ),
        descriptor(
            "instagram",
            "Instagram",
            page,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "tumblr",
            "Tumblr",
            searchable,
            false,
            "available",
            &["creator", "tag", "post"],
            Some("https://www.tumblr.com/search/{QUERY}"),
        ),
        descriptor(
            "reddit",
            "Reddit",
            searchable,
            false,
            "available",
            &["creator", "tag", "post"],
            Some("https://www.reddit.com/search/?q={QUERY}"),
        ),
        descriptor(
            "imgur",
            "Imgur",
            searchable,
            false,
            "available",
            &["album", "post"],
            Some("https://imgur.com/search?q={QUERY}"),
        ),
        descriptor(
            "flickr",
            "Flickr",
            searchable,
            false,
            "available",
            &["creator", "album", "post"],
            Some("https://www.flickr.com/search/?text={QUERY}"),
        ),
        descriptor(
            "pinterest",
            "Pinterest",
            searchable,
            true,
            "authentication_optional",
            &["creator", "board", "post"],
            None,
        ),
        descriptor(
            "artstation",
            "ArtStation",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://www.artstation.com/search?q={QUERY}"),
        ),
        descriptor(
            "behance",
            "Behance",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://www.behance.net/search/projects?search={QUERY}"),
        ),
        descriptor(
            "fanbox",
            "FANBOX",
            page,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "fantia",
            "Fantia",
            page,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "patreon",
            "Patreon",
            page,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "coomer",
            "Coomer",
            searchable,
            false,
            "experimental",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "danbooru",
            "Danbooru",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://danbooru.donmai.us/posts?tags={TAG}"),
        ),
        descriptor(
            "gelbooru",
            "Gelbooru",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://gelbooru.com/index.php?page=post&s=list&tags={TAG}"),
        ),
        descriptor(
            "safebooru",
            "Safebooru",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://safebooru.org/index.php?page=post&s=list&tags={TAG}"),
        ),
        descriptor(
            "yandere",
            "yande.re",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://yande.re/post?tags={TAG}"),
        ),
        descriptor(
            "konachan",
            "Konachan",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://konachan.com/post?tags={TAG}"),
        ),
        descriptor(
            "e621",
            "e621",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://e621.net/posts?tags={TAG}"),
        ),
        descriptor(
            "e926",
            "e926",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://e926.net/posts?tags={TAG}"),
        ),
        descriptor(
            "rule34",
            "Rule 34",
            searchable,
            false,
            "available",
            &["tag", "post"],
            Some("https://rule34.xxx/index.php?page=post&s=list&tags={TAG}"),
        ),
        descriptor(
            "sankaku",
            "Sankaku",
            searchable,
            true,
            "authentication_optional",
            &["tag", "post"],
            None,
        ),
        descriptor(
            "nhentai",
            "nhentai",
            searchable,
            false,
            "available",
            &["tag", "album"],
            Some("https://nhentai.net/search/?q={QUERY}"),
        ),
        descriptor(
            "hentaifoundry",
            "Hentai Foundry",
            page,
            true,
            "authentication_optional",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "newgrounds",
            "Newgrounds",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://www.newgrounds.com/search/conduct/art?terms={QUERY}"),
        ),
        descriptor(
            "weasyl",
            "Weasyl",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://www.weasyl.com/search?q={QUERY}"),
        ),
        descriptor(
            "inkbunny",
            "Inkbunny",
            searchable,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "furaffinity",
            "Fur Affinity",
            page,
            true,
            "authentication_required",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "smugmug",
            "SmugMug",
            page,
            false,
            "available",
            &["album", "post"],
            None,
        ),
        descriptor(
            "vsco",
            "VSCO",
            page,
            false,
            "available",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "500px",
            "500px",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://500px.com/search?q={QUERY}"),
        ),
        descriptor(
            "gofile",
            "GoFile",
            page,
            false,
            "available",
            &["album"],
            None,
        ),
        descriptor("mega", "MEGA", page, false, "available", &["album"], None),
        descriptor(
            "telegram",
            "Telegram",
            page,
            false,
            "experimental",
            &["creator", "post"],
            None,
        ),
        descriptor(
            "youtube",
            "YouTube",
            searchable,
            false,
            "available",
            &["creator", "playlist", "post"],
            Some("https://www.youtube.com/results?search_query={QUERY}"),
        ),
        descriptor(
            "vimeo",
            "Vimeo",
            searchable,
            false,
            "available",
            &["creator", "post"],
            Some("https://vimeo.com/search?q={QUERY}"),
        ),
        descriptor(
            "soundcloud",
            "SoundCloud",
            searchable,
            false,
            "available",
            &["creator", "playlist"],
            Some("https://soundcloud.com/search?q={QUERY}"),
        ),
    ]
}

fn generated_provider_templates(extractors: &str) -> Vec<ProviderDescriptor> {
    let mut output = Vec::new();
    let mut seen = HashSet::new();
    for line in extractors.lines() {
        let lower = line.to_ascii_lowercase();
        if !(lower.contains("query")
            || lower.contains("{query}")
            || lower.contains("<query>")
            || lower.contains("tag"))
        {
            continue;
        }
        let raw = line.split_whitespace().next().unwrap_or_default();
        let id = raw
            .trim_matches(|ch: char| !ch.is_ascii_alphanumeric() && ch != '-' && ch != '_')
            .to_ascii_lowercase();
        if id.is_empty() || !seen.insert(id.clone()) {
            continue;
        }
        // Keep the extractor identity as the provider id.  This lets a
        // curated adapter with the same id replace the generated template
        // when gallery-dl learns a new search extractor or changes its list.
        output.push(ProviderDescriptor {
            id: id.clone(),
            name: format!("gallery-dl: {id}"),
            capabilities: vec!["page".into(), "download".into(), "direct_url".into()],
            authentication_required: false,
            availability: "direct_url_only".into(),
            generated: true,
            curated: false,
            result_types: vec!["post".into(), "tag".into()],
            // A gallery-dl extractor listing is useful for discovery but not
            // proof that Curator has a safe, normalized query adapter. Keep
            // generated providers direct-URL-only until an adapter exists.
            search_template: None,
        });
    }
    output
}

/// Build once at startup. gallery-dl's extractor list is version dependent;
/// generated templates supplement, but never overwrite, curated adapters.
pub fn default_provider_registry() -> ProviderRegistry {
    ProviderRegistry {
        providers: curated_providers(),
        gallery_dl_version: None,
    }
}

pub fn build_provider_registry(gallery_dl_bin: &str) -> ProviderRegistry {
    let version = crate::process::output_timeout(
        crate::process::blocking_command(gallery_dl_bin).arg("--version"),
        Duration::from_secs(3),
    )
    .ok()
    .filter(|output| output.status.success())
    .map(|output| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .next()
            .unwrap_or_default()
            .trim()
            .to_string()
    })
    .filter(|value| !value.is_empty());
    let extractors = crate::process::output_timeout(
        crate::process::blocking_command(gallery_dl_bin).arg("--list-extractors"),
        Duration::from_secs(5),
    )
    .ok()
    .filter(|output| output.status.success())
    .map(|output| String::from_utf8_lossy(&output.stdout).to_string())
    .unwrap_or_default();
    let mut providers = curated_providers();
    let curated_ids = providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<HashSet<_>>();
    providers.extend(
        generated_provider_templates(&extractors)
            .into_iter()
            .filter(|provider| !curated_ids.contains(&provider.id)),
    );
    ProviderRegistry {
        providers,
        gallery_dl_version: version,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub creator: Option<String>,
    pub thumbnail: Option<String>,
    pub source: String,
    pub source_url: String,
    pub provider: String,
    pub result_type: String,
    pub item_count: Option<i64>,
    pub date: Option<String>,
    pub gallery_dl_compatible: bool,
    /// Set only by Curator's adapters after the URL has passed the page-URL
    /// guard. CDN/media file URLs are preview-only.
    #[serde(default)]
    pub gallery_dl_validated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance: Option<i64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct SearchQuery {
    #[serde(alias = "q")]
    pub query: Option<String>,
    pub provider: Option<String>,
    /// Comma-separated multi-select. `provider` remains compatible with
    /// older callers and is merged when both fields are present.
    pub providers: Option<String>,
    pub result_type: Option<String>,
    pub sort: Option<String>,
    pub limit: Option<usize>,
}

/// Provider implementations are intentionally backend-only. The UI receives
/// only the common `SearchResult` shape and does not accumulate one-off
/// site-specific controls or downloader code.
trait SearchProvider: Send + Sync {
    /// Local catalog work is deliberately synchronous: a rusqlite connection
    /// must never be held across a network await in an Axum handler.
    fn search_catalog(
        &self,
        _conn: &Connection,
        _query: &str,
    ) -> rusqlite::Result<Vec<SearchResult>> {
        Ok(Vec::new())
    }
}

struct CuratorCatalogProvider;

impl SearchProvider for CuratorCatalogProvider {
    fn search_catalog(
        &self,
        conn: &Connection,
        query: &str,
    ) -> rusqlite::Result<Vec<SearchResult>> {
        let needle = format!("%{}%", query.trim());
        let mut results = Vec::new();
        let mut sources = conn.prepare(
            "SELECT s.name,s.url,s.item_count,s.added_at,
                    (SELECT sm.creator FROM source_metadata sm
                     JOIN media m ON m.id=sm.media_id WHERE m.source_id=s.id
                     ORDER BY sm.id DESC LIMIT 1) AS creator
             FROM sources s WHERE s.name LIKE ?1 COLLATE NOCASE OR s.url LIKE ?1 COLLATE NOCASE
             ORDER BY s.item_count DESC,s.id DESC LIMIT 80",
        )?;
        for result in sources
            .query_map([&needle], |row| {
                Ok(SearchResult {
                    title: row.get(0)?,
                    creator: row.get(4)?,
                    thumbnail: None,
                    source: "Curator library".to_string(),
                    source_url: row.get(1)?,
                    provider: "local".to_string(),
                    result_type: "album".to_string(),
                    item_count: row.get(2)?,
                    date: row.get(3)?,
                    gallery_dl_compatible: true,
                    gallery_dl_validated: true,
                    relevance: Some(100),
                })
            })?
            .flatten()
        {
            results.push(result);
        }

        let mut creators = conn.prepare(
            "SELECT sm.creator,COUNT(DISTINCT m.id),MIN(s.url),MAX(sm.captured_at)
             FROM source_metadata sm JOIN media m ON m.id=sm.media_id
             JOIN sources s ON s.id=m.source_id
             WHERE sm.creator IS NOT NULL AND sm.creator<>''
               AND sm.creator LIKE ?1 COLLATE NOCASE
             GROUP BY sm.creator ORDER BY COUNT(DISTINCT m.id) DESC,sm.creator COLLATE NOCASE LIMIT 80",
        )?;
        for result in creators
            .query_map([&needle], |row| {
                let title: String = row.get(0)?;
                Ok(SearchResult {
                    creator: Some(title.clone()),
                    title,
                    thumbnail: None,
                    source: "Curator library".to_string(),
                    source_url: row.get(2)?,
                    provider: "local".to_string(),
                    result_type: "creator".to_string(),
                    item_count: row.get(1)?,
                    date: row.get(3)?,
                    gallery_dl_compatible: true,
                    gallery_dl_validated: true,
                    relevance: Some(90),
                })
            })?
            .flatten()
        {
            results.push(result);
        }
        Ok(results)
    }
}

const BALBUMS_MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

static BALBUMS_CARD: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<a\s+href="(?P<url>[^"]+)"(?P<attrs>[^>]*)>(?P<body>.*?)</a>"#)
        .expect("valid balbums card matcher")
});
static BALBUMS_TITLE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<h3[^>]*>(?P<title>.*?)</h3>"#).expect("valid balbums title matcher")
});
static BALBUMS_IMAGE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)<img\s+[^>]*src="(?P<url>[^"]+)"[^>]*>"#)
        .expect("valid balbums image matcher")
});
static BALBUMS_COUNT: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?i)>\s*(?P<count>\d+)\s+files?\s*</span>"#)
        .expect("valid balbums file-count matcher")
});
static HTML_TAGS: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<[^>]+>").expect("valid HTML tag matcher"));

fn is_balbums_url(value: &str) -> bool {
    value
        .trim()
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_ascii_lowercase()
        .starts_with("balbums.st/")
}

fn is_bunkr_album_url(value: &str) -> bool {
    let Some((_, authority_and_path)) = value.split_once("://") else {
        return false;
    };
    let mut parts = authority_and_path.split('/');
    let host = parts
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default();
    let path = parts.collect::<Vec<_>>().join("/");
    (host.eq_ignore_ascii_case("bunkr.cr") || host.to_ascii_lowercase().ends_with(".bunkr.cr"))
        && path.starts_with("a/")
}

fn html_text(value: &str) -> String {
    HTML_TAGS
        .replace_all(value, " ")
        .replace("&amp;", "&")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn parse_balbums_html(html: &str) -> Vec<SearchResult> {
    let mut results = Vec::new();
    for card in BALBUMS_CARD.captures_iter(html) {
        let url = card
            .name("url")
            .map(|value| value.as_str())
            .unwrap_or_default();
        let attrs = card
            .name("attrs")
            .map(|value| value.as_str())
            .unwrap_or_default();
        if !attrs.to_ascii_lowercase().contains("card") || !is_bunkr_album_url(url) {
            continue;
        }
        let body = card
            .name("body")
            .map(|value| value.as_str())
            .unwrap_or_default();
        let Some(title) = BALBUMS_TITLE
            .captures(body)
            .and_then(|value| value.name("title"))
            .map(|value| html_text(value.as_str()))
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        // The provider's decorative Bunkr logo appears before a real cover,
        // so prefer the last absolute image in a card.
        let thumbnail = BALBUMS_IMAGE
            .captures_iter(body)
            .filter_map(|value| value.name("url").map(|url| url.as_str()))
            .filter(|url| url.starts_with("https://") || url.starts_with("http://"))
            .last()
            .map(ToOwned::to_owned);
        let item_count = BALBUMS_COUNT
            .captures(body)
            .and_then(|value| value.name("count"))
            .and_then(|value| value.as_str().parse::<i64>().ok());
        results.push(SearchResult {
            title,
            creator: None,
            thumbnail,
            source: "balbums.st".to_string(),
            source_url: url.to_string(),
            provider: "balbums".to_string(),
            result_type: "album".to_string(),
            item_count,
            date: None,
            gallery_dl_compatible: true,
            gallery_dl_validated: true,
            relevance: Some(80),
        });
    }
    results
}

async fn search_balbums(query: &str) -> Result<Vec<SearchResult>, String> {
    let trimmed = query.trim();
    if is_balbums_url(trimmed) {
        let source_url = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
            trimmed.to_string()
        } else {
            format!("https://{trimmed}")
        };
        return Ok(vec![SearchResult {
            title: "balbums.st collection".to_string(),
            creator: None,
            thumbnail: None,
            source: "balbums.st".to_string(),
            source_url,
            provider: "balbums".to_string(),
            result_type: "collection".to_string(),
            item_count: None,
            date: None,
            // An index URL must be resolved to its source page before it can
            // enter gallery-dl. The UI keeps it available for preview/open.
            gallery_dl_compatible: false,
            gallery_dl_validated: false,
            relevance: Some(100),
        }]);
    }
    // URLs belong to the direct gallery-dl provider. Do not send them to an
    // unrelated index search endpoint.
    if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
        return Ok(Vec::new());
    }
    let url = format!(
        "https://balbums.st/?mode=broad&page=1&per=50&search={}&sort=latest",
        urlencoding::encode(trimmed)
    );
    let client = reqwest::Client::builder()
        .user_agent("Curator/0.1 discovery")
        .connect_timeout(std::time::Duration::from_secs(4))
        .timeout(std::time::Duration::from_secs(10))
        .redirect(crate::url_guard::public_redirect_policy())
        .build()
        .map_err(|error| format!("could not initialize balbums provider: {error}"))?;
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| format!("balbums.st is unavailable: {error}"))?
        .error_for_status()
        .map_err(|error| format!("balbums.st search failed: {error}"))?;
    if response
        .content_length()
        .is_some_and(|size| size as usize > BALBUMS_MAX_RESPONSE_BYTES)
    {
        return Err("balbums.st returned an unexpectedly large search response".to_string());
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("reading balbums.st results: {error}"))?;
        if bytes.len().saturating_add(chunk.len()) > BALBUMS_MAX_RESPONSE_BYTES {
            return Err("balbums.st returned an unexpectedly large search response".to_string());
        }
        bytes.extend_from_slice(&chunk);
    }
    let html = String::from_utf8(bytes)
        .map_err(|_| "balbums.st returned non-text search data".to_string())?;
    Ok(parse_balbums_html(&html))
}

// ─── Kemono / Coomer query adapter ───────────────────────────────────────────
// Both sites expose `/api/v1/posts?q=<query>&o=<offset>`, the same endpoint
// gallery-dl's kemono extractor queries. Post records carry creator/account
// metadata (service + user id), the post title, publish date, and file paths,
// so free-text search maps onto result cards without inventing behavior the
// sites do not offer. Domains migrate between suffixes, so each provider tries
// its primary base first and falls back to the secondary one.

const KEMONO_MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const KEMONO_PAGE_SIZE: usize = 50;
const KEMONO_MAX_PAGES: usize = 2;

fn kemono_bases(provider: &str) -> &'static [&'static str] {
    match provider {
        "kemono" => &["https://kemono.su", "https://kemono.cr"],
        "coomer" => &["https://coomer.su", "https://coomer.party"],
        _ => &[],
    }
}

/// Reads a string-or-number JSON value as text. The posts API is loosely
/// typed across site generations.
fn kemono_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// Extracts a file `path` from either an object or a JSON-encoded string.
/// Newer API generations embed file records as strings containing JSON.
fn kemono_file_path(value: &Value) -> Option<String> {
    kemono_file_path_depth(value, 8)
}

/// A `file` entry is either an object with a `path` field or a JSON-encoded
/// string of one. Cap the re-parse depth so a hostile response cannot nest
/// JSON-encoded strings arbitrarily deep and overflow the stack; the 4 MiB
/// response cap bounds input size but not nesting depth.
fn kemono_file_path_depth(value: &Value, depth: u8) -> Option<String> {
    if depth == 0 {
        return None;
    }
    match value {
        Value::Object(_) => value.get("path").and_then(kemono_text),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|parsed| kemono_file_path_depth(&parsed, depth - 1)),
        _ => None,
    }
}

fn parse_kemono_posts(base: &str, provider: &str, value: &Value) -> Vec<SearchResult> {
    // The live endpoint wraps results as {"posts": [...], "count": N};
    // tolerate a bare array too for older generations and tests.
    let posts = value
        .as_array()
        .cloned()
        .or_else(|| value.get("posts").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    posts
        .iter()
        .filter_map(|post| {
            let id = kemono_text(post.get("id")?)?;
            let user = kemono_text(post.get("user")?)?;
            let service = post
                .get("service")
                .and_then(|service| service.as_str())
                .unwrap_or("post")
                .to_string();
            let title = post
                .get("title")
                .and_then(|title| title.as_str())
                .filter(|title| !title.trim().is_empty())
                .map(|title| title.trim().to_string())
                .unwrap_or_else(|| "Untitled post".to_string());
            let mut file_count = 0;
            if post.get("file").and_then(kemono_file_path).is_some() {
                file_count += 1;
            }
            if let Some(attachments) = post.get("attachments").and_then(|value| value.as_array()) {
                file_count += attachments
                    .iter()
                    .filter(|attachment| kemono_file_path(attachment).is_some())
                    .count();
            }
            Some(SearchResult {
                title,
                creator: Some(format!("{service}/{user}")),
                thumbnail: None,
                source: base
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    .to_string(),
                source_url: format!("{base}/{service}/user/{user}/post/{id}"),
                provider: provider.to_string(),
                result_type: "post".to_string(),
                item_count: (file_count > 0).then_some(file_count as i64),
                date: post.get("published").and_then(kemono_text),
                // gallery-dl's kemono extractor consumes exactly these
                // canonical post URLs.
                gallery_dl_compatible: true,
                gallery_dl_validated: true,
                relevance: None,
            })
        })
        .collect()
}

async fn kemono_fetch_page(
    client: &reqwest::Client,
    base: &str,
    provider: &str,
    query: &str,
    offset: usize,
) -> Result<Vec<SearchResult>, String> {
    let url = format!(
        "{base}/api/v1/posts?q={}&o={offset}",
        urlencoding::encode(query)
    );
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|error| format!("{provider} search request failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("{provider} search failed: {error}"))?;
    if response
        .content_length()
        .is_some_and(|size| size as usize > KEMONO_MAX_RESPONSE_BYTES)
    {
        return Err(format!(
            "{provider} returned an unexpectedly large search response"
        ));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| format!("reading {provider} results: {error}"))?;
        if bytes.len().saturating_add(chunk.len()) > KEMONO_MAX_RESPONSE_BYTES {
            return Err(format!(
                "{provider} returned an unexpectedly large search response"
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| format!("{provider} returned malformed search data"))?;
    Ok(parse_kemono_posts(base, provider, &value))
}

async fn search_kemono_like(provider: &str, query: &str) -> Result<Vec<SearchResult>, String> {
    let trimmed = query.trim();
    // Verified page URLs keep the direct-URL path.
    if let Some(result) = provider_direct_url_result(provider, trimmed) {
        return Ok(vec![result]);
    }
    // URLs belong to the direct gallery-dl provider. Do not send them to an
    // unrelated index search endpoint.
    if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
        return Ok(Vec::new());
    }
    let bases = kemono_bases(provider);
    if bases.is_empty() {
        return Err("Search is not available for this provider".to_string());
    }
    let client = reqwest::Client::builder()
        .user_agent("Curator/0.1 discovery")
        .connect_timeout(std::time::Duration::from_secs(4))
        .timeout(std::time::Duration::from_secs(10))
        .redirect(crate::url_guard::public_redirect_policy())
        .build()
        .map_err(|error| format!("could not initialize {provider} provider: {error}"))?;
    let mut last_error = String::new();
    for base in bases {
        let mut results = Vec::new();
        let mut failed = false;
        for page in 0..KEMONO_MAX_PAGES {
            match kemono_fetch_page(&client, base, provider, trimmed, page * KEMONO_PAGE_SIZE).await
            {
                Ok(posts) => {
                    let full_page = posts.len() >= KEMONO_PAGE_SIZE;
                    results.extend(posts);
                    if !full_page {
                        break;
                    }
                }
                Err(error) => {
                    last_error = error;
                    failed = true;
                    break;
                }
            }
        }
        if !failed {
            // Rank earlier, more relevant pages first.
            for (index, result) in results.iter_mut().enumerate() {
                result.relevance = Some(100i64.saturating_sub(index as i64));
            }
            return Ok(results);
        }
    }
    Err(if last_error.is_empty() {
        format!("{provider} search is unavailable")
    } else {
        last_error
    })
}

/// A gallery/source page has a stable host and a non-media path. This is a
/// deliberately conservative server-side guard: CDN URLs, direct image/video
/// files, data URLs, and malformed URLs remain preview-only even if a client
/// posts a forged `gallery_dl_compatible` flag back to us.
fn is_verified_page_url(value: &str) -> bool {
    if crate::url_guard::normalize_public_http_url(value).is_err() {
        return false;
    }
    let value = value.trim();
    let Some((scheme, rest)) = value.split_once("://") else {
        return false;
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "https" | "http") {
        return false;
    }
    let mut parts = rest.splitn(2, '/');
    let host = parts
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if host.is_empty()
        || host.starts_with("cdn.")
        || host.starts_with("media.")
        || host.starts_with("img.")
        || host.contains("image-cdn")
    {
        return false;
    }
    let path = parts
        .next()
        .unwrap_or_default()
        .split('?')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let direct_extension = [
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".mp4", ".webm", ".mkv", ".mov", ".m4v",
    ];
    !direct_extension
        .iter()
        .any(|extension| path.ends_with(extension))
}

fn direct_url_result(query: &str) -> Option<SearchResult> {
    let value = query.trim();
    if !(value.starts_with("https://") || value.starts_with("http://")) {
        return None;
    }
    let host = value
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(value)
        .split('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if host.is_empty() {
        return None;
    }
    Some(SearchResult {
        title: value.to_string(),
        creator: None,
        thumbnail: None,
        source: host.clone(),
        source_url: value.to_string(),
        provider: "gallery-dl".to_string(),
        result_type: "post".to_string(),
        item_count: None,
        date: None,
        gallery_dl_compatible: !host.ends_with("balbums.st") && is_verified_page_url(value),
        gallery_dl_validated: is_verified_page_url(value),
        relevance: Some(110),
    })
}

/// GET /api/search/providers.  The registry is built at startup from the
/// installed gallery-dl version plus Curator's curated overlay.
pub async fn providers(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(match crate::services::discovery::providers(&state) {
        Ok(catalog) => serde_json::to_value(catalog).unwrap_or_default(),
        Err(error) => json!({"error":error}),
    })
}

fn requested_provider_ids(query: &SearchQuery, defaults: &[String]) -> Vec<String> {
    let mut values = Vec::new();
    for raw in query
        .providers
        .as_deref()
        .into_iter()
        .chain(query.provider.as_deref())
    {
        values.extend(
            raw.split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase()),
        );
    }
    if values.is_empty() {
        values.extend(defaults.iter().cloned());
    }
    let mut seen = HashSet::new();
    values
        .into_iter()
        .map(|value| {
            if value == "gallery-dl" {
                "local".to_string()
            } else {
                value
            }
        })
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn provider_direct_url_result(provider: &str, query: &str) -> Option<SearchResult> {
    if !is_verified_page_url(query) {
        return None;
    }
    let host = query
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(query)
        .split('/')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    Some(SearchResult {
        title: query.to_string(),
        creator: None,
        thumbnail: None,
        source: host,
        source_url: query.to_string(),
        provider: provider.to_string(),
        result_type: "post".to_string(),
        item_count: None,
        date: None,
        gallery_dl_compatible: true,
        gallery_dl_validated: true,
        relevance: Some(105),
    })
}

async fn search_remote_provider(
    provider: String,
    text: String,
) -> Result<Vec<SearchResult>, String> {
    match provider.as_str() {
        "balbums" => search_balbums(&text).await,
        "kemono" => search_kemono_like("kemono", &text).await,
        "coomer" => search_kemono_like("coomer", &text).await,
        // These curated adapters safely accept known page URLs right now.
        // Search/query support depends on gallery-dl version and site access;
        // return a useful per-provider error rather than attempting a fake
        // universal API request.
        "erome" | "redgifs" | "deviantart" => {
            if let Some(result) = provider_direct_url_result(&provider, &text) {
                Ok(vec![result])
            } else {
                Err("This provider needs a gallery/page URL or an enabled version-specific search adapter".to_string())
            }
        }
        _ => Err(
            "Search is not available for this provider in the installed gallery-dl version"
                .to_string(),
        ),
    }
}

pub async fn search(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let text = query.query.clone().unwrap_or_default().trim().to_string();
    if text.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A search query is required"})),
        ));
    }
    if text.len() > 2_000 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Search query is too long"})),
        ));
    }
    let defaults = state.settings.read().await.search_providers.clone();
    let requested = requested_provider_ids(&query, &defaults);
    let known = state.search_registry.ids();
    if let Some(unknown) = requested.iter().find(|provider| !known.contains(*provider)) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":format!("Unknown search provider: {unknown}")})),
        ));
    }

    let catalog = CuratorCatalogProvider;
    let mut results = Vec::new();
    let mut provider_errors = Vec::new();
    if requested.iter().any(|provider| provider == "local") {
        let catalog_results = {
            let conn = state.pool.get().map_err(db_err)?;
            catalog.search_catalog(&conn, &text).map_err(db_err)?
        };
        results.extend(catalog_results.into_iter().take(50));
        if let Some(result) = direct_url_result(&text) {
            results.push(result);
        }
    }

    // No more than four remote providers run at once. Every request has its
    // own ten-second wall-clock deadline and returns partial results.
    let remote_ids = requested
        .iter()
        .filter(|provider| provider.as_str() != "local")
        .cloned()
        .collect::<Vec<_>>();
    let remote_outcomes = futures::stream::iter(remote_ids.into_iter().map(|provider| {
        let text = text.clone();
        async move {
            let result = tokio::time::timeout(
                Duration::from_secs(10),
                search_remote_provider(provider.clone(), text),
            )
            .await;
            match result {
                Ok(Ok(results)) => (provider, Ok(results)),
                Ok(Err(error)) => (provider, Err(error)),
                Err(_) => (
                    provider,
                    Err("Search timed out after 10 seconds".to_string()),
                ),
            }
        }
    }))
    .buffer_unordered(4)
    .collect::<Vec<_>>()
    .await;
    for (provider, outcome) in remote_outcomes {
        match outcome {
            Ok(values) => results.extend(values.into_iter().take(50)),
            Err(error) => provider_errors.push(json!({"provider":provider,"error":error})),
        }
    }

    let requested_type = query.result_type.unwrap_or_default().to_ascii_lowercase();
    if !requested_type.is_empty() {
        results.retain(|result| result.result_type == requested_type);
    }
    let mut seen = HashSet::new();
    results.retain(|result| {
        seen.insert((
            result.provider.clone(),
            result.source_url.clone(),
            result.result_type.clone(),
        ))
    });
    match query.sort.as_deref().unwrap_or("relevance") {
        "relevance" => results.sort_by(|a, b| {
            b.relevance
                .cmp(&a.relevance)
                .then_with(|| a.title.cmp(&b.title))
        }),
        "date_desc" => {
            results.sort_by(|a, b| b.date.cmp(&a.date).then_with(|| a.title.cmp(&b.title)))
        }
        "date_asc" => {
            results.sort_by(|a, b| a.date.cmp(&b.date).then_with(|| a.title.cmp(&b.title)))
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown search sort"})),
            ))
        }
    }
    results.truncate(query.limit.unwrap_or(100).clamp(1, 250));
    Ok(Json(
        json!({"results":results,"providers":requested,"provider_errors":provider_errors}),
    ))
}

#[derive(Debug, Deserialize)]
pub struct DownloadSearchResultsBody {
    #[serde(default)]
    pub results: Vec<SearchResult>,
}

pub async fn download_selected(
    State(state): State<Arc<AppState>>,
    Json(body): Json<DownloadSearchResultsBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if body.results.is_empty() || body.results.len() > 500 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Select between one and 500 compatible search results"})),
        ));
    }
    let urls = body
        .results
        .into_iter()
        .filter(|result| {
            result.gallery_dl_compatible
                && result.gallery_dl_validated
                && state.search_registry.descriptor(&result.provider).is_some()
                && is_verified_page_url(&result.source_url)
        })
        .map(|result| result.source_url.trim().to_string())
        .collect::<Vec<_>>();
    if urls.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error":"Selected results must be verified gallery-dl page URLs; CDN/media links are preview-only"}),
            ),
        ));
    }
    let queued = super::sources::create_sources_from_urls(state, urls).await?;
    Ok(Json(
        json!({"queued":queued,"status":"queued_with_gallery_dl"}),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[tokio::test]
    async fn direct_urls_use_existing_source_queue_contract() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let result = search(
            State(state),
            Query(SearchQuery {
                query: Some("https://example.test/post/1".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["results"][0]["provider"], "gallery-dl");
        assert_eq!(result["results"][0]["gallery_dl_compatible"], true);
    }

    #[tokio::test]
    async fn balbums_index_entries_are_not_sent_to_the_downloader_until_resolved() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let result = search(
            State(state),
            Query(SearchQuery {
                query: Some("balbums.st/collection/example".into()),
                provider: Some("balbums".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["results"][0]["gallery_dl_compatible"], false);
    }

    #[test]
    fn balbums_cards_are_normalized_to_gallery_dl_album_results() {
        let html = r#"
            <a href="https://bunkr.cr/a/curator-test" target="_blank" class="card search-card">
              <img src="/assets/bunkr.svg" alt="Bunkr">
              <img src="https://cdn.example.test/cover.jpg" alt="Cover">
              <h3>Example &amp; Gallery</h3>
              <span>42 files</span>
            </a>
        "#;

        let results = parse_balbums_html(html);
        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.title, "Example & Gallery");
        assert_eq!(result.source_url, "https://bunkr.cr/a/curator-test");
        assert_eq!(
            result.thumbnail.as_deref(),
            Some("https://cdn.example.test/cover.jpg")
        );
        assert_eq!(result.item_count, Some(42));
        assert!(result.gallery_dl_compatible);
    }

    #[test]
    fn curated_registry_exposes_at_least_forty_version_aware_adapters() {
        let registry = default_provider_registry();
        assert!(
            registry.providers.len() >= 40,
            "only {} providers",
            registry.providers.len()
        );
        let ids = registry
            .providers
            .iter()
            .map(|provider| provider.id.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(
            ids.len(),
            registry.providers.len(),
            "provider ids must be unique"
        );
        assert!(registry.providers.iter().all(|provider| provider.curated));
        assert!(registry.providers.iter().any(|provider| {
            provider.id == "kemono"
                && provider.availability == "direct_url_only"
                && !provider
                    .capabilities
                    .iter()
                    .any(|capability| capability == "search")
        }));
    }

    #[test]
    fn kemono_posts_are_normalized_to_gallery_dl_post_results() {
        let payload = serde_json::json!([
            {
                "id": "101",
                "user": "alice42",
                "service": "patreon",
                "title": "Example shoot",
                "published": "2026-05-01T12:00:00",
                "file": { "name": "cover.jpg", "path": "/ab/cover.jpg" },
                "attachments": [
                    { "name": "a.mp4", "path": "/ab/a.mp4" },
                    { "name": "broken", "path": null }
                ]
            },
            {
                "id": 202,
                "user": 7,
                "service": "fanbox",
                "title": "  ",
                "file": "{\"name\":\"x.png\",\"path\":\"/xy/x.png\"}",
                "attachments": []
            }
        ]);
        let results = parse_kemono_posts("https://kemono.su", "kemono", &payload);
        assert_eq!(results.len(), 2);
        let first = &results[0];
        assert_eq!(first.title, "Example shoot");
        assert_eq!(first.creator.as_deref(), Some("patreon/alice42"));
        assert_eq!(first.source, "kemono.su");
        assert_eq!(
            first.source_url,
            "https://kemono.su/patreon/user/alice42/post/101"
        );
        assert_eq!(first.result_type, "post");
        assert_eq!(first.item_count, Some(2));
        assert_eq!(first.date.as_deref(), Some("2026-05-01T12:00:00"));
        assert!(first.gallery_dl_compatible);
        assert!(first.gallery_dl_validated);
        // Number-typed ids and JSON-encoded file records still normalize.
        let second = &results[1];
        assert_eq!(second.title, "Untitled post");
        assert_eq!(second.creator.as_deref(), Some("fanbox/7"));
        assert_eq!(second.item_count, Some(1));
    }

    #[test]
    fn kemono_posts_parse_the_live_wrapped_response_shape() {
        // The live /api/v1/posts endpoint wraps results in an object; a
        // bare-array-only parser silently returned zero results.
        let payload = serde_json::json!({
            "count": 50000,
            "true_count": 156492,
            "posts": [
                {
                    "id": "141133143",
                    "user": "80293853",
                    "service": "patreon",
                    "title": "Example post",
                    "published": "2025-10-13T17:13:13",
                    "file": { "name": "a.jpg", "path": "/35/a.jpg" },
                    "attachments": [{ "name": "b.mp4", "path": "/35/b.mp4" }]
                }
            ]
        });
        let results = parse_kemono_posts("https://kemono.cr", "coomer", &payload);
        assert_eq!(results.len(), 1);
        let first = &results[0];
        assert_eq!(first.title, "Example post");
        assert_eq!(first.creator.as_deref(), Some("patreon/80293853"));
        assert_eq!(first.source, "kemono.cr");
        assert_eq!(
            first.source_url,
            "https://kemono.cr/patreon/user/80293853/post/141133143"
        );
        assert_eq!(first.item_count, Some(2));
    }

    #[test]
    fn kemono_posts_without_ids_are_skipped() {
        let payload = serde_json::json!([{ "title": "no id" }]);
        assert!(parse_kemono_posts("https://coomer.su", "coomer", &payload).is_empty());
        assert!(
            parse_kemono_posts("https://coomer.su", "coomer", &serde_json::json!({})).is_empty()
        );
    }

    #[test]
    fn kemono_file_path_rejects_deeply_nested_json_strings() {
        // A hostile response could nest JSON-encoded strings arbitrarily
        // deep; the parser must bail out instead of overflowing the stack.
        // (Each nesting level roughly doubles the fixture size through
        // escaping, so a dozen levels is plenty to exceed the depth cap.)
        let mut nested = serde_json::json!({ "path": "/35/a.jpg" }).to_string();
        for _ in 0..12 {
            nested = serde_json::to_string(&nested).unwrap();
        }
        assert!(kemono_file_path(&serde_json::Value::String(nested)).is_none());

        // One level of JSON-encoded indirection still resolves.
        let single = serde_json::to_string(&serde_json::json!({ "path": "/35/a.jpg" })).unwrap();
        assert_eq!(
            kemono_file_path(&serde_json::Value::String(single)).as_deref(),
            Some("/35/a.jpg")
        );
        // Plain objects still resolve.
        assert_eq!(
            kemono_file_path(&serde_json::json!({ "path": "/35/a.jpg" })).as_deref(),
            Some("/35/a.jpg")
        );
    }

    #[test]
    fn generated_extractors_are_overridden_by_curated_ids() {
        let generated = generated_provider_templates("balbums QUERY\nnewsite TAG\nnoise");
        assert!(generated.iter().any(|provider| provider.id == "balbums"));
        let curated = curated_providers();
        let curated_ids = curated
            .iter()
            .map(|provider| provider.id.clone())
            .collect::<HashSet<_>>();
        let merged = curated
            .into_iter()
            .chain(
                generated
                    .into_iter()
                    .filter(|provider| !curated_ids.contains(&provider.id)),
            )
            .collect::<Vec<_>>();
        assert_eq!(
            merged
                .iter()
                .filter(|provider| provider.id == "balbums")
                .count(),
            1
        );
        assert!(merged
            .iter()
            .any(|provider| provider.id == "newsite" && provider.generated));
    }
}

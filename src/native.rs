//! Native client operations. No sockets or synthetic HTTP requests are used
//! by the local client. Existing operation implementations are reused while
//! their remaining transport types are migrated out of the route modules.
use crate::{routes, AppState};
use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryQuery {
    pub search: Option<String>,
    pub cursor: Option<String>,
    pub media_type: Option<String>,
    pub sort: String,
    pub rating_status: Option<String>,
    pub max_rating: Option<i64>,
    pub source_id: Option<i64>,
    pub group_id: Option<i64>,
    pub tag: Option<String>,
    pub tags: Option<String>,
    pub any_tags: Option<String>,
    pub exclude_tags: Option<String>,
    pub creator: Option<String>,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
    pub unknown_size: Option<bool>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct MediaItem {
    pub id: i64,
    pub filename: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub rating: i64,
    pub source: String,
    pub filepath: String,
    pub playback_filepath: Option<String>,
    pub tags: Vec<String>,
    #[serde(default)]
    pub rating_reviewed_at: Option<String>,
    // Enriched metadata the library service already returns. All optional
    // with defaults so older payloads and the remote Viewer path keep
    // deserializing unchanged.
    #[serde(default)]
    pub duration_secs: Option<f64>,
    #[serde(default)]
    pub auto_rating: Option<i64>,
    #[serde(default)]
    pub human_rating: Option<i64>,
    #[serde(default)]
    pub rating_source: Option<String>,
    #[serde(default)]
    pub rating_reviewed: Option<bool>,
    #[serde(default)]
    pub file_size_bytes: Option<i64>,
    #[serde(default)]
    pub added_at: Option<String>,
    #[serde(default)]
    pub creator: Option<String>,
    #[serde(default)]
    pub downloaded: Option<i64>,
}

#[derive(Deserialize)]
pub struct MediaPage {
    pub media: Vec<MediaItem>,
    pub next_cursor: Option<String>,
}

pub enum Command {
    StartSession,
    Session(crate::session::SessionControl),
    CreateGroup(String),
    MoveToGroup(Vec<i64>, Option<i64>),
    ImportFolder(std::path::PathBuf),
    Rate(Vec<i64>, i64),
    /// Single-item human rating that returns the review record (including
    /// the reviewed-at undo token). Backed by the same typed service as the
    /// bulk rate path.
    RateOne(i64, i64),
    Tag(Vec<i64>, String),
    /// Remove a tag from many media items. Unknown tags and items that do
    /// not carry the tag are reported as failed rather than aborting.
    Untag(Vec<i64>, String),
    Approve(i64),
    UndoRating(i64, String),
    PauseDownloads,
    ResumeDownloads,
    PauseSource(i64),
    ResumeSource(i64),
    AddSources(String),
    ResyncAll,
    DeleteMedia(Vec<i64>),
    RefreshMetadata(Vec<i64>),
    CreateClips {
        media_id: i64,
        seconds: u32,
    },
    UpdateSettings(Value),
    QueueSearchResults(Vec<Value>),
}

/// Read-only data needed by the native Manage workspace. Values preserve the
/// established API payloads while callers use one service operation rather
/// than assembling unguarded route requests themselves.
#[derive(Clone, Serialize, Deserialize)]
pub struct ManageSnapshot {
    pub settings: Value,
    pub storage: Value,
    pub stats: Value,
    pub providers: Value,
    pub remote_access: Value,
    pub log_location: String,
}

pub use crate::services::backup::BackupSnapshot as RecoverySnapshot;

#[derive(Clone)]
pub struct LocalClient {
    state: Arc<AppState>,
}

#[derive(Clone)]
pub enum Client {
    Local(LocalClient),
    Remote(RemoteClient),
}

#[derive(Clone)]
pub struct RemoteClient {
    origin: reqwest::Url,
    http: reqwest::Client,
    permissions: ViewerPermissions,
    instance_id: Option<String>,
}

enum ReconnectError {
    Unavailable(String),
    Changed(String),
}

/// Permissions advertised for the native Viewer by a compatible Host or
/// Server. Missing fields from an older peer grant only read access.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ViewerPermissions {
    pub library_read: bool,
    pub playback: bool,
    pub discovery: bool,
    pub library_edit: bool,
    pub session_control: bool,
}

impl Default for ViewerPermissions {
    fn default() -> Self {
        Self {
            library_read: true,
            playback: true,
            discovery: true,
            library_edit: false,
            session_control: false,
        }
    }
}

impl RemoteClient {
    /// The caller must validate this numeric origin against the connected
    /// Tailscale peer inventory and negotiate Curator's protocol first.
    pub fn from_validated_peer(origin: &str) -> Result<Self, String> {
        Self::from_validated_peer_with_permissions(origin, ViewerPermissions::default())
    }

    pub fn from_validated_peer_with_permissions(
        origin: &str,
        permissions: ViewerPermissions,
    ) -> Result<Self, String> {
        let origin = reqwest::Url::parse(origin).map_err(|e| e.to_string())?;
        let ip = origin
            .host_str()
            .unwrap_or("")
            .trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .map_err(|_| "Expected a validated numeric peer address")?;
        let tailnet = match ip {
            std::net::IpAddr::V4(ip) => {
                let bytes = ip.octets();
                bytes[0] == 100 && bytes[1] & 0xc0 == 0x40
            }
            std::net::IpAddr::V6(ip) => ip.segments()[..3] == [0xfd7a, 0x115c, 0xa1e0],
        };
        if !tailnet
            || origin.scheme() != "http"
            || origin.path() != "/"
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !origin.username().is_empty()
            || origin.password().is_some()
        {
            return Err("Invalid Tailnet origin".into());
        }
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            origin,
            http,
            permissions,
            instance_id: None,
        })
    }

    pub fn from_validated_peer_with_identity(
        origin: &str,
        permissions: ViewerPermissions,
        instance_id: String,
    ) -> Result<Self, String> {
        if instance_id.trim().is_empty() {
            return Err("The Host did not provide a stable library identity".into());
        }
        let mut client = Self::from_validated_peer_with_permissions(origin, permissions)?;
        client.instance_id = Some(instance_id);
        Ok(client)
    }

    pub fn permissions(&self) -> ViewerPermissions {
        self.permissions
    }

    fn url(&self, path: &str) -> Result<reqwest::Url, String> {
        let url = self.origin.join(path).map_err(|e| e.to_string())?;
        if url.origin() != self.origin.origin()
            || !(url.path().starts_with("/api/") || url.path().starts_with("/library/"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err("Refusing a destination outside the connected Curator host".into());
        }
        Ok(url)
    }

    /// Build an ID-based media URL on the pinned Tailnet peer for native playback.
    pub fn media_stream_url(&self, id: i64) -> Result<reqwest::Url, String> {
        if id <= 0 {
            return Err("Invalid media ID".into());
        }
        self.url(&format!("/api/media/{id}/stream"))
    }

    async fn bytes(
        &self,
        method: reqwest::Method,
        url: reqwest::Url,
        body: Option<Value>,
    ) -> Result<Vec<u8>, String> {
        let retry_read = method == reqwest::Method::GET && self.instance_id.is_some();
        let mut response = {
            let mut attempt = 0;
            loop {
                let mut request = self.http.request(method.clone(), url.clone());
                if let Some(body) = &body {
                    request = request
                        .header("content-type", "application/json")
                        .body(body.to_string());
                }
                match request.send().await {
                    Ok(response) => break response,
                    Err(error) if retry_read && attempt < 2 => {
                        loop {
                            attempt += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(250 * attempt))
                                .await;
                            match self.verify_reconnected_host().await {
                                Ok(()) => break,
                                Err(ReconnectError::Changed(reason)) => {
                                    return Err(reason);
                                }
                                Err(ReconnectError::Unavailable(reason)) if attempt == 2 => {
                                    return Err(format!(
                                        "Viewer could not reconnect to the same Host: {reason}; request failed: {error}"
                                    ));
                                }
                                Err(_) => {}
                            }
                        }
                        continue;
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
        };
        if !response.status().is_success() {
            return Err(format!("Host returned {}", response.status()));
        }
        const MAX_BYTES: usize = 32 * 1024 * 1024;
        if response
            .content_length()
            .is_some_and(|n| n > MAX_BYTES as u64)
        {
            return Err("Response exceeds 32 MiB limit".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            if bytes.len() + chunk.len() > MAX_BYTES {
                return Err("Response exceeds 32 MiB limit".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    fn validate_reconnected_info(&self, info: &Value) -> Result<(), String> {
        let expected_id = self
            .instance_id
            .as_deref()
            .ok_or("Viewer has no pinned library identity. Switch Host to reconnect safely.")?;
        if info["instance_id"].as_str() != Some(expected_id)
            || info["api_protocol"].as_str() != Some(crate::API_PROTOCOL)
            || !matches!(info["edition"].as_str(), Some("host" | "server"))
            || info["tailnet_only"].as_bool() != Some(true)
        {
            return Err(
                "The saved Host identity or protocol changed. Switch Host to reconnect safely."
                    .into(),
            );
        }
        let permissions: ViewerPermissions = match info.get("viewer_permissions") {
            Some(value) => serde_json::from_value(value.clone())
                .map_err(|_| "The Host returned invalid Viewer permissions".to_owned())?,
            None => ViewerPermissions::default(),
        };
        if permissions != self.permissions {
            return Err("Viewer permissions changed. Switch Host to renegotiate access.".into());
        }
        Ok(())
    }

    async fn verify_reconnected_host(&self) -> Result<(), ReconnectError> {
        let url = self
            .url("/api/system/info")
            .map_err(ReconnectError::Changed)?;
        let mut response = self
            .http
            .get(url)
            .timeout(std::time::Duration::from_secs(3))
            .send()
            .await
            .map_err(|error| ReconnectError::Unavailable(error.to_string()))?
            .error_for_status()
            .map_err(|error| ReconnectError::Unavailable(error.to_string()))?;
        if response
            .content_length()
            .is_some_and(|bytes| bytes > 64 * 1024)
        {
            return Err(ReconnectError::Changed(
                "Host handshake exceeds size limit".into(),
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| ReconnectError::Unavailable(error.to_string()))?
        {
            if bytes.len() + chunk.len() > 64 * 1024 {
                return Err(ReconnectError::Changed(
                    "Host handshake exceeds size limit".into(),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        let info: Value = serde_json::from_slice(&bytes)
            .map_err(|error| ReconnectError::Changed(error.to_string()))?;
        self.validate_reconnected_info(&info)
            .map_err(ReconnectError::Changed)
    }

    async fn request_method(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, String> {
        let bytes = self.bytes(method, self.url(path)?, body).await?;
        let result: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if let Some(error) = result["error"].as_str() {
            return Err(error.into());
        }
        Ok(result)
    }

    async fn request(&self, path: &str, body: Option<Value>) -> Result<Value, String> {
        let method = if body.is_some() {
            reqwest::Method::POST
        } else {
            reqwest::Method::GET
        };
        self.request_method(method, path, body).await
    }
}

pub struct NativeImage {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

pub struct NavigationItem {
    pub id: i64,
    pub name: String,
    pub group: bool,
    pub media_count: Option<i64>,
    pub depth: usize,
}

fn navigation_items(sources: &Value, groups: &Value, summary: &Value) -> Vec<NavigationItem> {
    #[derive(Clone)]
    struct Node {
        id: i64,
        name: String,
        parent_id: Option<i64>,
        group: bool,
    }
    fn rows(value: &Value, key: &str, parent_key: &str, group: bool) -> Vec<Node> {
        value[key]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|row| {
                Some(Node {
                    id: row["id"].as_i64()?,
                    name: row["name"].as_str()?.to_owned(),
                    parent_id: row[parent_key].as_i64(),
                    group,
                })
            })
            .collect()
    }
    fn append_group(
        group: &Node,
        depth: usize,
        groups: &[Node],
        sources: &[Node],
        summary: &Value,
        visited: &mut std::collections::HashSet<i64>,
        output: &mut Vec<NavigationItem>,
    ) {
        if !visited.insert(group.id) {
            return;
        }
        output.push(navigation_item(group, depth, summary));
        for child in groups
            .iter()
            .filter(|child| child.parent_id == Some(group.id))
        {
            append_group(child, depth + 1, groups, sources, summary, visited, output);
        }
        for source in sources
            .iter()
            .filter(|source| source.parent_id == Some(group.id))
        {
            output.push(navigation_item(source, depth + 1, summary));
        }
    }
    fn navigation_item(node: &Node, depth: usize, summary: &Value) -> NavigationItem {
        let key = if node.group { "groups" } else { "sources" };
        NavigationItem {
            id: node.id,
            name: node.name.clone(),
            group: node.group,
            media_count: summary[key]
                .as_array()
                .and_then(|counts| counts.iter().find(|entry| entry["id"] == node.id))
                .and_then(|entry| entry["items"].as_i64()),
            depth,
        }
    }
    let groups = rows(groups, "groups", "parent_id", true);
    let sources = rows(sources, "sources", "group_id", false);
    let mut output = Vec::with_capacity(groups.len() + sources.len());
    let mut visited = std::collections::HashSet::new();
    for group in groups.iter().filter(|group| {
        group.parent_id.is_none()
            || !groups
                .iter()
                .any(|candidate| Some(candidate.id) == group.parent_id)
    }) {
        append_group(
            group,
            0,
            &groups,
            &sources,
            summary,
            &mut visited,
            &mut output,
        );
    }
    // Legacy cyclic groups have no root; show them once rather than hiding
    // their sources or recursing indefinitely.
    for group in &groups {
        append_group(
            group,
            0,
            &groups,
            &sources,
            summary,
            &mut visited,
            &mut output,
        );
    }
    for source in sources.iter().filter(|source| {
        source.parent_id.is_none()
            || !groups
                .iter()
                .any(|group| Some(group.id) == source.parent_id)
    }) {
        output.push(navigation_item(source, 0, summary));
    }
    output
}

#[derive(Serialize, Deserialize)]
pub struct NativePreferences {
    pub version: u32,
    pub queue: Vec<MediaItem>,
    pub width: u32,
    pub height: u32,
    pub workspace: i32,
    #[serde(default = "default_native_theme")]
    pub theme: String,
    #[serde(default = "default_native_layout")]
    pub layout: String,
    #[serde(flatten)]
    pub extra: std::collections::BTreeMap<String, Value>,
}

fn default_native_theme() -> String {
    "system".into()
}
fn default_native_layout() -> String {
    "grid".into()
}

fn write_preferences(
    path: &std::path::Path,
    preferences: &NativePreferences,
) -> Result<(), String> {
    use std::io::Write;
    let directory = path.parent().ok_or("Invalid preferences path")?;
    std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
    let mut temporary = tempfile::NamedTempFile::new_in(directory).map_err(|e| e.to_string())?;
    serde_json::to_writer(&mut temporary, preferences).map_err(|e| e.to_string())?;
    temporary.flush().map_err(|e| e.to_string())?;
    temporary.as_file().sync_all().map_err(|e| e.to_string())?;
    temporary.persist(path).map_err(|e| e.to_string())?;
    Ok(())
}

impl Default for NativePreferences {
    fn default() -> Self {
        Self {
            version: 1,
            queue: Vec::new(),
            width: 1200,
            height: 800,
            workspace: 0,
            theme: default_native_theme(),
            layout: default_native_layout(),
            extra: Default::default(),
        }
    }
}

impl Client {
    pub fn can_edit_library(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Remote(client) => client.permissions.library_edit,
        }
    }

    pub fn can_control_sessions(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Remote(client) => client.permissions.session_control,
        }
    }

    pub fn can_playback(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Remote(client) => client.permissions.playback,
        }
    }

    pub fn can_discover(&self) -> bool {
        match self {
            Self::Local(_) => true,
            Self::Remote(client) => client.permissions.discovery,
        }
    }

    pub async fn diagnostic_log(&self) -> Result<String, String> {
        match self {
            Self::Local(client) => {
                crate::services::diagnostics::read_tail(&client.state.log_path, 5000)
                    .map_err(|error| error.to_string())
            }
            Self::Remote(_) => Err("Diagnostic logs are available only on Host".into()),
        }
    }

    pub async fn manage_snapshot(&self) -> Result<ManageSnapshot, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.library_read {
                return Err("Viewer role cannot read the library".into());
            }
        }
        match self {
            Self::Local(client) => Ok(ManageSnapshot {
                settings: crate::services::settings::read(
                    &client.state,
                    crate::services::settings::SettingsAudience::Local,
                )
                .await,
                storage: serde_json::to_value(
                    crate::services::storage::dashboard(&client.state, None)
                        .await
                        .map_err(|error| error.message())?,
                )
                .map_err(|error| error.to_string())?,
                stats: response(routes::misc::stats(State(client.state.clone())).await)?,
                providers: serde_json::to_value(
                    crate::services::discovery::providers(&client.state).map_err(str::to_owned)?,
                )
                .map_err(|error| error.to_string())?,
                remote_access: serde_json::to_value(
                    routes::remote::status(State(client.state.clone())).await.0,
                )
                .map_err(|error| error.to_string())?,
                log_location: crate::services::diagnostics::location(&client.state.log_path)
                    .display()
                    .to_string(),
            }),
            Self::Remote(client) => Ok(ManageSnapshot {
                settings: client.request("/api/settings", None).await?,
                storage: client.request("/api/storage", None).await?,
                stats: client.request("/api/stats", None).await?,
                providers: client.request("/api/search/providers", None).await?,
                remote_access: client.request("/api/remote-access", None).await?,
                log_location: "Host diagnostic log (path available on Host only)".into(),
            }),
        }
    }

    pub async fn discover(&self, query: String, provider: Option<String>) -> Result<Value, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.discovery {
                return Err("Viewer role cannot use discovery".into());
            }
        }
        match self {
            Self::Local(client) => response(
                routes::search::search(
                    State(client.state.clone()),
                    Query(routes::search::SearchQuery {
                        query: Some(query),
                        provider,
                        ..Default::default()
                    }),
                )
                .await,
            ),
            Self::Remote(client) => {
                let mut url = client.url("/api/search")?;
                {
                    let mut pairs = url.query_pairs_mut();
                    pairs.append_pair("query", &query);
                    if let Some(provider) =
                        provider.as_deref().filter(|value| !value.trim().is_empty())
                    {
                        pairs.append_pair("provider", provider);
                    }
                }
                let bytes = client.bytes(reqwest::Method::GET, url, None).await?;
                serde_json::from_slice(&bytes).map_err(|error| error.to_string())
            }
        }
    }

    fn preferences_path(&self) -> Result<std::path::PathBuf, String> {
        use sha1::{Digest, Sha1};
        let key = match self {
            Self::Local(client) => format!("host-{}", client.state.instance_id),
            Self::Remote(client) => format!("viewer-{}", client.origin),
        };
        let key = hex::encode(Sha1::digest(key.as_bytes()));
        let directory = if let Some(override_dir) = std::env::var_os("CURATOR_NATIVE_PREFS_DIR") {
            std::path::PathBuf::from(override_dir)
        } else {
            dirs::config_dir()
                .ok_or("User configuration directory is unavailable")?
                .join("Curator")
                .join("native-v1")
        };
        Ok(directory.join(format!("{key}.json")))
    }

    pub fn load_preferences(&self) -> Result<NativePreferences, String> {
        let path = self.preferences_path()?;
        if !path.exists() {
            return Ok(NativePreferences::default());
        }
        if std::fs::metadata(&path).map_err(|e| e.to_string())?.len() > 4 * 1024 * 1024 {
            return Err("Native preferences exceed size limit".into());
        }
        let preferences: NativePreferences =
            serde_json::from_slice(&std::fs::read(path).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        if preferences.version != 1 || preferences.queue.len() > 1000 {
            return Err("Unsupported native preferences format".into());
        }
        Ok(preferences)
    }

    pub fn save_preferences(&self, preferences: &NativePreferences) -> Result<(), String> {
        let path = self.preferences_path()?;
        write_preferences(&path, preferences)
    }
}

impl Client {
    pub async fn export_source_list(&self) -> Result<crate::services::export::SourceList, String> {
        let Self::Local(client) = self else {
            return Err("Source-list export is available only on the Host device".into());
        };
        crate::services::export::source_list(&client.state)
            .await
            .map_err(|error| error.message().to_owned())
    }

    pub async fn import_source_list(
        &self,
        entries: Vec<Value>,
    ) -> Result<crate::services::sources::CreateSourcesResult, String> {
        let Self::Local(client) = self else {
            return Err("Source-list import is available only on the Host device".into());
        };
        crate::services::export::import_source_list(client.state.clone(), entries)
            .await
            .map_err(|error| error.message().to_owned())
    }

    pub async fn recovery_status(&self) -> Result<RecoverySnapshot, String> {
        let Self::Local(client) = self else {
            return Err("Recovery is available only on the Host device".into());
        };
        crate::services::backup::snapshot(&client.state)
            .await
            .map_err(|error| error.message().to_owned())
    }

    /// Best-effort cached thumbnail for a library item. Generates the
    /// thumbnail from the local file on first use and returns its cache
    /// path; `None` for remote viewers, non-image media, or files that
    /// cannot be decoded. Never performs network I/O.
    pub fn thumbnail_path(&self, item: &MediaItem) -> Option<std::path::PathBuf> {
        let Self::Local(client) = self else {
            return None;
        };
        if item.kind != "image" {
            return None;
        }
        let source = std::path::Path::new(&item.filepath);
        crate::thumb_worker::get_or_create_thumb_sync(item.id, source, &client.state.thumbs_dir)
            .ok()?;
        Some(client.state.thumbs_dir.join(format!("{}.jpg", item.id)))
    }

    pub async fn recovery(
        &self,
        request: crate::maintenance::MaintenanceRequest,
    ) -> Result<(), String> {
        let Self::Local(client) = self else {
            return Err("Recovery is available only on the Host device".into());
        };
        // Maintenance owns admission and waits for ordinary workers itself.
        // Holding a background-worker lease here would deadlock that wait.
        crate::services::jobs::start(client.state.clone(), request)
            .await
            .map_err(|error| error.message().to_owned())?;
        Ok(())
    }

    pub async fn session(&self) -> Result<Option<crate::session::SessionState>, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.playback {
                return Err("Viewer role cannot view sessions".into());
            }
        }
        match self {
            Self::Local(client) => Ok(crate::services::session::current(&client.state)),
            Self::Remote(client) => {
                serde_json::from_value(client.request("/api/session", None).await?)
                    .map_err(|e| e.to_string())
            }
        }
    }
    pub async fn navigation(&self) -> Result<Vec<NavigationItem>, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.library_read {
                return Err("Viewer role cannot read the library".into());
            }
        }
        let (sources, groups, summary) = match self {
            Self::Local(client) => (
                response(routes::sources::list(State(client.state.clone())).await)?,
                response(routes::groups::list(State(client.state.clone())).await)?,
                crate::library_summary(&client.state)
                    .await
                    .map_err(|error| error.to_string())?,
            ),
            Self::Remote(client) => (
                client.request("/api/sources", None).await?,
                client.request("/api/groups", None).await?,
                client.request("/api/library/summary", None).await?,
            ),
        };
        Ok(navigation_items(&sources, &groups, &summary))
    }
    pub async fn library(&self, query: LibraryQuery) -> Result<MediaPage, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.library_read {
                return Err("Viewer role cannot read the library".into());
            }
        }
        match self {
            Self::Local(client) => client.library(query).await,
            Self::Remote(client) => {
                let mut url = client.url("/api/media")?;
                for (key, value) in serde_json::to_value(query)
                    .map_err(|e| e.to_string())?
                    .as_object()
                    .ok_or("Invalid query")?
                {
                    if !value.is_null() {
                        url.query_pairs_mut().append_pair(
                            key,
                            &value
                                .as_str()
                                .map(str::to_owned)
                                .unwrap_or_else(|| value.to_string()),
                        );
                    }
                }
                url.query_pairs_mut().append_pair("limit", "100");
                let bytes = client.bytes(reqwest::Method::GET, url, None).await?;
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())
            }
        }
    }

    pub async fn downloads(&self) -> Result<Value, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.library_read {
                return Err("Viewer role cannot read activity".into());
            }
        }
        match self {
            Self::Local(client) => client.downloads().await,
            Self::Remote(client) => client.request("/api/downloads/status", None).await,
        }
    }

    pub async fn execute(&self, command: Command) -> Result<Value, String> {
        match self {
            Self::Local(client) => client.execute(command).await,
            Self::Remote(client) => {
                let permitted = match &command {
                    Command::StartSession | Command::Session(_) => {
                        client.permissions.session_control
                    }
                    Command::CreateGroup(_)
                    | Command::MoveToGroup(_, _)
                    | Command::Rate(_, _)
                    | Command::RateOne(_, _)
                    | Command::Tag(_, _)
                    | Command::Untag(_, _)
                    | Command::Approve(_)
                    | Command::UndoRating(_, _)
                    | Command::CreateClips { .. } => client.permissions.library_edit,
                    // These operations enqueue or control downloads on the
                    // remote library. The read-only Viewer role never gains
                    // them from a local UI affordance.
                    Command::AddSources(_)
                    | Command::ResyncAll
                    | Command::QueueSearchResults(_) => false,
                    Command::PauseDownloads
                    | Command::ResumeDownloads
                    | Command::PauseSource(_)
                    | Command::ResumeSource(_)
                    | Command::ImportFolder(_)
                    | Command::DeleteMedia(_)
                    | Command::RefreshMetadata(_)
                    | Command::UpdateSettings(_) => true,
                };
                if !permitted {
                    return Err("Viewer role does not permit this operation".into());
                }
                let (path, body) = match command {
                    Command::StartSession => (
                        "/api/session/start".into(),
                        serde_json::to_value(crate::session::GameConfig::quick_default())
                            .map_err(|e| e.to_string())?,
                    ),
                    Command::Session(control) => (
                        "/api/session/command".into(),
                        serde_json::to_value(control).map_err(|e| e.to_string())?,
                    ),
                    Command::CreateGroup(name) => ("/api/groups".into(), json!({"name":name})),
                    Command::MoveToGroup(ids, group) => (
                        "/api/media/bulk".into(),
                        json!({"ids":ids,"action":"move","group_id":group}),
                    ),
                    Command::ImportFolder(_) => {
                        return Err("Folder import is available only on Host".into())
                    }
                    Command::Rate(ids, rating) => (
                        "/api/media/bulk".into(),
                        json!({"action":"set_rating","ids":ids,"rating":rating}),
                    ),
                    Command::RateOne(id, rating) => {
                        (format!("/api/media/{id}/rating"), json!({"rating":rating}))
                    }
                    Command::Tag(ids, tag) => (
                        "/api/media/bulk".into(),
                        json!({"action":"add_tag","ids":ids,"tag":tag}),
                    ),
                    Command::Untag(ids, tag) => (
                        "/api/media/bulk".into(),
                        json!({"action":"remove_tag","ids":ids,"tag":tag}),
                    ),
                    Command::Approve(id) => (format!("/api/media/{id}/rating/approve"), json!({})),
                    Command::UndoRating(id, reviewed_at) => (
                        format!("/api/media/{id}/rating/undo"),
                        json!({"rating_reviewed_at":reviewed_at}),
                    ),
                    Command::PauseDownloads | Command::ResumeDownloads => {
                        return Err("Download control is available only on Host".into())
                    }
                    Command::PauseSource(_) | Command::ResumeSource(_) => {
                        return Err("Per-source download control is available only on Host".into())
                    }
                    Command::AddSources(text) => ("/api/sources".into(), json!({"text":text})),
                    Command::ResyncAll => ("/api/sources/resync-all".into(), json!({})),
                    Command::DeleteMedia(_) => {
                        return Err("File deletion is available only on Host".into())
                    }
                    Command::RefreshMetadata(_) => {
                        return Err("Local metadata refresh is available only on Host".into())
                    }
                    Command::CreateClips { media_id, seconds } => (
                        format!("/api/media/{media_id}/clips"),
                        json!({"seconds":seconds}),
                    ),
                    Command::UpdateSettings(_) => {
                        return Err(
                            "Host settings cannot be changed by Viewer without a negotiated role"
                                .into(),
                        );
                    }
                    Command::QueueSearchResults(results) => {
                        ("/api/search/download".into(), json!({"results":results}))
                    }
                };
                client.request(&path, Some(body)).await
            }
        }
    }

    pub async fn image(&self, item: &MediaItem) -> Result<NativeImage, String> {
        if let Self::Remote(client) = self {
            if !client.permissions.playback {
                return Err("Viewer role cannot stream media".into());
            }
        }
        let mut reader = match self {
            Self::Local(client) => {
                let path = client.media_path(item.id)?;
                if std::fs::metadata(&path).map_err(|e| e.to_string())?.len() > 32 * 1024 * 1024 {
                    return Err("Image exceeds 32 MiB limit".into());
                }
                image::ImageReader::new(std::io::Cursor::new(
                    std::fs::read(path).map_err(|e| e.to_string())?,
                ))
            }
            Self::Remote(client) => {
                let bytes = client
                    .bytes(
                        reqwest::Method::GET,
                        client.media_stream_url(item.id)?,
                        None,
                    )
                    .await?;
                image::ImageReader::new(std::io::Cursor::new(bytes))
            }
        }
        .with_guessed_format()
        .map_err(|e| e.to_string())?;
        let mut limits = image::Limits::default();
        limits.max_image_width = Some(16384);
        limits.max_image_height = Some(16384);
        limits.max_alloc = Some(128 * 1024 * 1024);
        reader.limits(limits);
        let image = reader
            .decode()
            .map_err(|e| e.to_string())?
            .thumbnail(1920, 1920)
            .to_rgba8();
        Ok(NativeImage {
            width: image.width(),
            height: image.height(),
            pixels: image.into_raw(),
        })
    }

    /// Return the native player's only supported source for an item. Local
    /// Hosts hand mpv an absolute managed-library path; Viewers hand it the
    /// already validated, pinned Host stream URL. Neither route accepts a
    /// caller supplied arbitrary media URL.
    pub fn playback_source(&self, item: &MediaItem) -> Result<String, String> {
        if item.id <= 0 {
            return Err("Media ID must be positive".into());
        }
        match self {
            Self::Local(client) => client
                .media_path(item.id)
                .map(|path| path.to_string_lossy().into_owned()),
            Self::Remote(client) => {
                if !client.permissions.playback {
                    return Err("Viewer role cannot stream media".into());
                }
                Ok(client.media_stream_url(item.id)?.to_string())
            }
        }
    }
}

fn response(
    result: Result<Json<Value>, (axum::http::StatusCode, Json<Value>)>,
) -> Result<Value, String> {
    result.map(|v| v.0).map_err(|(_, v)| {
        v.0["error"]
            .as_str()
            .unwrap_or("Operation failed")
            .to_owned()
    })
}

impl LocalClient {
    pub fn new(state: AppState) -> Result<Self, String> {
        if !state.edition.owns_library() {
            return Err("Viewer cannot open a local library".into());
        }
        Ok(Self {
            state: Arc::new(state),
        })
    }

    pub async fn library(&self, query: LibraryQuery) -> Result<MediaPage, String> {
        let mut value = serde_json::to_value(query).map_err(|e| e.to_string())?;
        value["limit"] = json!(100);
        let query = serde_json::from_value(value).map_err(|e| e.to_string())?;
        let page = serde_json::to_value(
            crate::services::library::list(&self.state, query)
                .await
                .map_err(|error| error.message().to_owned())?,
        )
        .map_err(|e| e.to_string())?;
        serde_json::from_value(page).map_err(|e| e.to_string())
    }

    pub fn media_path(&self, id: i64) -> Result<std::path::PathBuf, String> {
        crate::media_path(&self.state, id).map_err(|e| e.to_string())
    }

    pub async fn downloads(&self) -> Result<Value, String> {
        serde_json::to_value(crate::services::downloads::status(&self.state).await)
            .map_err(|error| error.to_string())
    }

    pub async fn execute(&self, command: Command) -> Result<Value, String> {
        if self.state.shutdown.is_cancelled() {
            return Err("Curator is shutting down".into());
        }
        // Keep admission closed for the entire operation, including async
        // cancellation/requeue work. A flag check alone races maintenance.
        let _lease = self
            .state
            .maintenance
            .try_acquire_background_worker()
            .ok_or("A local maintenance job is active")?;
        let state = State(self.state.clone());
        match command {
            Command::StartSession => serde_json::to_value(
                crate::services::session::start(
                    &self.state,
                    crate::session::GameConfig::quick_default(),
                )
                .map_err(|error| error.to_string())?,
            )
            .map_err(|e| e.to_string()),
            Command::Session(control) => serde_json::to_value(
                crate::services::session::control(&self.state, control)
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|e| e.to_string()),
            Command::CreateGroup(name) => response(
                routes::groups::create(
                    state,
                    Json(routes::groups::CreateGroupBody {
                        name,
                        parent_id: None,
                    }),
                )
                .await,
            ),
            Command::MoveToGroup(ids, group_id) => response(
                routes::media::bulk(
                    state,
                    None,
                    Json(routes::media::BulkMediaBody {
                        ids,
                        action: "move".into(),
                        group_id,
                        tag: None,
                        rating: None,
                    }),
                )
                .await,
            ),
            Command::ImportFolder(path) => {
                crate::local_import::import_folder(&self.state, &path, None)
                    .map(|id| json!({"id":id}))
                    .map_err(|e| e.to_string())
            }
            Command::Rate(ids, rating) => {
                if ids.is_empty() || ids.len() > 500 || ids.iter().any(|id| *id <= 0) {
                    return Err("Select between one and 500 valid media items".into());
                }
                crate::services::media::rate_many(&self.state, crate::services::access::Actor::LocalOwner, &ids, rating)
                    .map(|updated| json!({"action":"set_rating","updated":updated,"failed":[]}))
                    .map_err(|error| error.message().to_owned())
            }
            Command::RateOne(id, rating) => {
                if id <= 0 {
                    return Err("Media ID must be positive".into());
                }
                crate::services::media::review(&self.state, crate::services::access::Actor::LocalOwner, id, Some(rating))
                    .and_then(|review| {
                        serde_json::to_value(review).map_err(|error| {
                            crate::services::media::MediaError::Database(error.to_string())
                        })
                    })
                    .map_err(|error| error.message().to_owned())
            }
            Command::Tag(ids, tag) => crate::services::media::add_tag_many(&self.state, crate::services::access::Actor::LocalOwner, &ids, &tag)
                .map(|result| json!({"action":"add_tag","updated":result.updated,"failed":result.failed}))
                .map_err(|error| error.message().to_owned()),
            Command::Untag(ids, tag) => crate::services::media::remove_tag_many(&self.state, crate::services::access::Actor::LocalOwner, &ids, &tag)
                .map(|result| json!({"action":"remove_tag","updated":result.updated,"failed":result.failed}))
                .map_err(|error| error.message().to_owned()),
            Command::Approve(id) => crate::services::media::review(&self.state, crate::services::access::Actor::LocalOwner, id, None)
                .and_then(|review| {
                    serde_json::to_value(review).map_err(|error| {
                        crate::services::media::MediaError::Database(error.to_string())
                    })
                })
                .map_err(|error| error.message().to_owned()),
            Command::UndoRating(id, reviewed_at) => {
                crate::services::media::undo_review(&self.state, crate::services::access::Actor::LocalOwner, id, &reviewed_at)
                    .and_then(|review| {
                        serde_json::to_value(review).map_err(|error| {
                            crate::services::media::MediaError::Database(error.to_string())
                        })
                    })
                    .map_err(|error| error.message().to_owned())
            }
            Command::PauseDownloads => {
                let result = crate::services::downloads::pause(&self.state).await;
                if let Some(error) = result["error"].as_str() {
                    return Err(error.into());
                }
                Ok(result)
            }
            Command::ResumeDownloads => {
                let result = crate::services::downloads::resume(self.state.clone()).await;
                if let Some(error) = result["error"].as_str() {
                    return Err(error.into());
                }
                Ok(result)
            }
            Command::PauseSource(id) => {
                let result = crate::services::downloads::pause_source(&self.state, id).await;
                if let Some(error) = result["error"].as_str() {
                    return Err(error.into());
                }
                Ok(result)
            }
            Command::ResumeSource(id) => {
                let result =
                    crate::services::downloads::resume_source(self.state.clone(), id).await;
                if let Some(error) = result["error"].as_str() {
                    return Err(error.into());
                }
                Ok(result)
            }
            Command::AddSources(text) => {
                let candidates = crate::slug::split_bulk_input(&text);
                if candidates.is_empty() {
                    return Err("No valid URLs provided".into());
                }
                let result = crate::services::sources::create(self.state.clone(), candidates)
                    .map_err(|error| error.message().to_owned())?;
                serde_json::to_value(result).map_err(|error| error.to_string())
            }
            Command::ResyncAll => Ok(routes::sources::resync_all(state).await.0),
            Command::DeleteMedia(ids) => response(
                routes::media::bulk(
                    state,
                    None,
                    Json(routes::media::BulkMediaBody {
                        ids,
                        action: "delete".into(),
                        group_id: None,
                        tag: None,
                        rating: None,
                    }),
                )
                .await,
            ),
            Command::RefreshMetadata(ids) => response(
                routes::media::bulk(
                    state,
                    None,
                    Json(routes::media::BulkMediaBody {
                        ids,
                        action: "refresh_metadata".into(),
                        group_id: None,
                        tag: None,
                        rating: None,
                    }),
                )
                .await,
            ),
            Command::CreateClips { media_id, seconds } => response(
                routes::clips::create(
                    state,
                    Path(media_id),
                    Json(routes::clips::ClipBody { seconds }),
                )
                .await,
            ),
            Command::UpdateSettings(value) => {
                let body = serde_json::from_value(value).map_err(|error| error.to_string())?;
                response(routes::settings::patch(state, None, Json(body)).await)
            }
            Command::QueueSearchResults(values) => {
                let results =
                    serde_json::from_value(json!(values)).map_err(|error| error.to_string())?;
                response(
                    routes::search::download_selected(
                        state,
                        Json(routes::search::DownloadSearchResultsBody { results }),
                    )
                    .await,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_media_items_deserialize_without_enriched_metadata() {
        let item: MediaItem = serde_json::from_value(json!({
            "id": 7,
            "filename": "old.jpg",
            "type": "image",
            "rating": 0,
            "source": "archive",
            "filepath": "old.jpg",
            "playback_filepath": null,
            "tags": [],
            "rating_reviewed_at": null
        }))
        .unwrap();
        assert_eq!(item.id, 7);
        assert!(item.duration_secs.is_none());
        assert!(item.auto_rating.is_none());
        assert!(item.creator.is_none());
    }

    #[test]
    fn native_preferences_migrate_device_appearance_defaults() {
        let old = json!({"version":1,"queue":[],"width":1200,"height":800,"workspace":0,"future_option":{"enabled":true}});
        let preferences: NativePreferences = serde_json::from_value(old).unwrap();
        assert_eq!(preferences.theme, "system");
        assert_eq!(preferences.layout, "grid");
        assert_eq!(preferences.extra["future_option"]["enabled"], true);
        assert_eq!(
            serde_json::to_value(preferences).unwrap()["future_option"]["enabled"],
            true
        );
    }

    #[test]
    fn queue_preferences_replace_a_complete_json_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("native-v1").join("prefs.json");
        let mut preferences = NativePreferences::default();
        preferences.queue.push(MediaItem {
            id: 1,
            filename: "first.jpg".into(),
            kind: "image".into(),
            rating: 0,
            source: "test".into(),
            filepath: "first.jpg".into(),
            playback_filepath: None,
            tags: vec![],
            rating_reviewed_at: None,
            duration_secs: None,
            auto_rating: None,
            human_rating: None,
            rating_source: None,
            rating_reviewed: None,
            file_size_bytes: None,
            added_at: None,
            creator: None,
            downloaded: None,
        });
        write_preferences(&path, &preferences).unwrap();
        preferences.queue.clear();
        preferences.extra.insert("future_option".into(), json!(42));
        write_preferences(&path, &preferences).unwrap();
        let saved: NativePreferences =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(saved.queue.is_empty());
        assert_eq!(saved.extra["future_option"], 42);
    }

    #[tokio::test]
    async fn native_navigation_counts_match_server_library_summary() {
        use axum::body::{to_bytes, Body};
        use tower::ServiceExt;

        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state
            .pool
            .get()
            .unwrap()
            .execute_batch(
                "INSERT INTO groups(id,name,added_at) VALUES(10,'Parent','now');
             INSERT INTO groups(id,name,parent_id,added_at) VALUES(11,'Child',10,'now');
             UPDATE sources SET group_id=11 WHERE id=1;",
            )
            .unwrap();
        state.pool.get().unwrap().execute(
            "INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES(1,1,'test/a.jpg','a.jpg','image','now')",
            [],
        ).unwrap();
        let navigation = Client::Local(LocalClient::new((*state).clone()).unwrap())
            .navigation()
            .await
            .unwrap();
        let source = navigation
            .iter()
            .find(|item| !item.group && item.id == 1)
            .unwrap();
        assert_eq!(source.media_count, Some(1));
        assert_eq!(source.depth, 2);
        assert_eq!(navigation[0].id, 10);
        assert_eq!(navigation[0].depth, 0);
        assert_eq!(navigation[1].id, 11);
        assert_eq!(navigation[1].depth, 1);
        assert_eq!(navigation[1].media_count, Some(1));

        let response = crate::router((*state).clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/library/summary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let summary: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(summary["sources"][0]["items"], source.media_count.unwrap());
    }
    use crate::test_support;
    use tower::ServiceExt;

    #[tokio::test]
    async fn native_rating_and_http_queries_share_persistent_state() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        test_support::source(&state);
        state.pool.get().unwrap().execute("INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES(1,1,'test/a.jpg','a.jpg','image','2026')", []).unwrap();
        let client = LocalClient::new((*state).clone()).unwrap();
        client.execute(Command::Rate(vec![1], 4)).await.unwrap();
        client
            .execute(Command::Tag(vec![1], "favorite".into()))
            .await
            .unwrap();
        let page = client
            .library(LibraryQuery {
                search: Some("a.jpg".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(page.media.len(), 1);
        assert_eq!(page.media[0].rating, 4);
        assert!(page.media[0].tags.contains(&"favorite".into()));
        // Tag removal is non-destructive to the rest of the record and
        // reports unknown tags instead of failing the batch.
        let untag = client
            .execute(Command::Untag(vec![1], "favorite".into()))
            .await
            .unwrap();
        assert_eq!(untag["action"], "remove_tag");
        assert_eq!(untag["updated"], 1);
        let page = client
            .library(LibraryQuery {
                search: Some("a.jpg".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(!page.media[0].tags.contains(&"favorite".into()));
        let missing = client
            .execute(Command::Untag(vec![1], "no-such-tag".into()))
            .await
            .unwrap();
        assert_eq!(missing["updated"], 0);
        assert_eq!(missing["failed"].as_array().unwrap().len(), 1);
        let http = crate::router((*state).clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/media?search=a.jpg")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(http.status(), axum::http::StatusCode::OK);
        let body = axum::body::to_bytes(http.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["media"][0]["rating"], 4);
        let reviewed_at = page.media[0].rating_reviewed_at.clone().unwrap();
        client
            .execute(Command::UndoRating(1, reviewed_at))
            .await
            .unwrap();
        assert_eq!(
            client.library(LibraryQuery::default()).await.unwrap().media[0].rating,
            0
        );
        assert!(client
            .library(LibraryQuery {
                search: Some("%".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .media
            .is_empty());
    }

    #[tokio::test]
    async fn native_pause_cancels_workers_and_persists_pending_sources() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        test_support::source(&state);
        let token = tokio_util::sync::CancellationToken::new();
        state
            .source_cancellations
            .lock()
            .await
            .insert(1, token.clone());
        state
            .pool
            .get()
            .unwrap()
            .execute("UPDATE sources SET status='pending'", [])
            .unwrap();
        LocalClient::new((*state).clone())
            .unwrap()
            .execute(Command::PauseDownloads)
            .await
            .unwrap();
        assert!(token.is_cancelled());
        let status: String = state
            .pool
            .get()
            .unwrap()
            .query_row("SELECT status FROM sources WHERE id=1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(status, "paused");
    }

    #[tokio::test]
    async fn native_mutations_reject_shutdown_and_maintenance() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        let client = LocalClient::new((*state).clone()).unwrap();
        let lease = state.maintenance.try_acquire_background_worker().unwrap();
        state
            .maintenance
            .start(
                state.clone(),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    confirmation: String::new(),
                    backup_id: None,
                },
            )
            .await
            .unwrap();
        assert!(client
            .execute(Command::Rate(vec![1], 2))
            .await
            .unwrap_err()
            .contains("maintenance"));
        drop(lease);
        state.shutdown.cancel();
        assert!(client
            .execute(Command::PauseDownloads)
            .await
            .unwrap_err()
            .contains("shutting down"));
        state.server_tasks.close();
        state.server_tasks.wait().await;
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert!(LocalClient::new(viewer).is_err());
    }

    #[test]
    fn remote_destinations_cannot_escape_the_validated_tailnet_origin() {
        for origin in [
            "http://127.0.0.1",
            "http://example.com",
            "http://100.63.0.1",
            "http://100.128.0.1",
            "http://user@100.64.0.1",
            "https://100.64.0.1",
        ] {
            assert!(RemoteClient::from_validated_peer(origin).is_err());
        }
        let client = RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap();
        assert!(client.url("/api/media").is_ok());
        assert_eq!(
            client.media_stream_url(7).unwrap().as_str(),
            "http://100.64.1.2:42168/api/media/7/stream"
        );
        assert!(client.media_stream_url(0).is_err());
        for path in [
            "//100.64.1.3/api/media",
            "http://127.0.0.1/api/media",
            "/library/../../secret",
            "/api/media#fragment",
        ] {
            assert!(client.url(path).is_err());
        }
        assert!(RemoteClient::from_validated_peer("http://[fd7a:115c:a1e0::2]:42168").is_ok());
    }

    #[test]
    fn viewer_reconnect_requires_the_same_library_and_permissions() {
        let client = RemoteClient::from_validated_peer_with_identity(
            "http://100.64.1.2:42168",
            ViewerPermissions::default(),
            "library-a".into(),
        )
        .unwrap();
        let info = serde_json::json!({
            "instance_id": "library-a",
            "api_protocol": crate::API_PROTOCOL,
            "edition": "host",
            "tailnet_only": true,
            "viewer_permissions": ViewerPermissions::default(),
        });
        assert!(client.validate_reconnected_info(&info).is_ok());
        let mut replaced = info.clone();
        replaced["instance_id"] = serde_json::json!("library-b");
        assert!(client.validate_reconnected_info(&replaced).is_err());
        let mut elevated = info;
        elevated["viewer_permissions"]["library_edit"] = serde_json::json!(true);
        assert!(client.validate_reconnected_info(&elevated).is_err());
        assert!(RemoteClient::from_validated_peer_with_identity(
            "http://100.64.1.2:42168",
            ViewerPermissions::default(),
            String::new(),
        )
        .is_err());
    }

    #[tokio::test]
    async fn remote_recovery_is_rejected_without_network_access() {
        let client =
            Client::Remote(RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap());
        assert!(client
            .export_source_list()
            .await
            .unwrap_err()
            .contains("only on the Host"));
        assert!(client
            .import_source_list(Vec::new())
            .await
            .unwrap_err()
            .contains("only on the Host"));
        assert!(client
            .recovery_status()
            .await
            .unwrap_err()
            .contains("only on the Host"));
        assert!(client
            .recovery(crate::maintenance::MaintenanceRequest {
                kind: crate::maintenance::MaintenanceKind::CreateBackup,
                confirmation: String::new(),
                backup_id: None,
            })
            .await
            .unwrap_err()
            .contains("only on the Host"));
    }

    #[tokio::test]
    async fn viewer_cannot_delete_files_or_refresh_local_metadata() {
        let client =
            Client::Remote(RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap());
        assert!(client
            .execute(Command::DeleteMedia(vec![1]))
            .await
            .unwrap_err()
            .contains("only on Host"));
        assert!(client
            .execute(Command::RefreshMetadata(vec![1]))
            .await
            .unwrap_err()
            .contains("only on Host"));
        assert!(client
            .execute(Command::PauseSource(1))
            .await
            .unwrap_err()
            .contains("only on Host"));
        assert!(client
            .execute(Command::PauseDownloads)
            .await
            .unwrap_err()
            .contains("only on Host"));
        assert!(client
            .execute(Command::ResumeDownloads)
            .await
            .unwrap_err()
            .contains("only on Host"));
        assert!(client
            .execute(Command::UpdateSettings(json!({"theme":"ember"})))
            .await
            .unwrap_err()
            .contains("negotiated role"));
        assert!(client
            .execute(Command::ResumeSource(1))
            .await
            .unwrap_err()
            .contains("only on Host"));
    }

    #[tokio::test]
    async fn negotiated_read_only_viewer_denies_mutations_before_network() {
        let permissions: ViewerPermissions = serde_json::from_value(json!({})).unwrap();
        assert_eq!(permissions, ViewerPermissions::default());
        let client = Client::Remote(
            RemoteClient::from_validated_peer_with_permissions(
                "http://100.64.1.2:42168",
                permissions,
            )
            .unwrap(),
        );
        assert!(!client.can_edit_library());
        assert!(!client.can_control_sessions());
        assert!(client.can_playback());
        for command in [
            Command::Rate(vec![1], 4),
            Command::Tag(vec![1], "test".into()),
            Command::Approve(1),
            Command::UndoRating(1, "token".into()),
            Command::CreateGroup("test".into()),
            Command::AddSources("https://example.com".into()),
            Command::StartSession,
        ] {
            assert!(client
                .execute(command)
                .await
                .unwrap_err()
                .contains("Viewer role"));
        }
    }

    #[tokio::test]
    async fn negotiated_library_read_denial_covers_snapshots() {
        let permissions = ViewerPermissions {
            library_read: false,
            playback: false,
            discovery: false,
            ..ViewerPermissions::default()
        };
        let client = Client::Remote(
            RemoteClient::from_validated_peer_with_permissions(
                "http://100.64.1.2:42168",
                permissions,
            )
            .unwrap(),
        );
        assert!(client.library(LibraryQuery::default()).await.is_err());
        assert!(client.navigation().await.is_err());
        assert!(client.manage_snapshot().await.is_err());
        assert!(client.downloads().await.is_err());
        assert!(client.session().await.is_err());
        assert!(client.discover("test".into(), None).await.is_err());
    }

    #[tokio::test]
    async fn native_manage_snapshot_and_settings_share_host_state() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        let client = Client::Local(LocalClient::new((*state).clone()).unwrap());
        let snapshot = client.manage_snapshot().await.unwrap();
        assert_eq!(snapshot.stats["total_media"], 0);
        assert!(snapshot.providers["providers"].as_array().is_some());

        client
            .execute(Command::UpdateSettings(json!({ "theme": "midnight" })))
            .await
            .unwrap();
        let settings = routes::settings::get(State(state.clone()), None).await.0;
        assert_eq!(settings["theme"], "midnight");
    }

    #[tokio::test]
    async fn recovery_snapshot_exposes_selectable_backup_records_only_to_host() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        let backup_dir = state.data_dir.join("backups");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::write(backup_dir.join("curator-snapshot.zip"), b"fixture").unwrap();
        let host = Client::Local(LocalClient::new((*state).clone()).unwrap());
        let snapshot = host.recovery_status().await.unwrap();
        assert_eq!(snapshot.backups.len(), 1);
        assert_eq!(snapshot.backups[0].id, "curator-snapshot.zip");
        assert_eq!(snapshot.backups[0].size_bytes, 7);
        let viewer =
            Client::Remote(RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap());
        assert!(viewer.recovery_status().await.is_err());
    }

    #[tokio::test]
    async fn only_server_exposes_browser_assets() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        std::fs::write(root.path().join("oobe.html"), "setup").unwrap();
        std::fs::write(root.path().join("app.js"), "browser").unwrap();
        for edition in [
            crate::edition::Edition::Host,
            crate::edition::Edition::Server,
        ] {
            let mut state = (*state).clone();
            state.edition = edition;
            for path in ["/", "/app.js"] {
                let response = crate::router(state.clone())
                    .oneshot(
                        axum::http::Request::builder()
                            .uri(path)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    response.status().as_u16(),
                    if edition == crate::edition::Edition::Server {
                        200
                    } else {
                        404
                    }
                );
            }
        }
    }
}

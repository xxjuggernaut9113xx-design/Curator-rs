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

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct LibraryQuery {
    pub search: Option<String>,
    pub cursor: Option<String>,
    pub media_type: Option<String>,
    pub sort: String,
    pub rating_status: Option<String>,
    pub source_id: Option<i64>,
    pub group_id: Option<i64>,
    pub tag: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
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
    Tag(Vec<i64>, String),
    Approve(i64),
    PauseDownloads,
    ResumeDownloads,
    AddSources(String),
    ResyncAll,
    DeleteMedia(Vec<i64>),
    RefreshMetadata(Vec<i64>),
    CreateClips { media_id: i64, seconds: u32 },
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
}

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
}

impl RemoteClient {
    /// The caller must validate this numeric origin against the connected
    /// Tailscale peer inventory and negotiate Curator's protocol first.
    pub fn from_validated_peer(origin: &str) -> Result<Self, String> {
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
        Ok(Self { origin, http })
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
        let mut request = self.http.request(method, url);
        if let Some(body) = body {
            request = request
                .header("content-type", "application/json")
                .body(body.to_string());
        }
        let mut response = request.send().await.map_err(|e| e.to_string())?;
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
}

#[derive(Serialize, Deserialize)]
pub struct NativePreferences {
    pub version: u32,
    pub queue: Vec<MediaItem>,
    pub width: u32,
    pub height: u32,
    pub workspace: i32,
}

impl Default for NativePreferences {
    fn default() -> Self {
        Self {
            version: 1,
            queue: Vec::new(),
            width: 1200,
            height: 800,
            workspace: 0,
        }
    }
}

impl Client {
    pub async fn diagnostic_log(&self) -> Result<String, String> {
        const MAX_LOG_BYTES: usize = 512 * 1024;
        let bytes = match self {
            Self::Local(client) => {
                let metadata =
                    std::fs::metadata(&client.state.log_path).map_err(|error| error.to_string())?;
                let mut file = std::fs::File::open(&client.state.log_path)
                    .map_err(|error| error.to_string())?;
                use std::io::{Read, Seek, SeekFrom};
                let start = metadata.len().saturating_sub(MAX_LOG_BYTES as u64);
                file.seek(SeekFrom::Start(start))
                    .map_err(|error| error.to_string())?;
                let mut bytes = Vec::with_capacity((metadata.len() - start) as usize);
                file.read_to_end(&mut bytes)
                    .map_err(|error| error.to_string())?;
                bytes
            }
            Self::Remote(client) => {
                client
                    .bytes(reqwest::Method::GET, client.url("/api/log")?, None)
                    .await?
            }
        };
        let text = String::from_utf8_lossy(&bytes);
        Ok(if bytes.len() == MAX_LOG_BYTES {
            format!("… log truncated to the latest {MAX_LOG_BYTES} bytes …\n{text}")
        } else {
            text.into_owned()
        })
    }

    pub async fn manage_snapshot(&self) -> Result<ManageSnapshot, String> {
        match self {
            Self::Local(client) => Ok(ManageSnapshot {
                settings: routes::settings::get(State(client.state.clone()), None)
                    .await
                    .0,
                storage: routes::storage::dashboard(
                    State(client.state.clone()),
                    Query(routes::storage::StorageQuery::default()),
                )
                .await
                .0,
                stats: response(routes::misc::stats(State(client.state.clone())).await)?,
                providers: routes::search::providers(State(client.state.clone()))
                    .await
                    .0,
                remote_access: serde_json::to_value(
                    routes::remote::status(State(client.state.clone())).await.0,
                )
                .map_err(|error| error.to_string())?,
            }),
            Self::Remote(client) => Ok(ManageSnapshot {
                settings: client.request("/api/settings", None).await?,
                storage: client.request("/api/storage", None).await?,
                stats: client.request("/api/stats", None).await?,
                providers: client.request("/api/search/providers", None).await?,
                remote_access: client.request("/api/remote-access", None).await?,
            }),
        }
    }

    pub async fn discover(&self, query: String, provider: Option<String>) -> Result<Value, String> {
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
        Ok(dirs::config_dir()
            .ok_or("User configuration directory is unavailable")?
            .join("Curator")
            .join("native-v1")
            .join(format!("{key}.json")))
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
        use std::io::Write;
        let path = self.preferences_path()?;
        let directory = path.parent().ok_or("Invalid preferences path")?;
        std::fs::create_dir_all(directory).map_err(|e| e.to_string())?;
        let mut temporary =
            tempfile::NamedTempFile::new_in(directory).map_err(|e| e.to_string())?;
        serde_json::to_writer(&mut temporary, preferences).map_err(|e| e.to_string())?;
        temporary.flush().map_err(|e| e.to_string())?;
        temporary.as_file().sync_all().map_err(|e| e.to_string())?;
        temporary.persist(path).map_err(|e| e.to_string())?;
        Ok(())
    }
}

impl Client {
    pub async fn recovery_status(&self) -> Result<String, String> {
        let Self::Local(client) = self else {
            return Err("Recovery is available only on the Host device".into());
        };
        let backups = crate::maintenance::list_backups(&client.state.data_dir)
            .map_err(|error| error.to_string())?;
        let mut lines = vec![format!("{} saved backups", backups.len())];
        for backup in backups {
            lines.push(format!(
                "{} · {} bytes · {}",
                backup.id, backup.size_bytes, backup.created_at
            ));
        }
        for job in client.state.maintenance.jobs().await {
            lines.push(format!(
                "{:?}: {:?} · {}{}{}",
                job.kind,
                job.phase,
                job.message,
                job.error
                    .map(|error| format!(" · {error}"))
                    .unwrap_or_default(),
                if job.restart_required {
                    " · Restart required"
                } else {
                    ""
                }
            ));
        }
        Ok(lines.join("\n"))
    }

    pub async fn recovery(
        &self,
        request: crate::maintenance::MaintenanceRequest,
    ) -> Result<(), String> {
        let Self::Local(client) = self else {
            return Err("Recovery is available only on the Host device".into());
        };
        if client.state.shutdown.is_cancelled() {
            return Err("Curator is shutting down".into());
        }
        // Maintenance owns admission and waits for ordinary workers itself.
        // Holding a background-worker lease here would deadlock that wait.
        response(routes::admin::start_job(State(client.state.clone()), None, Json(request)).await)?;
        Ok(())
    }

    pub async fn session(&self) -> Result<Option<crate::session::SessionState>, String> {
        match self {
            Self::Local(client) => Ok(crate::services::session::current(&client.state)),
            Self::Remote(client) => {
                serde_json::from_value(client.request("/api/session", None).await?)
                    .map_err(|e| e.to_string())
            }
        }
    }
    pub async fn navigation(&self) -> Result<Vec<NavigationItem>, String> {
        let (sources, groups) = match self {
            Self::Local(client) => (
                response(routes::sources::list(State(client.state.clone())).await)?,
                response(routes::groups::list(State(client.state.clone())).await)?,
            ),
            Self::Remote(client) => (
                client.request("/api/sources", None).await?,
                client.request("/api/groups", None).await?,
            ),
        };
        let mut items = Vec::new();
        for (kind, value) in [("source", sources), ("group", groups)] {
            if let Some(rows) = value[format!("{kind}s")].as_array() {
                for row in rows {
                    if let (Some(id), Some(name)) = (row["id"].as_i64(), row["name"].as_str()) {
                        items.push(NavigationItem {
                            id,
                            name: name.into(),
                            group: kind == "group",
                        });
                    }
                }
            }
        }
        Ok(items)
    }
    pub async fn library(&self, query: LibraryQuery) -> Result<MediaPage, String> {
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
        match self {
            Self::Local(client) => client.downloads().await,
            Self::Remote(client) => client.request("/api/downloads/status", None).await,
        }
    }

    pub async fn execute(&self, command: Command) -> Result<Value, String> {
        match self {
            Self::Local(client) => client.execute(command).await,
            Self::Remote(client) => {
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
                    Command::Tag(ids, tag) => (
                        "/api/media/bulk".into(),
                        json!({"action":"add_tag","ids":ids,"tag":tag}),
                    ),
                    Command::Approve(id) => (format!("/api/media/{id}/rating/approve"), json!({})),
                    Command::PauseDownloads => ("/api/downloads/pause".into(), json!({})),
                    Command::ResumeDownloads => ("/api/downloads/resume".into(), json!({})),
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
                    Command::UpdateSettings(value) => {
                        return client
                            .request_method(reqwest::Method::PATCH, "/api/settings", Some(value))
                            .await;
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
        let page = response(routes::media::list(State(self.state.clone()), Query(query)).await)?;
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
            Command::Rate(ids, rating) => response(
                routes::media::bulk(
                    state,
                    Json(routes::media::BulkMediaBody {
                        ids,
                        action: "set_rating".into(),
                        rating: Some(rating),
                        tag: None,
                        group_id: None,
                    }),
                )
                .await,
            ),
            Command::Tag(ids, tag) => response(
                routes::media::bulk(
                    state,
                    Json(routes::media::BulkMediaBody {
                        ids,
                        action: "add_tag".into(),
                        rating: None,
                        tag: Some(tag),
                        group_id: None,
                    }),
                )
                .await,
            ),
            Command::Approve(id) => response(routes::media::approve_rating(state, Path(id)).await),
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
            Command::AddSources(text) => response(
                routes::sources::add(
                    state,
                    Json(routes::sources::AddSourcesBody {
                        urls: Vec::new(),
                        text: Some(text),
                    }),
                )
                .await,
            ),
            Command::ResyncAll => Ok(routes::sources::resync_all(state).await.0),
            Command::DeleteMedia(ids) => response(
                routes::media::bulk(
                    state,
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

    #[tokio::test]
    async fn remote_recovery_is_rejected_without_network_access() {
        let client =
            Client::Remote(RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap());
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

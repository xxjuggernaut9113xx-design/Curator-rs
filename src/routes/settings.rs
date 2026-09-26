use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::sync::Semaphore;

use crate::db::save_settings;
use crate::AppState;

fn is_local_client(peer: &Option<ConnectInfo<SocketAddr>>) -> bool {
    peer.as_ref().is_none_or(|peer| peer.0.ip().is_loopback())
}

fn host_integrations_available(state: &AppState, peer: &Option<ConnectInfo<SocketAddr>>) -> bool {
    is_local_client(peer) && matches!(state.edition, crate::edition::Edition::Host)
}

/// Shared with `routes::oobe` (the Appearance step reuses the exact same
/// allow-list rather than re-declaring it) — see "Do not introduce
/// conflicting configuration systems" in the OOBE build notes.
pub(crate) use crate::services::settings::VALID_THEMES;

// `Option<Option<T>>` normally cannot distinguish a missing JSON property
// from an explicit `null`. Settings uses that distinction for optional byte
// limits: null means "Unlimited", while omission means "leave unchanged".
fn deserialize_nullable_u64<'de, D>(deserializer: D) -> Result<Option<Option<u64>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Option::<u64>::deserialize(deserializer)?))
}

fn deserialize_nullable_u32<'de, D>(deserializer: D) -> Result<Option<Option<u32>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Option::<u32>::deserialize(deserializer)?))
}

fn normalize_optional_bytes(value: Option<u64>) -> Option<u64> {
    value.filter(|value| *value > 0)
}

fn normalize_optional_days(value: Option<u32>) -> Result<Option<u32>, &'static str> {
    match value {
        None | Some(0) => Ok(None),
        Some(value) if value <= 36_500 => Ok(Some(value)),
        Some(_) => Err("Archive retention must be at most 36,500 days"),
    }
}

#[derive(Deserialize)]
pub struct PatchSettingsBody {
    pub start_with_windows: Option<bool>,
    pub keep_running_in_tray: Option<bool>,
    pub max_clip_length_secs: Option<u32>,
    pub goon_default_limit: Option<u32>,
    pub goon_log_sessions: Option<bool>,
    pub max_concurrent: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_nullable_u64")]
    pub max_download_file_size_bytes: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_nullable_u64")]
    pub max_source_storage_bytes: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_nullable_u64")]
    pub minimum_free_disk_bytes: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_nullable_u64")]
    pub thumbnail_cache_max_bytes: Option<Option<u64>>,
    pub apply_download_limits_to_local_imports: Option<bool>,
    pub automatic_cleanup_mode: Option<String>,
    /// A browser confirmation is repeated at the API boundary so a direct
    /// PATCH cannot silently arm deletion of existing originals.
    pub automatic_cleanup_confirmation: Option<String>,
    #[serde(default, deserialize_with = "deserialize_nullable_u64")]
    pub automatic_cleanup_low_disk_bytes: Option<Option<u64>>,
    #[serde(default, deserialize_with = "deserialize_nullable_u32")]
    pub archive_retention_days: Option<Option<u32>>,
    /// Archive deletion is separately acknowledged because clearing the
    /// gallery-dl archive can make old posts eligible on a later sync.
    pub archive_retention_confirmation: Option<String>,
    pub default_slideshow_speed: Option<f64>,
    pub default_slideshow_loop: Option<bool>,
    pub default_slideshow_shuffle: Option<bool>,
    pub theme: Option<String>,
    pub export_reminder_days: Option<u32>,
    pub export_reminder_snoozed_until: Option<String>,
    // Cock Hero settings
    pub ch_log_sessions: Option<bool>,
    pub ch_default_interval: Option<f64>,
    pub ch_default_limit: Option<u32>,
    pub ch_default_shuffle: Option<bool>,
    pub ch_default_media_type: Option<String>,
    // NSFW auto-rating
    pub nsfw_filter_enabled: Option<bool>,
    pub library_layout: Option<String>,
    pub last_play_mode: Option<String>,
    pub search_providers: Option<Vec<String>>,
    pub metronome_enabled: Option<bool>,
    pub metronome_volume: Option<f64>,
    pub goon_persona: Option<String>,
    pub tts_voice: Option<String>,
    pub tts_rate: Option<f64>,
    pub tts_pitch: Option<f64>,
    pub tts_volume: Option<f64>,
    pub soundtrack_provider: Option<String>,
    /// Bootstrap settings live in config.json because they are consumed
    /// before the database is opened. They are exposed here for the normal
    /// Settings UI but intentionally take effect on the next launch.
    pub ffmpeg_bin: Option<String>,
}

// ─── GET /api/settings ───────────────────────────────────────────────────────

pub async fn get(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Json<Value> {
    Json(crate::services::settings::read(&state, settings_audience(&peer)).await)
}

fn settings_audience(
    peer: &Option<ConnectInfo<SocketAddr>>,
) -> crate::services::settings::SettingsAudience {
    if is_local_client(peer) {
        crate::services::settings::SettingsAudience::Local
    } else {
        crate::services::settings::SettingsAudience::Remote
    }
}

fn settings_response(
    state: &AppState,
    peer: &Option<ConnectInfo<SocketAddr>>,
    settings: &crate::db::Settings,
    startup: Option<crate::StartupRegistration>,
) -> Value {
    crate::services::settings::response(state, settings_audience(peer), settings, startup)
}

// ─── PATCH /api/settings ─────────────────────────────────────────────────────

pub async fn patch(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<PatchSettingsBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let local = is_local_client(&peer);
    if !local
        && (body.ffmpeg_bin.is_some()
            || body.start_with_windows.is_some()
            || body.keep_running_in_tray.is_some())
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(
                json!({"error":"Executable paths and local startup/tray controls are available only on this device."}),
            ),
        ));
    }
    if !host_integrations_available(&state, &peer)
        && (body.start_with_windows.is_some() || body.keep_running_in_tray.is_some())
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(
                json!({"error":"Startup and tray controls are available only in the local Curator Host app."}),
            ),
        ));
    }
    // Validate the complete request before changing the Windows Run entry,
    // config.json, or the in-memory settings value. This keeps a malformed
    // multi-control PATCH from partially applying the controls that happened
    // to appear before its invalid field.
    if let Some(value) = body.ffmpeg_bin.as_deref() {
        let value = value.trim();
        if value.is_empty() || value.len() > 4096 || value.contains('\0') {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid ffmpeg executable"})),
            ));
        }
    }
    if let Some(value) = body.automatic_cleanup_mode.as_deref() {
        if !["never", "low_disk", "weekly"].contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Automatic cleanup must be Never, low disk, or weekly"})),
            ));
        }
    }
    if let Some(value) = body.archive_retention_days.flatten() {
        normalize_optional_days(Some(value))
            .map_err(|error| (StatusCode::BAD_REQUEST, Json(json!({"error": error}))))?;
    }
    if let Some(value) = body.theme.as_deref() {
        if !VALID_THEMES.contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Unknown theme: {value}")})),
            ));
        }
    }
    if let Some(value) = body.library_layout.as_deref() {
        if !["grid", "table"].contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Library layout must be grid or table"})),
            ));
        }
    }
    if let Some(value) = body.last_play_mode.as_deref() {
        if !matches!(
            value,
            "feed" | "mobile-feed" | "slideshow" | "portrait" | "portrait-wall" | "review" | "goon"
        ) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown playback mode"})),
            ));
        }
    }
    if let Some(values) = body.search_providers.as_ref() {
        if values.len() > 64
            || values.iter().any(|raw| {
                let provider = raw.trim().to_ascii_lowercase();
                provider.is_empty()
                    || provider.len() > 80
                    || !provider.bytes().all(|b| {
                        b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_'
                    })
            })
        {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid search provider selection"})),
            ));
        }
    }
    if let Some(value) = body.metronome_volume {
        if !value.is_finite() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid metronome volume"})),
            ));
        }
    }
    if let Some(value) = body.goon_persona.as_deref() {
        if !["neutral", "mommy", "dom", "brat"].contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown GOON persona"})),
            ));
        }
    }
    if let Some(value) = body.tts_rate {
        if !value.is_finite() || !(0.1..=3.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS rate must be between 0.1 and 3.0"})),
            ));
        }
    }
    if let Some(value) = body.tts_pitch {
        if !value.is_finite() || !(0.0..=2.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS pitch must be between 0 and 2.0"})),
            ));
        }
    }
    if let Some(value) = body.tts_volume {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS volume must be between 0 and 1.0"})),
            ));
        }
    }
    if let Some(value) = body.soundtrack_provider.as_deref() {
        if !["local", "youtube", "soundcloud", "apple_music", "spotify"].contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown soundtrack provider"})),
            ));
        }
    }
    let thumbnail_cache_limit_changed = body.thumbnail_cache_max_bytes.is_some();
    let current_settings = state.settings.read().await.clone();
    let cleanup_mode = body
        .automatic_cleanup_mode
        .as_deref()
        .unwrap_or(&current_settings.automatic_cleanup_mode);
    let cleanup_threshold = body
        .automatic_cleanup_low_disk_bytes
        .map(normalize_optional_bytes)
        .unwrap_or(current_settings.automatic_cleanup_low_disk_bytes);
    if cleanup_mode == "low_disk" && cleanup_threshold.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error":"Choose a low-disk cleanup threshold before enabling automatic cleanup"}),
            ),
        ));
    }
    if current_settings.automatic_cleanup_mode == "never"
        && cleanup_mode != "never"
        && body.automatic_cleanup_confirmation.as_deref() != Some("ENABLE AUTOMATIC CLEANUP")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error":"Type ENABLE AUTOMATIC CLEANUP before enabling automatic cleanup."
            })),
        ));
    }
    if current_settings.archive_retention_days.is_none()
        && body
            .archive_retention_days
            .flatten()
            .is_some_and(|days| days > 0)
        && body.archive_retention_confirmation.as_deref() != Some("ENABLE ARCHIVE RETENTION")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error":"Type ENABLE ARCHIVE RETENTION before enabling archive age cleanup."
            })),
        ));
    }
    let mut ffmpeg_changed = false;
    if body.ffmpeg_bin.is_some() {
        let mut config = crate::config::load_config_for(state.install_scope);
        if let Some(value) = body.ffmpeg_bin.as_deref() {
            let value = value.trim();
            if value.is_empty() || value.len() > 4096 || value.contains('\0') {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Invalid ffmpeg executable"})),
                ));
            }
            ffmpeg_changed = config.ffmpeg_bin.as_deref().unwrap_or("ffmpeg") != value;
            config.ffmpeg_bin = Some(value.to_string());
        }
        crate::config::save_config_for(state.install_scope, &config).map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":format!("Could not save external tool settings: {error}")})),
            )
        })?;
    }
    if let Some(enabled) = body.start_with_windows {
        crate::set_start_with_windows_preference(&state, enabled)
            .await
            .map_err(|error| (StatusCode::BAD_REQUEST, Json(json!({"error": error}))))?;
    }

    let mut settings = state.settings.write().await;

    if let Some(v) = body.max_clip_length_secs {
        settings.max_clip_length_secs = v.clamp(5, 3600);
    }
    if let Some(v) = body.goon_default_limit {
        settings.goon_default_limit = v.clamp(1, 10_000);
    }
    if let Some(v) = body.goon_log_sessions {
        settings.goon_log_sessions = v;
    }
    if let Some(v) = body.keep_running_in_tray {
        settings.keep_running_in_tray = v;
    }
    if let Some(v) = body.max_concurrent {
        let v = v.clamp(1, 20);
        settings.max_concurrent = v;
        // Swap the semaphore so future downloads use the new limit
        let mut sem_guard = state.download_semaphore.lock().await;
        *sem_guard = Arc::new(Semaphore::new(v as usize));
    }
    if let Some(value) = body.max_download_file_size_bytes {
        settings.max_download_file_size_bytes = normalize_optional_bytes(value);
    }
    if let Some(value) = body.max_source_storage_bytes {
        settings.max_source_storage_bytes = normalize_optional_bytes(value);
    }
    if let Some(value) = body.minimum_free_disk_bytes {
        settings.minimum_free_disk_bytes = normalize_optional_bytes(value);
    }
    if let Some(value) = body.thumbnail_cache_max_bytes {
        settings.thumbnail_cache_max_bytes = normalize_optional_bytes(value);
    }
    if let Some(value) = body.apply_download_limits_to_local_imports {
        settings.apply_download_limits_to_local_imports = value;
    }
    if let Some(value) = body.automatic_cleanup_mode {
        if !["never", "low_disk", "weekly"].contains(&value.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Automatic cleanup must be Never, low disk, or weekly"})),
            ));
        }
        settings.automatic_cleanup_mode = value;
    }
    if let Some(value) = body.automatic_cleanup_low_disk_bytes {
        settings.automatic_cleanup_low_disk_bytes = normalize_optional_bytes(value);
    }
    if let Some(value) = body.archive_retention_days {
        settings.archive_retention_days = normalize_optional_days(value)
            .map_err(|error| (StatusCode::BAD_REQUEST, Json(json!({"error": error}))))?;
    }
    if settings.automatic_cleanup_mode == "low_disk"
        && settings.automatic_cleanup_low_disk_bytes.is_none()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error":"Choose a low-disk cleanup threshold before enabling automatic cleanup"}),
            ),
        ));
    }
    if let Some(v) = body.default_slideshow_speed {
        settings.default_slideshow_speed = v.clamp(500.0, 60000.0);
    }
    if let Some(v) = body.default_slideshow_loop {
        settings.default_slideshow_loop = v;
    }
    if let Some(v) = body.default_slideshow_shuffle {
        settings.default_slideshow_shuffle = v;
    }
    if let Some(ref theme) = body.theme {
        if !VALID_THEMES.contains(&theme.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": format!("Unknown theme: {}", theme)})),
            ));
        }
        settings.theme = theme.clone();
    }
    if let Some(v) = body.export_reminder_days {
        settings.export_reminder_days = v.clamp(1, 365);
    }
    if let Some(v) = body.export_reminder_snoozed_until {
        settings.export_reminder_snoozed_until = if v.is_empty() { None } else { Some(v) };
    }
    if let Some(v) = body.ch_log_sessions {
        settings.ch_log_sessions = v;
    }
    if let Some(v) = body.ch_default_interval {
        settings.ch_default_interval = v;
    }
    if let Some(v) = body.ch_default_limit {
        settings.ch_default_limit = v;
    }
    if let Some(v) = body.ch_default_shuffle {
        settings.ch_default_shuffle = v;
    }
    if let Some(v) = body.ch_default_media_type {
        settings.ch_default_media_type = v;
    }
    if let Some(v) = body.nsfw_filter_enabled {
        if settings.nsfw_filter_enabled != v {
            settings.nsfw_restart_required = true;
        }
        settings.nsfw_filter_enabled = v;
    }
    if let Some(v) = body.library_layout {
        if !["grid", "table"].contains(&v.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Library layout must be grid or table"})),
            ));
        }
        settings.library_layout = v;
    }
    if let Some(v) = body.last_play_mode {
        let normalized = match v.as_str() {
            "feed" | "mobile-feed" => "feed",
            "slideshow" | "portrait" | "portrait-wall" | "review" | "goon" => v.as_str(),
            _ => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Unknown playback mode"})),
                ));
            }
        };
        settings.last_play_mode = normalized.to_string();
    }
    if let Some(values) = body.search_providers {
        if values.len() > 64 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Choose at most 64 search providers"})),
            ));
        }
        let mut providers = Vec::new();
        for raw in values {
            let provider = raw.trim().to_ascii_lowercase();
            if provider.is_empty()
                || provider.len() > 80
                || !provider
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
            {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Invalid search provider id"})),
                ));
            }
            if !providers.contains(&provider) {
                providers.push(provider);
            }
        }
        // A local catalog is always available.  Keeping it in the durable
        // list makes the selection explicit while still avoiding an empty
        // search experience after a user unticks every remote provider.
        if !providers.iter().any(|id| id == "local") {
            providers.insert(0, "local".to_string());
        }
        settings.search_providers = providers;
    }
    if let Some(v) = body.metronome_enabled {
        settings.metronome_enabled = v;
    }
    if let Some(v) = body.metronome_volume {
        if !v.is_finite() {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid metronome volume"})),
            ));
        }
        settings.metronome_volume = v.clamp(0.0, 1.0);
    }
    if let Some(v) = body.goon_persona {
        if !["neutral", "mommy", "dom", "brat"].contains(&v.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown GOON persona"})),
            ));
        }
        settings.goon_persona = v;
    }
    if let Some(v) = body.tts_voice {
        let voice = v.trim();
        settings.tts_voice = if voice.is_empty() {
            None
        } else {
            Some(voice.chars().take(160).collect())
        };
    }
    if let Some(value) = body.tts_rate {
        if !value.is_finite() || !(0.1..=3.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS rate must be between 0.1 and 3.0"})),
            ));
        }
        settings.tts_rate = value;
    }
    if let Some(value) = body.tts_pitch {
        if !value.is_finite() || !(0.0..=2.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS pitch must be between 0 and 2.0"})),
            ));
        }
        settings.tts_pitch = value;
    }
    if let Some(value) = body.tts_volume {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"TTS volume must be between 0 and 1.0"})),
            ));
        }
        settings.tts_volume = value;
    }
    if let Some(v) = body.soundtrack_provider {
        if !["local", "youtube", "soundcloud", "apple_music", "spotify"].contains(&v.as_str()) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown soundtrack provider"})),
            ));
        }
        settings.soundtrack_provider = v;
    }

    if ffmpeg_changed {
        settings.ffmpeg_restart_required = true;
    }
    // A cache limit is an explicit opt-in to evict derived artifacts. Apply
    // it before confirming the PATCH so Settings does not misleadingly show
    // a ceiling that will only be enforced on some later thumbnail request
    // or automatic-cleanup timer tick. Removing the limit remains
    // non-destructive and therefore does not trim anything.
    let thumbnail_cache_limit_to_enforce = if thumbnail_cache_limit_changed {
        settings.thumbnail_cache_max_bytes
    } else {
        None
    };
    let response = settings_response(&state, &peer, &settings, None);
    save_settings(&state.data_dir, &settings);
    drop(settings);
    if let Some(limit) = thumbnail_cache_limit_to_enforce {
        let thumbs_dir = state.thumbs_dir.clone();
        match tokio::task::spawn_blocking(move || {
            crate::storage::trim_thumbnail_cache(&thumbs_dir, limit)
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => tracing::warn!("Could not enforce thumbnail cache limit: {error}"),
            Err(error) => tracing::warn!("Thumbnail cache limiter did not complete: {error}"),
        }
    }
    Ok(Json(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remote_settings_do_not_disclose_local_tool_paths() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let Json(value) = get(
            State(state),
            Some(ConnectInfo(SocketAddr::from(([100, 80, 0, 2], 42168)))),
        )
        .await;
        assert!(value.get("ffmpeg_bin").is_none());
        assert_eq!(value["external_tool_settings_local_only"], true);
        assert_eq!(value["local_integration_settings_local_only"], true);
    }

    #[tokio::test]
    async fn remote_settings_cannot_change_local_integration_controls() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let body: PatchSettingsBody = serde_json::from_value(json!({
            "keep_running_in_tray": false
        }))
        .unwrap();
        let response = patch(
            State(state),
            Some(ConnectInfo(SocketAddr::from(([100, 80, 0, 2], 42168)))),
            Json(body),
        )
        .await;
        assert!(matches!(response, Err((StatusCode::FORBIDDEN, _))));
    }

    #[tokio::test]
    async fn server_settings_cannot_enable_host_tray_controls() {
        let root = tempfile::tempdir().unwrap();
        let host_state = crate::test_support::state(root.path());
        let mut server_state = (*host_state).clone();
        server_state.edition = crate::edition::Edition::Server;
        let body: PatchSettingsBody = serde_json::from_value(json!({
            "start_with_windows": true
        }))
        .unwrap();
        let response = patch(State(Arc::new(server_state)), None, Json(body)).await;
        assert!(matches!(response, Err((StatusCode::FORBIDDEN, _))));
    }

    #[tokio::test]
    async fn local_settings_response_surfaces_an_actionable_stale_startup_entry() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let settings = state.settings.read().await;
        let value = settings_response(
            &state,
            &None,
            &settings,
            Some(crate::StartupRegistration {
                supported: true,
                registered: false,
                state: "stale".into(),
                message: "Windows startup points to a different Curator executable. Enable Start with Windows to repair it.".into(),
                actual_command: Some(r#""C:\\Old Curator\\Curator.exe" --background"#.into()),
                expected_command: Some(r#""C:\\Curator\\Curator.exe" --background"#.into()),
                repair_available: true,
            }),
        );
        assert_eq!(value["startup_registration"]["state"], "stale");
        assert_eq!(value["startup_registration"]["repair_available"], true);
        assert!(value["startup_registration"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("repair"));
    }

    #[tokio::test]
    async fn displayed_settings_round_trip_through_patch_and_disk() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let body: PatchSettingsBody = serde_json::from_value(serde_json::json!({
            "max_concurrent": 8,
            "keep_running_in_tray": false,
            "max_clip_length_secs": 91,
            "max_download_file_size_bytes": 50 * 1024 * 1024,
            "max_source_storage_bytes": 1024 * 1024 * 1024,
            "minimum_free_disk_bytes": 2 * 1024 * 1024,
            "thumbnail_cache_max_bytes": 3 * 1024 * 1024,
            "apply_download_limits_to_local_imports": true,
            "automatic_cleanup_mode": "low_disk",
            "automatic_cleanup_confirmation": "ENABLE AUTOMATIC CLEANUP",
            "automatic_cleanup_low_disk_bytes": 4 * 1024 * 1024,
            "archive_retention_days": 30,
            "archive_retention_confirmation": "ENABLE ARCHIVE RETENTION",
            "default_slideshow_speed": 2500.0,
            "default_slideshow_loop": false,
            "default_slideshow_shuffle": true,
            "theme": "midnight",
            "export_reminder_days": 14,
            "export_reminder_snoozed_until": "2027-01-02T00:00:00Z",
            "ch_log_sessions": true,
            "ch_default_interval": 7.5,
            "ch_default_limit": 12,
            "ch_default_shuffle": true,
            "ch_default_media_type": "video",
            "nsfw_filter_enabled": true,
            "library_layout": "table",
            "last_play_mode": "slideshow",
            "search_providers": ["local", "booru"],
            "metronome_enabled": true,
            "metronome_volume": 0.4,
            "goon_persona": "dom",
            "tts_voice": "en-US-test",
            "tts_rate": 0.1,
            "tts_pitch": 0.0,
            "tts_volume": 0.0,
            "soundtrack_provider": "spotify"
        }))
        .unwrap();
        let Json(value) = patch(State(state.clone()), None, Json(body)).await.unwrap();
        assert_eq!(value["max_download_file_size_bytes"], 50 * 1024 * 1024u64);
        assert_eq!(value["max_source_storage_bytes"], 1024 * 1024 * 1024u64);
        assert_eq!(value["automatic_cleanup_mode"], "low_disk");
        assert_eq!(value["tts_rate"], 0.1);
        assert_eq!(value["tts_pitch"], 0.0);
        assert_eq!(value["tts_volume"], 0.0);
        assert_eq!(value["nsfw_restart_required"], true);
        for (field, expected) in [
            ("max_concurrent", json!(8)),
            ("keep_running_in_tray", json!(false)),
            ("max_clip_length_secs", json!(91)),
            ("minimum_free_disk_bytes", json!(2 * 1024 * 1024)),
            ("thumbnail_cache_max_bytes", json!(3 * 1024 * 1024)),
            ("apply_download_limits_to_local_imports", json!(true)),
            ("automatic_cleanup_low_disk_bytes", json!(4 * 1024 * 1024)),
            ("archive_retention_days", json!(30)),
            ("default_slideshow_speed", json!(2500.0)),
            ("default_slideshow_loop", json!(false)),
            ("default_slideshow_shuffle", json!(true)),
            ("theme", json!("midnight")),
            ("export_reminder_days", json!(14)),
            ("nsfw_filter_enabled", json!(true)),
            ("library_layout", json!("table")),
            ("metronome_enabled", json!(true)),
            ("metronome_volume", json!(0.4)),
            ("goon_persona", json!("dom")),
            ("tts_voice", json!("en-US-test")),
            ("soundtrack_provider", json!("spotify")),
        ] {
            assert_eq!(
                value[field], expected,
                "PATCH response lost displayed {field}"
            );
        }

        let persisted = crate::db::load_settings(root.path());
        assert_eq!(persisted.max_concurrent, 8);
        assert!(!persisted.keep_running_in_tray);
        assert_eq!(persisted.max_clip_length_secs, 91);
        assert_eq!(
            persisted.max_download_file_size_bytes,
            Some(50 * 1024 * 1024)
        );
        assert_eq!(persisted.max_source_storage_bytes, Some(1024 * 1024 * 1024));
        assert_eq!(persisted.minimum_free_disk_bytes, Some(2 * 1024 * 1024));
        assert_eq!(persisted.thumbnail_cache_max_bytes, Some(3 * 1024 * 1024));
        assert!(persisted.apply_download_limits_to_local_imports);
        assert_eq!(persisted.automatic_cleanup_mode, "low_disk");
        assert_eq!(
            persisted.automatic_cleanup_low_disk_bytes,
            Some(4 * 1024 * 1024)
        );
        assert_eq!(persisted.archive_retention_days, Some(30));
        assert_eq!(persisted.default_slideshow_speed, 2500.0);
        assert!(!persisted.default_slideshow_loop);
        assert!(persisted.default_slideshow_shuffle);
        assert_eq!(persisted.theme, "midnight");
        assert_eq!(persisted.export_reminder_days, 14);
        assert!(persisted.nsfw_filter_enabled);
        assert_eq!(persisted.library_layout, "table");
        assert!(persisted.metronome_enabled);
        assert_eq!(persisted.metronome_volume, 0.4);
        assert_eq!(persisted.goon_persona, "dom");
        assert_eq!(persisted.tts_voice.as_deref(), Some("en-US-test"));
        assert_eq!(persisted.tts_rate, 0.1);
        assert_eq!(persisted.tts_pitch, 0.0);
        assert_eq!(persisted.tts_volume, 0.0);
        assert_eq!(persisted.soundtrack_provider, "spotify");

        let Json(reopened) = get(State(state), None).await;
        for (field, expected) in [
            ("max_download_file_size_bytes", json!(50 * 1024 * 1024u64)),
            ("max_source_storage_bytes", json!(1024 * 1024 * 1024u64)),
            ("minimum_free_disk_bytes", json!(2 * 1024 * 1024)),
            ("thumbnail_cache_max_bytes", json!(3 * 1024 * 1024)),
            ("automatic_cleanup_mode", json!("low_disk")),
            ("default_slideshow_speed", json!(2500.0)),
            ("theme", json!("midnight")),
            ("nsfw_filter_enabled", json!(true)),
            ("goon_persona", json!("dom")),
            ("tts_voice", json!("en-US-test")),
            ("soundtrack_provider", json!("spotify")),
        ] {
            assert_eq!(
                reopened[field], expected,
                "GET after persistence lost {field}"
            );
        }
    }

    #[tokio::test]
    async fn saving_a_thumbnail_limit_evicts_derived_cache_immediately() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let cached = state.thumbs_dir.join("1.jpg");
        std::fs::write(&cached, vec![7; 16]).unwrap();

        let body: PatchSettingsBody =
            serde_json::from_value(json!({"thumbnail_cache_max_bytes": 8})).unwrap();
        let Json(value) = patch(State(state.clone()), None, Json(body)).await.unwrap();

        assert_eq!(value["thumbnail_cache_max_bytes"], 8);
        assert!(
            !cached.exists(),
            "a saved cap must be enforced now, not deferred until another thumbnail request"
        );
    }

    #[tokio::test]
    async fn destructive_cleanup_policies_require_server_side_confirmation() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());

        let missing_cleanup_confirmation: PatchSettingsBody =
            serde_json::from_value(json!({"automatic_cleanup_mode": "weekly"})).unwrap();
        assert!(matches!(
            patch(
                State(state.clone()),
                None,
                Json(missing_cleanup_confirmation)
            )
            .await,
            Err((StatusCode::BAD_REQUEST, _))
        ));

        let enabled_cleanup: PatchSettingsBody = serde_json::from_value(json!({
            "automatic_cleanup_mode": "weekly",
            "automatic_cleanup_confirmation": "ENABLE AUTOMATIC CLEANUP"
        }))
        .unwrap();
        assert!(patch(State(state.clone()), None, Json(enabled_cleanup))
            .await
            .is_ok());

        let missing_archive_confirmation: PatchSettingsBody =
            serde_json::from_value(json!({"archive_retention_days": 30})).unwrap();
        assert!(matches!(
            patch(
                State(state.clone()),
                None,
                Json(missing_archive_confirmation)
            )
            .await,
            Err((StatusCode::BAD_REQUEST, _))
        ));

        let enabled_archive: PatchSettingsBody = serde_json::from_value(json!({
            "archive_retention_days": 30,
            "archive_retention_confirmation": "ENABLE ARCHIVE RETENTION"
        }))
        .unwrap();
        assert!(patch(State(state), None, Json(enabled_archive))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn tts_boundaries_are_accepted_and_invalid_values_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        for (field, value) in [
            ("tts_rate", 0.09),
            ("tts_rate", 3.01),
            ("tts_pitch", -0.01),
            ("tts_pitch", 2.01),
            ("tts_volume", -0.01),
            ("tts_volume", 1.01),
        ] {
            let payload = match field {
                "tts_rate" => serde_json::json!({"tts_rate": value}),
                "tts_pitch" => serde_json::json!({"tts_pitch": value}),
                _ => serde_json::json!({"tts_volume": value}),
            };
            let body: PatchSettingsBody = serde_json::from_value(payload).unwrap();
            let response = patch(State(state.clone()), None, Json(body)).await;
            assert!(
                matches!(response, Err((StatusCode::BAD_REQUEST, _))),
                "{field}={value}"
            );
        }
        for payload in [
            serde_json::json!({"tts_rate": 0.1}),
            serde_json::json!({"tts_rate": 3.0}),
            serde_json::json!({"tts_pitch": 0.0}),
            serde_json::json!({"tts_pitch": 2.0}),
            serde_json::json!({"tts_volume": 0.0}),
            serde_json::json!({"tts_volume": 1.0}),
        ] {
            let body: PatchSettingsBody = serde_json::from_value(payload).unwrap();
            assert!(patch(State(state.clone()), None, Json(body)).await.is_ok());
        }
    }
}

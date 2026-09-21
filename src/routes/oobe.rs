use std::path::Path;
use std::sync::Arc;

use axum::{
    extract::{ConnectInfo, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use tokio::sync::Semaphore;

use crate::oobe as logic;
use crate::{config, db, phar, AppState};

fn err(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": message.into() })))
}

// OOBE can change the data directory and executable paths. It is deliberately
// local-only even though normal library administration may be reached through
// a deliberately granted Tailnet device. In-process Tauri requests do not
// carry ConnectInfo, and are local by construction.
fn ensure_local(peer: &Option<ConnectInfo<SocketAddr>>) -> Result<(), (StatusCode, Json<Value>)> {
    if peer.as_ref().is_none_or(|peer| peer.0.ip().is_loopback()) {
        Ok(())
    } else {
        Err(err(
            StatusCode::FORBIDDEN,
            "Setup and executable-path configuration are local-only.",
        ))
    }
}

// ─── "/" and "/index.html" — the actual OOBE gate ───────────────────────────
//
// Registered as explicit routes ahead of the static-file fallback_service in
// main.rs, so they win the match for exactly these two paths; every other
// static asset (app.js, style.css, oobe.js, ...) still falls through to
// ServeDir untouched. This is the entire mechanism by which "OOBE appears
// instead of the normal UI" — no changes to app.js's own boot sequence were
// needed.

pub async fn serve_root(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Response {
    if state.edition != crate::edition::Edition::Server {
        return StatusCode::NOT_FOUND.into_response();
    }
    let completed = state.settings.read().await.oobe_completed;
    if !completed && ensure_local(&peer).is_err() {
        return (
            StatusCode::FORBIDDEN,
            "Initial setup is available only on this device.",
        )
            .into_response();
    }
    let filename = if completed { "index.html" } else { "oobe.html" };
    match tokio::fs::read_to_string(state.static_dir.join(filename)).await {
        Ok(body) => ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body).into_response(),
        Err(e) => {
            tracing::error!("could not read {filename} from {:?}: {e}", state.static_dir);
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    }
}

// ─── Shared status payload ───────────────────────────────────────────────────

/// True if a directory is writable, without ever creating one that doesn't
/// already exist — used for the *currently active* data_dir, which should
/// already exist since Curator is running against it right now. Candidate
/// directories the person is considering go through `/api/oobe/validate`
/// instead, which is allowed to create them.
fn quick_writable_check(path: &Path) -> bool {
    let probe = path.join(".curator_write_test");
    if std::fs::write(&probe, b"ok").is_ok() {
        let _ = std::fs::remove_file(&probe);
        true
    } else {
        false
    }
}

async fn build_status(state: &Arc<AppState>) -> Value {
    let gallery_dl_bin = state.gallery_dl_bin.clone();
    let ffprobe_bin = state.ffprobe_bin.clone();
    let ffmpeg_bin = state.ffmpeg_bin.clone();
    let python_bin = state.python_bin.clone();

    // Command::output() blocks the calling thread, so keep it off the async
    // runtime's worker threads — these three are independent, run them
    // concurrently rather than one after another.
    let (gallery_dl, ffprobe, ffmpeg, nsfw) = tokio::join!(
        tokio::task::spawn_blocking(move || logic::detect_gallery_dl(&gallery_dl_bin)),
        tokio::task::spawn_blocking(move || logic::detect_ffprobe(&ffprobe_bin)),
        tokio::task::spawn_blocking(move || logic::detect_ffprobe(&ffmpeg_bin)),
        tokio::task::spawn_blocking(move || logic::detect_nsfw_env_with_timeout(
            &python_bin,
            std::time::Duration::from_secs(10)
        )),
    );

    let existing_installation = {
        let pool = state.pool.clone();
        tokio::task::spawn_blocking(move || {
            pool.get()
                .map(|conn| logic::existing_installation_has_data(&conn))
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    };

    let settings = state.settings.read().await;
    let cfg = config::load_config_for(state.install_scope);

    json!({
        "oobe_completed": settings.oobe_completed,
        "existing_installation": existing_installation,
        "dependencies": {
            "gallery_dl": dep_json(gallery_dl.unwrap_or(missing_status("gallery-dl")), true),
            "ffmpeg":     dep_json(ffprobe.unwrap_or(missing_status("ffprobe")), false),
            "ffmpeg_sampler": dep_json(ffmpeg.unwrap_or(missing_status("ffmpeg")), false),
            "nsfw":       dep_json(nsfw.unwrap_or(missing_status("python")), false),
        },
        "data_dir": {
            "path": state.data_dir.to_string_lossy(),
            "writable": quick_writable_check(&state.data_dir),
        },
        "config": {
            // What's actually active *this run* (may differ from a value
            // just saved to config.json until the next restart).
            "gallery_dl_bin": state.gallery_dl_bin,
            "ffprobe_bin": state.ffprobe_bin,
            "ffmpeg_bin": state.ffmpeg_bin,
            "python_bin": state.python_bin,
            "phar": crate::phar::status(&state.data_dir, state.install_scope),
            // What's on disk right now, for the frontend to detect
            // "you have unsaved / pending-restart changes".
            "pending_data_dir": cfg.data_dir,
        },
        "settings": {
            "max_concurrent": settings.max_concurrent,
            "theme": settings.theme,
            "default_slideshow_speed": settings.default_slideshow_speed,
            "default_slideshow_loop": settings.default_slideshow_loop,
            "default_slideshow_shuffle": settings.default_slideshow_shuffle,
            "nsfw_filter_enabled": settings.nsfw_filter_enabled,
        },
    })
}

fn missing_status(checked: &str) -> logic::DependencyStatus {
    logic::DependencyStatus {
        found: false,
        version: None,
        detail: Some("Check failed unexpectedly.".to_string()),
        checked: checked.to_string(),
    }
}

fn dep_json(status: logic::DependencyStatus, required: bool) -> Value {
    let mut v = serde_json::to_value(status).unwrap_or_default();
    v["required"] = json!(required);
    v
}

// ─── GET /api/oobe/status ────────────────────────────────────────────────────

pub async fn status(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Response {
    match ensure_local(&peer) {
        Ok(()) => Json(build_status(&state).await).into_response(),
        Err(error) => error.into_response(),
    }
}

// ─── POST /api/oobe/validate ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ValidateBody {
    pub check: String,
    pub path: Option<String>,
}

pub async fn validate(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<ValidateBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    ensure_local(&peer)?;
    match body.check.as_str() {
        "gallery_dl" | "ffprobe" | "ffmpeg" | "nsfw" => {
            let bin = match body.path {
                Some(p) => logic::sanitize_path_input(&p)
                    .map_err(|e| err(StatusCode::BAD_REQUEST, e))?
                    .to_string_lossy()
                    .to_string(),
                None => match body.check.as_str() {
                    "gallery_dl" => state.gallery_dl_bin.clone(),
                    "ffprobe" => state.ffprobe_bin.clone(),
                    "ffmpeg" => state.ffmpeg_bin.clone(),
                    _ => state.python_bin.clone(),
                },
            };
            let check = body.check.clone();
            let status = tokio::task::spawn_blocking(move || match check.as_str() {
                "gallery_dl" => logic::detect_gallery_dl(&bin),
                "ffprobe" | "ffmpeg" => logic::detect_ffprobe(&bin),
                _ => logic::detect_nsfw_env_with_timeout(&bin, std::time::Duration::from_secs(10)),
            })
            .await
            .map_err(|_| {
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "The check crashed unexpectedly.",
                )
            })?;
            Ok(Json(serde_json::to_value(status).unwrap_or_default()))
        }
        "data_dir" => {
            let raw = body
                .path
                .ok_or_else(|| err(StatusCode::BAD_REQUEST, "No directory was given."))?;
            let path =
                logic::sanitize_path_input(&raw).map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
            let result = tokio::task::spawn_blocking(move || logic::check_writable_dir(&path))
                .await
                .map_err(|_| {
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "The check crashed unexpectedly.",
                    )
                })?;
            match result {
                Ok(()) => Ok(Json(json!({ "writable": true, "path": raw }))),
                Err(e) => Ok(Json(json!({ "writable": false, "path": raw, "error": e }))),
            }
        }
        other => Err(err(
            StatusCode::BAD_REQUEST,
            format!("Unknown check: {other}"),
        )),
    }
}

// ─── POST /api/oobe/settings ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct OobeSettingsBody {
    // config.json-backed — take effect on next restart.
    pub data_dir: Option<String>,
    pub gallery_dl_bin: Option<String>,
    pub ffprobe_bin: Option<String>,
    pub ffmpeg_bin: Option<String>,
    /// Recording consent is local-only. Managed setup is evaluated after the
    /// backend has already started and never runs inside an OS installer.
    pub phar_setup_requested: Option<bool>,
    // settings.json-backed — take effect immediately, same fields the
    // normal Settings modal exposes (see routes/settings.rs).
    pub max_concurrent: Option<u32>,
    pub theme: Option<String>,
    pub default_slideshow_speed: Option<f64>,
    pub default_slideshow_loop: Option<bool>,
    pub default_slideshow_shuffle: Option<bool>,
    pub nsfw_filter_enabled: Option<bool>,
}

/// A configured executable is either a bare command name (resolved via
/// PATH at call time, e.g. the "gallery-dl" default) or an explicit path.
/// If it looks like a path, it must actually exist — this is the one place
/// OOBE could otherwise be tricked into silently "saving" a typo'd path
/// that will just fail on every future launch.
fn validate_executable_field(raw: &str) -> Result<String, String> {
    let path = logic::sanitize_path_input(raw)?;
    let looks_like_path = raw.contains('/') || raw.contains('\\');
    if looks_like_path && !path.is_file() {
        return Err(format!("'{raw}' doesn't exist."));
    }
    Ok(path.to_string_lossy().to_string())
}

pub async fn save_settings(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<OobeSettingsBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    ensure_local(&peer)?;
    // ── config.json fields ────────────────────────────────────────────────
    let mut cfg_dirty = false;
    let mut cfg = config::load_config_for(state.install_scope);

    if let Some(raw) = &body.data_dir {
        let path = logic::sanitize_path_input(raw).map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
        let path_for_check = path.clone();
        let result =
            tokio::task::spawn_blocking(move || logic::check_writable_dir(&path_for_check))
                .await
                .map_err(|_| {
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "The check crashed unexpectedly.",
                    )
                })?;
        result.map_err(|e| err(StatusCode::BAD_REQUEST, e))?;
        cfg.data_dir = Some(path.to_string_lossy().to_string());
        cfg_dirty = true;
    }
    if let Some(raw) = &body.gallery_dl_bin {
        cfg.gallery_dl_bin =
            Some(validate_executable_field(raw).map_err(|e| err(StatusCode::BAD_REQUEST, e))?);
        cfg_dirty = true;
    }
    if let Some(raw) = &body.ffprobe_bin {
        cfg.ffprobe_bin =
            Some(validate_executable_field(raw).map_err(|e| err(StatusCode::BAD_REQUEST, e))?);
        cfg_dirty = true;
    }
    if let Some(raw) = &body.ffmpeg_bin {
        cfg.ffmpeg_bin =
            Some(validate_executable_field(raw).map_err(|e| err(StatusCode::BAD_REQUEST, e))?);
        cfg_dirty = true;
    }
    if cfg_dirty {
        config::save_config_for(state.install_scope, &cfg).map_err(|e| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Could not save config.json: {e}"),
            )
        })?;
    }

    if let Some(enabled) = body.phar_setup_requested {
        // This path persists only consent/runtime metadata. It deliberately
        // performs no clone, dependency install, or checkpoint download.
        phar::record_install_intent(&state.data_dir, state.install_scope, enabled, None).map_err(
            |error| {
                err(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Could not record P-HAR setup intent: {error}"),
                )
            },
        )?;
        if enabled {
            // OOBE occurs after the backend has started. With unverified
            // checkpoints this transparently records Blocked rather than
            // pretending an environment is ready.
            phar::resume_requested_setup(&state.data_dir, state.install_scope).map_err(
                |error| {
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Could not evaluate P-HAR setup: {error}"),
                    )
                },
            )?;
        }
    }

    // ── settings.json fields (reuses the exact same field semantics as
    //    PATCH /api/settings — see routes/settings.rs) ──────────────────────
    {
        let mut settings = state.settings.write().await;
        if let Some(v) = body.max_concurrent {
            let v = v.clamp(1, 20);
            settings.max_concurrent = v;
            let mut sem_guard = state.download_semaphore.lock().await;
            *sem_guard = Arc::new(Semaphore::new(v as usize));
        }
        if let Some(ref theme) = body.theme {
            if !crate::routes::settings::VALID_THEMES.contains(&theme.as_str()) {
                return Err(err(
                    StatusCode::BAD_REQUEST,
                    format!("Unknown theme: {theme}"),
                ));
            }
            settings.theme = theme.clone();
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
        if let Some(v) = body.nsfw_filter_enabled {
            settings.nsfw_filter_enabled = v;
        }
        db::save_settings(&state.data_dir, &settings);
    }

    Ok(Json(build_status(&state).await))
}

// ─── POST /api/oobe/complete ──────────────────────────────────────────────────

pub async fn complete(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    ensure_local(&peer)?;
    let bin = state.gallery_dl_bin.clone();
    let gallery_dl = tokio::task::spawn_blocking(move || logic::detect_gallery_dl(&bin))
        .await
        .map_err(|_| {
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "The check crashed unexpectedly.",
            )
        })?;

    if !gallery_dl.found {
        // Never silently mark setup complete without a working gallery-dl —
        // it's the one dependency Curator can't function without.
        return Err(err(
            StatusCode::BAD_REQUEST,
            gallery_dl
                .detail
                .unwrap_or_else(|| "gallery-dl was not found.".to_string()),
        ));
    }

    {
        let mut settings = state.settings.write().await;
        settings.oobe_completed = true;
        db::save_settings(&state.data_dir, &settings);
    }

    Ok(Json(build_status(&state).await))
}

// ─── POST /api/oobe/reset ("Run Setup Again") ─────────────────────────────────

pub async fn reset(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    ensure_local(&peer)?;
    // Deliberately touches nothing but the one flag — no downloads, no
    // database rows, no other settings are affected. Re-running the wizard
    // just reopens it with everything prefilled from current state.
    let mut settings = state.settings.write().await;
    settings.oobe_completed = false;
    db::save_settings(&state.data_dir, &settings);
    Ok(Json(json!({ "ok": true })))
}

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};

use crate::routes::media::db_err;
use crate::AppState;

// ─── GET /api/stats ──────────────────────────────────────────────────────────

pub async fn stats(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;

    let total_media: i64 = conn
        .query_row("SELECT COUNT(*) FROM media WHERE downloaded=1", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);

    let total_sources: i64 = conn
        .query_row("SELECT COUNT(*) FROM sources", [], |r| r.get(0))
        .unwrap_or(0);

    let sources_done: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sources WHERE status='done'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let sources_error: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sources WHERE status='error'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let sources_downloading: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sources WHERE status='downloading'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);

    let total_groups: i64 = conn
        .query_row("SELECT COUNT(*) FROM groups", [], |r| r.get(0))
        .unwrap_or(0);

    let total_tags: i64 = conn
        .query_row("SELECT COUNT(*) FROM tags", [], |r| r.get(0))
        .unwrap_or(0);

    // Placeholder count (pre-scanned but not yet on disk)
    let placeholder_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM media WHERE downloaded=0", [], |r| {
            r.get(0)
        })
        .unwrap_or(0);

    let settings = state.settings.read().await;

    Ok(Json(json!({
        "total_media":            total_media,
        "placeholder_count":      placeholder_count,
        "total_sources":          total_sources,
        "sources_done":           sources_done,
        "sources_error":          sources_error,
        "sources_downloading":    sources_downloading,
        "total_groups":           total_groups,
        "total_tags":             total_tags,
        "last_export_at":         settings.last_export_at,
        "export_reminder_days":   settings.export_reminder_days,
        "export_reminder_snoozed_until": settings.export_reminder_snoozed_until,
    })))
}

// ─── GET /api/log ────────────────────────────────────────────────────────────
// Streams the last 5000 lines of the current redacted diagnostic log as plain text.

pub async fn get_log(State(state): State<Arc<AppState>>) -> Response {
    let tail = crate::services::diagnostics::read_tail(&state.log_path, 5000).unwrap_or_default();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        tail,
    )
        .into_response()
}

// ─── GET /api/sources/:id/log ────────────────────────────────────────────────

pub async fn source_log(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let row = conn.query_row(
        "SELECT log, error_message FROM sources WHERE id=?1",
        [id],
        |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
            ))
        },
    );
    match row {
        Ok((log, err_msg)) => Ok(Json(json!({
            "log": log.map(|text| crate::services::diagnostics::redact(&text)),
            "error_message": err_msg.map(|text| crate::services::diagnostics::redact(&text)),
        }))),
        Err(_) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Source not found"})),
        )),
    }
}

// ─── Helper ───────────────────────────────────────────────────────────────────

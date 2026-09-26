use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Deserializer};
use serde_json::{json, Value};

use crate::db::now_iso;
use crate::downloader::run_download;
use crate::routes::media::db_err;
use crate::services::sources::row_to_json;
use crate::slug::split_bulk_input;
use crate::AppState;

// ─── Models ───────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct AddSourcesBody {
    #[serde(default)]
    pub urls: Vec<String>,
    pub text: Option<String>,
}

#[derive(Deserialize)]
pub struct PatchSourceBody {
    pub name: Option<String>,
    pub included: Option<bool>,
    /// Zero disables retention. A positive value keeps that many newest
    /// downloaded items; protected media may cause the actual retained count
    /// to be larger.
    /// An explicit JSON `null` also disables the rule.  Keeping it distinct
    /// from an omitted property makes PATCH a predictable round trip for
    /// clients that model optional settings as nullable values.
    #[serde(default, deserialize_with = "deserialize_nullable_u32")]
    pub retention_keep_newest: Option<Option<u32>>,
    /// Required when automatic cleanup is already armed. A retention rule is
    /// otherwise only a dormant preference until the later global cleanup
    /// confirmation enables it.
    pub retention_confirmation: Option<String>,
}

fn deserialize_nullable_u32<'de, D>(deserializer: D) -> Result<Option<Option<u32>>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Some(Option::<u32>::deserialize(deserializer)?))
}

#[derive(Deserialize)]
pub struct SetGroupBody {
    pub group_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct DeleteQuery {
    #[serde(default)]
    pub delete_files: bool,
}

// ─── GET /api/sources ────────────────────────────────────────────────────────

pub async fn list(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let mut stmt = conn
        .prepare(
            "SELECT s.*, \
            (SELECT id FROM media m WHERE m.source_id = s.id AND m.type = 'image' \
             ORDER BY m.id LIMIT 1) AS thumbnail_id \
         FROM sources s ORDER BY s.added_at DESC",
        )
        .map_err(db_err)?;

    let sources: Vec<Value> = stmt
        .query_map([], row_to_json)
        .map_err(db_err)?
        .filter_map(|r| r.ok())
        .collect();

    Ok(Json(json!({ "sources": sources })))
}

// ─── GET /api/sources/:id ────────────────────────────────────────────────────

pub async fn get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let row = conn.query_row("SELECT * FROM sources WHERE id=?1", [id], row_to_json);
    match row {
        Ok(v) => Ok(Json(v)),
        Err(_) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Source not found"})),
        )),
    }
}

// ─── POST /api/sources ───────────────────────────────────────────────────────

pub async fn add(
    State(state): State<Arc<AppState>>,
    Json(body): Json<AddSourcesBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut candidates = body.urls;
    if let Some(text) = body.text {
        candidates.extend(split_bulk_input(&text));
    }
    if candidates.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No valid URLs provided"})),
        ));
    }
    let result = create_sources_from_urls(Arc::clone(&state), candidates).await?;
    if result["sources"]
        .as_array()
        .map(|a| a.is_empty())
        .unwrap_or(true)
        && result["duplicates"]
            .as_array()
            .map(|a| a.is_empty())
            .unwrap_or(true)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No valid URLs provided"})),
        ));
    }
    Ok(Json(result))
}

// ─── PATCH /api/sources/:id ──────────────────────────────────────────────────

pub async fn patch(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<PatchSourceBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let automatic_cleanup_armed = state.settings.read().await.automatic_cleanup_mode != "never";
    let conn = state.pool.get().map_err(db_err)?;

    let mut fields: Vec<String> = Vec::new();
    let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

    if let Some(name) = body.name {
        fields.push("name=?".to_string());
        values.push(Box::new(name));
    }
    if let Some(included) = body.included {
        fields.push("included=?".to_string());
        values.push(Box::new(if included { 1i64 } else { 0i64 }));
    }
    if let Some(keep_newest) = body.retention_keep_newest {
        let keep_newest = keep_newest.unwrap_or(0);
        if keep_newest > 1_000_000 {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Retention must be at most 1,000,000 items"})),
            ));
        }
        if keep_newest > 0
            && automatic_cleanup_armed
            && body.retention_confirmation.as_deref() != Some("ENABLE RETENTION")
        {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error":"Type ENABLE RETENTION before adding a rule while automatic cleanup is enabled."
                })),
            ));
        }
        if keep_newest == 0 {
            fields.push("retention_keep_newest=NULL".to_string());
        } else {
            fields.push("retention_keep_newest=?".to_string());
            values.push(Box::new(i64::from(keep_newest)));
        }
    }
    if fields.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Nothing to update"})),
        ));
    }

    let sql = format!("UPDATE sources SET {} WHERE id=?", fields.join(", "));
    values.push(Box::new(id));
    let refs: Vec<&dyn rusqlite::ToSql> = values.iter().map(|b| b.as_ref()).collect();
    conn.execute(&sql, refs.as_slice()).map_err(db_err)?;

    match conn.query_row("SELECT * FROM sources WHERE id=?1", [id], row_to_json) {
        Ok(v) => Ok(Json(v)),
        Err(_) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Source not found"})),
        )),
    }
}

// ─── PATCH /api/sources/:id/group ────────────────────────────────────────────

pub async fn set_group(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<SetGroupBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;

    let exists: bool = conn
        .query_row("SELECT COUNT(*) FROM sources WHERE id=?1", [id], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap_or(0)
        > 0;
    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Source not found"})),
        ));
    }

    if let Some(gid) = body.group_id {
        let g_exists: bool = conn
            .query_row("SELECT COUNT(*) FROM groups WHERE id=?1", [gid], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap_or(0)
            > 0;
        if !g_exists {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "Group not found"})),
            ));
        }
    }

    conn.execute(
        "UPDATE sources SET group_id=?1 WHERE id=?2",
        rusqlite::params![body.group_id, id],
    )
    .map_err(db_err)?;

    Ok(Json(
        conn.query_row("SELECT * FROM sources WHERE id=?1", [id], row_to_json)
            .map_err(db_err)?,
    ))
}

// ─── POST /api/sources/:id/resync ────────────────────────────────────────────

pub async fn resync(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if state.maintenance.is_active() {
        return Ok(Json(json!({ "status": "maintenance" })));
    }
    let conn = state.pool.get().map_err(db_err)?;
    let row = conn.query_row("SELECT status FROM sources WHERE id=?1", [id], |r| {
        r.get::<_, String>(0)
    });
    let status = match row {
        Ok(s) => s,
        Err(_) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Source not found"})),
            ))
        }
    };

    if status == "pending" || status == "downloading" {
        return Ok(Json(json!({ "status": "already_syncing" })));
    }
    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Ok(Json(json!({ "status": "paused" })));
    }

    conn.execute(
        "UPDATE sources SET status='pending',queued_at=?1,progress_updated_at=?1,current_filename=NULL,error_message=NULL WHERE id=?2",
        rusqlite::params![now_iso(), id],
    ).map_err(db_err)?;

    state
        .download_tasks
        .spawn(run_download(Arc::clone(&state), id));
    Ok(Json(json!({ "status": "queued" })))
}

// ─── POST /api/sources/resync-all ────────────────────────────────────────────

pub async fn resync_all(State(state): State<Arc<AppState>>) -> Json<Value> {
    if state.maintenance.is_active() {
        return Json(json!({ "queued": 0, "maintenance": true }));
    }
    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        return Json(json!({ "queued": 0, "paused": true }));
    }
    let ids: Vec<i64> = {
        let conn = state.pool.get().unwrap();
        let mut stmt = conn
            .prepare("SELECT id FROM sources WHERE status NOT IN ('pending','downloading')")
            .unwrap();
        stmt.query_map([], |r| r.get(0))
            .unwrap()
            .filter_map(|r| r.ok())
            .collect()
    };
    let count = ids.len();
    if let Ok(conn) = state.pool.get() {
        let now = now_iso();
        for id in &ids {
            let _ = conn.execute("UPDATE sources SET status='pending',queued_at=?1,progress_updated_at=?1,current_filename=NULL,error_message=NULL WHERE id=?2", rusqlite::params![now, id]);
        }
    }
    for id in ids {
        state
            .download_tasks
            .spawn(run_download(Arc::clone(&state), id));
    }
    Json(json!({ "queued": count }))
}

// ─── DELETE /api/sources/:id ─────────────────────────────────────────────────

pub async fn delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(q): Query<DeleteQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let slug: String = {
        let conn = state.pool.get().map_err(db_err)?;
        conn.query_row("SELECT slug FROM sources WHERE id=?1", [id], |r| r.get(0))
            .map_err(|_| {
                (
                    StatusCode::NOT_FOUND,
                    Json(json!({"error":"Source not found"})),
                )
            })?
    };
    if let Some(cancel) = state.source_cancellations.lock().await.get(&id).cloned() {
        cancel.cancel();
    }
    // Wait for the owning task to reap the child and finish its serialized index work.
    while state.running_sources.lock().await.contains(&id) {
        if let Some(cancel) = state.source_cancellations.lock().await.get(&id).cloned() {
            cancel.cancel();
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    {
        let conn = state.pool.get().map_err(db_err)?;
        conn.execute("DELETE FROM sources WHERE id=?1", [id])
            .map_err(db_err)?;
    }

    if q.delete_files {
        // A large source directory can take seconds to remove. Keep the
        // endpoint's existing best-effort semantics, but never occupy a Tokio
        // worker while Windows walks it.
        let library_dir = state.library_dir.clone();
        let archives_dir = state.archives_dir.clone();
        let slug_for_delete = slug.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let dest = library_dir.join(&slug_for_delete);
            if dest.exists() {
                let dest_long = dunce::simplified(&dest).to_path_buf();
                let _ = std::fs::remove_dir_all(&dest_long);
            }
            let archive = archives_dir.join(format!("{}.sqlite3", slug_for_delete));
            if archive.exists() {
                let _ = std::fs::remove_file(dunce::simplified(&archive));
            }
        })
        .await;
    }

    Ok(Json(json!({ "status": "deleted" })))
}

// ─── Shared create logic ──────────────────────────────────────────────────────

pub async fn create_sources_from_urls(
    state: Arc<AppState>,
    candidates: Vec<String>,
) -> Result<Value, (StatusCode, Json<Value>)> {
    let result = crate::services::sources::create(state, candidates).map_err(|error| {
        let status = match error {
            crate::services::sources::SourceError::Forbidden => StatusCode::FORBIDDEN,
            crate::services::sources::SourceError::ShuttingDown
            | crate::services::sources::SourceError::MaintenanceActive => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            crate::services::sources::SourceError::InvalidInput(_)
            | crate::services::sources::SourceError::InvalidUrl(_) => StatusCode::BAD_REQUEST,
            crate::services::sources::SourceError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({"error": error.message()})))
    })?;
    Ok(serde_json::to_value(result).expect("serializable source result"))
}

#[cfg(test)]
mod tests {
    use axum::{extract::Path, extract::State, Json};

    use super::*;

    #[test]
    fn nullable_retention_rule_has_an_explicit_disable_state() {
        let omitted: PatchSourceBody = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(omitted.retention_keep_newest, None);

        let disabled: PatchSourceBody =
            serde_json::from_value(serde_json::json!({"retention_keep_newest": null})).unwrap();
        assert_eq!(disabled.retention_keep_newest, Some(None));

        let enabled: PatchSourceBody =
            serde_json::from_value(serde_json::json!({"retention_keep_newest": 25})).unwrap();
        assert_eq!(enabled.retention_keep_newest, Some(Some(25)));
    }

    #[tokio::test]
    async fn retention_rule_round_trips_and_nullable_disable_persists() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);

        let enabled: PatchSourceBody =
            serde_json::from_value(serde_json::json!({"retention_keep_newest": 25})).unwrap();
        let Json(value) = patch(State(state.clone()), Path(1), Json(enabled))
            .await
            .unwrap();
        assert_eq!(value["retention_keep_newest"], 25);
        let persisted: Option<i64> = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT retention_keep_newest FROM sources WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted, Some(25));

        let disabled: PatchSourceBody =
            serde_json::from_value(serde_json::json!({"retention_keep_newest": null})).unwrap();
        let _ = patch(State(state.clone()), Path(1), Json(disabled))
            .await
            .unwrap();
        let cleared: Option<i64> = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT retention_keep_newest FROM sources WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(cleared, None);
    }

    #[tokio::test]
    async fn retention_rule_needs_confirmation_when_automatic_cleanup_is_armed() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.settings.write().await.automatic_cleanup_mode = "weekly".into();

        let missing: PatchSourceBody =
            serde_json::from_value(serde_json::json!({"retention_keep_newest": 5})).unwrap();
        assert!(matches!(
            patch(State(state.clone()), Path(1), Json(missing)).await,
            Err((StatusCode::BAD_REQUEST, _))
        ));

        let confirmed: PatchSourceBody = serde_json::from_value(serde_json::json!({
            "retention_keep_newest": 5,
            "retention_confirmation": "ENABLE RETENTION"
        }))
        .unwrap();
        assert!(patch(State(state), Path(1), Json(confirmed)).await.is_ok());
    }
}

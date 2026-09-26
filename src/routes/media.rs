use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, Query, State},
    http::{header, HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::now_iso;
use crate::provenance;
use crate::AppState;

/// Native clients address media by ID. The file resolver confines the path to
/// the library and the shared range plan accepts only a single byte interval.
pub async fn stream(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    headers: HeaderMap,
    method: Method,
) -> Response {
    let path = match crate::media_path(&state, id) {
        Ok(path) => path,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"Media not found"})),
            )
                .into_response()
        }
    };
    let mut file = match tokio::fs::File::open(&path).await {
        Ok(file) => file,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"Media not found"})),
            )
                .into_response()
        }
    };
    let total = match file.metadata().await {
        Ok(metadata) if metadata.is_file() => metadata.len(),
        _ => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"Media not found"})),
            )
                .into_response()
        }
    };
    let range = headers.get(header::RANGE).map(HeaderValue::to_str);
    let plan = match range {
        None => crate::services::media::plan_range(total, None),
        Some(Ok(value)) => crate::services::media::plan_range(total, Some(value)),
        Some(Err(_)) => Err(crate::services::media::InvalidRange),
    };
    let plan = match plan {
        Ok(plan) => plan,
        Err(_) => {
            let mut response = (
                StatusCode::RANGE_NOT_SATISFIABLE,
                Json(json!({"error":"Invalid or unsatisfiable byte range"})),
            )
                .into_response();
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{total}")).unwrap(),
            );
            return response;
        }
    };
    let mut response_headers = HeaderMap::new();
    response_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&plan.length.to_string()).unwrap(),
    );
    response_headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(media_content_type(&path)),
    );
    response_headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        HeaderValue::from_static("nosniff"),
    );
    if plan.partial {
        response_headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!("bytes {}-{}/{total}", plan.start, plan.end())).unwrap(),
        );
    }
    let status = if plan.partial {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    if method == Method::HEAD {
        return (status, response_headers, Body::empty()).into_response();
    }
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    if file
        .seek(std::io::SeekFrom::Start(plan.start))
        .await
        .is_err()
    {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(file.take(plan.length)));
    (status, response_headers, body).into_response()
}

fn media_content_type(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "mp4" | "m4v" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mov" => "video/quicktime",
        "mp3" => "audio/mpeg",
        "m4a" | "aac" => "audio/mp4",
        "ogg" | "oga" => "audio/ogg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        _ => "application/octet-stream",
    }
}

pub use crate::services::library::{tag_predicate, MediaQuery, EFFECTIVE_RATING_SQL};

pub async fn list(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MediaQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    crate::services::library::list(&state, query)
        .await
        .map(|page| Json(json!(page)))
        .map_err(|error| {
            let status = match error {
                crate::services::library::LibraryError::BadRequest(_) => StatusCode::BAD_REQUEST,
                crate::services::library::LibraryError::Internal(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
                crate::services::library::LibraryError::Forbidden => StatusCode::FORBIDDEN,
                crate::services::library::LibraryError::ShuttingDown => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                crate::services::library::LibraryError::Maintenance => StatusCode::CONFLICT,
            };
            (status, Json(json!({"error": error.message()})))
        })
}

// ─── PUT /api/media/:id/rating ───────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RatingBody {
    pub rating: i64,
}

pub async fn set_rating(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<i64>,
    Json(body): Json<RatingBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    review_response(crate::services::media::review(
        &state,
        super::actor_for_peer(peer),
        id,
        Some(body.rating),
    ))
}

pub async fn approve_rating(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    review_response(crate::services::media::review(
        &state,
        super::actor_for_peer(peer),
        id,
        None,
    ))
}

fn review_response(
    result: Result<crate::services::media::RatingReview, crate::services::media::MediaError>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    result
        .map(|review| Json(serde_json::to_value(review).expect("rating review serializes")))
        .map_err(review_error)
}

fn review_error(error: crate::services::media::MediaError) -> (StatusCode, Json<Value>) {
    use crate::services::media::MediaError;
    let status = match error {
        MediaError::Forbidden => StatusCode::FORBIDDEN,
        MediaError::ShuttingDown => StatusCode::SERVICE_UNAVAILABLE,
        MediaError::Maintenance | MediaError::NoAutomatedRating | MediaError::ChangedReview => {
            StatusCode::CONFLICT
        }
        MediaError::InvalidRating | MediaError::InvalidSelection | MediaError::InvalidTag => {
            StatusCode::BAD_REQUEST
        }
        MediaError::Missing => StatusCode::NOT_FOUND,
        MediaError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"error":error.message()})))
}

#[derive(Deserialize)]
pub struct DurationBody {
    pub duration_secs: f64,
}

pub async fn set_duration(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<DurationBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !body.duration_secs.is_finite() || body.duration_secs <= 0.0 || body.duration_secs > 604800.0
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid video duration"})),
        ));
    }
    let conn = state.pool.get().map_err(db_err)?;
    conn.execute("UPDATE media SET duration_secs=?1,duration_attempted=1 WHERE id=?2 AND type='video' AND duration_secs IS NULL", rusqlite::params![body.duration_secs,id]).map_err(db_err)?;
    Ok(Json(json!({"id":id})))
}

#[derive(Deserialize)]
pub struct UndoRatingBody {
    pub rating_reviewed_at: String,
}

pub async fn undo_rating(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<i64>,
    Json(body): Json<UndoRatingBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    review_response(crate::services::media::undo_review(
        &state,
        super::actor_for_peer(peer),
        id,
        &body.rating_reviewed_at,
    ))
}

#[derive(Deserialize)]
pub struct TagBody {
    pub name: String,
}

pub async fn add_tag(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<TagBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let exists: bool = conn
        .query_row("SELECT COUNT(*) FROM media WHERE id=?1", [id], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap_or(0)
        > 0;
    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Media not found"})),
        ));
    }

    provenance::attach_tag(&conn, id, &body.name, provenance::HUMAN, None).map_err(db_err)?;

    let tags: Vec<String> = {
        let mut stmt = conn.prepare("SELECT t.name FROM media_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.media_id=?1 ORDER BY t.name COLLATE NOCASE").map_err(db_err)?;
        let out = stmt
            .query_map([id], |r| r.get(0))
            .map_err(db_err)?
            .filter_map(|r| r.ok())
            .collect();
        out
    };

    Ok(Json(json!({ "media_id": id, "tags": tags })))
}

// ─── DELETE /api/media/:id/tags/:tag_id ──────────────────────────────────────

pub async fn remove_tag(
    State(state): State<Arc<AppState>>,
    Path((id, tag_id)): Path<(i64, i64)>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    conn.execute(
        "DELETE FROM media_tags WHERE media_id=?1 AND tag_id=?2",
        rusqlite::params![id, tag_id],
    )
    .map_err(db_err)?;
    Ok(Json(json!({ "status": "removed" })))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct BulkMediaBody {
    #[serde(default)]
    pub ids: Vec<i64>,
    pub action: String,
    pub group_id: Option<i64>,
    pub tag: Option<String>,
    pub rating: Option<i64>,
}

/// Server-side bulk organization for Explorer multi-select. The action names
/// intentionally mirror the UI and reuse the existing media/tag/metadata
/// models instead of creating a separate library store.
pub async fn bulk(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<BulkMediaBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut ids = body.ids.clone();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() || ids.len() > 500 || ids.iter().any(|id| *id <= 0) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Select between one and 500 valid media items"})),
        ));
    }
    let (updated, failed) = match body.action.as_str() {
        "add_group" | "move" => bulk_groups(&state, &ids, &body).await?,
        "add_tag" => bulk_add_tag(
            &state,
            super::actor_for_peer(peer),
            &ids,
            body.tag.as_deref(),
        )?,
        "remove_tag" => bulk_remove_tag(
            &state,
            super::actor_for_peer(peer),
            &ids,
            body.tag.as_deref(),
        )?,
        "set_rating" => bulk_set_rating(&state, super::actor_for_peer(peer), &ids, body.rating)?,
        "refresh_metadata" => {
            let worker_state = Arc::clone(&state);
            let worker_ids = ids.clone();
            tokio::task::spawn_blocking(move || bulk_refresh_metadata(&worker_state, &worker_ids))
                .await
                .map_err(db_err)??
        }
        "delete" => {
            let worker_state = Arc::clone(&state);
            let worker_ids = ids.clone();
            tokio::task::spawn_blocking(move || bulk_delete_files(&worker_state, &worker_ids))
                .await
                .map_err(db_err)??
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown bulk action"})),
            ))
        }
    };
    Ok(Json(
        json!({"action":body.action,"updated":updated,"failed":failed}),
    ))
}

async fn bulk_groups(
    state: &AppState,
    ids: &[i64],
    body: &BulkMediaBody,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    if body.action == "add_group" && body.group_id.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A group is required"})),
        ));
    }
    let updated = {
        let conn = state.pool.get().map_err(db_err)?;
        if let Some(group_id) = body.group_id {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM groups WHERE id=?1)",
                    [group_id],
                    |row| row.get(0),
                )
                .map_err(db_err)?;
            if !exists {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error":"Group not found"})),
                ));
            }
        }
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let mut changed = 0;
        for id in ids {
            if body.action == "move" {
                tx.execute("DELETE FROM media_groups WHERE media_id=?1", [id])
                    .map_err(db_err)?;
            }
            if let Some(group_id) = body.group_id {
                changed += tx.execute(
                    "INSERT OR IGNORE INTO media_groups(media_id,group_id,added_at) VALUES(?1,?2,?3)",
                    rusqlite::params![id, group_id, now_iso()],
                ).map_err(db_err)?;
            } else if body.action == "move" {
                changed += 1;
            }
        }
        tx.commit().map_err(db_err)?;
        changed
    };
    *state.group_tag_cache.write().await = None;
    Ok((updated, Vec::new()))
}

fn bulk_add_tag(
    state: &AppState,
    actor: crate::services::access::Actor,
    ids: &[i64],
    name: Option<&str>,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let result = crate::services::media::add_tag_many(state, actor, ids, name.unwrap_or_default())
        .map_err(review_error)?;
    Ok((result.updated, result.failed))
}

fn bulk_remove_tag(
    state: &AppState,
    actor: crate::services::access::Actor,
    ids: &[i64],
    name: Option<&str>,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let result =
        crate::services::media::remove_tag_many(state, actor, ids, name.unwrap_or_default())
            .map_err(review_error)?;
    Ok((result.updated, result.failed))
}

fn bulk_set_rating(
    state: &AppState,
    actor: crate::services::access::Actor,
    ids: &[i64],
    rating: Option<i64>,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let rating = rating.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A rating is required"})),
        )
    })?;
    let updated =
        crate::services::media::rate_many(state, actor, ids, rating).map_err(review_error)?;
    Ok((updated, Vec::new()))
}

fn bulk_refresh_metadata(
    state: &AppState,
    ids: &[i64],
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let root = dunce::canonicalize(&state.library_dir).map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let tx_conn = &*tx;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let row: Option<(String, Option<String>)> = tx_conn
            .query_row(
                "SELECT filepath,origin_url FROM media WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();
        let Some((filepath, origin_url)) = row else {
            failed.push(json!({"id":id,"error":"Media not found"}));
            continue;
        };
        let file = root.join(filepath);
        let sidecar = file.with_file_name(format!(
            "{}.json",
            file.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
        ));
        match std::fs::read_to_string(sidecar)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        {
            Some(metadata) => match provenance::capture_source_metadata(
                tx_conn,
                *id,
                origin_url.as_deref(),
                &metadata,
            ) {
                Ok(()) => {
                    tx_conn.execute("UPDATE media SET nsfw_state='pending',nsfw_attempts=0,nsfw_retry_at=0 WHERE id=?1", [id]).map_err(db_err)?;
                    updated += 1;
                }
                Err(error) => failed.push(json!({"id":id,"error":error.to_string()})),
            },
            None => failed.push(json!({"id":id,"error":"No valid gallery-dl metadata sidecar"})),
        }
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, failed))
}

fn bulk_delete_files(
    state: &AppState,
    ids: &[i64],
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let root = dunce::canonicalize(&state.library_dir).map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let tx_conn = &*tx;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let row: Option<(String, bool)> = tx_conn
            .query_row(
                "SELECT filepath,clip_start_secs IS NOT NULL FROM media WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();
        let Some((filepath, virtual_clip)) = row else {
            failed.push(json!({"id":id,"error":"Media not found"}));
            continue;
        };
        if virtual_clip {
            tx_conn
                .execute("UPDATE media SET downloaded=0,missing=1 WHERE id=?1", [id])
                .map_err(db_err)?;
            updated += 1;
            continue;
        }
        let Ok(path) = dunce::canonicalize(root.join(filepath)) else {
            failed.push(json!({"id":id,"error":"Local file is already unavailable"}));
            continue;
        };
        if !path.starts_with(&root) {
            failed.push(
                json!({"id":id,"error":"Refusing to delete a file outside Curator's library"}),
            );
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                tx_conn
                    .execute(
                        "UPDATE media SET downloaded=0,missing=1,file_size_bytes=NULL WHERE id=?1",
                        [id],
                    )
                    .map_err(db_err)?;
                updated += 1;
            }
            Err(error) => failed.push(json!({"id":id,"error":error.to_string()})),
        }
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, failed))
}

pub fn get_or_create_tag(conn: &rusqlite::Connection, name: &str) -> rusqlite::Result<i64> {
    let name = name.trim().to_lowercase();
    if name.is_empty() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if let Ok(id) = conn.query_row("SELECT id FROM tags WHERE name=?1", [&name], |r| {
        r.get::<_, i64>(0)
    }) {
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO tags (name, added_at) VALUES (?1, ?2)",
        rusqlite::params![name, now_iso()],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn db_err(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": e.to_string()})),
    )
}

pub async fn effective_tags(
    state: &AppState,
) -> Result<Arc<HashMap<i64, HashSet<String>>>, (StatusCode, Json<Value>)> {
    crate::services::library::effective_tags(state)
        .await
        .map_err(|error| db_err(error.message()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unknown_videos_stay_visible_and_browser_duration_moves_clips() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES(1,1,'v','v','video','2026');").unwrap();
        let videos = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("video".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(videos["media"].as_array().unwrap().len(), 1);
        let _ = set_duration(
            State(state.clone()),
            Path(1),
            Json(DurationBody {
                duration_secs: 45.0,
            }),
        )
        .await
        .unwrap();
        let clips = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("clip".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(clips["media"].as_array().unwrap().len(), 1);
        let videos = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("video".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(videos["media"].as_array().unwrap().is_empty());
        assert_eq!(
            set_duration(
                State(state),
                Path(1),
                Json(DurationBody {
                    duration_secs: -1.0
                })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn undo_review_restores_auto_queue_and_rejects_stale_undo() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at,auto_rating,rating,rating_source) VALUES(1,1,'a','a','image','2026',4,4,'auto');").unwrap();
        let saved = set_rating(
            State(state.clone()),
            None,
            Path(1),
            Json(RatingBody { rating: 3 }),
        )
        .await
        .unwrap()
        .0;
        let token = saved["rating_reviewed_at"].as_str().unwrap().to_string();
        let undone = undo_rating(
            State(state.clone()),
            None,
            Path(1),
            Json(UndoRatingBody {
                rating_reviewed_at: token.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(undone["rating"], 4);
        assert_eq!(undone["rating_reviewed"], false);
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(queue["media"].as_array().unwrap().len(), 1);
        state.pool.get().unwrap().execute_batch("UPDATE media SET rating=2,rating_reviewed=1,rating_source='human',rating_reviewed_at='future';").unwrap();
        assert_eq!(
            undo_rating(
                State(state.clone()),
                None,
                Path(1),
                Json(UndoRatingBody {
                    rating_reviewed_at: token
                })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT rating FROM media WHERE id=1", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn automated_and_human_rating_lifecycle() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let conn = state.pool.get().unwrap();
        conn.execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES
            (1,1,'a','a','image','2026'),(2,1,'b','b','image','2026'),(3,1,'c','c','image','2026'),(4,1,'d','d','image','2026');").unwrap();
        crate::nsfw::persist_score(&conn, 1, 0.72).unwrap();
        crate::nsfw::persist_score(&conn, 2, 0.4).unwrap();
        crate::nsfw::persist_score(&conn, 4, 0.9).unwrap();
        conn.execute("UPDATE media SET missing=1 WHERE id=4", [])
            .unwrap();
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(queue["media"][0]["auto_rating"], 3);
        assert_eq!(queue["media"][0]["rating"], 3);
        assert_eq!(queue["media"][0]["rating_source"], "auto");
        assert_eq!(queue["media"][0]["rating_reviewed"], false);
        assert!((queue["media"][0]["auto_rating_score"].as_f64().unwrap() - 0.72).abs() < 0.00001);
        assert_eq!(queue["has_more"], true);
        // Approving an automatic recommendation is intentionally a separate
        // endpoint; star-rating requests themselves accept only 1..=5.
        let manual = approve_rating(State(state.clone()), None, Path(1))
            .await
            .unwrap()
            .0;
        assert_eq!(manual["rating_source"], "human");
        assert_eq!(manual["rating_reviewed"], true);
        assert!(manual["rating_reviewed_at"].is_string());
        crate::nsfw::persist_score(&conn, 1, 0.99).unwrap();
        let row: (i64, i64, String) = conn
            .query_row(
                "SELECT rating,auto_rating,rating_source FROM media WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        // A later automated score is retained, but never replaces the
        // reviewer’s effective rating.
        assert_eq!(row, (3, 3, "human".into()));
        let next = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                cursor: queue["next_cursor"].as_str().map(str::to_owned),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(next["media"][0]["id"], 2);
        assert_eq!(next["has_more"], false);
        let approved = approve_rating(State(state.clone()), None, Path(2))
            .await
            .unwrap()
            .0;
        assert_eq!(approved["rating"], approved["auto_rating"]);
        assert_eq!(approved["auto_rating"], 2);
        assert_eq!(approved["rating_source"], "human");
        assert_eq!(approved["rating_reviewed"], true);
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(queue["media"].as_array().unwrap().is_empty());
        // Rating APIs intentionally accept only 1-5. Undo restores the
        // independently stored automatic recommendation for review.
        let cleared = undo_rating(
            State(state.clone()),
            None,
            Path(1),
            Json(UndoRatingBody {
                rating_reviewed_at: manual["rating_reviewed_at"].as_str().unwrap().to_owned(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(cleared["rating"], 3);
        assert!(cleared["human_rating"].is_null());
        assert_eq!(cleared["rating_source"], "auto");
        assert_eq!(cleared["rating_reviewed"], false);
        let reopened = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(reopened["media"][0]["id"], 1);
        assert_eq!(
            approve_rating(State(state.clone()), None, Path(3))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            approve_rating(State(state.clone()), None, Path(999))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            set_rating(State(state), None, Path(1), Json(RatingBody { rating: 6 }))
                .await
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn human_pace_tag_is_reviewed_and_controls_effective_rating() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let conn = state.pool.get().unwrap();
        conn.execute(
            "INSERT INTO media(id,source_id,filepath,filename,type,added_at,auto_rating,rating,rating_source)
             VALUES(1,1,'a','a','image','2026',3,3,'auto')",
            [],
        )
        .unwrap();
        crate::provenance::attach_tag(&conn, 1, "pace:cum", crate::provenance::HUMAN, None)
            .unwrap();

        let needs_review = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(needs_review["media"].as_array().unwrap().is_empty());

        let reviewed = list(
            State(state),
            Query(MediaQuery {
                rating_status: Some("reviewed".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(reviewed["media"][0]["rating"], 5);
        assert_eq!(reviewed["media"][0]["pace_label"], "cum");
    }

    #[tokio::test]
    async fn page_limit_is_capped_and_missing_media_excluded() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("WITH RECURSIVE nums(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<600)
            INSERT INTO media(id,source_id,filepath,filename,type,added_at) SELECT n,1,'test/'||n,'file','image','2026' FROM nums;
            UPDATE media SET missing=1,downloaded=0 WHERE id=1;").unwrap();
        let result = list(
            State(state),
            Query(MediaQuery {
                limit: Some(usize::MAX),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"].as_array().unwrap().len(), 500);
        assert_eq!(result["media"][0]["id"], 2);
        assert_eq!(result["has_more"], true);
    }

    #[tokio::test]
    async fn keyset_pages_preserve_every_sort_and_survive_deleted_anchor() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        {
            let conn = state.pool.get().unwrap();
            for id in 1..=21 {
                conn.execute("INSERT INTO media(id,source_id,filepath,filename,type,added_at,rating) VALUES(?1,1,?2,?3,'image',?4,?5)",
                    rusqlite::params![id,format!("test/{id}.jpg"),if id%2==0 {"Same"}else{"same"},format!("2026-01-{:02}",id%3+1),id%5]).unwrap();
            }
        }
        for sort in [
            "default",
            "rating_desc",
            "rating_asc",
            "date_desc",
            "date_asc",
            "filename_asc",
            "filename_desc",
            "shuffle",
        ] {
            let whole = list(
                State(state.clone()),
                Query(MediaQuery {
                    sort: Some(sort.into()),
                    limit: Some(500),
                    shuffle_seed: Some(1234567),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .0;
            let expected: Vec<i64> = whole["media"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["id"].as_i64().unwrap())
                .collect();
            let mut actual = Vec::new();
            let mut cursor = None;
            loop {
                let page = list(
                    State(state.clone()),
                    Query(MediaQuery {
                        sort: Some(sort.into()),
                        limit: Some(4),
                        shuffle_seed: Some(1234567),
                        cursor,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .0;
                actual.extend(
                    page["media"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|m| m["id"].as_i64().unwrap()),
                );
                cursor = page["next_cursor"].as_str().map(str::to_owned);
                if cursor.is_none() {
                    break;
                }
                assert!(actual.len() <= 21, "cursor must advance");
            }
            assert_eq!(actual, expected, "sort {sort}");
        }
        let first = list(
            State(state.clone()),
            Query(MediaQuery {
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        state
            .pool
            .get()
            .unwrap()
            .execute("DELETE FROM media WHERE id=1", [])
            .unwrap();
        let next = list(
            State(state),
            Query(MediaQuery {
                cursor: first["next_cursor"].as_str().map(str::to_owned),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(next["media"][0]["id"], 2);
    }

    #[tokio::test]
    async fn sql_filters_inherited_and_or_exclusion_before_pagination() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch("INSERT INTO groups(id,name,added_at) VALUES(1,'Parent','2026');
                INSERT INTO groups(id,name,parent_id,added_at) VALUES(2,'Child',1,'2026');
                UPDATE sources SET group_id=2;
                INSERT INTO tags(id,name,added_at) VALUES(1,'own','2026'),(2,'exclude','2026');
                INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES
                (1,1,'test/1','1','image','2026'),(2,1,'test/2','2','image','2026'),(3,1,'test/3','3','image','2026');
                INSERT INTO media_tags VALUES(2,1),(3,1),(3,2);").unwrap();
        }
        let result = list(
            State(state.clone()),
            Query(MediaQuery {
                tags: Some("parent,own".into()),
                exclude_tags: Some("exclude".into()),
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"][0]["id"], 2);
        assert_eq!(result["has_more"], false);
        let result = list(
            State(state.clone()),
            Query(MediaQuery {
                any_tags: Some("absent,own".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"].as_array().unwrap().len(), 2);
        let cache1 = effective_tags(&state).await.unwrap();
        let cache2 = effective_tags(&state).await.unwrap();
        assert!(Arc::ptr_eq(&cache1, &cache2));
    }
}

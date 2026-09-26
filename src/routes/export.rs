use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::chpack::{build_chpack, safe_pack_filename, ExportRow};
use crate::AppState;

// ─── GET /api/export ─────────────────────────────────────────────────────────

pub async fn export_sources(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let result = crate::services::export::source_list(&state)
        .await
        .map_err(|error| {
            let status = match error {
                crate::services::export::ExportError::Forbidden => StatusCode::FORBIDDEN,
                crate::services::export::ExportError::ShuttingDown
                | crate::services::export::ExportError::MaintenanceActive => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                crate::services::export::ExportError::Database(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            (status, Json(json!({"error": error.message()})))
        })?;
    Ok(Json(
        serde_json::to_value(result).expect("serializable source export"),
    ))
}

// ─── POST /api/import ────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ImportBody {
    pub sources: Vec<Value>,
}

pub async fn import_sources(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ImportBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let result = crate::services::export::import_source_list(state, body.sources)
        .await
        .map_err(|error| {
            use crate::services::sources::SourceError;
            let status = match error {
                SourceError::Forbidden => StatusCode::FORBIDDEN,
                SourceError::ShuttingDown | SourceError::MaintenanceActive => {
                    StatusCode::SERVICE_UNAVAILABLE
                }
                SourceError::InvalidInput(_) | SourceError::InvalidUrl(_) => {
                    StatusCode::BAD_REQUEST
                }
                SourceError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            };
            (status, Json(json!({"error": error.message()})))
        })?;
    Ok(Json(
        serde_json::to_value(result).expect("serializable source import"),
    ))
}

// ─── POST /api/export/chpack ─────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ChpackBody {
    pub source_id: Option<i64>,
    pub name: Option<String>,
    #[serde(default = "default_author")]
    pub author: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub unlock_cost: i64,
}
fn default_author() -> String {
    "Curator".into()
}

pub async fn export_chpack(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ChpackBody>,
) -> Response {
    let conn = match state.pool.get() {
        Ok(c) => c,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response()
        }
    };

    struct Row {
        filepath: String,
        kind: String,
        rating: i64,
        tags_csv: Option<String>,
    }

    let (pack_name, rows) = if let Some(sid) = body.source_id {
        let src_name: Option<String> = conn
            .query_row("SELECT name FROM sources WHERE id=?1", [sid], |r| r.get(0))
            .ok();
        if src_name.is_none() {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "Source not found"})),
            )
                .into_response();
        }
        let pname = body
            .name
            .as_deref()
            .unwrap_or(src_name.as_deref().unwrap_or("Curator Export"))
            .to_string();

        let mut stmt = conn.prepare(
            "SELECT m.filepath, m.type, m.rating, \
                (SELECT GROUP_CONCAT(t.name, ',') FROM media_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.media_id=m.id) AS tags_csv \
             FROM media m WHERE m.source_id=?1 AND m.downloaded=1 ORDER BY m.id"
        ).unwrap();
        let rows: Vec<Row> = stmt
            .query_map([sid], |r| {
                Ok(Row {
                    filepath: r.get(0)?,
                    kind: r.get(1)?,
                    rating: r.get(2)?,
                    tags_csv: r.get(3)?,
                })
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        (pname, rows)
    } else {
        let pname = body.name.as_deref().unwrap_or("Curator Export").to_string();
        let mut stmt = conn.prepare(
            "SELECT m.filepath, m.type, m.rating, \
                (SELECT GROUP_CONCAT(t.name, ',') FROM media_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.media_id=m.id) AS tags_csv \
             FROM media m WHERE m.downloaded=1 ORDER BY m.id"
        ).unwrap();
        let rows: Vec<Row> = stmt
            .query_map([], |r| {
                Ok(Row {
                    filepath: r.get(0)?,
                    kind: r.get(1)?,
                    rating: r.get(2)?,
                    tags_csv: r.get(3)?,
                })
            })
            .unwrap()
            .filter_map(|r| r.ok())
            .collect();

        (pname, rows)
    };

    if rows.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "No downloaded media found for this selection"})),
        )
            .into_response();
    }

    let export_rows: Vec<ExportRow> = rows
        .into_iter()
        .map(|r| ExportRow {
            filepath: r.filepath,
            kind: r.kind,
            rating: r.rating,
            tags: r
                .tags_csv
                .as_deref()
                .unwrap_or("")
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.trim().to_lowercase())
                .collect(),
        })
        .collect();

    let library_dir = state.library_dir.clone();
    let filename = safe_pack_filename(&pack_name);
    let author = body.author.clone();
    let description = body.description.clone();
    let unlock_cost = body.unlock_cost;

    let result = tokio::task::spawn_blocking(move || {
        build_chpack(
            pack_name,
            author,
            description,
            unlock_cost,
            export_rows,
            &library_dir,
        )
    })
    .await;

    match result {
        Ok(Ok(tmp)) => match File::open(tmp.path()).await {
            Ok(f) => {
                let stream = ReaderStream::new(f);
                let body = Body::from_stream(stream);
                let cd = format!("attachment; filename=\"{}\"", filename);
                let mut resp = body.into_response();
                resp.headers_mut()
                    .insert(header::CONTENT_TYPE, "application/zip".parse().unwrap());
                resp.headers_mut()
                    .insert(header::CONTENT_DISPOSITION, cd.parse().unwrap());
                resp
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
                .into_response(),
        },
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

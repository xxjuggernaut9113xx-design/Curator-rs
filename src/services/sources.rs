//! Source creation shared by native Host and the Server adapters.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    db::now_iso,
    downloader::run_download,
    slug::{derive_name_from_url, normalize_for_compare, slugify},
    AppState,
};

#[derive(Debug, Clone, Serialize)]
pub struct CreateSourcesResult {
    pub sources: Vec<Value>,
    pub duplicates: Vec<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceError {
    Forbidden,
    ShuttingDown,
    MaintenanceActive,
    InvalidInput(String),
    InvalidUrl(String),
    Database(String),
}

impl SourceError {
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden => "Viewer cannot create local sources",
            Self::ShuttingDown => "Curator is shutting down",
            Self::MaintenanceActive => {
                "A local maintenance job is active. Try again when it completes."
            }
            Self::InvalidInput(message) | Self::InvalidUrl(message) | Self::Database(message) => {
                message
            }
        }
    }
}

pub fn create(
    state: Arc<AppState>,
    candidates: Vec<String>,
) -> Result<CreateSourcesResult, SourceError> {
    if !state.edition.owns_library() {
        return Err(SourceError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(SourceError::ShuttingDown);
    }
    let _lease = state
        .maintenance
        .try_acquire_background_worker()
        .ok_or(SourceError::MaintenanceActive)?;
    let mut normalized = Vec::new();
    let mut seen = HashSet::new();
    for candidate in candidates {
        let url = crate::url_guard::normalize_public_http_url(&candidate)
            .map_err(|error| SourceError::InvalidUrl(error.to_string()))?;
        if seen.insert(url.clone()) {
            normalized.push(url);
        }
    }
    if normalized.is_empty() {
        return Ok(CreateSourcesResult {
            sources: Vec::new(),
            duplicates: Vec::new(),
        });
    }
    let conn = state
        .pool
        .get()
        .map_err(|error| SourceError::Database(error.to_string()))?;
    let existing: HashMap<String, String> = {
        let mut stmt = conn
            .prepare("SELECT url, name FROM sources")
            .map_err(|error| SourceError::Database(error.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| SourceError::Database(error.to_string()))?;
        rows.filter_map(Result::ok)
            .map(|(url, name)| (normalize_for_compare(&url), name))
            .collect()
    };
    let mut existing = existing;
    let mut to_create = Vec::new();
    let mut duplicates = Vec::new();
    for url in &normalized {
        let key = normalize_for_compare(url);
        if let Some(name) = existing.get(&key) {
            duplicates.push(json!({"url": url, "name": name}));
        } else {
            existing.insert(key, String::new());
            to_create.push(url.clone());
        }
    }
    let mut created_ids = Vec::new();
    for url in &to_create {
        let name = derive_name_from_url(url);
        let base_slug = slugify(&name);
        conn.execute(
            "INSERT INTO sources (name, url, slug, status, added_at, queued_at, progress_updated_at) VALUES (?1,?2,?3,'pending',?4,?4,?4)",
            rusqlite::params![name, url, base_slug, now_iso()],
        ).map_err(|error| SourceError::Database(error.to_string()))?;
        let id = conn.last_insert_rowid();
        conn.execute(
            "UPDATE sources SET slug=?1 WHERE id=?2",
            rusqlite::params![format!("{id}-{base_slug}"), id],
        )
        .map_err(|error| SourceError::Database(error.to_string()))?;
        created_ids.push(id);
    }
    for &id in &created_ids {
        state
            .download_tasks
            .spawn(run_download(Arc::clone(&state), id));
    }
    let sources = if created_ids.is_empty() {
        Vec::new()
    } else {
        let placeholders = created_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!("SELECT * FROM sources WHERE id IN ({placeholders})");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|error| SourceError::Database(error.to_string()))?;
        let params: Vec<&dyn rusqlite::ToSql> = created_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt
            .query_map(params.as_slice(), row_to_json)
            .map_err(|error| SourceError::Database(error.to_string()))?;
        rows.filter_map(Result::ok).collect()
    };
    Ok(CreateSourcesResult {
        sources,
        duplicates,
    })
}

pub fn row_to_json(row: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    let count = row.as_ref().column_count();
    let mut map = serde_json::Map::new();
    for index in 0..count {
        let name = row.as_ref().column_name(index).unwrap_or("?").to_string();
        let value = match row.get_ref(index)? {
            rusqlite::types::ValueRef::Null => Value::Null,
            rusqlite::types::ValueRef::Integer(number) => json!(number),
            rusqlite::types::ValueRef::Real(number) => json!(number),
            rusqlite::types::ValueRef::Text(bytes) | rusqlite::types::ValueRef::Blob(bytes) => {
                json!(std::str::from_utf8(bytes).unwrap_or(""))
            }
        };
        map.insert(name, value);
    }
    Ok(Value::Object(map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, Json};

    #[tokio::test]
    async fn duplicate_source_result_matches_the_server_adapter() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let url: String = state
            .pool
            .get()
            .unwrap()
            .query_row("SELECT url FROM sources LIMIT 1", [], |row| row.get(0))
            .unwrap();
        let direct = create(state.clone(), vec![url.clone()]).unwrap();
        let http = crate::routes::sources::add(
            State(state.clone()),
            Json(crate::routes::sources::AddSourcesBody {
                urls: vec![url],
                text: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(direct.sources.is_empty());
        assert_eq!(
            serde_json::to_value(direct.duplicates).unwrap(),
            http["duplicates"]
        );
    }

    #[tokio::test]
    async fn source_creation_denies_viewer_shutdown_and_invalid_urls() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(
            create(Arc::new(viewer), vec![]).unwrap_err(),
            SourceError::Forbidden
        );
        assert!(matches!(
            create(state.clone(), vec!["file:///private".into()]),
            Err(SourceError::InvalidUrl(_))
        ));
        state.shutdown.cancel();
        assert_eq!(
            create(state, vec![]).unwrap_err(),
            SourceError::ShuttingDown
        );
    }

    #[tokio::test]
    async fn source_creation_waits_out_maintenance_admission() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
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
        assert_eq!(
            create(state.clone(), vec![]).unwrap_err(),
            SourceError::MaintenanceActive
        );
        drop(lease);
        state.server_tasks.close();
        state.server_tasks.wait().await;
    }
}

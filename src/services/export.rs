//! Typed source-list export shared by native Host and the Server adapter.

use serde::Serialize;

use crate::{db, slug::normalize_for_compare, AppState};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExportSource {
    pub name: String,
    pub url: String,
    pub included: bool,
    pub group: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceList {
    pub exported_at: String,
    pub sources: Vec<ExportSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExportError {
    Forbidden,
    ShuttingDown,
    MaintenanceActive,
    Database(String),
}

impl ExportError {
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden => "Viewer cannot export a local library",
            Self::ShuttingDown => "Curator is shutting down",
            Self::MaintenanceActive => {
                "A local maintenance job is active. Try this change again when it completes."
            }
            Self::Database(message) => message,
        }
    }
}

pub async fn source_list(state: &AppState) -> Result<SourceList, ExportError> {
    if !state.edition.owns_library() {
        return Err(ExportError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(ExportError::ShuttingDown);
    }
    let _lease = state
        .maintenance
        .try_acquire_background_worker()
        .ok_or(ExportError::MaintenanceActive)?;
    let sources = {
        let conn = state
            .pool
            .get()
            .map_err(|error| ExportError::Database(error.to_string()))?;
        let mut stmt = conn
            .prepare(
                "SELECT s.name, s.url, s.included, g.name AS group_name \
                 FROM sources s LEFT JOIN groups g ON g.id = s.group_id \
                 ORDER BY s.added_at",
            )
            .map_err(|error| ExportError::Database(error.to_string()))?;
        let rows = stmt
            .query_map([], |row| {
                Ok(ExportSource {
                    name: row.get(0)?,
                    url: row.get(1)?,
                    included: row.get::<_, i64>(2)? != 0,
                    group: row.get(3)?,
                })
            })
            .map_err(|error| ExportError::Database(error.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| ExportError::Database(error.to_string()))?;
        rows
    };
    let exported_at = db::now_iso();
    {
        let mut settings = state.settings.write().await;
        settings.last_export_at = Some(exported_at.clone());
        settings.export_reminder_snoozed_until = None;
        db::save_settings(&state.data_dir, &settings);
    }
    Ok(SourceList {
        exported_at,
        sources,
    })
}

pub async fn import_source_list(
    state: Arc<AppState>,
    entries: Vec<Value>,
) -> Result<super::sources::CreateSourcesResult, super::sources::SourceError> {
    use super::sources::SourceError;
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
    let urls = entries
        .iter()
        .filter_map(|entry| entry.get("url").and_then(Value::as_str).map(str::to_owned))
        .collect::<Vec<_>>();
    if urls.is_empty() {
        return Err(SourceError::InvalidInput(
            "No valid entries to import".into(),
        ));
    }
    let result = super::sources::create(state.clone(), urls)?;
    if !result.sources.is_empty() {
        let entries_by_url: HashMap<String, String> = entries
            .iter()
            .filter_map(|entry| {
                let url = entry.get("url")?.as_str()?;
                let group = entry.get("group")?.as_str()?;
                (!group.is_empty()).then(|| (normalize_for_compare(url), group.to_owned()))
            })
            .collect();
        if !entries_by_url.is_empty() {
            let conn = state
                .pool
                .get()
                .map_err(|error| SourceError::Database(error.to_string()))?;
            let mut group_by_name: HashMap<String, i64> = {
                let mut stmt = conn
                    .prepare("SELECT id, name FROM groups")
                    .map_err(|error| SourceError::Database(error.to_string()))?;
                let rows = stmt
                    .query_map([], |row| {
                        Ok((row.get::<_, String>(1)?, row.get::<_, i64>(0)?))
                    })
                    .map_err(|error| SourceError::Database(error.to_string()))?;
                rows.filter_map(Result::ok).collect()
            };
            for source in &result.sources {
                let Some(url) = source["url"].as_str() else {
                    continue;
                };
                let Some(id) = source["id"].as_i64() else {
                    continue;
                };
                let Some(group_name) = entries_by_url.get(&normalize_for_compare(url)) else {
                    continue;
                };
                let group_id = if let Some(id) = group_by_name.get(group_name) {
                    *id
                } else {
                    conn.execute(
                        "INSERT INTO groups (name, added_at) VALUES (?1,?2)",
                        rusqlite::params![group_name, db::now_iso()],
                    )
                    .map_err(|error| SourceError::Database(error.to_string()))?;
                    let id = conn.last_insert_rowid();
                    group_by_name.insert(group_name.clone(), id);
                    id
                };
                conn.execute(
                    "UPDATE sources SET group_id=?1 WHERE id=?2",
                    rusqlite::params![group_id, id],
                )
                .map_err(|error| SourceError::Database(error.to_string()))?;
            }
            drop(conn);
            *state.group_tag_cache.write().await = None;
        }
    }
    Ok(result)
}

pub fn parse_source_file(bytes: &[u8]) -> Result<Vec<Value>, String> {
    if bytes.len() > 8 * 1024 * 1024 {
        return Err("Source-list file exceeds 8 MiB".into());
    }
    let document: Value = serde_json::from_slice(bytes)
        .map_err(|error| format!("Source-list file is not valid JSON: {error}"))?;
    let entries = document["sources"]
        .as_array()
        .ok_or("Source-list file must contain a sources array")?;
    if entries.is_empty() || entries.len() > 10_000 {
        return Err("Source-list file must contain 1 to 10,000 sources".into());
    }
    if entries.iter().any(|entry| {
        entry["url"]
            .as_str()
            .is_none_or(|url| url.trim().is_empty())
    }) {
        return Err("Every imported source needs a URL".into());
    }
    Ok(entries.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::State, Json};

    #[tokio::test]
    async fn source_list_matches_the_existing_http_payload() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let direct = source_list(&state).await.unwrap();
        let http = crate::routes::export::export_sources(State(state.clone()))
            .await
            .unwrap()
            .0;
        assert_eq!(
            serde_json::to_value(&direct.sources).unwrap(),
            http["sources"]
        );
        assert!(http["exported_at"].is_string());
        assert_eq!(
            state.settings.read().await.last_export_at.as_deref(),
            http["exported_at"].as_str()
        );
    }

    #[tokio::test]
    async fn viewer_shutdown_and_maintenance_deny_source_export() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(
            source_list(&viewer).await.unwrap_err(),
            ExportError::Forbidden
        );
        state.shutdown.cancel();
        assert_eq!(
            source_list(&state).await.unwrap_err(),
            ExportError::ShuttingDown
        );

        let maintenance_root = tempfile::tempdir().unwrap();
        let maintenance_state = crate::test_support::state(maintenance_root.path());
        let lease = maintenance_state
            .maintenance
            .try_acquire_background_worker()
            .unwrap();
        maintenance_state
            .maintenance
            .start(
                maintenance_state.clone(),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    confirmation: String::new(),
                    backup_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            source_list(&maintenance_state).await.unwrap_err(),
            ExportError::MaintenanceActive
        );
        drop(lease);
        maintenance_state.server_tasks.close();
        maintenance_state.server_tasks.wait().await;
    }

    #[tokio::test]
    async fn source_import_duplicate_payload_matches_http_and_denies_viewer() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let url: String = state
            .pool
            .get()
            .unwrap()
            .query_row("SELECT url FROM sources LIMIT 1", [], |row| row.get(0))
            .unwrap();
        let entries = vec![serde_json::json!({"url": url, "group": "Existing"})];
        let direct = import_source_list(state.clone(), entries.clone())
            .await
            .unwrap();
        let http = crate::routes::export::import_sources(
            State(state.clone()),
            Json(crate::routes::export::ImportBody { sources: entries }),
        )
        .await
        .unwrap()
        .0;
        assert!(direct.sources.is_empty());
        assert_eq!(
            serde_json::to_value(direct.duplicates).unwrap(),
            http["duplicates"]
        );
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(
            import_source_list(Arc::new(viewer), vec![])
                .await
                .unwrap_err(),
            super::super::sources::SourceError::Forbidden
        );
    }

    #[test]
    fn native_source_file_parser_rejects_missing_urls_before_mutation() {
        let valid = parse_source_file(
            br#"{"sources":[{"url":"https://example.com/gallery","group":"Collection"}]}"#,
        )
        .unwrap();
        assert_eq!(valid[0]["group"], "Collection");
        assert!(parse_source_file(br#"{"sources":[{"name":"missing"}]}"#).is_err());
        assert!(parse_source_file(br#"{"sources":[]}"#).is_err());
    }
}

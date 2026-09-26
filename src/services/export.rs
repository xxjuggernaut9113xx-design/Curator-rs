//! Typed source-list export shared by native Host and the Server adapter.

use serde::Serialize;

use crate::{db, slug::normalize_for_compare, AppState};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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

/// Exported metadata reapplied to a newly created source on import.
struct ImportMeta {
    name: Option<String>,
    included: Option<bool>,
    group: Option<String>,
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
    // One bad URL must not abort the batch: partition entries into valid and
    // invalid up front and report the invalid ones in the result. Entries
    // that repeat a URL already seen in this batch are reported as
    // duplicates instead of being silently folded into the first one.
    let mut urls = Vec::new();
    let mut invalid = Vec::new();
    let mut batch_duplicates = Vec::new();
    let mut seen_urls = HashSet::new();
    for (index, entry) in entries.iter().enumerate() {
        let raw = entry
            .get("url")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim();
        if raw.is_empty() {
            invalid.push(serde_json::json!({
                "index": index,
                "url": entry.get("url"),
                "error": "Every imported source needs a URL",
            }));
            continue;
        }
        match crate::url_guard::normalize_public_http_url(raw) {
            Ok(normalized) => {
                if seen_urls.insert(normalized.clone()) {
                    urls.push(normalized);
                } else {
                    batch_duplicates.push(serde_json::json!({
                        "index": index,
                        "url": raw,
                        "name": entry.get("name").and_then(Value::as_str),
                        "error": "Duplicate of an earlier entry in this import",
                    }));
                }
            }
            Err(error) => invalid.push(serde_json::json!({
                "index": index,
                "url": raw,
                "error": error.to_string(),
            })),
        }
    }
    if urls.is_empty() {
        return Err(SourceError::InvalidInput(
            "No valid entries to import".into(),
        ));
    }
    let mut result = super::sources::create(state.clone(), urls)?;
    result.invalid = invalid;
    result.duplicates.extend(batch_duplicates);
    if !result.sources.is_empty() {
        // The first entry for a URL wins: a later duplicate's metadata must
        // not overwrite the values from the entry that actually created the
        // source.
        let mut meta_by_url: HashMap<String, ImportMeta> = HashMap::new();
        for entry in entries.iter() {
            let Some(url) = entry.get("url").and_then(Value::as_str) else {
                continue;
            };
            meta_by_url
                .entry(normalize_for_compare(url))
                .or_insert_with(|| ImportMeta {
                    name: entry
                        .get("name")
                        .and_then(Value::as_str)
                        .filter(|name| !name.trim().is_empty())
                        .map(str::to_owned),
                    included: entry.get("included").and_then(Value::as_bool),
                    group: entry
                        .get("group")
                        .and_then(Value::as_str)
                        .filter(|group| !group.is_empty())
                        .map(str::to_owned),
                });
        }
        if meta_by_url
            .values()
            .any(|meta| meta.name.is_some() || meta.included.is_some() || meta.group.is_some())
        {
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
            let mut restored = 0;
            let mut groups_changed = false;
            for source in &result.sources {
                let (Some(url), Some(id)) = (source["url"].as_str(), source["id"].as_i64()) else {
                    continue;
                };
                let Some(meta) = meta_by_url.get(&normalize_for_compare(url)) else {
                    continue;
                };
                let group_id = match &meta.group {
                    Some(group_name) => {
                        let id = if let Some(id) = group_by_name.get(group_name) {
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
                        groups_changed = true;
                        Some(id)
                    }
                    None => None,
                };
                if meta.name.is_some() || meta.included.is_some() || group_id.is_some() {
                    conn.execute(
                        "UPDATE sources SET name = COALESCE(?1, name), included = COALESCE(?2, included), group_id = COALESCE(?3, group_id) WHERE id = ?4",
                        rusqlite::params![
                            meta.name.clone(),
                            meta.included.map(i64::from),
                            group_id,
                            id
                        ],
                    )
                    .map_err(|error| SourceError::Database(error.to_string()))?;
                    restored += 1;
                }
            }
            drop(conn);
            if groups_changed {
                *state.group_tag_cache.write().await = None;
            }
            result.metadata_restored = restored;
        }
    }
    Ok(result)
}

/// A source-list file with per-entry diagnostics. Malformed entries are
/// reported, not fatal: the caller still imports every valid entry.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedSourceFile {
    pub entries: Vec<Value>,
    pub skipped: Vec<SkippedSourceEntry>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SkippedSourceEntry {
    pub index: usize,
    pub url: Option<String>,
    pub error: String,
}

pub fn parse_source_file(bytes: &[u8]) -> Result<ParsedSourceFile, String> {
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
    let mut valid = Vec::with_capacity(entries.len());
    let mut skipped = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let url = entry.get("url").and_then(Value::as_str);
        if url.is_none_or(|url| url.trim().is_empty()) {
            skipped.push(SkippedSourceEntry {
                index,
                url: url.map(str::to_owned),
                error: "Every imported source needs a URL".to_string(),
            });
        } else {
            valid.push(entry.clone());
        }
    }
    Ok(ParsedSourceFile {
        entries: valid,
        skipped,
    })
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
    fn native_source_file_parser_reports_bad_entries_without_aborting() {
        let parsed = parse_source_file(
            br#"{"sources":[{"url":"https://example.com/gallery","group":"Collection"},{"name":"missing"},{"url":"   "}]}"#,
        )
        .unwrap();
        assert_eq!(parsed.entries.len(), 1);
        assert_eq!(parsed.entries[0]["group"], "Collection");
        assert_eq!(parsed.skipped.len(), 2);
        assert_eq!(parsed.skipped[0].index, 1);
        assert!(parsed.skipped[0].error.contains("URL"));
        assert!(parse_source_file(br#"{"sources":[]}"#).is_err());
    }

    #[tokio::test]
    async fn import_restores_exported_metadata_and_reports_problems() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let entries = vec![
            serde_json::json!({
                "name": "My Gallery",
                "url": "https://example.com/gallery",
                "included": false,
                "group": "Collection",
            }),
            // Duplicate of the first URL: existing record must win.
            serde_json::json!({
                "name": "Sneaky Rename",
                "url": "https://example.com/gallery",
                "included": true,
            }),
            // Invalid URL: reported, does not abort the batch.
            serde_json::json!({"url": "not a url"}),
        ];
        let result = import_source_list(state.clone(), entries).await.unwrap();
        assert_eq!(result.sources.len(), 1);
        assert_eq!(result.duplicates.len(), 1);
        assert_eq!(result.invalid.len(), 1);
        assert_eq!(result.invalid[0]["url"], "not a url");
        assert_eq!(result.metadata_restored, 1);

        let conn = state.pool.get().unwrap();
        let (name, included, group_name): (String, i64, Option<String>) = conn
            .query_row(
                "SELECT s.name, s.included, g.name FROM sources s LEFT JOIN groups g ON g.id = s.group_id WHERE s.url LIKE '%example.com/gallery'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        // The duplicate's rename was not applied: the first import won.
        assert_eq!(name, "My Gallery");
        assert_eq!(included, 0);
        assert_eq!(group_name.as_deref(), Some("Collection"));
    }
}

//! Typed download activity snapshot shared by the native shell and HTTP adapter.

use std::{collections::HashSet, sync::atomic::Ordering};

use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DownloadStatus {
    pub paused: bool,
    pub active_count: usize,
    pub queued_count: i64,
    pub retrying_count: i64,
    pub paused_source_ids: Vec<i64>,
    pub sources: Vec<SourceDownloadStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SourceDownloadStatus {
    pub id: i64,
    pub name: String,
    pub status: String,
    pub phase: String,
    pub queue_position: Option<i64>,
    pub known_total: Option<i64>,
    pub completed_count: i64,
    pub percentage: Option<f64>,
    pub indeterminate: bool,
    pub current_filename: Option<String>,
    pub retry_at: Option<i64>,
    pub error: Option<String>,
    pub queued_at: Option<String>,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub updated_at: Option<String>,
}

pub async fn status(state: &AppState) -> DownloadStatus {
    let paused = state.downloads_paused.load(Ordering::SeqCst);
    let active_ids: HashSet<i64> = state
        .active_processes
        .lock()
        .await
        .keys()
        .copied()
        .collect();
    let active_count = active_ids.len();
    let mut paused_source_ids: Vec<i64> = state
        .paused_source_ids
        .lock()
        .await
        .iter()
        .copied()
        .collect();
    paused_source_ids.sort_unstable();
    let rows = state.pool.get().ok().and_then(|conn| {
        let mut statement = conn.prepare(
            "SELECT s.id,s.name,s.status,s.item_count,s.known_total,
                    (SELECT COUNT(*) FROM media m WHERE m.source_id=s.id AND m.downloaded=1 AND m.missing=0) AS indexed_count,
                    s.completed_count,s.current_filename,s.retry_at,s.error_message,s.queued_at,s.started_at,s.completed_at,s.progress_updated_at
             FROM sources s ORDER BY COALESCE(s.queued_at,s.added_at),s.id",
        ).ok()?;
        let mapped = statement.query_map([], |row| Ok((
            row.get::<_, i64>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?,
            row.get::<_, i64>(3)?, row.get::<_, Option<i64>>(4)?, row.get::<_, i64>(5)?,
            row.get::<_, i64>(6)?, row.get::<_, Option<String>>(7)?, row.get::<_, i64>(8)?,
            row.get::<_, Option<String>>(9)?, row.get::<_, Option<String>>(10)?, row.get::<_, Option<String>>(11)?,
            row.get::<_, Option<String>>(12)?, row.get::<_, Option<String>>(13)?,
        ))).ok()?;
        mapped.collect::<rusqlite::Result<Vec<_>>>().ok()
    }).unwrap_or_default();
    let mut queue_position = 0_i64;
    let sources: Vec<SourceDownloadStatus> = rows
        .into_iter()
        .map(|row| {
            let (
                id,
                name,
                status,
                item_count,
                known_total,
                indexed_count,
                persisted_completed,
                current_filename,
                retry_at,
                error,
                queued_at,
                started_at,
                completed_at,
                updated_at,
            ) = row;
            let phase = match status.as_str() {
                "pending" => "queued",
                "downloading" if active_ids.contains(&id) => "active",
                "downloading" => "queued",
                "indexing" => "indexing",
                "retrying" => "retrying",
                "paused" => "paused",
                "storage_limit" => "storage_limit",
                "low_disk" => "low_disk",
                "done" => "completed",
                "error" => "failed",
                _ => "queued",
            };
            let position = if phase == "queued" {
                queue_position += 1;
                Some(queue_position)
            } else {
                None
            };
            let completed = indexed_count
                .max(persisted_completed)
                .max(item_count.min(indexed_count));
            let percentage = known_total.map(|total| {
                if total == 0 {
                    if phase == "completed" {
                        100.0
                    } else {
                        0.0
                    }
                } else {
                    ((completed as f64 / total as f64) * 100.0).clamp(0.0, 100.0)
                }
            });
            SourceDownloadStatus {
                id,
                name,
                status,
                phase: phase.into(),
                queue_position: position,
                known_total,
                completed_count: completed,
                percentage,
                indeterminate: known_total.is_none(),
                current_filename,
                retry_at: (retry_at > 0).then_some(retry_at),
                error,
                queued_at,
                started_at,
                completed_at,
                updated_at,
            }
        })
        .collect();
    let queued_count = sources.iter().filter(|row| row.phase == "queued").count() as i64;
    let retrying_count = sources.iter().filter(|row| row.phase == "retrying").count() as i64;
    DownloadStatus {
        paused,
        active_count,
        queued_count,
        retrying_count,
        paused_source_ids,
        sources,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn direct_and_http_status_share_the_typed_activity_snapshot() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute(
            "UPDATE sources SET status='downloading',known_total=10,completed_count=3,current_filename='item.mp4' WHERE id=1",
            [],
        ).unwrap();
        state.active_processes.lock().await.insert(1, 42);
        let direct = status(&state).await;
        assert_eq!(direct.active_count, 1);
        assert_eq!(direct.sources[0].phase, "active");
        assert_eq!(direct.sources[0].known_total, Some(10));
        assert_eq!(
            direct.sources[0].current_filename.as_deref(),
            Some("item.mp4")
        );

        let response = crate::router((*state).clone())
            .oneshot(
                Request::builder()
                    .uri("/api/downloads/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let http: DownloadStatus = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(http, direct);

        let native = crate::native::LocalClient::new((*state).clone()).unwrap();
        assert_eq!(
            native.downloads().await.unwrap(),
            serde_json::to_value(direct).unwrap()
        );
    }
}

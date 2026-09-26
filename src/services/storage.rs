//! Typed storage accounting shared by Host and the Server adapter.

use serde::{Deserialize, Serialize};

use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageCategories {
    pub original_media: u64,
    pub thumbnail_cache: u64,
    pub metadata_sidecars: u64,
    pub backups: u64,
    pub phar_environment: u64,
    pub gallery_dl_archives: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DiskSpace {
    pub free_bytes: Option<u64>,
    pub total_bytes: Option<u64>,
    pub minimum_free_disk_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceUsage {
    pub id: i64,
    pub name: String,
    pub slug: String,
    pub status: String,
    pub used_bytes: u64,
    pub allowed_bytes: Option<u64>,
    pub at_or_over_limit: bool,
    pub retention_keep_newest: Option<i64>,
    pub storage_override_once: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageDashboard {
    pub categories: StorageCategories,
    pub disk: DiskSpace,
    pub sources: Vec<SourceUsage>,
    pub note: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageError {
    Forbidden,
    ShuttingDown,
    Maintenance,
    AccountingFailed,
}

impl StorageError {
    pub fn message(self) -> &'static str {
        match self {
            Self::Forbidden => "Viewer cannot account for a local library",
            Self::ShuttingDown => "Curator is shutting down",
            Self::Maintenance => "A local maintenance job is active",
            Self::AccountingFailed => "Storage accounting did not complete.",
        }
    }
}

pub async fn dashboard(
    state: &AppState,
    sort: Option<String>,
) -> Result<StorageDashboard, StorageError> {
    if !state.edition.owns_library() {
        return Err(StorageError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(StorageError::ShuttingDown);
    }
    let _lease = state
        .maintenance
        .try_acquire_background_worker()
        .ok_or(StorageError::Maintenance)?;
    if state.shutdown.is_cancelled() {
        return Err(StorageError::ShuttingDown);
    }
    let settings = state.settings.read().await.clone();
    let pool = state.pool.clone();
    let data_dir = state.data_dir.clone();
    let library_dir = state.library_dir.clone();
    let archives_dir = state.archives_dir.clone();
    let thumbs_dir = state.thumbs_dir.clone();
    let sort = sort.unwrap_or_else(|| "usage_desc".into());
    let snapshot = tokio::task::spawn_blocking(move || {
        crate::storage::dashboard_snapshot(
            &pool,
            &data_dir,
            &library_dir,
            &archives_dir,
            &thumbs_dir,
            &settings,
            &sort,
        )
    })
    .await
    .map_err(|_| StorageError::AccountingFailed)?;
    serde_json::from_value(snapshot).map_err(|_| StorageError::AccountingFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{Query, State};

    #[tokio::test]
    async fn direct_and_http_dashboard_match() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let direct = dashboard(&state, Some("name".into())).await.unwrap();
        let http = crate::routes::storage::dashboard(
            State(state),
            Query(crate::routes::storage::StorageQuery {
                sort: Some("name".into()),
            }),
        )
        .await
        .0;
        let mut direct = serde_json::to_value(direct).unwrap();
        assert!(direct["disk"]["free_bytes"].is_number());
        assert!(http["disk"]["free_bytes"].is_number());
        direct["disk"].as_object_mut().unwrap().remove("free_bytes");
        let mut http = http;
        http["disk"].as_object_mut().unwrap().remove("free_bytes");
        assert_eq!(direct, http);
    }

    #[tokio::test]
    async fn viewer_local_storage_and_shutdown_are_denied() {
        let root = tempfile::tempdir().unwrap();
        let host = crate::test_support::state(root.path());
        let mut viewer = (*host).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(dashboard(&viewer, None).await, Err(StorageError::Forbidden));
        host.shutdown.cancel();
        assert_eq!(
            dashboard(&host, None).await,
            Err(StorageError::ShuttingDown)
        );
    }

    #[tokio::test]
    async fn maintenance_excludes_storage_accounting() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        state.running_sources.lock().await.insert(1);
        state
            .maintenance
            .start(
                state.clone(),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    backup_id: None,
                    confirmation: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            dashboard(&state, None).await,
            Err(StorageError::Maintenance)
        );
        state.running_sources.lock().await.clear();
        for _ in 0..100 {
            if !state.maintenance.is_active() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!state.maintenance.is_active());
        state.server_tasks.close();
        state.server_tasks.wait().await;
    }
}

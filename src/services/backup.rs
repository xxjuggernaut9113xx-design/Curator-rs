//! Backup inventory shared by native Host and the Server admin adapter.

use std::path::PathBuf;

use crate::{maintenance, AppState};

#[derive(Debug, Clone)]
pub struct BackupSnapshot {
    pub backups: Vec<maintenance::BackupRecord>,
    pub jobs: Vec<maintenance::MaintenanceJob>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackupError {
    Forbidden,
    ShuttingDown,
    InvalidId(String),
    ReadFailed(String),
}

impl BackupError {
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden => "Recovery is available only on the Host or Server device",
            Self::ShuttingDown => "Curator is shutting down",
            Self::InvalidId(message) => message,
            Self::ReadFailed(message) => message,
        }
    }
}

pub fn list(state: &AppState) -> Result<Vec<maintenance::BackupRecord>, BackupError> {
    admit(state)?;
    maintenance::list_backups(&state.data_dir)
        .map_err(|error| BackupError::ReadFailed(error.to_string()))
}

fn admit(state: &AppState) -> Result<(), BackupError> {
    if !state.edition.has_local_admin() {
        return Err(BackupError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(BackupError::ShuttingDown);
    }
    Ok(())
}

pub fn download_path(state: &AppState, id: &str) -> Result<PathBuf, BackupError> {
    admit(state)?;
    maintenance::backup_file(&state.data_dir, id)
        .map_err(|error| BackupError::InvalidId(error.to_string()))
}

pub async fn snapshot(state: &AppState) -> Result<BackupSnapshot, BackupError> {
    let backups = list(state)?;
    let jobs = super::jobs::list(state)
        .await
        .map_err(|error| match error {
            super::jobs::JobError::Forbidden => BackupError::Forbidden,
            super::jobs::JobError::ShuttingDown => BackupError::ShuttingDown,
            other => BackupError::ReadFailed(other.message().to_owned()),
        })?;
    Ok(BackupSnapshot { backups, jobs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use std::sync::Arc;

    #[tokio::test]
    async fn host_snapshot_and_server_adapter_share_backup_inventory() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let backup_dir = state.data_dir.join("backups");
        std::fs::create_dir_all(&backup_dir).unwrap();
        std::fs::write(backup_dir.join("test.zip"), b"fixture").unwrap();
        let direct = snapshot(&state).await.unwrap();
        let http = crate::routes::admin::list_backups(State(state), None)
            .await
            .unwrap()
            .0;
        assert_eq!(direct.backups.len(), 1);
        assert_eq!(direct.backups[0].id, http["backups"][0]["id"]);
        assert_eq!(direct.backups[0].size_bytes, 7);
    }

    #[tokio::test]
    async fn viewer_and_shutdown_deny_direct_backup_inventory() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(list(&viewer).unwrap_err(), BackupError::Forbidden);
        assert_eq!(
            download_path(&viewer, "test.zip").unwrap_err(),
            BackupError::Forbidden
        );
        state.shutdown.cancel();
        assert_eq!(list(&state).unwrap_err(), BackupError::ShuttingDown);
        assert_eq!(
            download_path(&state, "test.zip").unwrap_err(),
            BackupError::ShuttingDown
        );
        assert!(snapshot(&Arc::new(viewer)).await.is_err());
    }

    #[test]
    fn download_path_rejects_traversal_inside_the_service() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        assert!(matches!(
            download_path(&state, "../settings.json"),
            Err(BackupError::InvalidId(_))
        ));
    }
}

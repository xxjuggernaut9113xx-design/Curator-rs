//! Typed maintenance job operations for native Host and Server adapters.

use std::sync::Arc;

use crate::{
    maintenance::{MaintenanceJob, MaintenanceRequest},
    AppState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobError {
    Forbidden,
    ShuttingDown,
    NotFound,
    Rejected(String),
}

impl JobError {
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden => "This Curator edition has no local Admin surface.",
            Self::ShuttingDown => "Curator is shutting down",
            Self::NotFound => "Maintenance job not found.",
            Self::Rejected(message) => message,
        }
    }
}

fn admit(state: &AppState) -> Result<(), JobError> {
    if !state.edition.has_local_admin() {
        return Err(JobError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(JobError::ShuttingDown);
    }
    Ok(())
}

pub async fn list(state: &AppState) -> Result<Vec<MaintenanceJob>, JobError> {
    admit(state)?;
    Ok(state.maintenance.jobs().await)
}

pub async fn get(state: &AppState, id: &str) -> Result<MaintenanceJob, JobError> {
    admit(state)?;
    state.maintenance.job(id).await.ok_or(JobError::NotFound)
}

pub async fn start(
    state: Arc<AppState>,
    request: MaintenanceRequest,
) -> Result<MaintenanceJob, JobError> {
    admit(&state)?;
    state
        .maintenance
        .start(Arc::clone(&state), request)
        .await
        .map_err(JobError::Rejected)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;

    #[tokio::test]
    async fn direct_and_http_job_lists_agree() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let direct = list(&state).await.unwrap();
        let http = crate::routes::admin::list_jobs(State(state.clone()), None)
            .await
            .unwrap()
            .0;
        assert_eq!(direct.len(), http["jobs"].as_array().unwrap().len());
        assert_eq!(
            get(&state, "missing").await.unwrap_err(),
            JobError::NotFound
        );
    }

    #[tokio::test]
    async fn viewer_and_shutdown_denials_are_service_owned() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(list(&viewer).await.unwrap_err(), JobError::Forbidden);
        state.shutdown.cancel();
        assert_eq!(list(&state).await.unwrap_err(), JobError::ShuttingDown);
        let request = MaintenanceRequest {
            kind: crate::maintenance::MaintenanceKind::CreateBackup,
            confirmation: String::new(),
            backup_id: None,
        };
        assert_eq!(
            start(state, request).await.unwrap_err(),
            JobError::ShuttingDown
        );
    }

    #[tokio::test]
    async fn direct_and_http_rejection_match_without_starting_a_job() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let request = MaintenanceRequest {
            kind: crate::maintenance::MaintenanceKind::FactoryReset,
            confirmation: String::new(),
            backup_id: None,
        };
        let direct = start(state.clone(), request).await.unwrap_err();
        let request = MaintenanceRequest {
            kind: crate::maintenance::MaintenanceKind::FactoryReset,
            confirmation: String::new(),
            backup_id: None,
        };
        let http = crate::routes::admin::start_job(State(state.clone()), None, axum::Json(request))
            .await
            .unwrap_err();
        assert_eq!(http.0, axum::http::StatusCode::CONFLICT);
        assert_eq!(http.1 .0["error"], direct.message());
        assert!(list(&state).await.unwrap().is_empty());
    }
}

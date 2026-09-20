//! Local-only recovery controls. Viewers have normal library-management
//! access, but never receive this destructive surface over Tailnet.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{ConnectInfo, Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::fs::File;
use tokio_util::io::ReaderStream;

use crate::maintenance::{self, MaintenanceKind, MaintenanceRequest};
use crate::phar::{self, PharBackend};
use crate::AppState;

fn local_only(
    state: &AppState,
    peer: &Option<ConnectInfo<SocketAddr>>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if !state.edition.has_local_admin() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error":"This Curator edition has no local Admin surface."})),
        ));
    }
    if peer.as_ref().is_none_or(|peer| peer.0.ip().is_loopback()) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error":"Curator Admin is available only from this device."})),
        ))
    }
}

fn job_error(error: String) -> (StatusCode, Json<Value>) {
    (StatusCode::CONFLICT, Json(json!({"error": error})))
}

pub async fn list_jobs(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    Ok(Json(json!({"jobs": state.maintenance.jobs().await})))
}

pub async fn get_job(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    state
        .maintenance
        .job(&id)
        .await
        .map(|job| Json(json!(job)))
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error":"Maintenance job not found."})),
            )
        })
}

pub async fn start_job(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(request): Json<MaintenanceRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let job = state
        .maintenance
        .start(Arc::clone(&state), request)
        .await
        .map_err(job_error)?;
    Ok(Json(json!(job)))
}

pub async fn create_backup(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let job = state
        .maintenance
        .start(
            Arc::clone(&state),
            MaintenanceRequest {
                kind: MaintenanceKind::CreateBackup,
                confirmation: String::new(),
                backup_id: None,
            },
        )
        .await
        .map_err(job_error)?;
    Ok(Json(json!(job)))
}

pub async fn list_backups(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let backups = maintenance::list_backups(&state.data_dir).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!({"backups": backups})))
}

pub async fn download_backup(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<String>,
) -> Response {
    if let Err(error) = local_only(&state, &peer) {
        return error.into_response();
    }
    let path = match maintenance::backup_file(&state.data_dir, &id) {
        Ok(path) => path,
        Err(error) => {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"error": error.to_string()})),
            )
                .into_response()
        }
    };
    match File::open(path).await {
        Ok(file) => {
            let body = Body::from_stream(ReaderStream::new(file));
            let mut response = body.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                "application/zip".parse().expect("valid content type"),
            );
            response.headers_mut().insert(
                header::CONTENT_DISPOSITION,
                format!("attachment; filename=\"{id}\"")
                    .parse()
                    .expect("valid content disposition"),
            );
            response
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
            .into_response(),
    }
}

pub async fn validate_backup(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let job = state
        .maintenance
        .start(
            Arc::clone(&state),
            MaintenanceRequest {
                kind: MaintenanceKind::ValidateBackup,
                confirmation: String::new(),
                backup_id: Some(id),
            },
        )
        .await
        .map_err(job_error)?;
    Ok(Json(json!(job)))
}

#[derive(Debug, Deserialize)]
pub struct RestoreBody {
    pub confirmation: String,
}

pub async fn restore_backup(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Path(id): Path<String>,
    Json(body): Json<RestoreBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let job = state
        .maintenance
        .start(
            Arc::clone(&state),
            MaintenanceRequest {
                kind: MaintenanceKind::RestoreBackup,
                confirmation: body.confirmation,
                backup_id: Some(id),
            },
        )
        .await
        .map_err(job_error)?;
    Ok(Json(json!(job)))
}

pub async fn phar_status(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    Ok(Json(json!(phar::status(
        &state.data_dir,
        state.install_scope
    ))))
}

#[derive(Debug, Deserialize)]
pub struct PharIntentBody {
    pub enabled: bool,
    pub backend: Option<PharBackend>,
}

pub async fn phar_intent(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<PharIntentBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let status = phar::record_install_intent(
        &state.data_dir,
        state.install_scope,
        body.enabled,
        body.backend,
    )
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!(status)))
}

pub async fn phar_install(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let status = phar::start_install(&state.data_dir, state.install_scope).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!({
        "accepted": true,
        "job_id": status.job_id,
        "message": status.message,
        "status": status,
    })))
}

pub async fn phar_cancel(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let status = phar::cancel(&state.data_dir, state.install_scope).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!(status)))
}

pub async fn phar_repair(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let status = phar::repair(&state.data_dir, state.install_scope).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!(status)))
}

pub async fn phar_self_test(
    State(state): State<Arc<AppState>>,
    peer: Option<ConnectInfo<SocketAddr>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    local_only(&state, &peer)?;
    let status = phar::self_test(&state.data_dir, state.install_scope).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": error.to_string()})),
        )
    })?;
    Ok(Json(json!(status)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tailnet_peer_cannot_use_admin() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let peer = Some(ConnectInfo(SocketAddr::from(([100, 88, 0, 1], 42168))));
        assert!(local_only(&state, &peer).is_err());
    }

    #[test]
    fn loopback_host_can_use_admin_but_viewer_cannot() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let peer = Some(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 42168))));
        assert!(local_only(&state, &peer).is_ok());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert!(local_only(&viewer, &peer).is_err());
    }
}

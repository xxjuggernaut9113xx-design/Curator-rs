use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::Value;

use crate::AppState;

// ─── GET /api/downloads/status ───────────────────────────────────────────────

pub async fn status(
    State(state): State<Arc<AppState>>,
) -> Json<crate::services::downloads::DownloadStatus> {
    Json(crate::services::downloads::status(&state).await)
}

pub async fn pause(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(crate::services::downloads::pause(&state).await)
}

pub async fn pause_source(State(state): State<Arc<AppState>>, Path(id): Path<i64>) -> Json<Value> {
    Json(crate::services::downloads::pause_source(&state, id).await)
}

pub async fn resume_source(State(state): State<Arc<AppState>>, Path(id): Path<i64>) -> Json<Value> {
    Json(crate::services::downloads::resume_source(state, id).await)
}

pub async fn resume(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(crate::services::downloads::resume(state).await)
}

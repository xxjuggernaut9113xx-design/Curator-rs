//! Thin remote/recovery adapter for the shared Rust session service.
//!
//! There is deliberately no scheduling, timing, or session mutation policy in
//! these handlers. The Slint shell will call the same `SessionService` methods.

use crate::{
    services::session::{self as session_service, SessionError},
    session::{GameConfig, SessionControl, SessionState, SessionUpdate},
    AppState,
};
use axum::{extract::State, http::StatusCode, Json};
use serde_json::json;
use std::sync::Arc;

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<serde_json::Value>)>;

fn session_error(error: SessionError) -> (StatusCode, Json<serde_json::Value>) {
    let status = match &error {
        SessionError::ShuttingDown | SessionError::Maintenance => StatusCode::SERVICE_UNAVAILABLE,
        SessionError::Engine(message) if message == "A session is already active" => {
            StatusCode::CONFLICT
        }
        SessionError::Engine(message) if message == "No active session" => StatusCode::NOT_FOUND,
        SessionError::Engine(_) => StatusCode::BAD_REQUEST,
    };
    (status, Json(json!({ "error": error.to_string() })))
}

pub async fn current(State(state): State<Arc<AppState>>) -> Json<Option<SessionState>> {
    Json(session_service::current(&state))
}

pub async fn start(
    State(state): State<Arc<AppState>>,
    Json(config): Json<GameConfig>,
) -> ApiResult<SessionUpdate> {
    session_service::start(&state, config)
        .map(Json)
        .map_err(session_error)
}

pub async fn control(
    State(state): State<Arc<AppState>>,
    Json(command): Json<SessionControl>,
) -> ApiResult<SessionUpdate> {
    session_service::control(&state, command)
        .map(Json)
        .map_err(session_error)
}

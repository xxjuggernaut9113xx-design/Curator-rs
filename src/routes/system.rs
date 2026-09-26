//! Small, path-free capability handshake used by Viewer before it stores a
//! Tailnet host. This endpoint is intentionally safe to expose over the same
//! Tailnet listener as ordinary library operations.

use std::sync::Arc;

use axum::{extract::State, Json};
use serde::Serialize;

use crate::{AppState, API_PROTOCOL, PRODUCT_VERSION};

#[derive(Debug, Serialize)]
pub struct SystemCapabilities {
    pub browser_ui: bool,
    pub downloads: bool,
    pub media_serving: bool,
    pub native_integrations: bool,
    pub background_service: bool,
    pub local_admin: bool,
}

#[derive(Debug, Serialize)]
pub struct SystemInfo {
    pub edition: &'static str,
    pub version: &'static str,
    pub api_protocol: &'static str,
    pub instance_id: String,
    pub capabilities: SystemCapabilities,
    pub viewer_permissions: crate::native::ViewerPermissions,
    pub tailnet_only: bool,
}

impl SystemInfo {
    fn from_state(state: &AppState) -> Self {
        Self {
            edition: state.edition.as_str(),
            version: PRODUCT_VERSION,
            api_protocol: API_PROTOCOL,
            instance_id: state.instance_id.clone(),
            capabilities: SystemCapabilities {
                browser_ui: true,
                downloads: state.edition.owns_library(),
                media_serving: state.edition.owns_library(),
                native_integrations: matches!(state.edition, crate::edition::Edition::Host),
                background_service: matches!(state.edition, crate::edition::Edition::Server),
                local_admin: state.edition.has_local_admin(),
            },
            viewer_permissions: crate::native::ViewerPermissions::default(),
            tailnet_only: true,
        }
    }
}

pub async fn info(State(state): State<Arc<AppState>>) -> Json<SystemInfo> {
    Json(SystemInfo::from_state(&state))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn info_exposes_identity_but_not_a_filesystem_location() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        let text = serde_json::to_string(&SystemInfo::from_state(&state)).unwrap();
        assert!(text.contains("test-instance"));
        assert!(!text.contains(&dir.path().to_string_lossy().to_string()));
        assert!(text.contains(API_PROTOCOL));
        assert!(text.contains("\"viewer_permissions\""));
        assert!(text.contains("\"library_edit\":false"));
    }
}

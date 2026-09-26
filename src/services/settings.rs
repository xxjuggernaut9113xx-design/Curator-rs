//! Settings reads shared by native Host and the Server HTTP adapter.
//! Local integration details are available only to a client on the library
//! device; a Tailnet Viewer never receives executable paths.

use serde_json::{json, Value};

use crate::{db::Settings, edition::Edition, AppState, StartupRegistration};

/// Accepted persistent theme names shared by Server validation and native
/// device preferences. Legacy names remain valid for existing libraries.
pub const VALID_THEMES: &[&str] = &[
    "system",
    "atelier-dark",
    "midnight",
    "ember",
    "linen",
    "sage",
    "aurora",
    "oled",
    "gtk-system",
    "adwaita-light",
    "adwaita-dark",
    "yaru-light",
    "yaru-dark",
    "arc-light",
    "arc-dark",
    "breeze-light",
    "breeze-dark",
    "yotsuba",
    "yotsuba-b",
    "futaba",
    "burichan",
    "tomorrow",
    "photon",
    "light",
    "oled-dark",
    "dark",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingsAudience {
    Local,
    Remote,
}

impl SettingsAudience {
    fn is_local(self) -> bool {
        matches!(self, Self::Local)
    }
}

pub async fn read(state: &AppState, audience: SettingsAudience) -> Value {
    // The local Host verifies its startup entry on every open. A persisted
    // preference alone cannot tell whether the entry points at this binary.
    let startup = if audience.is_local() && matches!(state.edition, Edition::Host) {
        Some(crate::reconcile_start_with_windows_preference(state).await)
    } else {
        None
    };
    let settings = state.settings.read().await;
    response(state, audience, &settings, startup)
}

pub fn response(
    state: &AppState,
    audience: SettingsAudience,
    settings: &Settings,
    startup: Option<StartupRegistration>,
) -> Value {
    let mut value = serde_json::to_value(settings).unwrap_or_default();
    let config = crate::config::load_config_for(state.install_scope);
    if let Some(object) = value.as_object_mut() {
        let local = audience.is_local() && state.edition.owns_library();
        let host_integrations = local && matches!(state.edition, Edition::Host);
        object.insert(
            "host_integration_settings_available".into(),
            json!(host_integrations),
        );
        if local {
            object.insert(
                "ffmpeg_bin".into(),
                json!(config.ffmpeg_bin.unwrap_or_else(|| "ffmpeg".into())),
            );
            object.insert(
                "external_tool_settings_restart_required".into(),
                json!(true),
            );
            if let Some(startup) = startup {
                object.insert("startup_registration".into(), json!(startup));
            }
        } else {
            object.insert("external_tool_settings_local_only".into(), json!(true));
            object.insert("local_integration_settings_local_only".into(), json!(true));
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::{ConnectInfo, State};
    use std::{net::SocketAddr, sync::Arc};

    #[tokio::test]
    async fn direct_host_and_http_settings_reads_agree() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let direct = read(&state, SettingsAudience::Local).await;
        let http = crate::routes::settings::get(State(state.clone()), None)
            .await
            .0;
        assert_eq!(direct, http);
        assert_eq!(direct["host_integration_settings_available"], true);
        assert!(direct["ffmpeg_bin"].is_string());
    }

    #[tokio::test]
    async fn direct_remote_and_http_settings_reads_hide_local_paths() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let direct = read(&state, SettingsAudience::Remote).await;
        let http = crate::routes::settings::get(
            State(state),
            Some(ConnectInfo(SocketAddr::from(([100, 80, 0, 2], 42168)))),
        )
        .await
        .0;
        assert_eq!(direct, http);
        assert!(direct.get("ffmpeg_bin").is_none());
        assert_eq!(direct["host_integration_settings_available"], false);
        assert_eq!(direct["external_tool_settings_local_only"], true);
    }

    #[tokio::test]
    async fn server_local_settings_do_not_offer_host_integrations() {
        let root = tempfile::tempdir().unwrap();
        let host = crate::test_support::state(root.path());
        let mut server = (*host).clone();
        server.edition = Edition::Server;
        let value = read(&Arc::new(server), SettingsAudience::Local).await;
        assert_eq!(value["host_integration_settings_available"], false);
    }

    #[tokio::test]
    async fn viewer_cannot_claim_local_settings_audience() {
        let root = tempfile::tempdir().unwrap();
        let host = crate::test_support::state(root.path());
        let mut viewer = (*host).clone();
        viewer.edition = Edition::Viewer;
        let value = read(&viewer, SettingsAudience::Local).await;
        assert!(value.get("ffmpeg_bin").is_none());
        assert_eq!(value["host_integration_settings_available"], false);
    }
}

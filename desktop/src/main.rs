#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

// The Host is a Slint application. It calls the Curator core in-process and
// has no embedded browser, browser profile, or loopback HTTP dependency.
slint::include_modules!();

use std::sync::Arc;

#[derive(Clone)]
struct HostClient {
    state: curator::AppState,
    runtime: tokio::runtime::Handle,
}

impl HostClient {
    fn summary(&self) -> Result<String, String> {
        let value = self
            .runtime
            .block_on(curator::library_summary(&self.state))
            .map_err(|error| error.to_string())?;
        let media = value
            .get("media_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let sources = value
            .get("source_count")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        Ok(format!("{media} items in {sources} sources"))
    }

    fn toggle_downloads(&self) -> String {
        let paused = !self
            .state
            .downloads_paused
            .load(std::sync::atomic::Ordering::Acquire);
        self.state
            .downloads_paused
            .store(paused, std::sync::atomic::Ordering::Release);
        if paused {
            "Downloads paused".into()
        } else {
            "Downloads resumed".into()
        }
    }

    fn start_session(&self) -> Result<String, String> {
        self.state
            .sessions
            .start_running(curator::session::GameConfig::quick_default())
            .map(|update| {
                format!(
                    "Quick session {} started at {:.0} BPM.",
                    update.state.session_id, update.state.tempo.current_bpm
                )
            })
            .map_err(|error| error.to_string())
    }
}

fn main() -> Result<(), slint::PlatformError> {
    let runtime = tokio::runtime::Runtime::new().expect("Could not start Curator runtime");
    let state = runtime
        .block_on(curator::initialize_host())
        .unwrap_or_else(|error| {
            eprintln!("Library initialization failed: {error:#}");
            std::process::exit(1);
        });
    let client = Arc::new(HostClient {
        state: state.clone(),
        runtime: runtime.handle().clone(),
    });
    let window = CuratorNativeWindow::new()?;

    let weak = window.as_weak();
    window.on_select_workspace(move |workspace| {
        if let Some(window) = weak.upgrade() {
            window.set_active_workspace(workspace);
        }
    });
    let weak = window.as_weak();
    let summary_client = Arc::clone(&client);
    window.on_refresh_library(move || {
        if let Some(window) = weak.upgrade() {
            window.set_status(
                summary_client
                    .summary()
                    .unwrap_or_else(|error| error)
                    .into(),
            );
        }
    });
    let weak = window.as_weak();
    let download_client = Arc::clone(&client);
    window.on_toggle_downloads(move || {
        if let Some(window) = weak.upgrade() {
            window.set_status(download_client.toggle_downloads().into());
        }
    });
    let weak = window.as_weak();
    window.on_start_session(move || {
        if let Some(window) = weak.upgrade() {
            window.set_status(client.start_session().unwrap_or_else(|error| error).into());
        }
    });
    window.set_status("Local library ready. HTTP is optional for this Host.".into());
    window.run()?;
    runtime.block_on(curator::shutdown(&state));
    Ok(())
}

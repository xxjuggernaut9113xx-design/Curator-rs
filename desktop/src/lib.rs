mod player;

slint::include_modules!();

use curator::native::{
    Client, Command, LibraryQuery, ManageSnapshot, MediaItem, MediaPage, NativeImage,
    NativePreferences, NavigationItem, RecoverySnapshot,
};
use player::{NativePlayer, PlayerCommand, PlayerStatus};
use slint::{ComponentHandle, Model, ModelRc, VecModel};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    future::Future,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Mutex,
    },
    time::{Duration, Instant},
};

fn run_until_shutdown<T>(
    handle: &tokio::runtime::Handle,
    stop: &mut tokio::sync::watch::Receiver<bool>,
    future: impl Future<Output = T>,
) -> Option<T> {
    if *stop.borrow() {
        return None;
    }
    handle.block_on(async {
        tokio::select! {
            biased;
            _ = stop.changed() => None,
            result = future => Some(result),
        }
    })
}

fn write_export_file(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    let directory = path.parent().ok_or("Choose an export destination")?;
    let mut staged = tempfile::NamedTempFile::new_in(directory).map_err(|e| e.to_string())?;
    staged.write_all(bytes).map_err(|e| e.to_string())?;
    staged.flush().map_err(|e| e.to_string())?;
    staged.as_file().sync_all().map_err(|e| e.to_string())?;
    staged.persist(path).map_err(|e| e.to_string())?;
    Ok(())
}

enum Work {
    Recovery(Option<curator::maintenance::MaintenanceRequest>),
    Navigation,
    ImportFolder,
    ExportSources,
    ImportSources,
    Browse(Box<LibraryQuery>, u64),
    DownloadsStatus,
    ManageSnapshot,
    DiagnosticLog,
    Discover {
        query: String,
        provider: Option<String>,
    },
    SaveSettings(serde_json::Value),
    Commands(Vec<Command>),
}
enum Update {
    Recovery(Result<RecoverySnapshot, String>),
    Navigation(Result<Vec<NavigationItem>, String>),
    Image(u64, String, Result<NativeImage, String>),
    Page(u64, Result<MediaPage, String>),
    Manage(Result<ManageSnapshot, String>),
    DiagnosticLog(Result<String, String>),
    Discover(Result<serde_json::Value, String>),
    SettingsSaved(Result<(), String>),
    Exported(Result<String, String>),
    Imported(Result<String, String>),
    Changed(Result<(), String>),
    Downloads(Result<serde_json::Value, String>),
    PreferenceError(String),
}

#[derive(Default)]
struct ViewState {
    navigation: Vec<NavigationItem>,
    items: Vec<MediaItem>,
    media_model: Rc<VecModel<MediaRow>>,
    selected: BTreeMap<i64, MediaItem>,
    queue: Vec<MediaItem>,
    query: LibraryQuery,
    cursor: Option<String>,
    page_cursors: Vec<Option<String>>,
    page_scrolls: HashMap<Option<String>, f32>,
    discovery_results: Vec<serde_json::Value>,
    // Index zero is the explicit all-provider search. Subsequent entries
    // retain the API identifiers while Slint displays the human-facing name.
    discovery_provider_ids: Vec<Option<String>>,
    download_source_ids: Vec<i64>,
    preview_request: u64,
    browse_request: u64,
    preference_extras: BTreeMap<String, serde_json::Value>,
    settings: serde_json::Value,
    backup_ids: Vec<String>,
}

impl ViewState {
    fn browse_work(&mut self) -> Work {
        self.browse_request = self.browse_request.wrapping_add(1);
        Work::Browse(Box::new(self.query.clone()), self.browse_request)
    }

    fn navigate_to(&mut self, target: Option<(i64, bool)>) {
        self.query.source_id = target.filter(|(_, group)| !group).map(|(id, _)| id);
        self.query.group_id = target.filter(|(_, group)| *group).map(|(id, _)| id);
        self.query.cursor = None;
        self.cursor = None;
        self.page_cursors.clear();
        self.page_cursors.push(None);
        self.page_scrolls.clear();
    }
}

const WORK_QUEUE_CAPACITY: usize = 128;
const CONTROL_QUEUE_CAPACITY: usize = 16;
static REJECTED_WORK: AtomicUsize = AtomicUsize::new(0);

fn parse_size_filter(text: &str) -> Result<Option<i64>, String> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    text.parse::<i64>()
        .ok()
        .filter(|value| *value >= 0)
        .map(Some)
        .ok_or_else(|| "Size filters must be non-negative whole bytes".to_owned())
}

fn settings_integer(text: &str, name: &str, min: u32, max: u32) -> Result<u32, String> {
    let value = text
        .trim()
        .parse::<u32>()
        .map_err(|_| format!("{name} must be a whole number from {min} to {max}"))?;
    if !(min..=max).contains(&value) {
        return Err(format!("{name} must be a whole number from {min} to {max}"));
    }
    Ok(value)
}

fn settings_optional_bytes(text: &str, name: &str) -> Result<Option<u64>, String> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(None);
    }
    text.parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .map(Some)
        .ok_or_else(|| format!("{name} must be a positive whole number of bytes"))
}

#[derive(Clone)]
struct WorkSender {
    regular: mpsc::SyncSender<Work>,
    control: mpsc::SyncSender<Vec<Command>>,
}

#[derive(Clone)]
struct PreferenceSaver {
    latest: Arc<Mutex<Option<NativePreferences>>>,
    wake: mpsc::SyncSender<()>,
}

impl PreferenceSaver {
    fn queue(&self, preferences: NativePreferences) -> Result<(), String> {
        *self.latest.lock().map_err(|error| error.to_string())? = Some(preferences);
        match self.wake.try_send(()) {
            Ok(()) | Err(mpsc::TrySendError::Full(())) => Ok(()),
            Err(mpsc::TrySendError::Disconnected(())) => {
                Err("Native preference writer stopped".into())
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum EnqueueResult {
    Queued,
    Full,
    Closed,
}

impl WorkSender {
    // Slint callbacks must never wait for a full background queue.
    fn send(&self, work: Work) -> EnqueueResult {
        let result = match work {
            Work::Commands(commands) if commands.iter().all(interactive_control) => self
                .control
                .try_send(commands)
                .map_err(|error| match error {
                    mpsc::TrySendError::Full(commands) => {
                        mpsc::TrySendError::Full(Work::Commands(commands))
                    }
                    mpsc::TrySendError::Disconnected(commands) => {
                        mpsc::TrySendError::Disconnected(Work::Commands(commands))
                    }
                }),
            work => self.regular.try_send(work),
        };
        match result {
            Ok(()) => EnqueueResult::Queued,
            Err(mpsc::TrySendError::Full(_)) => {
                REJECTED_WORK.fetch_add(1, Ordering::Relaxed);
                EnqueueResult::Full
            }
            Err(mpsc::TrySendError::Disconnected(_)) => EnqueueResult::Closed,
        }
    }
}

fn interactive_control(command: &Command) -> bool {
    matches!(
        command,
        Command::StartSession
            | Command::Session(_)
            | Command::PauseDownloads
            | Command::ResumeDownloads
            | Command::PauseSource(_)
            | Command::ResumeSource(_)
    )
}

fn session_text(state: Option<&curator::session::SessionState>) -> String {
    let Some(state) = state else {
        return "No active session".into();
    };
    let elapsed = state.active_elapsed_ms / 1_000;
    let phase_elapsed = state.phase.elapsed_ms / 1_000;
    let mut text = format!(
        "{:?} · {} ({phase_elapsed}s) · {:.0} BPM · {:02}:{:02} active",
        state.status,
        state.phase.id,
        state.tempo.current_bpm,
        elapsed / 60,
        elapsed % 60,
    );
    if let Some(event) = &state.current_event {
        text.push_str(&format!("\nEvent: {}", event.id));
    }
    if let Some(instruction) = &state.instruction {
        text.push_str(&format!("\n{instruction}"));
    }
    text
}

#[derive(Clone, Copy)]
struct NativePalette {
    background: u32,
    panel: u32,
    elevated: u32,
    text: u32,
    muted: u32,
    line: u32,
    accent: u32,
    selection: u32,
    selection_text: u32,
}

fn native_palette(name: &str) -> NativePalette {
    match name {
        "adwaita-light" => NativePalette {
            background: 0xf6f5f4,
            panel: 0xffffff,
            elevated: 0xffffff,
            text: 0x241f31,
            muted: 0x5e5c64,
            line: 0xc9c7c5,
            accent: 0x3584e4,
            selection: 0x1c71d8,
            selection_text: 0xffffff,
        },
        "adwaita-dark" => NativePalette {
            background: 0x1e1e1e,
            panel: 0x242424,
            elevated: 0x303030,
            text: 0xffffff,
            muted: 0xc0bfbc,
            line: 0x4a4a4a,
            accent: 0x78aeed,
            selection: 0x1c71d8,
            selection_text: 0xffffff,
        },
        "yaru-light" => NativePalette {
            background: 0xf8f7f5,
            panel: 0xffffff,
            elevated: 0xffffff,
            text: 0x2c001e,
            muted: 0x635566,
            line: 0xd8d2cd,
            accent: 0xe95420,
            selection: 0xc64618,
            selection_text: 0xffffff,
        },
        "yaru-dark" => NativePalette {
            background: 0x2c001e,
            panel: 0x3d1230,
            elevated: 0x4d1d3d,
            text: 0xffffff,
            muted: 0xd6c2d0,
            line: 0x70415d,
            accent: 0xff7800,
            selection: 0xe66100,
            selection_text: 0x2c001e,
        },
        "arc-light" => NativePalette {
            background: 0xf5f6f7,
            panel: 0xffffff,
            elevated: 0xffffff,
            text: 0x3c4b5b,
            muted: 0x5f6e7d,
            line: 0xcdd6df,
            accent: 0x5294e2,
            selection: 0x3470b5,
            selection_text: 0xffffff,
        },
        "arc-dark" => NativePalette {
            background: 0x383c4a,
            panel: 0x404552,
            elevated: 0x4a5060,
            text: 0xd3dae3,
            muted: 0xb3bdc9,
            line: 0x626b7e,
            accent: 0x5294e2,
            selection: 0x3973b6,
            selection_text: 0xffffff,
        },
        "breeze-light" => NativePalette {
            background: 0xeff0f1,
            panel: 0xfcfcfc,
            elevated: 0xffffff,
            text: 0x232629,
            muted: 0x5c6165,
            line: 0xbdc3c7,
            accent: 0x3daee9,
            selection: 0x1c719c,
            selection_text: 0xffffff,
        },
        "breeze-dark" => NativePalette {
            background: 0x232629,
            panel: 0x2a2e32,
            elevated: 0x31363b,
            text: 0xeff0f1,
            muted: 0xc4c8cc,
            line: 0x4d545a,
            accent: 0x3daee9,
            selection: 0x17668e,
            selection_text: 0xffffff,
        },
        "midnight" => NativePalette {
            background: 0x0b1020,
            panel: 0x111936,
            elevated: 0x192347,
            text: 0xe8efff,
            muted: 0xa4b3d8,
            line: 0x31416f,
            accent: 0x79a8ff,
            selection: 0x365bba,
            selection_text: 0xffffff,
        },
        "ember" => NativePalette {
            background: 0x1a1110,
            panel: 0x251716,
            elevated: 0x34201d,
            text: 0xfff0e8,
            muted: 0xd0a89a,
            line: 0x603b32,
            accent: 0xff8a4c,
            selection: 0xa84824,
            selection_text: 0xffffff,
        },
        "linen" => NativePalette {
            background: 0xf5f0e8,
            panel: 0xfffbf5,
            elevated: 0xffffff,
            text: 0x302821,
            muted: 0x76685d,
            line: 0xd9ccbc,
            accent: 0xab5e38,
            selection: 0x7f4025,
            selection_text: 0xffffff,
        },
        "sage" => NativePalette {
            background: 0xedf2e9,
            panel: 0xf8fbf5,
            elevated: 0xffffff,
            text: 0x233328,
            muted: 0x627466,
            line: 0xc8d6c0,
            accent: 0x4e8468,
            selection: 0x2f614b,
            selection_text: 0xffffff,
        },
        "aurora" => NativePalette {
            background: 0x171326,
            panel: 0x211b37,
            elevated: 0x2c2547,
            text: 0xf5f0ff,
            muted: 0xc1b4dc,
            line: 0x514574,
            accent: 0xd494ff,
            selection: 0x824caf,
            selection_text: 0xffffff,
        },
        "oled" => NativePalette {
            background: 0x000000,
            panel: 0x050505,
            elevated: 0x101010,
            text: 0xf2f2f2,
            muted: 0xa0a0a0,
            line: 0x282828,
            accent: 0x73e6bd,
            selection: 0x227858,
            selection_text: 0xffffff,
        },
        _ => NativePalette {
            background: 0x121214,
            panel: 0x19191c,
            elevated: 0x201f23,
            text: 0xedebe6,
            muted: 0x8c8a85,
            line: 0x353438,
            accent: 0xe8a33d,
            selection: 0x6b4e1e,
            selection_text: 0xffffff,
        },
    }
}

fn canonical_native_theme(name: &str) -> &str {
    match name {
        "gtk-system" => "system",
        "yotsuba" | "tomorrow" | "photon" | "light" => "linen",
        "yotsuba-b" | "burichan" => "midnight",
        "futaba" => "ember",
        "oled-dark" => "oled",
        "dark" => "atelier-dark",
        _ => name,
    }
}

fn rgb(value: u32) -> slint::Color {
    slint::Color::from_rgb_u8((value >> 16) as u8, (value >> 8) as u8, value as u8)
}

fn apply_native_palette(window: &CuratorNativeWindow, name: &str) {
    let name = canonical_native_theme(name);
    let palette = native_palette(name);
    window.set_custom_background(rgb(palette.background));
    window.set_custom_panel(rgb(palette.panel));
    window.set_custom_elevated(rgb(palette.elevated));
    window.set_custom_text(rgb(palette.text));
    window.set_custom_muted(rgb(palette.muted));
    window.set_custom_line(rgb(palette.line));
    window.set_custom_accent(rgb(palette.accent));
    window.set_custom_selection(rgb(palette.selection));
    window.set_custom_selection_text(rgb(palette.selection_text));
    window.set_settings_theme(name.into());
    window.set_manage_theme(name.into());
}

fn manage_text(snapshot: &ManageSnapshot) -> String {
    let stats = &snapshot.stats;
    let storage = &snapshot.storage;
    let providers = snapshot.providers["providers"]
        .as_array()
        .map_or(0, Vec::len);
    format!(
        "Library: {} media · {} sources · {} groups · {} tags\nDownloads: {} active · {} errors\nStorage: {}\nDiscovery: {} providers\nRemote access: {}\nDiagnostic log: {}",
        stats["total_media"], stats["total_sources"], stats["total_groups"], stats["total_tags"],
        stats["sources_downloading"], stats["sources_error"],
        storage["total_bytes"].as_u64().map(|bytes| format!("{bytes} bytes")).unwrap_or_else(|| "calculating".into()),
        providers,
        snapshot.remote_access["status"].as_str().unwrap_or("unavailable"),
        snapshot.log_location,
    )
}

fn recovery_text(snapshot: &RecoverySnapshot) -> String {
    let count = snapshot.backups.len();
    let mut lines = vec![format!(
        "{count} saved {}",
        if count == 1 { "backup" } else { "backups" }
    )];
    for job in &snapshot.jobs {
        lines.push(format!(
            "{:?}: {:?} · {}{}{}",
            job.kind,
            job.phase,
            job.message,
            job.error
                .as_ref()
                .map(|error| format!(" · {error}"))
                .unwrap_or_default(),
            if job.restart_required {
                " · Restart required"
            } else {
                ""
            }
        ));
    }
    lines.join("\n")
}

fn maintenance_kind_for_action(action: &str) -> Option<curator::maintenance::MaintenanceKind> {
    use curator::maintenance::MaintenanceKind;
    match action {
        "Create backup" => Some(MaintenanceKind::CreateBackup),
        "Validate backup" => Some(MaintenanceKind::ValidateBackup),
        "Restore backup" => Some(MaintenanceKind::RestoreBackup),
        "Reconcile library" => Some(MaintenanceKind::ReconcileLibrary),
        "Clear human ratings" => Some(MaintenanceKind::ClearHumanRatings),
        "Reset ratings and evidence" => Some(MaintenanceKind::ResetRatingsAndEvidence),
        "Flatten groups" => Some(MaintenanceKind::FlattenGroups),
        "Delete groups" => Some(MaintenanceKind::DeleteGroups),
        "Clear tag assignments" => Some(MaintenanceKind::ClearTagAssignments),
        "Clear session history" => Some(MaintenanceKind::ClearInteractiveHistory),
        "Rebuild caches" => Some(MaintenanceKind::RebuildCaches),
        "Factory reset" => Some(MaintenanceKind::FactoryReset),
        "Remove P-HAR" => Some(MaintenanceKind::RemovePharEnvironment),
        "Remove archives" => Some(MaintenanceKind::RemoveArchives),
        _ => None,
    }
}

fn selected_backup_index(ids: &[String], previous: Option<&str>) -> usize {
    previous
        .and_then(|id| ids.iter().position(|candidate| candidate == id))
        .unwrap_or(0)
}

fn discovery_provider_options(providers: &serde_json::Value) -> (Vec<String>, Vec<Option<String>>) {
    let mut labels = vec!["All providers".to_owned()];
    let mut ids = vec![None];
    for provider in providers["providers"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        let Some(id) = provider["id"].as_str() else {
            continue;
        };
        let name = provider["name"].as_str().unwrap_or(id);
        let availability = provider["availability"].as_str().unwrap_or("unknown");
        labels.push(format!("{name} ({availability})"));
        ids.push(Some(id.to_owned()));
    }
    (labels, ids)
}

fn download_status_text(status: &serde_json::Value) -> String {
    let summary = if status["paused"].as_bool().unwrap_or(false) {
        format!(
            "Downloads paused · {} source(s) ready to resume",
            status["paused_source_ids"].as_array().map_or(0, Vec::len)
        )
    } else {
        format!(
            "{} active · {} queued · {} retrying",
            status["active_count"].as_i64().unwrap_or(0),
            status["queued_count"].as_i64().unwrap_or(0),
            status["retrying_count"].as_i64().unwrap_or(0),
        )
    };
    let source_lines = status["sources"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|row| {
            let completed = row["completed_count"].as_i64().unwrap_or(0);
            let total = match row["known_total"].as_i64() {
                Some(total) => format!(
                    "{completed} / {total} ({}%)",
                    row["percentage"].as_f64().unwrap_or(0.0).round()
                ),
                None => format!("{completed} completed · total not reported"),
            };
            let current = row["current_filename"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!(" · {value}"))
                .unwrap_or_default();
            let retry = row["retry_at"]
                .as_i64()
                .map(|value| format!(" · retry at {value}"))
                .unwrap_or_default();
            let error = row["error"]
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(|value| format!(" · error: {value}"))
                .unwrap_or_default();
            format!(
                "{} — {} · {}{}{}{}",
                row["name"].as_str().unwrap_or("Unnamed source"),
                row["phase"].as_str().unwrap_or("queued"),
                total,
                current,
                retry,
                error,
            )
        });
    std::iter::once(summary)
        .chain(source_lines)
        .collect::<Vec<_>>()
        .join("\n")
}

fn apply_downloads(
    window: &CuratorNativeWindow,
    view: &Rc<RefCell<ViewState>>,
    result: Result<serde_json::Value, String>,
) {
    match result {
        Ok(status) => {
            window.set_download_status(download_status_text(&status).into());
            let sources = status["sources"].as_array().cloned().unwrap_or_default();
            let source_rows = sources
                .iter()
                .filter_map(|row| {
                    Some((
                        row["id"].as_i64()?,
                        DownloadRow {
                            label: row["name"].as_str().unwrap_or("Unnamed source").into(),
                            phase: row["phase"].as_str().unwrap_or("queued").into(),
                        },
                    ))
                })
                .collect::<Vec<_>>();
            view.borrow_mut().download_source_ids = source_rows.iter().map(|(id, _)| *id).collect();
            window.set_download_sources(ModelRc::new(VecModel::from(
                source_rows
                    .into_iter()
                    .map(|(_, row)| row)
                    .collect::<Vec<_>>(),
            )));
        }
        Err(error) => window.set_download_status(error.into()),
    }
}

fn render(window: &CuratorNativeWindow, state: &ViewState) {
    window.set_selected_count(state.selected.len().min(i32::MAX as usize) as i32);
    window.set_can_undo_rating(
        state.selected.len() == 1
            && state
                .selected
                .values()
                .next()
                .is_some_and(|item| item.rating_reviewed_at.is_some()),
    );
    let rows = state
        .items
        .iter()
        .map(|item| MediaRow {
            title: item.filename.clone().into(),
            detail: format!("{} · {} · {} ★", item.source, item.kind, item.rating).into(),
            selected: state.selected.contains_key(&item.id),
        })
        .collect::<Vec<_>>();
    if state.media_model.row_count() == rows.len() {
        for (index, row) in rows.into_iter().enumerate() {
            state.media_model.set_row_data(index, row);
        }
    } else {
        state.media_model.set_vec(rows);
    }
    window.set_queue(ModelRc::new(VecModel::from(
        state
            .queue
            .iter()
            .map(|item| item.filename.clone().into())
            .collect::<Vec<slint::SharedString>>(),
    )));
    window.set_inspector(inspector_text(&state.selected).into());
}

fn inspector_text(selected: &BTreeMap<i64, MediaItem>) -> String {
    selected
        .values()
        .map(|item| {
            format!(
                "{}\n{}\nTags: {}",
                item.filename,
                item.source,
                item.tags.join(", ")
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn current_preferences(window: &CuratorNativeWindow, state: &ViewState) -> NativePreferences {
    let size = window.window().size();
    NativePreferences {
        version: 1,
        queue: state.queue.clone(),
        width: size.width,
        height: size.height,
        workspace: window.get_workspace(),
        theme: window.get_settings_theme().to_string(),
        layout: window.get_settings_layout().to_string(),
        extra: state.preference_extras.clone(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeExit {
    Close,
    SwitchHost,
}

pub fn run_ui(
    runtime: &tokio::runtime::Runtime,
    client: Client,
) -> Result<(), Box<dyn std::error::Error>> {
    run_ui_with_exit(runtime, client).map(|_| ())
}

pub fn run_ui_with_exit(
    runtime: &tokio::runtime::Runtime,
    client: Client,
) -> Result<NativeExit, Box<dyn std::error::Error>> {
    let window = CuratorNativeWindow::new()?;
    let local_host = matches!(client, Client::Local(_));
    window.set_local_host(local_host);
    window.set_can_edit_library(client.can_edit_library());
    window.set_can_control_sessions(client.can_control_sessions());
    window.set_can_playback(client.can_playback());
    window.set_can_discover(client.can_discover());
    let switch_requested = Rc::new(Cell::new(false));
    let switch_flag = switch_requested.clone();
    window.on_switch_host(move || {
        if !local_host {
            switch_flag.set(true);
            let _ = slint::quit_event_loop();
        }
    });
    let view = Rc::new(RefCell::new(ViewState::default()));
    window.set_media(ModelRc::new(view.borrow().media_model.clone()));
    let mut preferences_writable = true;
    match client.load_preferences() {
        Ok(preferences) => {
            view.borrow_mut().queue = preferences.queue;
            view.borrow_mut().preference_extras = preferences.extra;
            window.set_workspace(preferences.workspace.clamp(0, 2));
            apply_native_palette(&window, &preferences.theme);
            window.set_settings_layout(preferences.layout.into());
            window.window().set_size(slint::PhysicalSize::new(
                preferences.width.clamp(800, 7680),
                preferences.height.clamp(560, 4320),
            ));
            render(&window, &view.borrow());
        }
        Err(error) => {
            preferences_writable = false;
            window.set_status(format!("Could not restore preferences: {error}").into());
        }
    }
    let (regular, receive) = mpsc::sync_channel(WORK_QUEUE_CAPACITY);
    let (control, control_receive) = mpsc::sync_channel(CONTROL_QUEUE_CAPACITY);
    let send = WorkSender { regular, control };
    let (image_send, image_receive) = mpsc::sync_channel::<(u64, MediaItem)>(1);
    let (updates, inbox) = mpsc::channel();
    let (session_stop, mut session_stop_rx) = tokio::sync::watch::channel(false);
    let (preference_wake, preference_receive) = mpsc::sync_channel::<()>(1);
    let preference_saver = PreferenceSaver {
        latest: Arc::new(Mutex::new(None)),
        wake: preference_wake,
    };
    let preference_pending = preference_saver.latest.clone();
    let preference_client = client.clone();
    let preference_updates = updates.clone();
    let preference_worker = std::thread::spawn(move || {
        while preference_receive.recv().is_ok() {
            loop {
                let snapshot = preference_pending.lock().unwrap().take();
                let Some(snapshot) = snapshot else { break };
                if let Err(error) = preference_client.save_preferences(&snapshot) {
                    let _ = preference_updates.send(Update::PreferenceError(error));
                }
            }
        }
    });
    let image_client = client.clone();
    let image_handle = runtime.handle().clone();
    let image_updates = updates.clone();
    let mut image_stop = session_stop.subscribe();
    let image_worker = std::thread::spawn(move || {
        while let Ok((request, item)) = image_receive.recv() {
            let Some(image) =
                run_until_shutdown(&image_handle, &mut image_stop, image_client.image(&item))
            else {
                break;
            };
            if image_updates
                .send(Update::Image(request, item.filename, image))
                .is_err()
            {
                break;
            }
        }
    });
    let worker_client = client.clone();
    let handle = runtime.handle().clone();
    // Session snapshots have their own lane. A slow library scan or discovery
    // request must not hide authoritative phase and elapsed-time changes.
    let session_client = client.clone();
    let session_handle = runtime.handle().clone();
    let (session_updates, session_inbox) = mpsc::sync_channel(1);
    let session_worker = std::thread::spawn(move || loop {
        let result = session_handle.block_on(async {
            tokio::select! {
                result = session_client.session() => Some(result),
                _ = session_stop_rx.changed() => None,
            }
        });
        let Some(result) = result else { break };
        if matches!(
            session_updates.try_send(result),
            Err(mpsc::TrySendError::Disconnected(_))
        ) {
            break;
        }
        let period = if matches!(session_client, Client::Local(_)) {
            Duration::from_millis(250)
        } else {
            Duration::from_secs(1)
        };
        let stopped = session_handle.block_on(async {
            tokio::select! {
                _ = tokio::time::sleep(period) => false,
                _ = session_stop_rx.changed() => true,
            }
        });
        if stopped {
            break;
        }
    });
    // Keep activity fresh while other work is busy or the window is hidden.
    // The single slot replaces old snapshots instead of accumulating updates.
    let downloads_client = client.clone();
    let downloads_handle = runtime.handle().clone();
    let downloads_latest = Arc::new(Mutex::new(None));
    let downloads_pending = downloads_latest.clone();
    let mut downloads_stop_rx = session_stop.subscribe();
    let downloads_worker = std::thread::spawn(move || loop {
        let result = downloads_handle.block_on(async {
            tokio::select! {
                result = downloads_client.downloads() => Some(result),
                _ = downloads_stop_rx.changed() => None,
            }
        });
        let Some(result) = result else { break };
        if let Ok(mut pending) = downloads_pending.lock() {
            *pending = Some(result);
        }
        let stopped = downloads_handle.block_on(async {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(2)) => false,
                _ = downloads_stop_rx.changed() => true,
            }
        });
        if stopped {
            break;
        }
    });
    let control_client = client.clone();
    let control_handle = runtime.handle().clone();
    let control_updates = updates.clone();
    let mut control_stop = session_stop.subscribe();
    let control_worker = std::thread::spawn(move || {
        while let Ok(commands) = control_receive.recv() {
            let Some(result) = run_until_shutdown(&control_handle, &mut control_stop, async {
                for command in commands {
                    control_client.execute(command).await?;
                }
                Ok(())
            }) else {
                break;
            };
            if control_updates.send(Update::Changed(result)).is_err() {
                break;
            }
            let Some(downloads) = run_until_shutdown(
                &control_handle,
                &mut control_stop,
                control_client.downloads(),
            ) else {
                break;
            };
            if control_updates.send(Update::Downloads(downloads)).is_err() {
                break;
            }
        }
    });
    // General work stays ordered. Controls and image decoding have independent
    // bounded lanes so slow discovery or preview work cannot fill their queues.
    let mut work_stop = session_stop.subscribe();
    let mut next_recovery = Instant::now() + Duration::from_secs(2);
    let worker = std::thread::spawn(move || loop {
        if *work_stop.borrow() {
            break;
        }
        let work = match receive.recv_timeout(Duration::from_millis(100)) {
            Ok(work) => work,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if Instant::now() >= next_recovery && matches!(worker_client, Client::Local(_)) {
                    if let Some(result) =
                        run_until_shutdown(&handle, &mut work_stop, worker_client.recovery_status())
                    {
                        let _ = updates.send(Update::Recovery(result));
                    }
                }
                if Instant::now() >= next_recovery {
                    next_recovery = Instant::now() + Duration::from_secs(2);
                }
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        if *work_stop.borrow() {
            break;
        }
        match work {
            Work::Recovery(request) => {
                let result = run_until_shutdown(&handle, &mut work_stop, async {
                    if let Some(request) = request {
                        worker_client.recovery(request).await?;
                    }
                    worker_client.recovery_status().await
                });
                if let Some(result) = result {
                    let _ = updates.send(Update::Recovery(result));
                }
            }
            Work::Navigation => {
                if let Some(result) =
                    run_until_shutdown(&handle, &mut work_stop, worker_client.navigation())
                {
                    let _ = updates.send(Update::Navigation(result));
                }
            }
            Work::ImportFolder => {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Import folder into Curator")
                    .pick_folder()
                {
                    if let Some(result) = run_until_shutdown(
                        &handle,
                        &mut work_stop,
                        worker_client.execute(Command::ImportFolder(path)),
                    ) {
                        let _ = updates.send(Update::Changed(result.map(|_| ())));
                    }
                }
            }
            Work::ExportSources => {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Export Curator source list")
                    .set_file_name("curator-sources.json")
                    .save_file()
                {
                    let result = run_until_shutdown(
                        &handle,
                        &mut work_stop,
                        worker_client.export_source_list(),
                    );
                    if let Some(result) = result {
                        let result = result.and_then(|source_list| {
                            let bytes = serde_json::to_vec_pretty(&source_list)
                                .map_err(|error| error.to_string())?;
                            write_export_file(&path, &bytes)?;
                            Ok(format!("Source list exported to {}", path.display()))
                        });
                        let _ = updates.send(Update::Exported(result));
                    }
                }
            }
            Work::ImportSources => {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Import Curator source list")
                    .add_filter("JSON source lists", &["json"])
                    .pick_file()
                {
                    let result = (|| {
                        if std::fs::metadata(&path).map_err(|e| e.to_string())?.len()
                            > 8 * 1024 * 1024
                        {
                            return Err("Source-list file exceeds 8 MiB".to_owned());
                        }
                        let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
                        curator::services::export::parse_source_file(&bytes)
                    })();
                    let result = result.and_then(|entries| {
                        run_until_shutdown(
                            &handle,
                            &mut work_stop,
                            worker_client.import_source_list(entries),
                        )
                        .ok_or("Import stopped during shutdown".to_owned())
                        .and_then(|result| result)
                    });
                    let message = result.map(|result| {
                        format!(
                            "Imported {} sources; {} duplicates skipped",
                            result.sources.len(),
                            result.duplicates.len()
                        )
                    });
                    let _ = updates.send(Update::Imported(message));
                }
            }
            Work::Browse(query, request) => {
                if let Some(result) =
                    run_until_shutdown(&handle, &mut work_stop, worker_client.library(*query))
                {
                    let _ = updates.send(Update::Page(request, result));
                }
            }
            Work::DownloadsStatus => {
                if let Some(result) =
                    run_until_shutdown(&handle, &mut work_stop, worker_client.downloads())
                {
                    let _ = updates.send(Update::Downloads(result));
                }
            }
            Work::ManageSnapshot => {
                if let Some(result) =
                    run_until_shutdown(&handle, &mut work_stop, worker_client.manage_snapshot())
                {
                    let _ = updates.send(Update::Manage(result));
                }
            }
            Work::DiagnosticLog => {
                if let Some(result) =
                    run_until_shutdown(&handle, &mut work_stop, worker_client.diagnostic_log())
                {
                    let _ = updates.send(Update::DiagnosticLog(result));
                }
            }
            Work::Discover { query, provider } => {
                if let Some(result) = run_until_shutdown(
                    &handle,
                    &mut work_stop,
                    worker_client.discover(query, provider),
                ) {
                    let _ = updates.send(Update::Discover(result));
                }
            }
            Work::SaveSettings(settings) => {
                if let Some(result) = run_until_shutdown(
                    &handle,
                    &mut work_stop,
                    worker_client.execute(Command::UpdateSettings(settings)),
                ) {
                    let _ = updates.send(Update::SettingsSaved(result.map(|_| ())));
                }
            }
            Work::Commands(commands) => {
                let result = run_until_shutdown(&handle, &mut work_stop, async {
                    for command in commands {
                        let result = worker_client.execute(command).await?;
                        if let Some(failed) = result["failed"].as_array().filter(|v| !v.is_empty())
                        {
                            return Err(format!("Some items failed: {failed:?}"));
                        }
                    }
                    Ok(())
                });
                if let Some(result) = result {
                    let _ = updates.send(Update::Changed(result));
                }
            }
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    window.on_browse(move |next| {
        let Some(window) = weak.upgrade() else { return };
        let min_size = match parse_size_filter(window.get_filter_min_size().as_str()) {
            Ok(value) => value,
            Err(error) => {
                window.set_status(error.into());
                return;
            }
        };
        let max_size = match parse_size_filter(window.get_filter_max_size().as_str()) {
            Ok(value) => value,
            Err(error) => {
                window.set_status(error.into());
                return;
            }
        };
        if window.get_filter_unknown_size() && (min_size.is_some() || max_size.is_some()) {
            window.set_status("Unknown size cannot be combined with a byte range".into());
            return;
        }
        if min_size.zip(max_size).is_some_and(|(min, max)| min > max) {
            window.set_status("Minimum bytes must not exceed maximum bytes".into());
            return;
        }
        let optional =
            |text: slint::SharedString| (!text.trim().is_empty()).then(|| text.trim().to_string());
        let mut state = v.borrow_mut();
        let mut query = LibraryQuery {
            media_type: (window.get_filter_kind() != "All media")
                .then(|| window.get_filter_kind().to_string()),
            sort: window.get_filter_sort().to_string(),
            search: optional(window.get_filter_search()),
            rating_status: Some(window.get_filter_review().to_string()),
            max_rating: (window.get_filter_rating() != "Any rating")
                .then(|| window.get_filter_rating().parse::<i64>().unwrap_or(5)),
            tag: optional(window.get_filter_tag()),
            tags: optional(window.get_filter_all_tags()),
            any_tags: optional(window.get_filter_any_tags()),
            exclude_tags: optional(window.get_filter_exclude_tags()),
            creator: optional(window.get_filter_creator()),
            min_size,
            max_size,
            unknown_size: window.get_filter_unknown_size().then_some(true),
            cursor: None,
            source_id: state.query.source_id,
            group_id: state.query.group_id,
        };
        let mut previous = state.query.clone();
        previous.cursor = None;
        if next && query == previous {
            query.cursor = state.cursor.clone();
            if let Some(cursor) = state.cursor.clone() {
                let current_page = state.query.cursor.clone();
                state
                    .page_scrolls
                    .insert(current_page, window.get_library_scroll_y());
                state.page_cursors.push(Some(cursor));
                window.set_library_scroll_y(*state.page_scrolls.get(&query.cursor).unwrap_or(&0.0));
            }
        } else {
            state.page_cursors.clear();
            state.page_cursors.push(None);
            state.page_scrolls.clear();
            state.cursor = None;
            window.set_has_more(false);
            window.set_has_previous(false);
            window.set_library_scroll_y(0.0);
        }
        state.query = query;
        if let Some(w) = weak.upgrade() {
            w.set_busy(true);
            w.set_status("Loading library…".into());
        }
        let _ = tx.send(state.browse_work());
    });
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    window.on_previous_page(move || {
        let mut state = v.borrow_mut();
        if state.page_cursors.len() <= 1 {
            return;
        }
        if let Some(w) = weak.upgrade() {
            let current_page = state.query.cursor.clone();
            state
                .page_scrolls
                .insert(current_page, w.get_library_scroll_y());
        }
        state.page_cursors.pop();
        state.query.cursor = state.page_cursors.last().cloned().flatten();
        if let Some(w) = weak.upgrade() {
            w.set_library_scroll_y(*state.page_scrolls.get(&state.query.cursor).unwrap_or(&0.0));
            w.set_busy(true);
        }
        let _ = tx.send(state.browse_work());
    });
    let weak = window.as_weak();
    let v = view.clone();
    window.on_select_item(move |index, selected| {
        let mut state = v.borrow_mut();
        if let Some(item) = state.items.get(index as usize).cloned() {
            if selected {
                state.selected.insert(item.id, item);
            } else {
                state.selected.remove(&item.id);
            }
        }
        if let Some(w) = weak.upgrade() {
            render(&w, &state);
        }
    });
    let v = view.clone();
    let weak = window.as_weak();
    let tx = send.clone();
    window.on_navigate(move |index| {
        let mut state = v.borrow_mut();
        let selected = state
            .navigation
            .get(index as usize)
            .map(|item| (item.id, item.group, item.name.clone()));
        state.navigate_to(selected.as_ref().map(|(id, group, _)| (*id, *group)));
        if let Some(w) = weak.upgrade() {
            w.set_location_title(
                selected
                    .map(|(_, _, name)| name)
                    .unwrap_or_else(|| "All sources and groups".into())
                    .into(),
            );
            w.set_busy(true);
            w.set_has_more(false);
            w.set_has_previous(false);
            w.set_library_scroll_y(0.0);
        }
        let _ = tx.send(state.browse_work());
    });
    let tx = send.clone();
    window.on_create_group(move |name| {
        let _ = tx.send(Work::Commands(vec![Command::CreateGroup(name.to_string())]));
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_move_to_group(move |index| {
        let state = v.borrow();
        if let Some(group) = state
            .navigation
            .iter()
            .filter(|n| n.group)
            .nth(index as usize)
        {
            let _ = tx.send(Work::Commands(vec![Command::MoveToGroup(
                state.selected.keys().copied().collect(),
                Some(group.id),
            )]));
        }
    });
    let v = view.clone();
    let tx = send.clone();
    window.on_rate(move |rating| {
        let _ = tx.send(Work::Commands(vec![Command::Rate(
            v.borrow().selected.keys().copied().collect(),
            rating.into(),
        )]));
    });
    let v = view.clone();
    let tx = send.clone();
    window.on_tag(move |tag| {
        if !tag.trim().is_empty() {
            let _ = tx.send(Work::Commands(vec![Command::Tag(
                v.borrow().selected.keys().copied().collect(),
                tag.to_string(),
            )]));
        }
    });
    let v = view.clone();
    let tx = send.clone();
    window.on_approve(move || {
        let _ = tx.send(Work::Commands(
            v.borrow()
                .selected
                .keys()
                .copied()
                .map(Command::Approve)
                .collect(),
        ));
    });
    let v = view.clone();
    let tx = send.clone();
    window.on_undo_rating(move || {
        if v.borrow().selected.len() == 1 {
            if let Some(item) = v.borrow().selected.values().next() {
                if let Some(reviewed_at) = &item.rating_reviewed_at {
                    let _ = tx.send(Work::Commands(vec![Command::UndoRating(
                        item.id,
                        reviewed_at.clone(),
                    )]));
                }
            }
        }
    });
    let tx = send.clone();
    window.on_import_folder(move || {
        let _ = tx.send(Work::ImportFolder);
    });
    let tx = send.clone();
    window.on_add_sources(move |text| {
        let _ = tx.send(Work::Commands(vec![Command::AddSources(text.to_string())]));
    });
    let tx = send.clone();
    window.on_session_control(move |command| {
        use curator::session::SessionControl;
        let command = match command.as_str() {
            "Start" => Command::StartSession,
            "Pause" => Command::Session(SessionControl::Pause),
            "Resume" => Command::Session(SessionControl::Resume),
            "End" => Command::Session(SessionControl::End { completed: true }),
            _ => return,
        };
        let _ = tx.send(Work::Commands(vec![command]));
    });
    let weak = window.as_weak();
    window.on_fullscreen(move || {
        if let Some(w) = weak.upgrade() {
            w.window().set_fullscreen(!w.window().is_fullscreen());
        }
    });
    let tx = send.clone();
    window.on_downloads(move |pause| {
        let _ = tx.send(Work::Commands(vec![if pause {
            Command::PauseDownloads
        } else {
            Command::ResumeDownloads
        }]));
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_source_download(move |index, pause| {
        let id = v
            .borrow()
            .download_source_ids
            .get(index.max(0) as usize)
            .copied();
        if let Some(id) = id {
            let command = if pause {
                Command::PauseSource(id)
            } else {
                Command::ResumeSource(id)
            };
            let _ = tx.send(Work::Commands(vec![command]));
        }
    });
    let tx = send.clone();
    window.on_resync_all(move || {
        let _ = tx.send(Work::Commands(vec![Command::ResyncAll]));
    });
    let tx = send.clone();
    window.on_refresh_manage(move || {
        let _ = tx.send(Work::ManageSnapshot);
    });
    let tx = send.clone();
    window.on_export_sources(move || {
        let _ = tx.send(Work::ExportSources);
    });
    let tx = send.clone();
    window.on_import_sources(move || {
        let _ = tx.send(Work::ImportSources);
    });
    let weak = window.as_weak();
    let tx = send.clone();
    window.on_manage_destination(move |page| {
        if !(0..=3).contains(&page) {
            return;
        }
        if let Some(w) = weak.upgrade() {
            w.set_manage_page(page);
        }
        match page {
            0 | 1 => {
                let _ = tx.send(Work::ManageSnapshot);
                if page == 1 {
                    let _ = tx.send(Work::Navigation);
                }
            }
            2 => {
                let _ = tx.send(Work::DownloadsStatus);
            }
            3 => {
                let _ = tx.send(Work::ManageSnapshot);
                if local_host {
                    let _ = tx.send(Work::DiagnosticLog);
                }
            }
            _ => unreachable!(),
        }
    });
    let tx = send.clone();
    window.on_refresh_diagnostic_log(move || {
        if local_host {
            let _ = tx.send(Work::DiagnosticLog);
        }
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_discover(move |query, provider_index| {
        if !query.trim().is_empty() {
            let provider = v
                .borrow()
                .discovery_provider_ids
                .get(provider_index.max(0) as usize)
                .cloned()
                .flatten();
            let _ = tx.send(Work::Discover {
                query: query.to_string(),
                provider,
            });
        }
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_queue_discovery(move || {
        let results = v.borrow().discovery_results.clone();
        if !results.is_empty() {
            let _ = tx.send(Work::Commands(vec![Command::QueueSearchResults(results)]));
        }
    });
    let tx = send.clone();
    let weak = window.as_weak();
    let v = view.clone();
    let saver = preference_saver.clone();
    let local_host = matches!(client, Client::Local(_));
    window.on_save_theme(move |theme| {
        if !curator::services::settings::VALID_THEMES.contains(&theme.as_str()) {
            if let Some(window) = weak.upgrade() {
                window.set_status("Choose a supported native theme".into());
            }
            return;
        }
        if local_host {
            let _ = tx.send(Work::Commands(vec![Command::UpdateSettings(
                serde_json::json!({"theme": theme.to_string()}),
            )]));
        } else if let Some(window) = weak.upgrade() {
            apply_native_palette(&window, theme.as_str());
            let result = if preferences_writable {
                saver.queue(current_preferences(&window, &v.borrow()))
            } else {
                Err("Native preferences could not be restored; changes were not saved".into())
            };
            window.set_status(
                result
                    .map(|_| "Saving device appearance".to_owned())
                    .unwrap_or_else(|error| error)
                    .into(),
            );
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    window.on_open_settings(move || {
        let Some(window) = weak.upgrade() else { return };
        let state = v.borrow();
        let settings = &state.settings;
        window.set_settings_page(0);
        window.set_settings_error("".into());
        window.set_settings_saving(false);
        window.set_settings_draft_theme(window.get_settings_theme());
        window.set_settings_draft_tray(window.get_settings_tray());
        window.set_settings_draft_layout(window.get_settings_layout());
        if window.get_local_host() {
            window.set_settings_draft_concurrent(
                settings["max_concurrent"]
                    .as_u64()
                    .unwrap_or(4)
                    .to_string()
                    .into(),
            );
            window.set_settings_draft_max_file_bytes(
                settings["max_download_file_size_bytes"]
                    .as_u64()
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            window.set_settings_draft_max_source_bytes(
                settings["max_source_storage_bytes"]
                    .as_u64()
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            window.set_settings_draft_min_free_bytes(
                settings["minimum_free_disk_bytes"]
                    .as_u64()
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            window.set_settings_draft_cache_bytes(
                settings["thumbnail_cache_max_bytes"]
                    .as_u64()
                    .map(|value| value.to_string())
                    .unwrap_or_default()
                    .into(),
            );
            window.set_settings_draft_max_clip_seconds(
                settings["max_clip_length_secs"]
                    .as_u64()
                    .unwrap_or(60)
                    .to_string()
                    .into(),
            );
            window.set_settings_draft_slideshow_ms(
                settings["default_slideshow_speed"]
                    .as_f64()
                    .unwrap_or(3000.0)
                    .round()
                    .to_string()
                    .into(),
            );
            window.set_settings_draft_slideshow_loop(
                settings["default_slideshow_loop"].as_bool().unwrap_or(true),
            );
            window.set_settings_draft_slideshow_shuffle(
                settings["default_slideshow_shuffle"]
                    .as_bool()
                    .unwrap_or(false),
            );
            window.set_settings_draft_metronome(
                settings["metronome_enabled"].as_bool().unwrap_or(false),
            );
            window.set_settings_draft_reminder_days(
                settings["export_reminder_days"]
                    .as_u64()
                    .unwrap_or(30)
                    .to_string()
                    .into(),
            );
        }
        window.set_settings_open(true);
    });
    let tx = send.clone();
    let local_host = matches!(client, Client::Local(_));
    let weak = window.as_weak();
    let v = view.clone();
    let saver = preference_saver.clone();
    window.on_save_settings(move || {
        let Some(window) = weak.upgrade() else { return false };
        let theme = window.get_settings_draft_theme();
        let library_layout = window.get_settings_draft_layout();
        if !curator::services::settings::VALID_THEMES.contains(&theme.as_str()) {
            window.set_settings_error("Choose a supported native theme".into());
            return false;
        }
        if !["grid", "table"].contains(&library_layout.as_str()) {
            window.set_settings_error("Choose Grid or Table for the library layout".into());
            return false;
        }
        if !local_host {
            let result = if preferences_writable {
                let mut preferences = current_preferences(&window, &v.borrow());
                preferences.theme = theme.to_string();
                preferences.layout = library_layout.to_string();
                saver.queue(preferences)
            } else {
                Err("Native preferences could not be restored; changes were not saved".into())
            };
            if result.is_ok() {
                apply_native_palette(&window, theme.as_str());
                window.set_settings_layout(library_layout);
            }
            window.set_settings_error(
                result
                    .as_ref()
                    .map(|_| String::new())
                    .unwrap_or_else(|error| error.clone())
                    .into(),
            );
            return result.is_ok();
        }
        let validated = (|| {
            Ok::<_, String>(serde_json::json!({
                "theme": theme.trim(),
                "library_layout": library_layout.to_string(),
                "keep_running_in_tray": window.get_settings_draft_tray(),
                "max_concurrent": settings_integer(&window.get_settings_draft_concurrent(), "Concurrent downloads", 1, 20)?,
                "max_download_file_size_bytes": settings_optional_bytes(&window.get_settings_draft_max_file_bytes(), "Maximum download file size")?,
                "max_source_storage_bytes": settings_optional_bytes(&window.get_settings_draft_max_source_bytes(), "Maximum source storage")?,
                "minimum_free_disk_bytes": settings_optional_bytes(&window.get_settings_draft_min_free_bytes(), "Minimum free disk space")?,
                "thumbnail_cache_max_bytes": settings_optional_bytes(&window.get_settings_draft_cache_bytes(), "Thumbnail cache limit")?,
                "max_clip_length_secs": settings_integer(&window.get_settings_draft_max_clip_seconds(), "Maximum clip length", 5, 3600)?,
                "default_slideshow_speed": settings_integer(&window.get_settings_draft_slideshow_ms(), "Slideshow interval", 500, 60_000)?,
                "default_slideshow_loop": window.get_settings_draft_slideshow_loop(),
                "default_slideshow_shuffle": window.get_settings_draft_slideshow_shuffle(),
                "metronome_enabled": window.get_settings_draft_metronome(),
                "export_reminder_days": settings_integer(&window.get_settings_draft_reminder_days(), "Export reminder interval", 1, 365)?,
            }))
        })();
        let settings = match validated {
            Ok(settings) => settings,
            Err(error) => {
                window.set_settings_error(error.into());
                return false;
            }
        };
        match tx.send(Work::SaveSettings(settings)) {
            EnqueueResult::Queued => {
                window.set_settings_saving(true);
                window.set_settings_error("".into());
                false
            }
            EnqueueResult::Full => {
                window.set_settings_error("Background queue is full; try Save again".into());
                false
            }
            EnqueueResult::Closed => {
                window.set_settings_error("Settings writer stopped".into());
                false
            }
        }
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_delete_selected(move || {
        let ids = v.borrow().selected.keys().copied().collect::<Vec<_>>();
        if !ids.is_empty() {
            let _ = tx.send(Work::Commands(vec![Command::DeleteMedia(ids)]));
        }
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_refresh_selected(move || {
        let ids = v.borrow().selected.keys().copied().collect::<Vec<_>>();
        if !ids.is_empty() {
            let _ = tx.send(Work::Commands(vec![Command::RefreshMetadata(ids)]));
        }
    });
    let tx = send.clone();
    let v = view.clone();
    window.on_clip_selected(move |seconds| {
        let state = v.borrow();
        if let Some(item) = state.selected.values().find(|item| item.kind == "video") {
            let _ = tx.send(Work::Commands(vec![Command::CreateClips {
                media_id: item.id,
                seconds: seconds.max(15) as u32,
            }]));
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let saver = preference_saver.clone();
    window.on_enqueue(move |replace| {
        let mut state = v.borrow_mut();
        let selected = state.selected.values().cloned().collect::<Vec<_>>();
        if replace {
            state.queue.clear();
        }
        state.queue.extend(selected);
        state.queue.truncate(1000);
        if let Some(w) = weak.upgrade() {
            render(&w, &state);
            if preferences_writable {
                if let Err(error) = saver.queue(current_preferences(&w, &state)) {
                    w.set_status(error.into());
                }
            }
            if replace && !state.queue.is_empty() {
                drop(state);
                w.invoke_play(0);
            }
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let saver = preference_saver.clone();
    window.on_remove_queued(move |index| {
        let mut state = v.borrow_mut();
        let mut changed = false;
        if (index as usize) < state.queue.len() {
            state.queue.remove(index as usize);
            changed = true;
        }
        if let Some(w) = weak.upgrade() {
            render(&w, &state);
            if changed && preferences_writable {
                if let Err(error) = saver.queue(current_preferences(&w, &state)) {
                    w.set_status(error.into());
                }
            }
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let image_tx = image_send.clone();
    window.on_play(move |index| {
        let mut state = v.borrow_mut();
        if let (Some(item), Some(w)) = (state.queue.get(index as usize).cloned(), weak.upgrade()) {
            let request = state.preview_request.wrapping_add(1);
            let title = item.filename.clone();
            if image_tx.try_send((request, item)).is_ok() {
                state.preview_request = request;
                w.set_playing(format!("Loading {title}…").into());
            } else {
                w.set_status("Player preview is busy; try again shortly.".into());
            }
        }
    });
    let timer = slint::Timer::default();
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    timer.start(
        slint::TimerMode::Repeated,
        Duration::from_millis(50),
        move || {
            let Some(w) = weak.upgrade() else {
                return;
            };
            let rejected = REJECTED_WORK.swap(0, Ordering::Relaxed);
            if rejected > 0 {
                w.set_busy(false);
                w.set_status(format!("Background queue is full; {rejected} action(s) were not queued. Retry shortly.").into());
            }
            if let Ok(result) = session_inbox.try_recv() {
                w.set_session_status(
                    result
                        .as_ref()
                        .map(|state| session_text(state.as_ref()))
                        .unwrap_or_else(|error| format!("Session status unavailable: {error}"))
                        .into(),
                );
            }
            let activity = downloads_latest.lock().ok().and_then(|mut pending| pending.take());
            if let Some(result) = activity {
                apply_downloads(&w, &v, result);
            }
            while let Ok(update) = inbox.try_recv() {
                match update {
                    Update::Recovery(result) => match result {
                        Ok(snapshot) => {
                            let previous = v.borrow().backup_ids
                                .get(w.get_recovery_selected_backup().max(0) as usize)
                                .cloned();
                            let ids = snapshot.backups.iter().map(|backup| backup.id.clone()).collect::<Vec<_>>();
                            let selected = selected_backup_index(&ids, previous.as_deref());
                            let labels = snapshot.backups.iter().map(|backup| {
                                format!("{} · {} bytes · {}", backup.id, backup.size_bytes, backup.created_at).into()
                            }).collect::<Vec<slint::SharedString>>();
                            v.borrow_mut().backup_ids = ids;
                            w.set_recovery_backups(ModelRc::new(VecModel::from(labels)));
                            w.set_recovery_selected_backup(selected as i32);
                            w.set_recovery_has_backups(!snapshot.backups.is_empty());
                            w.set_recovery_status(recovery_text(&snapshot).into());
                        }
                        Err(error) => w.set_recovery_status(error.into()),
                    },
                    Update::PreferenceError(error) => w.set_status(format!("Could not save native preferences: {error}").into()),
                    Update::Navigation(result) => match result {
                        Ok(items) => {
                            w.set_navigation(ModelRc::new(VecModel::from(
                                items
                                    .iter()
                                    .map(|n| {
                                        format!(
                                            "{}{}: {} ({})",
                                            "› ".repeat(n.depth),
                                            if n.group { "Group" } else { "Source" },
                                            n.name,
                                            n.media_count
                                                .map(|count| count.to_string())
                                                .unwrap_or_else(|| "?".into())
                                        )
                                        .into()
                                    })
                                    .collect::<Vec<slint::SharedString>>(),
                            )));
                            w.set_groups(ModelRc::new(VecModel::from(
                                items
                                    .iter()
                                    .filter(|n| n.group)
                                    .map(|n| n.name.clone().into())
                                    .collect::<Vec<slint::SharedString>>(),
                            )));
                            v.borrow_mut().navigation = items;
                        }
                        Err(error) => w.set_status(error.into()),
                    },
                    Update::Image(request, title, result) if request == v.borrow().preview_request => match result {
                        Ok(image) => {
                            let buffer =
                                slint::SharedPixelBuffer::<slint::Rgba8Pixel>::clone_from_slice(
                                    &image.pixels,
                                    image.width,
                                    image.height,
                                );
                            w.set_preview(slint::Image::from_rgba8(buffer));
                            w.set_playing(title.into());
                        }
                        Err(error) => {
                            w.set_preview(slint::Image::default());
                            w.set_playing(title.into());
                            w.set_status(error.into());
                        }
                    },
                    Update::Image(..) => {},
                    Update::Page(request, result) if request == v.borrow().browse_request => {
                        w.set_busy(false);
                        match result {
                            Ok(page) => {
                                let mut state = v.borrow_mut();
                                state.items = page.media;
                                let refreshed_selection = state.items.iter()
                                    .filter(|item| state.selected.contains_key(&item.id))
                                    .cloned()
                                    .collect::<Vec<_>>();
                                for item in refreshed_selection {
                                    state.selected.insert(item.id, item);
                                }
                                state.cursor = page.next_cursor;
                                w.set_has_more(state.cursor.is_some());
                                w.set_has_previous(state.page_cursors.len() > 1);
                                w.set_status(
                                    format!(
                                        "{} {} on this page",
                                        state.items.len(),
                                        if state.items.len() == 1 { "item" } else { "items" }
                                    )
                                    .into(),
                                );
                                let restore_scroll = *state
                                    .page_scrolls
                                    .get(&state.query.cursor)
                                    .unwrap_or(&0.0);
                                render(&w, &state);
                                // The new model's viewport geometry is applied on the
                                // next event-loop turn. Restoring earlier is clamped by
                                // the previous page and loses the remembered position.
                                let weak = w.as_weak();
                                let view = v.clone();
                                slint::Timer::single_shot(Duration::from_millis(1), move || {
                                    if view.borrow().browse_request == request {
                                        if let Some(window) = weak.upgrade() {
                                            window.set_library_scroll_y(restore_scroll);
                                        }
                                    }
                                });
                            }
                            Err(error) => w.set_status(error.into()),
                        }
                    }
                    Update::Page(..) => {}
                    Update::Manage(result) => match result {
                        Ok(snapshot) => {
                            v.borrow_mut().settings = snapshot.settings.clone();
                            if w.get_local_host() {
                            apply_native_palette(
                                &w,
                                snapshot.settings["theme"].as_str().unwrap_or("system"),
                            );
                            w.set_settings_tray(
                                snapshot.settings["keep_running_in_tray"]
                                    .as_bool()
                                    .unwrap_or(true),
                            );
                            w.set_settings_layout(
                                snapshot.settings["library_layout"]
                                    .as_str()
                                    .unwrap_or("grid")
                                    .into(),
                            );
                            }
                            let (labels, ids) = discovery_provider_options(&snapshot.providers);
                            v.borrow_mut().discovery_provider_ids = ids;
                            w.set_discovery_providers(ModelRc::new(VecModel::from(
                                labels.into_iter().map(Into::into).collect::<Vec<_>>(),
                            )));
                            w.set_manage_status(manage_text(&snapshot).into());
                            w.set_settings_loaded(true);
                        }
                        Err(error) => w.set_manage_status(error.into()),
                    },
                    Update::DiagnosticLog(result) => {
                        w.set_diagnostic_log(result.unwrap_or_else(|error| error).into())
                    }
                    Update::Discover(result) => match result {
                        Ok(value) => {
                            let results = value["results"].as_array().cloned().unwrap_or_default();
                            let lines = results
                                .iter()
                                .map(|row| {
                                    format!(
                                        "{} — {} ({})",
                                        row["title"].as_str().unwrap_or("Untitled"),
                                        row["source"].as_str().unwrap_or(""),
                                        row["result_type"].as_str().unwrap_or("")
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join("\n");
                            v.borrow_mut().discovery_results = results;
                            w.set_discovery_status(if lines.is_empty() {
                                "No discovery results".into()
                            } else {
                                lines.into()
                            });
                        }
                        Err(error) => w.set_discovery_status(error.into()),
                    },
                    Update::SettingsSaved(result) => {
                        w.set_settings_saving(false);
                        match result {
                            Ok(()) => {
                                w.set_settings_open(false);
                                w.set_settings_error("".into());
                                w.set_status("Settings saved".into());
                                let _ = tx.send(Work::ManageSnapshot);
                            }
                            Err(error) => w.set_settings_error(error.into()),
                        }
                    }
                    Update::Exported(result) => match result {
                        Ok(message) => w.set_status(message.into()),
                        Err(error) => w.set_status(format!("Source export failed: {error}").into()),
                    },
                    Update::Imported(result) => match result {
                        Ok(message) => {
                            w.set_status(message.into());
                            let _ = tx.send(Work::Navigation);
                            let _ = tx.send(Work::ManageSnapshot);
                        }
                        Err(error) => w.set_status(format!("Source import failed: {error}").into()),
                    },
                    Update::Changed(result) => match result {
                        Ok(()) => {
                            w.set_status("Saved".into());
                            let _ = tx.send(v.borrow_mut().browse_work());
                            let _ = tx.send(Work::Navigation);
                            let _ = tx.send(Work::ManageSnapshot);
                            let _ = tx.send(Work::DownloadsStatus);
                        }
                        Err(error) => w.set_status(error.into()),
                    },
                    Update::Downloads(result) => apply_downloads(&w, &v, result),
                }
            }
        },
    );
    let recovery_send = send.clone();
    let recovery_view = view.clone();
    let recovery_window = window.as_weak();
    window.on_recovery(move |action, backup, confirmation| {
        use curator::maintenance::{MaintenanceKind, MaintenanceRequest};
        let kind = maintenance_kind_for_action(action.as_str());
        let selected_backup = if matches!(
            kind,
            Some(MaintenanceKind::ValidateBackup | MaintenanceKind::RestoreBackup)
        ) {
            recovery_window.upgrade().and_then(|window| {
                recovery_view
                    .borrow()
                    .backup_ids
                    .get(window.get_recovery_selected_backup().max(0) as usize)
                    .cloned()
            })
        } else {
            None
        };
        let request = kind.map(|kind| MaintenanceRequest {
            kind,
            backup_id: (!backup.trim().is_empty())
                .then(|| backup.trim().to_owned())
                .or(selected_backup),
            confirmation: confirmation.to_string(),
        });
        let _ = recovery_send.send(Work::Recovery(request));
    });
    let _ = send.send(Work::Navigation);
    let _ = send.send(Work::ManageSnapshot);
    window.invoke_browse(false);
    let result = window.run();
    let preferences = current_preferences(&window, &view.borrow());
    timer.stop();
    let _ = session_stop.send(true);
    drop(timer);
    drop(window);
    drop(preference_saver);
    let _ = preference_worker.join();
    let save_result = if preferences_writable {
        client.save_preferences(&preferences)
    } else {
        Ok(())
    };
    drop(send);
    drop(image_send);
    let _ = worker.join();
    let _ = control_worker.join();
    let _ = image_worker.join();
    let _ = session_worker.join();
    let _ = downloads_worker.join();
    save_result?;
    result?;
    Ok(if switch_requested.get() {
        NativeExit::SwitchHost
    } else {
        NativeExit::Close
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shutdown_cancels_an_in_flight_native_operation() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (stop, mut receiver) = tokio::sync::watch::channel(false);
        let handle = runtime.handle().clone();
        let operation = std::thread::spawn(move || {
            run_until_shutdown(&handle, &mut receiver, async {
                std::future::pending::<()>().await;
                42
            })
        });
        stop.send(true).unwrap();
        assert_eq!(operation.join().unwrap(), None);
    }

    #[test]
    fn settings_drafts_reject_values_the_server_would_clamp() {
        assert_eq!(settings_integer("20", "Downloads", 1, 20), Ok(20));
        assert!(settings_integer("21", "Downloads", 1, 20).is_err());
        assert!(settings_integer("1.5", "Downloads", 1, 20).is_err());
        assert_eq!(settings_optional_bytes("", "File size"), Ok(None));
        assert_eq!(
            settings_optional_bytes(" 1024 ", "File size"),
            Ok(Some(1024))
        );
        assert!(settings_optional_bytes("0", "File size").is_err());
    }

    #[test]
    fn recovery_refresh_keeps_the_selected_backup_by_id() {
        let ids = vec!["new.zip".to_owned(), "chosen.zip".to_owned()];
        assert_eq!(selected_backup_index(&ids, Some("chosen.zip")), 1);
        assert_eq!(selected_backup_index(&ids, Some("removed.zip")), 0);
        assert_eq!(selected_backup_index(&[], Some("chosen.zip")), 0);
    }

    #[test]
    fn local_admin_actions_use_the_backend_confirmation_contract() {
        for (label, phrase) in [
            ("Reconcile library", None),
            ("Clear human ratings", Some("CLEAR HUMAN RATINGS")),
            ("Reset ratings and evidence", Some("RESET RATINGS")),
            ("Flatten groups", Some("FLATTEN GROUPS")),
            ("Delete groups", Some("DELETE GROUPS")),
            ("Clear tag assignments", Some("CLEAR TAG ASSIGNMENTS")),
            ("Clear session history", Some("CLEAR SESSION HISTORY")),
            ("Rebuild caches", Some("REBUILD CACHES")),
            ("Factory reset", Some("RESET CURATOR")),
            ("Remove P-HAR", Some("DELETE P-HAR")),
            ("Remove archives", Some("DELETE ARCHIVES")),
        ] {
            assert_eq!(
                maintenance_kind_for_action(label)
                    .expect("Local Admin action must map to a backend kind")
                    .confirmation_phrase(),
                phrase,
                "incorrect confirmation for {label}"
            );
        }
    }

    fn contrast_ratio(a: u32, b: u32) -> f64 {
        fn luminance(color: u32) -> f64 {
            let channel = |shift| {
                let value = ((color >> shift) & 0xff_u32) as f64 / 255.0;
                if value <= 0.04045 {
                    value / 12.92
                } else {
                    ((value + 0.055) / 1.055).powf(2.4)
                }
            };
            0.2126 * channel(16) + 0.7152 * channel(8) + 0.0722 * channel(0)
        }
        let a = luminance(a);
        let b = luminance(b);
        (a.max(b) + 0.05) / (a.min(b) + 0.05)
    }

    #[test]
    fn native_palettes_keep_body_card_and_selection_text_readable() {
        for name in [
            "atelier-dark",
            "midnight",
            "ember",
            "linen",
            "sage",
            "aurora",
            "oled",
            "adwaita-light",
            "adwaita-dark",
            "yaru-light",
            "yaru-dark",
            "arc-light",
            "arc-dark",
            "breeze-light",
            "breeze-dark",
        ] {
            let palette = native_palette(name);
            assert!(
                contrast_ratio(palette.text, palette.background) >= 4.5,
                "{name} body"
            );
            assert!(
                contrast_ratio(palette.text, palette.elevated) >= 4.5,
                "{name} card"
            );
            assert!(
                contrast_ratio(palette.muted, palette.panel) >= 4.5,
                "{name} muted"
            );
            assert!(
                contrast_ratio(palette.selection_text, palette.selection) >= 4.5,
                "{name} selection"
            );
        }
    }

    #[test]
    fn native_legacy_themes_follow_browser_aliases() {
        for (legacy, current) in [
            ("gtk-system", "system"),
            ("yotsuba", "linen"),
            ("yotsuba-b", "midnight"),
            ("futaba", "ember"),
            ("burichan", "midnight"),
            ("tomorrow", "linen"),
            ("photon", "linen"),
            ("light", "linen"),
            ("oled-dark", "oled"),
            ("dark", "atelier-dark"),
        ] {
            assert_eq!(canonical_native_theme(legacy), current);
        }
    }

    #[test]
    fn session_display_uses_authoritative_phase_and_active_time() {
        use curator::session::{GameConfig, SessionCommand, SessionEngine};
        let mut engine = SessionEngine::new(GameConfig::quick_default()).unwrap();
        engine.dispatch(SessionCommand::Start { monotonic_ms: 100 });
        let update = engine.dispatch(SessionCommand::Tick {
            monotonic_ms: 12_100,
        });
        let text = session_text(Some(&update.state));
        assert!(text.contains("00:12 active"), "{text}");
        assert!(text.contains(&update.state.phase.id), "{text}");
        assert!(text.contains("BPM"), "{text}");
        assert_eq!(session_text(None), "No active session");
    }

    #[test]
    fn changing_location_discards_the_previous_page_cursor() {
        let mut state = ViewState::default();
        state.query.source_id = Some(9);
        state.query.cursor = Some("old-query".into());
        state.cursor = Some("next-from-old-query".into());
        state.page_cursors = vec![None, Some("old-query".into())];
        state.page_scrolls.insert(Some("old-query".into()), 48.0);
        state.navigate_to(Some((12, true)));
        assert_eq!(state.query.source_id, None);
        assert_eq!(state.query.group_id, Some(12));
        assert!(state.query.cursor.is_none());
        assert!(state.cursor.is_none());
        assert_eq!(state.page_cursors, vec![None]);
        assert!(state.page_scrolls.is_empty());

        state.navigate_to(Some((7, false)));
        assert_eq!(state.query.source_id, Some(7));
        assert_eq!(state.query.group_id, None);
        state.navigate_to(None);
        assert_eq!(state.query.source_id, None);
        assert_eq!(state.query.group_id, None);
    }

    #[test]
    fn inspector_keeps_selected_item_after_its_page_is_replaced() {
        let mut state = ViewState::default();
        let selected = MediaItem {
            id: 20,
            filename: "sample-020.png".into(),
            kind: "image".into(),
            rating: 0,
            source: "Blue source".into(),
            filepath: "blue/sample-020.png".into(),
            playback_filepath: None,
            tags: vec!["saved".into()],
            rating_reviewed_at: None,
        };
        state.selected.insert(selected.id, selected);
        state.items.clear();
        assert!(inspector_text(&state.selected).contains("sample-020.png"));
        assert!(inspector_text(&state.selected).contains("Tags: saved"));
    }

    #[test]
    fn size_filters_accept_whole_bytes_and_reject_invalid_values() {
        assert_eq!(parse_size_filter(" "), Ok(None));
        assert_eq!(parse_size_filter(" 1024 "), Ok(Some(1024)));
        assert!(parse_size_filter("-1").is_err());
        assert!(parse_size_filter("1.5").is_err());
        assert!(parse_size_filter("999999999999999999999999").is_err());
    }

    #[test]
    fn browse_requests_have_distinct_ids_and_capture_their_filters() {
        let mut state = ViewState::default();
        state.query.search = Some("first".into());
        let first = state.browse_work();
        state.query.search = Some("second".into());
        let second = state.browse_work();
        assert!(
            matches!(first, Work::Browse(query, 1) if query.search.as_deref() == Some("first"))
        );
        assert!(
            matches!(second, Work::Browse(query, 2) if query.search.as_deref() == Some("second"))
        );
        assert_eq!(state.browse_request, 2);
    }

    #[test]
    fn preference_writer_keeps_the_latest_snapshot_when_wake_is_full() {
        let (wake, receive) = mpsc::sync_channel(1);
        let saver = PreferenceSaver {
            latest: Arc::new(Mutex::new(None)),
            wake,
        };
        saver.queue(NativePreferences::default()).unwrap();
        let newest = NativePreferences {
            theme: "ember".into(),
            ..Default::default()
        };
        saver.queue(newest).unwrap();
        assert!(receive.try_recv().is_ok());
        assert_eq!(saver.latest.lock().unwrap().take().unwrap().theme, "ember");
    }

    #[test]
    fn control_lane_accepts_pause_when_regular_queue_is_full() {
        let (regular, regular_rx) = mpsc::sync_channel(1);
        let (control, control_rx) = mpsc::sync_channel(1);
        let sender = WorkSender { regular, control };
        assert_eq!(sender.send(Work::Navigation), EnqueueResult::Queued);
        assert_eq!(sender.send(Work::ManageSnapshot), EnqueueResult::Full);
        assert_eq!(
            sender.send(Work::Commands(vec![Command::PauseDownloads])),
            EnqueueResult::Queued
        );
        assert!(matches!(regular_rx.try_recv(), Ok(Work::Navigation)));
        assert!(matches!(
            control_rx.try_recv(),
            Ok(commands) if matches!(commands.as_slice(), [Command::PauseDownloads])
        ));
        assert_eq!(
            sender.send(Work::Commands(vec![Command::PauseSource(7)])),
            EnqueueResult::Queued
        );
        assert!(matches!(
            control_rx.try_recv(),
            Ok(commands) if matches!(commands.as_slice(), [Command::PauseSource(7)])
        ));
    }

    #[test]
    fn discovery_options_show_provider_names_but_send_stable_ids() {
        let (labels, ids) = discovery_provider_options(&serde_json::json!({
            "providers": [{
                "id": "gallery_dl_adapter",
                "name": "Example Gallery",
                "availability": "direct_url_only"
            }]
        }));
        assert_eq!(
            labels,
            vec![
                "All providers".to_owned(),
                "Example Gallery (direct_url_only)".to_owned()
            ]
        );
        assert_eq!(ids, vec![None, Some("gallery_dl_adapter".into())]);
    }

    #[test]
    fn download_status_explains_known_and_inaccurate_totals() {
        let text = download_status_text(&serde_json::json!({
            "active_count": 1,
            "queued_count": 2,
            "retrying_count": 0,
            "sources": [
                {"name": "Known", "phase": "active", "completed_count": 3, "known_total": 10, "percentage": 30.0, "current_filename": "clip.mp4"},
                {"name": "Unknown", "phase": "indexing", "completed_count": 7, "known_total": null}
            ]
        }));
        assert!(text.contains("1 active · 2 queued · 0 retrying"));
        assert!(text.contains("3 / 10 (30%) · clip.mp4"));
        assert!(text.contains("7 completed · total not reported"));
    }
}

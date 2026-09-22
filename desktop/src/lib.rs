slint::include_modules!();

use curator::native::{
    Client, Command, LibraryQuery, ManageSnapshot, MediaItem, MediaPage, NativeImage,
    NavigationItem,
};
use slint::{ComponentHandle, ModelRc, VecModel};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc, sync::mpsc, time::Duration};

enum Work {
    Recovery(Option<curator::maintenance::MaintenanceRequest>),
    Navigation,
    ImportFolder,
    Image(MediaItem),
    Browse(LibraryQuery),
    ManageSnapshot,
    DiagnosticLog,
    Discover {
        query: String,
        provider: Option<String>,
    },
    Commands(Vec<Command>),
}
enum Update {
    Recovery(Result<String, String>),
    Session(String),
    Navigation(Result<Vec<NavigationItem>, String>),
    Image(String, Result<NativeImage, String>),
    Page(Result<MediaPage, String>),
    Manage(Result<ManageSnapshot, String>),
    DiagnosticLog(Result<String, String>),
    Discover(Result<serde_json::Value, String>),
    Changed(Result<(), String>),
    Downloads(String),
}

#[derive(Default)]
struct ViewState {
    navigation: Vec<NavigationItem>,
    items: Vec<MediaItem>,
    selected: BTreeMap<i64, MediaItem>,
    queue: Vec<MediaItem>,
    query: LibraryQuery,
    cursor: Option<String>,
    page_cursors: Vec<Option<String>>,
    discovery_results: Vec<serde_json::Value>,
    // Index zero is the explicit all-provider search. Subsequent entries
    // retain the API identifiers while Slint displays the human-facing name.
    discovery_provider_ids: Vec<Option<String>>,
}

fn manage_text(snapshot: &ManageSnapshot) -> String {
    let stats = &snapshot.stats;
    let storage = &snapshot.storage;
    let providers = snapshot.providers["providers"]
        .as_array()
        .map_or(0, Vec::len);
    format!(
        "Library: {} media · {} sources · {} groups · {} tags\nDownloads: {} active · {} errors\nStorage: {}\nDiscovery: {} providers\nRemote access: {}",
        stats["total_media"], stats["total_sources"], stats["total_groups"], stats["total_tags"],
        stats["sources_downloading"], stats["sources_error"],
        storage["total_bytes"].as_u64().map(|bytes| format!("{bytes} bytes")).unwrap_or_else(|| "calculating".into()),
        providers,
        snapshot.remote_access["status"].as_str().unwrap_or("unavailable"),
    )
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

fn render(window: &CuratorNativeWindow, state: &ViewState) {
    window.set_selected_count(state.selected.len().min(i32::MAX as usize) as i32);
    window.set_media(ModelRc::new(VecModel::from(
        state
            .items
            .iter()
            .map(|item| MediaRow {
                title: item.filename.clone().into(),
                detail: format!("{} Â· {} Â· {} â˜…", item.source, item.kind, item.rating).into(),
                selected: state.selected.contains_key(&item.id),
            })
            .collect::<Vec<_>>(),
    )));
    window.set_queue(ModelRc::new(VecModel::from(
        state
            .queue
            .iter()
            .map(|item| item.filename.clone().into())
            .collect::<Vec<slint::SharedString>>(),
    )));
    window.set_inspector(
        state
            .items
            .iter()
            .filter(|item| state.selected.contains_key(&item.id))
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
            .into(),
    );
}

pub fn run_ui(
    runtime: &tokio::runtime::Runtime,
    client: Client,
) -> Result<(), Box<dyn std::error::Error>> {
    let window = CuratorNativeWindow::new()?;
    window.set_local_host(matches!(client, Client::Local(_)));
    let view = Rc::new(RefCell::new(ViewState::default()));
    let mut preferences_writable = true;
    match client.load_preferences() {
        Ok(preferences) => {
            view.borrow_mut().queue = preferences.queue;
            window.set_workspace(preferences.workspace.clamp(0, 2));
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
    let (send, receive) = mpsc::channel();
    let (updates, inbox) = mpsc::channel();
    let worker_client = client.clone();
    let handle = runtime.handle().clone();
    // A single worker preserves command order and keeps SQLite, cancellation,
    // and network work off Slint's event thread. No UI callback blocks on Tokio.
    let worker = std::thread::spawn(move || loop {
        let work = match receive.recv_timeout(Duration::from_secs(2)) {
            Ok(work) => work,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if matches!(worker_client, Client::Local(_)) {
                    let _ = updates.send(Update::Recovery(
                        handle.block_on(worker_client.recovery_status()),
                    ));
                }
                let summary = match handle.block_on(worker_client.session()) {
                    Ok(Some(state)) => format!(
                        "{:?} · {} · {:.0} BPM · {} seconds\n{}",
                        state.status,
                        state.phase.id,
                        state.tempo.current_bpm,
                        state.active_elapsed_ms / 1000,
                        state.instruction.unwrap_or_default()
                    ),
                    Ok(None) => "No active session".into(),
                    Err(error) => error,
                };
                let _ = updates.send(Update::Session(summary));
                let status = match handle.block_on(worker_client.downloads()) {
                    Ok(status) => status,
                    Err(error) => {
                        let _ = updates.send(Update::Downloads(error));
                        continue;
                    }
                };
                let rows = download_status_text(&status);
                let _ = updates.send(Update::Downloads(rows));
                continue;
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        match work {
            Work::Recovery(request) => {
                let result = handle.block_on(async {
                    if let Some(request) = request {
                        worker_client.recovery(request).await?;
                    }
                    worker_client.recovery_status().await
                });
                let _ = updates.send(Update::Recovery(result));
            }
            Work::Navigation => {
                let _ = updates.send(Update::Navigation(
                    handle.block_on(worker_client.navigation()),
                ));
            }
            Work::ImportFolder => {
                if let Some(path) = rfd::FileDialog::new()
                    .set_title("Import folder into Curator")
                    .pick_folder()
                {
                    let result = handle
                        .block_on(worker_client.execute(Command::ImportFolder(path)))
                        .map(|_| ());
                    let _ = updates.send(Update::Changed(result));
                }
            }
            Work::Image(item) => {
                let image = handle.block_on(worker_client.image(&item));
                let _ = updates.send(Update::Image(item.filename, image));
            }
            Work::Browse(query) => {
                let _ = updates.send(Update::Page(handle.block_on(worker_client.library(query))));
            }
            Work::ManageSnapshot => {
                let _ = updates.send(Update::Manage(
                    handle.block_on(worker_client.manage_snapshot()),
                ));
            }
            Work::DiagnosticLog => {
                let _ = updates.send(Update::DiagnosticLog(
                    handle.block_on(worker_client.diagnostic_log()),
                ));
            }
            Work::Discover { query, provider } => {
                let _ = updates.send(Update::Discover(
                    handle.block_on(worker_client.discover(query, provider)),
                ));
            }
            Work::Commands(commands) => {
                let result = handle.block_on(async {
                    for command in commands {
                        let result = worker_client.execute(command).await?;
                        if let Some(failed) = result["failed"].as_array().filter(|v| !v.is_empty())
                        {
                            return Err(format!("Some items failed: {failed:?}"));
                        }
                    }
                    Ok(())
                });
                let _ = updates.send(Update::Changed(result));
            }
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    window.on_browse(move |kind, sort, search, review, tag, next| {
        let mut state = v.borrow_mut();
        state.query = LibraryQuery {
            media_type: (kind != "All media").then(|| kind.to_string()),
            sort: sort.to_string(),
            search: (!search.trim().is_empty()).then(|| search.to_string()),
            rating_status: Some(review.to_string()),
            tag: (!tag.trim().is_empty()).then(|| tag.to_string()),
            cursor: if next { state.cursor.clone() } else { None },
            source_id: state.query.source_id,
            group_id: state.query.group_id,
        };
        if next {
            if let Some(cursor) = state.cursor.clone() {
                state.page_cursors.push(Some(cursor));
            }
        } else {
            state.page_cursors.clear();
            state.page_cursors.push(None);
        }
        if let Some(w) = weak.upgrade() {
            w.set_busy(true);
            w.set_status("Loading libraryâ€¦".into());
        }
        let _ = tx.send(Work::Browse(state.query.clone()));
    });
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    window.on_previous_page(move || {
        let mut state = v.borrow_mut();
        if state.page_cursors.len() <= 1 {
            return;
        }
        state.page_cursors.pop();
        state.query.cursor = state.page_cursors.last().cloned().flatten();
        if let Some(w) = weak.upgrade() {
            w.set_busy(true);
        }
        let _ = tx.send(Work::Browse(state.query.clone()));
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
    let tx = send.clone();
    window.on_navigate(move |index| {
        let mut state = v.borrow_mut();
        let target = state
            .navigation
            .get(index as usize)
            .map(|n| (n.id, n.group));
        state.query.source_id = target.filter(|(_, group)| !group).map(|(id, _)| id);
        state.query.group_id = target.filter(|(_, group)| *group).map(|(id, _)| id);
        state.query.cursor = None;
        let _ = tx.send(Work::Browse(state.query.clone()));
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
    window.on_resync_all(move || {
        let _ = tx.send(Work::Commands(vec![Command::ResyncAll]));
    });
    let tx = send.clone();
    window.on_refresh_manage(move || {
        let _ = tx.send(Work::ManageSnapshot);
    });
    let tx = send.clone();
    window.on_refresh_diagnostic_log(move || {
        let _ = tx.send(Work::DiagnosticLog);
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
    window.on_save_theme(move |theme| {
        let _ = tx.send(Work::Commands(vec![Command::UpdateSettings(
            serde_json::json!({"theme": theme.to_string()}),
        )]));
    });
    let tx = send.clone();
    let local_host = matches!(client, Client::Local(_));
    window.on_save_settings(move |theme, keep_running_in_tray, library_layout| {
        let mut settings = serde_json::json!({
            "theme": theme.trim(),
            "library_layout": library_layout.to_string(),
        });
        if local_host {
            settings["keep_running_in_tray"] = serde_json::json!(keep_running_in_tray);
        }
        let _ = tx.send(Work::Commands(vec![Command::UpdateSettings(settings)]));
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
            if replace && !state.queue.is_empty() {
                drop(state);
                w.invoke_play(0);
            }
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    window.on_remove_queued(move |index| {
        let mut state = v.borrow_mut();
        if (index as usize) < state.queue.len() {
            state.queue.remove(index as usize);
        }
        if let Some(w) = weak.upgrade() {
            render(&w, &state);
        }
    });
    let weak = window.as_weak();
    let v = view.clone();
    let tx = send.clone();
    window.on_play(move |index| {
        if let (Some(item), Some(w)) = (v.borrow().queue.get(index as usize), weak.upgrade()) {
            w.set_playing(format!("Loading {}…", item.filename).into());
            let _ = tx.send(Work::Image(item.clone()));
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
            while let Ok(update) = inbox.try_recv() {
                match update {
                    Update::Recovery(result) => {
                        w.set_recovery_status(result.unwrap_or_else(|error| error).into())
                    }
                    Update::Session(summary) => w.set_session_status(summary.into()),
                    Update::Navigation(result) => match result {
                        Ok(items) => {
                            w.set_navigation(ModelRc::new(VecModel::from(
                                items
                                    .iter()
                                    .map(|n| {
                                        format!(
                                            "{}: {}",
                                            if n.group { "Group" } else { "Source" },
                                            n.name
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
                    Update::Image(title, result) => match result {
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
                    Update::Page(result) => {
                        w.set_busy(false);
                        match result {
                            Ok(page) => {
                                let mut state = v.borrow_mut();
                                state.items = page.media;
                                state.cursor = page.next_cursor;
                                w.set_has_more(state.cursor.is_some());
                                w.set_has_previous(state.page_cursors.len() > 1);
                                w.set_status(
                                    format!("{} items on this page", state.items.len()).into(),
                                );
                                render(&w, &state);
                            }
                            Err(error) => w.set_status(error.into()),
                        }
                    }
                    Update::Manage(result) => match result {
                        Ok(snapshot) => {
                            w.set_settings_theme(
                                snapshot.settings["theme"]
                                    .as_str()
                                    .unwrap_or("system")
                                    .into(),
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
                            let (labels, ids) = discovery_provider_options(&snapshot.providers);
                            v.borrow_mut().discovery_provider_ids = ids;
                            w.set_discovery_providers(ModelRc::new(VecModel::from(
                                labels.into_iter().map(Into::into).collect::<Vec<_>>(),
                            )));
                            w.set_manage_status(manage_text(&snapshot).into());
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
                    Update::Changed(result) => match result {
                        Ok(()) => {
                            w.set_status("Saved".into());
                            let _ = tx.send(Work::Browse(v.borrow().query.clone()));
                            let _ = tx.send(Work::Navigation);
                            let _ = tx.send(Work::ManageSnapshot);
                        }
                        Err(error) => w.set_status(error.into()),
                    },
                    Update::Downloads(status) => w.set_download_status(status.into()),
                }
            }
        },
    );
    let recovery_send = send.clone();
    window.on_recovery(move |action, backup, confirmation| {
        use curator::maintenance::{MaintenanceKind, MaintenanceRequest};
        let kind = match action.as_str() {
            "Create backup" => Some(MaintenanceKind::CreateBackup),
            "Validate backup" => Some(MaintenanceKind::ValidateBackup),
            "Restore backup" => Some(MaintenanceKind::RestoreBackup),
            _ => None,
        };
        let request = kind.map(|kind| MaintenanceRequest {
            kind,
            backup_id: (!backup.trim().is_empty()).then(|| backup.trim().to_owned()),
            confirmation: confirmation.to_string(),
        });
        let _ = recovery_send.send(Work::Recovery(request));
    });
    let _ = send.send(Work::Navigation);
    let _ = send.send(Work::ManageSnapshot);
    window.invoke_browse(
        "All media".into(),
        "date_desc".into(),
        "".into(),
        "all".into(),
        "".into(),
        false,
    );
    let result = window.run();
    let size = window.window().size();
    let preferences = curator::native::NativePreferences {
        version: 1,
        queue: view.borrow().queue.clone(),
        width: size.width,
        height: size.height,
        workspace: window.get_workspace(),
    };
    let save_result = if preferences_writable {
        client.save_preferences(&preferences)
    } else {
        Ok(())
    };
    timer.stop();
    drop(timer);
    drop(window);
    drop(send);
    let _ = worker.join();
    save_result?;
    result.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

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

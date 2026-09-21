pub mod appearance;
mod beat;
mod chpack;
pub mod config;
mod data_lock;
mod db;
#[cfg(test)]
mod desktop_tests;
mod downloader;
mod duration;
pub mod edition;
mod hierarchy;
pub mod local_import;
pub mod maintenance;
mod media_files;
pub mod migration;
pub mod native;
mod nsfw;
mod oobe;
pub mod phar;
pub mod process;
mod provenance;
pub mod remote;
pub mod routes;
pub mod session;
mod slug;
mod startup;
mod storage;
#[cfg(test)]
mod test_support;
mod thumb_worker;
pub mod url_guard;
mod virtual_clips;

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::Instant;

use anyhow::{Context, Result};
use data_lock::DataDirectoryLock;
use edition::{Edition, InitializeOptions, InstallScope};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rand::RngCore;
use tokio::sync::{Mutex, RwLock, Semaphore};
use tower_http::compression::CompressionLayer;
use tower_http::services::ServeDir;
use tracing::{error, info, warn};

pub static DOCS_TEXT: &str = include_str!("../DOCS.txt");
pub static NSFW_WORKER_PY: &str = include_str!("../nsfw_worker.py");
pub static ACTION_WORKER_PY: &str = include_str!("../action_worker.py");

/// Tests that alter process-global environment variables share one guard.
/// Production configuration is per scope and does not rely on this lock.
#[cfg(test)]
pub(crate) static PROCESS_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

pub use edition::{Edition as ProductEdition, InitializeOptions as InitOptions};
pub use edition::{API_PROTOCOL, PRODUCT_VERSION};

pub type GroupTagCache = Arc<RwLock<Option<Arc<HashMap<i64, HashSet<String>>>>>>;

pub use startup::{set_start_with_windows, StartupRegistration};

/// Apply the Windows Run registration and persist the matching Curator
/// preference as one operation. Keeping this in the backend prevents the tray,
/// Settings dialog, and OOBE from drifting into contradictory states.
pub async fn set_start_with_windows_preference(
    state: &AppState,
    enabled: bool,
) -> std::result::Result<(), String> {
    tokio::task::spawn_blocking(move || startup::set_start_with_windows(enabled))
        .await
        .map_err(|_| "Windows startup update did not complete".to_string())??;
    let mut settings = state.settings.write().await;
    settings.start_with_windows = enabled;
    db::save_settings(&state.data_dir, &settings);
    Ok(())
}

/// Re-read the real Windows Run entry whenever the local Settings center
/// opens. A moved/uninstalled executable cannot be represented truthfully by
/// the JSON preference alone, so keep the saved checkbox synchronized with the
/// actual registration while preserving an actionable repair status.
pub async fn reconcile_start_with_windows_preference(state: &AppState) -> StartupRegistration {
    let registration = tokio::task::spawn_blocking(startup::inspect_startup_registration)
        .await
        .unwrap_or_else(|_| StartupRegistration {
            supported: cfg!(windows),
            registered: false,
            state: "unavailable".into(),
            message: "Windows startup reconciliation did not complete.".into(),
            actual_command: None,
            expected_command: None,
            repair_available: false,
        });
    if registration.supported && registration.state != "unavailable" {
        let mut settings = state.settings.write().await;
        if settings.start_with_windows != registration.registered {
            settings.start_with_windows = registration.registered;
            db::save_settings(&state.data_dir, &settings);
        }
    }
    registration
}

/// Shared application state passed to every Axum route handler.
#[derive(Clone)]
pub struct AppState {
    /// Product identity is intentionally stateful rather than inferred from
    /// the executable name. It powers capability responses and guards
    /// local-only operations when a browser arrives over Tailnet.
    pub edition: Edition,
    pub install_scope: InstallScope,
    /// Stable across restarts of one data directory; safe to expose for a
    /// Viewer to recognize a saved host, unlike its filesystem location.
    pub instance_id: String,
    /// Must outlive the SQLite pool and every worker. Its Drop implementation
    /// releases the lock only after the final cloned AppState disappears.
    pub data_lock: Arc<DataDirectoryLock>,
    /// Serialized local-only work such as database backup and recovery.
    pub maintenance: Arc<maintenance::MaintenanceController>,
    pub pool: Pool<SqliteConnectionManager>,
    /// Cached group-id → effective tag set. None = dirty, rebuild on next read.
    pub group_tag_cache: GroupTagCache,
    pub shutdown: tokio_util::sync::CancellationToken,
    pub download_tasks: tokio_util::task::TaskTracker,
    /// Listener tasks are intentionally separate from downloads so a desktop
    /// window can disappear without affecting the shared HTTP server.
    pub server_tasks: tokio_util::task::TaskTracker,
    pub source_cancellations: Arc<Mutex<HashMap<i64, tokio_util::sync::CancellationToken>>>,
    pub running_sources: Arc<Mutex<HashSet<i64>>>,
    pub downloads_paused: Arc<AtomicBool>,
    /// Serializes pause/resume transitions so repeated controls are idempotent.
    pub download_control: Arc<Mutex<()>>,
    /// source_id → PID of the running gallery-dl process.
    pub active_processes: Arc<Mutex<HashMap<i64, u32>>>,
    pub paused_source_ids: Arc<Mutex<HashSet<i64>>>,
    /// A sync that crossed a quota or the free-space reserve records its
    /// reason here until its owning task persists the final source status.
    pub storage_pauses: Arc<Mutex<HashMap<i64, storage::SyncPause>>>,
    /// Swapped out when max_concurrent changes (same semantics as Python's approach).
    pub download_semaphore: Arc<Mutex<Arc<Semaphore>>>,
    /// Limits concurrent populate_placeholder scans to 3, independent of real downloads.
    pub placeholder_semaphore: Arc<Semaphore>,
    /// Provider-wide cooldowns complement persisted source retries. They are
    /// process-local by design; source-level retry timestamps survive restart.
    pub download_cooldowns: Arc<Mutex<HashMap<String, Instant>>>,
    /// Tracks the one process-owned HTTP listener used by the desktop shell,
    /// loopback browser fallback, and explicitly bound Tailscale addresses.
    pub remote_server: Arc<remote::ServerStatus>,
    /// A small cross-mode history makes a freshly randomized session avoid
    /// picking the item that just played. It is intentionally ephemeral: media
    /// is not private activity telemetry and a restart begins a new session.
    pub playback_history: Arc<Mutex<VecDeque<i64>>>,
    /// The authoritative, serialized command boundary for an active native
    /// or remote session. UI clients only receive snapshots and effects.
    pub sessions: session::SessionService,
    pub settings: Arc<RwLock<db::Settings>>,
    /// Live, UI-visible progress for the cheap post-startup size backfill.
    /// This is not migration state: an interrupted run safely restarts from
    /// rows that still have NULL file_size_bytes.
    pub size_backfill: Arc<RwLock<media_files::SizeBackfillProgress>>,
    /// Version-aware discovery registry built once at process startup.
    pub search_registry: Arc<routes::search::ProviderRegistry>,
    pub data_dir: PathBuf,
    pub library_dir: PathBuf,
    pub archives_dir: PathBuf,
    pub thumbs_dir: PathBuf,
    pub log_path: PathBuf,
    /// Directory `static/` assets (index.html, oobe.html, app.js, ...) are
    /// served from — kept on state (rather than only a local in `main`) so
    /// the root-gate handler in `routes::oobe::serve_root` can pick between
    /// `index.html` and `oobe.html` without extra plumbing.
    pub static_dir: PathBuf,
    pub gallery_dl_bin: String,
    pub ffprobe_bin: String,
    pub ffmpeg_bin: String,
    /// Only otherwise used to spawn the NSFW worker at startup (see
    /// `nsfw::NsfwClassifier::spawn`) — kept on `AppState` as well so the
    /// OOBE dependency check can probe the *currently configured*
    /// interpreter on demand without re-deriving it from `config.json`.
    pub python_bin: String,
    /// None if NSFW auto-rating is off or its worker never started.
    pub nsfw: Option<nsfw::NsfwClassifier>,
    /// Optional P-HAR worker, independently supervised from NudeNet.
    pub action_classifier: Option<nsfw::ActionClassifier>,
    /// Deprecated compatibility field. Arbitrary action-model paths are no
    /// longer launched; managed P-HAR state lives beneath `data_dir/phar`.
    pub action_model_path: Option<String>,
}

fn setup_logging(log_path: &std::path::Path) {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let file_appender = tracing_appender::rolling::never(
        log_path.parent().unwrap_or(std::path::Path::new(".")),
        log_path
            .file_name()
            .unwrap_or(std::ffi::OsStr::new("curator.log")),
    );
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    // Keep _guard alive for the process lifetime
    std::mem::forget(_guard);

    // Capture diagnostic detail by default in both stdout and curator.log.
    // An explicit RUST_LOG remains authoritative for users who need a quieter
    // filter or narrower module selection.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("debug"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(std::io::stdout))
        .with(fmt::layer().with_writer(non_blocking).with_ansi(false))
        .try_init()
        .ok();
}

fn load_or_create_instance_id(data_dir: &std::path::Path) -> Result<String> {
    let path = data_dir.join("instance.json");
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(id) = value.get("instance_id").and_then(serde_json::Value::as_str) {
                if !id.trim().is_empty() {
                    return Ok(id.to_string());
                }
            }
        }
    }

    let mut bytes = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut bytes);
    let instance_id = hex::encode(bytes);
    let text = serde_json::to_string_pretty(&serde_json::json!({
        "instance_id": instance_id,
        "created_by_version": PRODUCT_VERSION,
    }))?;
    std::fs::write(&path, text)
        .with_context(|| format!("writing Curator instance identity {}", path.display()))?;
    Ok(instance_id)
}

// ─── main ─────────────────────────────────────────────────────────────────────

pub async fn initialize() -> Result<AppState> {
    initialize_with_options(InitializeOptions::server()).await
}

/// Host uses the same backend as Server but has the native bridge and local
/// integrations. Viewer deliberately never calls this function.
pub async fn initialize_host() -> Result<AppState> {
    initialize_with_options(InitializeOptions::host()).await
}

pub async fn initialize_with_options(options: InitializeOptions) -> Result<AppState> {
    anyhow::ensure!(
        options.edition.owns_library(),
        "Curator Viewer must not initialize a database or HTTP server"
    );
    let cfg = config::load_config_for(options.install_scope);
    let requested_data_dir = config::resolve_data_dir_for(
        &cfg,
        options.install_scope,
        options.data_dir_override.as_deref(),
    );
    std::fs::create_dir_all(&requested_data_dir)?;
    let data_dir = dunce::canonicalize(&requested_data_dir)?;
    let data_lock = Arc::new(DataDirectoryLock::acquire(&data_dir)?);
    maintenance::apply_pending_restart(&data_dir, options.install_scope)?;
    let library_dir = data_dir.join("library");
    let archives_dir = data_dir.join("archives");
    let thumbs_dir = data_dir.join("thumbnails");
    let log_path = data_dir.join("curator.log");

    // Ensure directories exist
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&library_dir)?;
    std::fs::create_dir_all(&archives_dir)?;
    std::fs::create_dir_all(&thumbs_dir)?;
    std::fs::create_dir_all(data_dir.join("webview"))?;

    setup_logging(&log_path);

    // Persist the resolved data_dir so future runs find the same place
    config::ensure_config_json_for(options.install_scope, &data_dir);
    let instance_id = load_or_create_instance_id(&data_dir)?;

    // gallery-dl binary (PATH default or config override)
    let gallery_dl_bin = cfg
        .gallery_dl_bin
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "gallery-dl".to_string());

    let python_bin = cfg.python_bin.filter(|s| !s.is_empty()).unwrap_or_else(|| {
        if cfg!(windows) {
            "python".to_string()
        } else {
            "python3".to_string()
        }
    });

    let ffprobe_bin = cfg
        .ffprobe_bin
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "ffprobe".to_string());
    let ffmpeg_bin = cfg
        .ffmpeg_bin
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "ffmpeg".to_string());
    let legacy_action_model_path = cfg
        .action_model_path
        .clone()
        .filter(|s| !s.trim().is_empty());
    if legacy_action_model_path.is_some() {
        warn!(
            "Ignoring legacy action_model_path; configure managed P-HAR from Local Admin instead"
        );
    }

    // Database pool + migrations
    let pool = db::init_pool(&data_dir).map_err(|e| {
        error!(
            "FATAL: could not set up database at {:?}: {}",
            data_dir.join("data.db"),
            e
        );
        e
    })?;

    // Settings (loaded from settings.json with DEFAULT_SETTINGS fallback)
    let mut settings = db::load_settings(&data_dir);
    // Restart-required notices are durable while a process is still running,
    // but this fresh process is already using the changed bootstrap/worker
    // configuration. Clear the acknowledgement point now rather than leaving
    // a stale warning forever after a successful restart.
    if settings.ffmpeg_restart_required || settings.nsfw_restart_required {
        settings.ffmpeg_restart_required = false;
        settings.nsfw_restart_required = false;
        db::save_settings(&data_dir, &settings);
    }
    let max_concurrent = settings.max_concurrent as usize;
    // Construct the maintenance coordinator before any startup workers.  The
    // workers receive clones of this exact controller, so a maintenance job
    // can close the admission gate before it waits for their in-flight writes.
    let maintenance = Arc::new(maintenance::MaintenanceController::new());

    // Second half of the OOBE self-heal (the first half lives in
    // db::load_settings, for installations that already had a
    // settings.json): a real, pre-OOBE installation whose settings.json
    // happened to never be written at all (every Settings field has a
    // default, so plenty of installs never trigger a save) would otherwise
    // still look "fresh" and get shown the first-run wizard. If the
    // database already has real sources/groups/media, treat setup as
    // already complete instead.
    if !settings.oobe_completed {
        if let Ok(conn) = pool.get() {
            if oobe::existing_installation_has_data(&conn) {
                settings.oobe_completed = true;
                db::save_settings(&data_dir, &settings);
            }
        }
    }

    // A crashed downloader is resumable, not still running on the next startup.
    pool.get()?.execute(
        "UPDATE sources SET status='paused' WHERE status='downloading'",
        [],
    )?;

    // NSFW auto-rating (opt-in — see nsfw.rs). Always refresh the
    // embedded worker script on disk so it matches this build, even if the
    // feature is currently off; that way turning it on later doesn't need
    // a fresh copy of the exe.
    let nsfw_worker_path = data_dir.join("nsfw_worker.py");
    if let Err(e) = std::fs::write(&nsfw_worker_path, NSFW_WORKER_PY) {
        warn!(
            "Could not write nsfw_worker.py to {:?}: {}",
            nsfw_worker_path, e
        );
    }
    let action_worker_path = data_dir.join("action_worker.py");
    if let Err(e) = std::fs::write(&action_worker_path, ACTION_WORKER_PY) {
        warn!(
            "Could not write action_worker.py to {:?}: {}",
            action_worker_path, e
        );
    }
    let nsfw_classifier = if settings.nsfw_filter_enabled {
        info!("NSFW auto-rating enabled — starting classifier worker");
        Some(nsfw::NsfwClassifier::spawn(
            python_bin.clone(),
            nsfw_worker_path,
        ))
    } else {
        None
    };
    let phar_status = match phar::resume_requested_setup(&data_dir, options.install_scope) {
        Ok(status) => status,
        Err(error) => {
            warn!("P-HAR managed setup could not be resumed: {error}");
            phar::status(&data_dir, options.install_scope)
        }
    };
    let action_classifier = if settings.nsfw_filter_enabled && phar_status.ready {
        if let Some(managed_python) = phar::managed_python(&data_dir) {
            info!("P-HAR managed environment passed validation — starting optional action worker");
            Some(nsfw::ActionClassifier::spawn(
                managed_python.to_string_lossy().into_owned(),
                action_worker_path,
                phar::environment_dir(&data_dir),
            ))
        } else {
            warn!("P-HAR ready marker lost its managed interpreter; action worker will not start");
            None
        }
    } else {
        None
    };
    if let Some(ref classifier) = nsfw_classifier {
        nsfw::spawn_backfill_loop(
            pool.clone(),
            classifier.clone(),
            action_classifier.clone(),
            library_dir.clone(),
            ffmpeg_bin.clone(),
            settings.max_clip_length_secs,
            Arc::clone(&maintenance),
        );
    }

    // Video duration backfill (for the clips/videos split — see
    // duration.rs). Checked once, here, rather than letting the loop
    // discover ffprobe is missing on every single video.
    if tokio::task::spawn_blocking({
        let bin = ffprobe_bin.clone();
        move || duration::ffprobe_available(&bin)
    })
    .await?
    {
        duration::spawn_backfill_loop(
            pool.clone(),
            ffprobe_bin.clone(),
            library_dir.clone(),
            Arc::clone(&maintenance),
        );
    } else {
        info!("ffprobe not found (\"{}\") — video duration (clips/videos split) won't be backfilled for existing videos; newly-downloaded ones are unaffected once ffprobe is available", ffprobe_bin);
    }

    // Static directories
    // Release bundles place `static/` beside the executable, while `cargo
    // run` keeps it at the workspace root. Prefer the bundled copy but fall
    // back to the working-tree asset directory so development and packaged
    // builds serve the same shell instead of silently returning 404s.
    let bundled_static = std::env::current_exe()
        .ok()
        .and_then(|path| path.parent().map(|dir| dir.join("static")))
        .filter(|path| path.is_dir());
    let static_dir = bundled_static
        .or_else(|| {
            std::env::current_dir()
                .ok()
                .map(|path| path.join("static"))
                .filter(|path| path.is_dir())
        })
        .unwrap_or_else(|| PathBuf::from("static"));
    let registry_bin = gallery_dl_bin.clone();
    let search_registry = Arc::new(
        tokio::task::spawn_blocking(move || routes::search::build_provider_registry(&registry_bin))
            .await
            .unwrap_or_else(|_| routes::search::default_provider_registry()),
    );

    let state = AppState {
        edition: options.edition,
        install_scope: options.install_scope,
        instance_id,
        data_lock,
        maintenance,
        pool,
        group_tag_cache: Arc::new(RwLock::new(None)),
        shutdown: tokio_util::sync::CancellationToken::new(),
        download_tasks: tokio_util::task::TaskTracker::new(),
        server_tasks: tokio_util::task::TaskTracker::new(),
        source_cancellations: Arc::new(Mutex::new(HashMap::new())),
        running_sources: Arc::new(Mutex::new(HashSet::new())),
        downloads_paused: Arc::new(AtomicBool::new(false)),
        download_control: Arc::new(Mutex::new(())),
        active_processes: Arc::new(Mutex::new(HashMap::new())),
        paused_source_ids: Arc::new(Mutex::new(HashSet::new())),
        storage_pauses: Arc::new(Mutex::new(HashMap::new())),
        download_semaphore: Arc::new(Mutex::new(Arc::new(Semaphore::new(max_concurrent)))),
        placeholder_semaphore: Arc::new(Semaphore::new(3)),
        download_cooldowns: Arc::new(Mutex::new(HashMap::new())),
        remote_server: Arc::new(remote::ServerStatus::new(remote::DEFAULT_SERVER_PORT)),
        playback_history: Arc::new(Mutex::new(VecDeque::with_capacity(24))),
        sessions: session::SessionService::default(),
        settings: Arc::new(RwLock::new(settings)),
        size_backfill: Arc::new(RwLock::new(media_files::SizeBackfillProgress::default())),
        search_registry,
        data_dir: data_dir.clone(),
        library_dir: library_dir.clone(),
        archives_dir,
        thumbs_dir,
        log_path,
        static_dir: static_dir.clone(),
        gallery_dl_bin,
        ffprobe_bin,
        ffmpeg_bin,
        python_bin,
        nsfw: nsfw_classifier,
        action_classifier,
        action_model_path: None,
    };

    spawn_session_summary_persistence(&state);

    // File size migration is schema-only. Reading metadata for legacy rows is
    // deferred to a bounded background job so startup stays responsive and
    // the Explorer can show real progress rather than a blocked window.
    let size_backfill_state = state.clone();
    state.download_tasks.spawn(async move {
        media_files::backfill_missing_file_sizes(
            size_backfill_state.pool.clone(),
            size_backfill_state.library_dir.clone(),
            size_backfill_state.shutdown.clone(),
            size_backfill_state.size_backfill.clone(),
            Arc::clone(&size_backfill_state.maintenance),
        )
        .await;
    });

    // A disabled cleanup policy costs only this cancellable timer. Once a
    // person explicitly enables one, cleanup remains conservative and is
    // serialized against local Admin jobs by the helper itself.
    let cleanup_state = Arc::new(state.clone());
    state
        .download_tasks
        .spawn(storage::automatic_cleanup_loop(cleanup_state));

    // Recover completed files whose final event was lost before a crash.
    let startup_state = state.clone();
    state.download_tasks.spawn(async move {
        // Startup recovery indexes files and writes durable metadata.  If a
        // local Admin job begins first, wait until it has released its
        // exclusive window rather than racing its snapshot or transaction.
        let _recovery_lease = loop {
            if startup_state.shutdown.is_cancelled() {
                return;
            }
            if let Some(lease) = startup_state.maintenance.try_acquire_background_worker() {
                break lease;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        };
        let result = tokio::task::spawn_blocking(move || -> Result<()> {
            media_files::reconcile_cancellable(
                &startup_state.pool,
                &startup_state.library_dir,
                Some(&startup_state.shutdown),
            )?;
            let rows = {
                let conn = startup_state.pool.get()?;
                let mut stmt = conn.prepare("SELECT id,slug FROM sources")?;
                let rows = stmt
                    .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                rows
            };
            for (id, slug) in rows {
                if startup_state.shutdown.is_cancelled() {
                    break;
                }
                let dest = startup_state.library_dir.join(slug);
                if dest.is_dir() {
                    if let Err(e) = downloader::scan_and_index(&startup_state, id, &dest) {
                        warn!("Startup index recovery for source {id}: {e}");
                    }
                }
            }
            Ok(())
        })
        .await;
        match result {
            Ok(Ok(())) => {}
            other => warn!("Startup metadata recovery: {other:?}"),
        }
    });

    Ok(state)
}

pub fn router(state: AppState) -> axum::Router {
    let router = routes::build_router(Arc::new(state.clone()))
        .layer(CompressionLayer::new())
        .nest(
            "/library",
            axum::Router::new()
                .fallback_service(ServeDir::new(&state.library_dir))
                .layer(axum::middleware::from_fn_with_state(
                    Arc::new(state.clone()),
                    routes::library::reconcile_not_found,
                )),
        );
    if state.edition == edition::Edition::Server {
        router.fallback_service(
            ServeDir::new(&state.static_dir).append_index_html_on_directories(true),
        )
    } else {
        router
    }
}

pub async fn shutdown(state: &AppState) {
    // Stamp and settle the active interval before cancelling observers. The
    // insert is idempotent, so it safely races the normal terminal observer.
    if let Some(update) = state.sessions.interrupt_active() {
        if update
            .effects
            .iter()
            .any(|effect| matches!(effect, session::SessionEffect::EndSession { .. }))
        {
            tracing::info!(session_id = %update.state.session_id, "Marked active session interrupted during shutdown");
        }
        if let Err(error) = persist_terminal_update(state, &update).await {
            tracing::warn!(session_id = %update.state.session_id, %error, "Could not persist interrupted session summary; the terminal snapshot remains available for retry");
        }
    }
    state.sessions.stop_runner();
    state.shutdown.cancel();
    if let Some(worker) = &state.nsfw {
        worker.shutdown().await;
    }
    if let Some(worker) = &state.action_classifier {
        worker.shutdown().await;
    }
    state.download_tasks.close();
    state.server_tasks.close();
    state.download_tasks.wait().await;
    state.server_tasks.wait().await;
}

/// Terminal persistence is driven by the engine's first EndSession effect,
/// not by the transport that happened to carry a terminal command. Thus an
/// unattended automatic completion is durable just like a remote cancel.
pub fn spawn_session_summary_persistence(state: &AppState) {
    let mut updates = state.sessions.subscribe();
    let persistence_state = state.clone();
    state.download_tasks.spawn(async move {
        loop {
            let update = tokio::select! {
                _ = persistence_state.shutdown.cancelled() => return,
                update = updates.recv() => match update {
                    Ok(update) => update,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                        tracing::warn!(skipped, "Session update subscriber lagged; recovering retained terminal snapshot if needed");
                        let Some(snapshot) = persistence_state.sessions.snapshot() else {
                            continue;
                        };
                        if !matches!(snapshot.status, session::SessionStatus::Completed | session::SessionStatus::Cancelled | session::SessionStatus::Interrupted) {
                            continue;
                        }
                        session::SessionUpdate {
                            effects: vec![session::SessionEffect::EndSession { status: snapshot.status.clone() }],
                            state: snapshot,
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                },
            };
            if !update
                .effects
                .iter()
                .any(|effect| matches!(effect, session::SessionEffect::EndSession { .. }))
            {
                continue;
            }
            loop {
                match persist_terminal_update(&persistence_state, &update).await {
                    Ok(_) => break,
                    Err(error) => {
                        tracing::warn!(session_id = %update.state.session_id, %error, "Could not persist terminal session summary; retrying locally");
                        tokio::select! {
                            _ = persistence_state.shutdown.cancelled() => return,
                            _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
                        }
                    }
                }
            }
        }
    });
}

/// Apply the one application-level persistence rule: only the update that
/// carries the engine's first terminal transition may request a summary
/// insert. The database key remains the second line of defense against an
/// observer/shutdown race or a local retry.
async fn persist_terminal_update(
    state: &AppState,
    update: &session::SessionUpdate,
) -> Result<bool> {
    if !update
        .effects
        .iter()
        .any(|effect| matches!(effect, session::SessionEffect::EndSession { .. }))
    {
        return Ok(false);
    }
    persist_session_summary(state, &update.state).await?;
    Ok(true)
}

/// Persist an immutable terminal outcome from the Rust session engine. Native
/// and remote adapters share this operation, so interruption records remain
/// local and inspectable even when the desktop window disappears.
pub async fn persist_session_summary(
    state: &AppState,
    summary: &session::SessionState,
) -> Result<()> {
    let pool = state.pool.clone();
    let summary = summary.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = pool.get()?;
        let transaction = conn.transaction()?;
        let detail = serde_json::json!({
            "session_id": summary.session_id,
            "seed": summary.seed,
            "active_elapsed_ms": summary.active_elapsed_ms,
            "paused_elapsed_ms": summary.paused_elapsed_ms,
            "phase": summary.phase,
            "statistics": summary.statistics,
            "tempo": summary.tempo,
        });
        transaction.execute(
            "INSERT INTO interactive_sessions(session_id,started_at,duration_s,item_count,plan,events,ended_state,timing_corrections)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(session_id) WHERE session_id IS NOT NULL DO NOTHING",
            rusqlite::params![
                summary.session_id,
                db::now_iso(),
                i64::try_from(summary.active_elapsed_ms / 1_000).unwrap_or(i64::MAX),
                i64::try_from(summary.event_history.len()).unwrap_or(i64::MAX),
                "native-session-v1",
                serde_json::to_string(&summary.event_history)?,
                serde_json::to_string(&summary.status)?.trim_matches('"'),
                detail.to_string(),
            ],
        )?;
        transaction.commit()?;
        Ok(())
    })
    .await??;
    Ok(())
}

impl AppState {
    /// Remember only a bounded recent window. Callers use the last entry to
    /// avoid an immediate duplicate while retaining enough variety for feeds.
    pub async fn remember_playback(&self, media_id: i64) {
        const HISTORY_LIMIT: usize = 24;
        let mut history = self.playback_history.lock().await;
        if history.back().copied() == Some(media_id) {
            return;
        }
        history.push_back(media_id);
        while history.len() > HISTORY_LIMIT {
            history.pop_front();
        }
    }
}

pub async fn library_summary(state: &AppState) -> Result<serde_json::Value> {
    let pool = state.pool.clone();
    let mut summary =
        tokio::task::spawn_blocking(move || hierarchy::summary(&*pool.get()?)).await??;
    summary["size_backfill"] = serde_json::to_value(state.size_backfill.read().await.clone())?;
    Ok(summary)
}

pub fn media_path(state: &AppState, id: i64) -> Result<PathBuf> {
    let relative: String = state.pool.get()?.query_row(
        "SELECT CASE WHEN m.clip_start_secs IS NOT NULL THEN (SELECT filepath FROM media parent WHERE parent.id=m.clip_parent_id AND parent.missing=0) ELSE m.filepath END FROM media m WHERE id=?1 AND downloaded=1 AND missing=0",
        [id],
        |r| r.get(0),
    )?;
    let root = dunce::canonicalize(&state.library_dir)?;
    let path = dunce::canonicalize(root.join(relative))?;
    anyhow::ensure!(path.starts_with(root), "File is outside the library");
    Ok(path)
}

#[cfg(test)]
mod session_persistence_tests {
    use super::*;

    #[tokio::test]
    async fn native_session_summary_insert_is_idempotent_by_session_id() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        let mut engine = session::SessionEngine::new(session::GameConfig::quick_default()).unwrap();
        let summary = engine
            .dispatch(session::SessionCommand::Start { monotonic_ms: 0 })
            .state;
        persist_session_summary(&state, &summary).await.unwrap();
        persist_session_summary(&state, &summary).await.unwrap();
        let count: i64 = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM interactive_sessions WHERE session_id=?1",
                [&summary.session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn terminal_snapshot_without_first_transition_effect_is_not_persisted() {
        let root = tempfile::tempdir().unwrap();
        let state = test_support::state(root.path());
        let mut engine = session::SessionEngine::new(session::GameConfig::quick_default()).unwrap();
        engine.dispatch(session::SessionCommand::Start { monotonic_ms: 0 });
        let terminal = engine.dispatch(session::SessionCommand::Interrupt { monotonic_ms: 1 });
        assert!(persist_terminal_update(&state, &terminal).await.unwrap());

        let replay = engine.dispatch(session::SessionCommand::Interrupt { monotonic_ms: 2 });
        assert!(replay.effects.is_empty());
        assert!(!persist_terminal_update(&state, &replay).await.unwrap());

        let count: i64 = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM interactive_sessions WHERE session_id=?1",
                [&replay.state.session_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}

//! Local, serialized recovery and maintenance work.
//!
//! These operations are deliberately independent of ordinary library routes:
//! each destructive action takes an online SQLite backup first, pauses active
//! download workers, and reports durable job state to the local Admin UI.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use rusqlite::{Connection, DatabaseName};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use zip::{write::FileOptions, ZipArchive, ZipWriter};

use crate::edition::InstallScope;
use crate::{db, media_files, AppState, PRODUCT_VERSION};

const BACKUP_FORMAT: &str = "curator-backup-v1";
const PENDING_RESTART: &str = "restart.pending.json";

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceKind {
    CreateBackup,
    ValidateBackup,
    RestoreBackup,
    ClearHumanRatings,
    ResetRatingsAndEvidence,
    FlattenGroups,
    DeleteGroups,
    ClearTagAssignments,
    ClearInteractiveHistory,
    RebuildCaches,
    ReconcileLibrary,
    FactoryReset,
    RemovePharEnvironment,
    RemoveArchives,
}

impl MaintenanceKind {
    pub const fn is_destructive(self) -> bool {
        !matches!(
            self,
            Self::CreateBackup | Self::ValidateBackup | Self::ReconcileLibrary
        )
    }

    pub const fn confirmation_phrase(self) -> Option<&'static str> {
        match self {
            Self::CreateBackup | Self::ValidateBackup | Self::ReconcileLibrary => None,
            Self::RestoreBackup => Some("RESTORE BACKUP"),
            Self::ClearHumanRatings => Some("CLEAR HUMAN RATINGS"),
            Self::ResetRatingsAndEvidence => Some("RESET RATINGS"),
            Self::FlattenGroups => Some("FLATTEN GROUPS"),
            Self::DeleteGroups => Some("DELETE GROUPS"),
            Self::ClearTagAssignments => Some("CLEAR TAG ASSIGNMENTS"),
            Self::ClearInteractiveHistory => Some("CLEAR SESSION HISTORY"),
            Self::RebuildCaches => Some("REBUILD CACHES"),
            Self::FactoryReset => Some("RESET CURATOR"),
            Self::RemovePharEnvironment => Some("DELETE P-HAR"),
            Self::RemoveArchives => Some("DELETE ARCHIVES"),
        }
    }

    pub const fn needs_restart(self) -> bool {
        matches!(self, Self::RestoreBackup | Self::FactoryReset)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct MaintenanceRequest {
    pub kind: MaintenanceKind,
    #[serde(default)]
    pub confirmation: String,
    #[serde(default)]
    pub backup_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenancePhase {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaintenanceJob {
    pub id: String,
    pub kind: MaintenanceKind,
    pub phase: MaintenancePhase,
    pub created_at: String,
    pub finished_at: Option<String>,
    pub message: String,
    pub error: Option<String>,
    pub backup_id: Option<String>,
    pub restart_required: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BackupRecord {
    pub id: String,
    pub size_bytes: u64,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackupManifest {
    format: String,
    created_at: String,
    version: String,
    instance_id: String,
    files: Vec<BackupFile>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BackupFile {
    name: String,
    size_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PendingRestart {
    operation: String,
}

#[derive(Debug)]
struct JobResult {
    message: String,
    backup_id: Option<String>,
    restart_required: bool,
}

pub struct MaintenanceController {
    serial: Arc<Mutex<()>>,
    active: AtomicBool,
    /// Background classifier and metadata workers hold a lease around their
    /// mutable unit of work. A maintenance job flips `active` first, then
    /// waits for these leases to drain before it snapshots or changes SQLite.
    background_workers: AtomicUsize,
    next_id: AtomicU64,
    jobs: RwLock<BTreeMap<String, MaintenanceJob>>,
}

/// A short-lived permission for a background worker to mutate library state.
/// Dropping it is sufficient even when an inference/task fails partway
/// through, so a failed worker cannot permanently block maintenance mode.
pub struct BackgroundWorkerLease {
    controller: Arc<MaintenanceController>,
}

impl Drop for BackgroundWorkerLease {
    fn drop(&mut self) {
        self.controller
            .background_workers
            .fetch_sub(1, Ordering::AcqRel);
    }
}

impl MaintenanceController {
    pub fn new() -> Self {
        Self {
            serial: Arc::new(Mutex::new(())),
            active: AtomicBool::new(false),
            background_workers: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            jobs: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// Acquire a worker lease only while maintenance is inactive. The second
    /// check closes the race where a job begins just after the first check;
    /// in that case the count is released before any database work begins.
    pub fn try_acquire_background_worker(self: &Arc<Self>) -> Option<BackgroundWorkerLease> {
        if self.is_active() {
            return None;
        }
        self.background_workers.fetch_add(1, Ordering::AcqRel);
        if self.is_active() {
            self.background_workers.fetch_sub(1, Ordering::AcqRel);
            return None;
        }
        Some(BackgroundWorkerLease {
            controller: Arc::clone(self),
        })
    }

    pub fn active_background_workers(&self) -> usize {
        self.background_workers.load(Ordering::Acquire)
    }

    pub async fn jobs(&self) -> Vec<MaintenanceJob> {
        self.jobs.read().await.values().cloned().collect()
    }

    pub async fn job(&self, id: &str) -> Option<MaintenanceJob> {
        self.jobs.read().await.get(id).cloned()
    }

    pub async fn start(
        self: &Arc<Self>,
        state: Arc<AppState>,
        request: MaintenanceRequest,
    ) -> std::result::Result<MaintenanceJob, String> {
        if let Some(phrase) = request.kind.confirmation_phrase() {
            if request.confirmation.trim() != phrase {
                return Err(format!("Type {phrase} to run this maintenance action."));
            }
        }
        if matches!(
            request.kind,
            MaintenanceKind::RestoreBackup | MaintenanceKind::ValidateBackup
        ) && request
            .backup_id
            .as_deref()
            .unwrap_or_default()
            .trim()
            .is_empty()
        {
            return Err("Choose a backup first.".into());
        }
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err("Another maintenance job is already running.".into());
        }

        let id = format!(
            "job-{}-{}",
            Utc::now().format("%Y%m%d%H%M%S"),
            self.next_id.fetch_add(1, Ordering::Relaxed)
        );
        let job = MaintenanceJob {
            id: id.clone(),
            kind: request.kind,
            phase: MaintenancePhase::Queued,
            created_at: db::now_iso(),
            finished_at: None,
            message: "Waiting for maintenance mode.".into(),
            error: None,
            backup_id: None,
            restart_required: false,
        };
        self.jobs.write().await.insert(id.clone(), job.clone());

        let controller = Arc::clone(self);
        let server_tasks = state.server_tasks.clone();
        server_tasks.spawn(async move {
            let _serial = controller.serial.lock().await;
            controller
                .set_running(&id, "Quiescing downloads and background workers.")
                .await;
            let was_paused = state.downloads_paused.load(Ordering::Acquire);
            let _ = crate::services::downloads::pause_for_maintenance(&state).await;

            let quiesced = wait_for_quiescence(&state).await;
            let result = if quiesced {
                controller
                    .set_running(&id, "Running maintenance transaction.")
                    .await;
                let state_for_work = Arc::clone(&state);
                let request_for_work = request.clone();
                tokio::task::spawn_blocking(move || {
                    run_blocking(&state_for_work, &request_for_work)
                })
                .await
                .map_err(|error| anyhow::anyhow!("Maintenance worker stopped: {error}"))
                .and_then(|result| result)
            } else {
                Err(anyhow::anyhow!(
                    "Timed out waiting for active download or background workers to stop; no changes were made."
                ))
            };

            // Group/tag and thumbnail views hold cheap in-process caches. A
            // maintenance transaction invalidates them before user traffic
            // can resume, regardless of whether the selected job touched it.
            *state.group_tag_cache.write().await = None;
            if matches!(request.kind, MaintenanceKind::ReconcileLibrary) {
                schedule_size_backfill(&state);
            }

            match result {
                Ok(result) => controller.succeed(&id, result).await,
                Err(error) => controller.fail(&id, error.to_string()).await,
            }
            // The backup/reset transaction and cache invalidation are now
            // complete. Reopen worker admission before requeueing downloads:
            // resumed tasks use the same lease gate and must not be discarded
            // merely because this controller is still tidying up.
            controller.active.store(false, Ordering::Release);
            if !was_paused {
                let _ =
                    crate::services::downloads::resume_after_maintenance(Arc::clone(&state)).await;
            }
        });
        Ok(job)
    }

    async fn set_running(&self, id: &str, message: &str) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.phase = MaintenancePhase::Running;
            job.message = message.into();
        }
    }

    async fn succeed(&self, id: &str, result: JobResult) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.phase = MaintenancePhase::Succeeded;
            job.message = result.message;
            job.backup_id = result.backup_id;
            job.restart_required = result.restart_required;
            job.finished_at = Some(db::now_iso());
        }
    }

    async fn fail(&self, id: &str, error: String) {
        if let Some(job) = self.jobs.write().await.get_mut(id) {
            job.phase = MaintenancePhase::Failed;
            job.message =
                "Maintenance did not complete; transactional changes were rolled back.".into();
            job.error = Some(error);
            job.finished_at = Some(db::now_iso());
        }
    }
}

impl Default for MaintenanceController {
    fn default() -> Self {
        Self::new()
    }
}

async fn wait_for_quiescence(state: &AppState) -> bool {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if state.running_sources.lock().await.is_empty()
            && state.maintenance.active_background_workers() == 0
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(75)).await;
    }
}

fn schedule_size_backfill(state: &Arc<AppState>) {
    let state_for_task = Arc::clone(state);
    state.download_tasks.spawn(async move {
        media_files::backfill_missing_file_sizes(
            state_for_task.pool.clone(),
            state_for_task.library_dir.clone(),
            state_for_task.shutdown.clone(),
            state_for_task.size_backfill.clone(),
            Arc::clone(&state_for_task.maintenance),
        )
        .await;
    });
}

fn run_blocking(state: &AppState, request: &MaintenanceRequest) -> Result<JobResult> {
    let backup_id = if request.kind.is_destructive() {
        Some(create_backup(state)?)
    } else {
        None
    };
    let message = match request.kind {
        MaintenanceKind::CreateBackup => {
            let id = create_backup(state)?;
            return Ok(JobResult {
                message: format!("Created backup {id}."),
                backup_id: Some(id),
                restart_required: false,
            });
        }
        MaintenanceKind::ValidateBackup => {
            validate_backup(&state.data_dir, required_backup_id(request)?)?;
            "Backup is valid.".into()
        }
        MaintenanceKind::RestoreBackup => {
            stage_restore(state, required_backup_id(request)?)?;
            "Restore is staged and will apply when Curator restarts.".into()
        }
        MaintenanceKind::ClearHumanRatings => {
            transactional(&state.pool, |tx| {
                tx.execute_batch(
                    "UPDATE media SET
                        human_rating=NULL,
                        rating=CASE WHEN auto_rating BETWEEN 1 AND 5 THEN auto_rating ELSE 0 END,
                        rating_source=CASE WHEN auto_rating BETWEEN 1 AND 5 THEN 'auto' ELSE 'none' END,
                        rating_reviewed=0,
                        rating_reviewed_at=NULL
                      WHERE human_rating IS NOT NULL OR rating_source='human' OR rating_reviewed=1;",
                )?;
                Ok(())
            })?;
            "Cleared human rating overrides.".into()
        }
        MaintenanceKind::ResetRatingsAndEvidence => {
            transactional(&state.pool, |tx| {
                tx.execute_batch(
                    "UPDATE media SET
                        rating=0, auto_rating=0, human_rating=NULL, rating_source='none',
                        rating_reviewed=0, rating_reviewed_at=NULL,
                        classifier_model=NULL, classifier_version=NULL, classifier_score=NULL,
                        classifier_evidence=NULL, action_model=NULL, action_model_version=NULL,
                        action_score=NULL, action_evidence=NULL, action_rating=0,
                        classification_label='unclassified', manual_review_required=0,
                        manual_review_reason=NULL, classification_updated_at=NULL,
                        nsfw_attempts=0, nsfw_retry_at=0,
                        nsfw_state=CASE
                            WHEN downloaded=1 AND missing=0 AND type='image' THEN 'pending'
                            ELSE 'manual'
                        END;",
                )?;
                Ok(())
            })?;
            "Reset ratings and classifier evidence; eligible images are queued for analysis.".into()
        }
        MaintenanceKind::FlattenGroups => {
            transactional(&state.pool, |tx| {
                tx.execute(
                    "UPDATE groups SET parent_id=NULL WHERE parent_id IS NOT NULL",
                    [],
                )?;
                Ok(())
            })?;
            "Flattened group nesting while retaining all assignments.".into()
        }
        MaintenanceKind::DeleteGroups => {
            transactional(&state.pool, |tx| {
                tx.execute_batch(
                    "DELETE FROM media_groups;
                     UPDATE sources SET group_id=NULL;
                     DELETE FROM group_tags;
                     DELETE FROM groups;",
                )?;
                Ok(())
            })?;
            "Deleted group structure and assignments; sources, media, and tags remain.".into()
        }
        MaintenanceKind::ClearTagAssignments => {
            transactional(&state.pool, |tx| {
                tx.execute_batch(
                    "DELETE FROM media_tags;
                     DELETE FROM media_tag_provenance;
                     DELETE FROM group_tags;
                     DELETE FROM source_tags;
                     DELETE FROM source_tag_rules;",
                )?;
                Ok(())
            })?;
            "Cleared tag assignments and source-tag rules; tag names remain.".into()
        }
        MaintenanceKind::ClearInteractiveHistory => {
            transactional(&state.pool, |tx| {
                tx.execute_batch("DELETE FROM ch_sessions; DELETE FROM interactive_sessions;")?;
                Ok(())
            })?;
            "Cleared interactive-session history.".into()
        }
        MaintenanceKind::RebuildCaches => {
            rebuild_caches(state)?;
            "Cleared thumbnail and derived cache files; they will be rebuilt on demand.".into()
        }
        MaintenanceKind::ReconcileLibrary => {
            let changed = media_files::reconcile(&state.pool, &state.library_dir)?;
            format!(
                "Reconciled library metadata ({changed} rows changed) and restarted size backfill."
            )
        }
        MaintenanceKind::FactoryReset => {
            stage_factory_reset(state)?;
            "Factory reset is staged and will apply when Curator restarts; media, archives, P-HAR, and backups are preserved.".into()
        }
        MaintenanceKind::RemovePharEnvironment => {
            crate::phar::disable(&state.data_dir, state.install_scope)?;
            remove_preserved_directory(&state.data_dir.join("phar"), &state.data_dir)?;
            "Removed the Curator-managed P-HAR environment and disabled its setup request.".into()
        }
        MaintenanceKind::RemoveArchives => {
            remove_preserved_directory(&state.archives_dir, &state.data_dir)?;
            std::fs::create_dir_all(&state.archives_dir)?;
            "Removed downloaded archives.".into()
        }
    };

    Ok(JobResult {
        message: match backup_id.as_deref() {
            Some(id) => format!("{message} Safety backup: {id}."),
            None => message,
        },
        backup_id,
        restart_required: request.kind.needs_restart(),
    })
}

fn required_backup_id(request: &MaintenanceRequest) -> Result<&str> {
    request
        .backup_id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| anyhow::anyhow!("A backup id is required."))
}

fn transactional<T>(
    pool: &db::DbPool,
    body: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T>,
) -> Result<T> {
    let conn = pool.get()?;
    let tx = conn.unchecked_transaction()?;
    let output = body(&tx)?;
    tx.commit()?;
    Ok(output)
}

fn backups_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("backups")
}

fn safe_backup_path(data_dir: &Path, id: &str) -> Result<PathBuf> {
    if id.is_empty()
        || !id.ends_with(".zip")
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("Invalid backup id.");
    }
    let path = backups_dir(data_dir).join(id);
    if !path.is_file() {
        bail!("Backup not found.");
    }
    Ok(path)
}

pub fn list_backups(data_dir: &Path) -> Result<Vec<BackupRecord>> {
    let dir = backups_dir(data_dir);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut backups = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let path = entry.path();
            let id = path.file_name()?.to_str()?.to_string();
            (id.ends_with(".zip") && path.is_file()).then_some((id, path))
        })
        .filter_map(|(id, path)| {
            let metadata = std::fs::metadata(&path).ok()?;
            let created_at = metadata
                .modified()
                .ok()
                .map(|time| chrono::DateTime::<Utc>::from(time).to_rfc3339())
                .unwrap_or_default();
            Some(BackupRecord {
                id,
                size_bytes: metadata.len(),
                created_at,
            })
        })
        .collect::<Vec<_>>();
    backups.sort_by(|left, right| right.id.cmp(&left.id));
    Ok(backups)
}

pub fn backup_file(data_dir: &Path, id: &str) -> Result<PathBuf> {
    safe_backup_path(data_dir, id)
}

fn create_backup(state: &AppState) -> Result<String> {
    let directory = backups_dir(&state.data_dir);
    std::fs::create_dir_all(&directory)?;
    let id = format!(
        "curator-backup-{}-{:08x}.zip",
        Utc::now().format("%Y%m%dT%H%M%SZ"),
        rand::random::<u32>()
    );
    let staging = tempfile::tempdir_in(&directory)?;
    let snapshot = staging.path().join("data.db");
    {
        let source = state.pool.get()?;
        // The SQLite online-backup API obtains a consistent snapshot even in
        // WAL mode; it does not copy a loose db/-wal pair.
        source.backup(DatabaseName::Main, &snapshot, None)?;
    }
    let settings = db::settings_path(&state.data_dir);
    let config = crate::config::config_path_for(state.install_scope);
    let mut files = vec![snapshot.clone()];
    if settings.is_file() {
        files.push(settings);
    }
    if config.is_file() {
        files.push(config);
    }
    let manifest = BackupManifest {
        format: BACKUP_FORMAT.into(),
        created_at: db::now_iso(),
        version: PRODUCT_VERSION.into(),
        instance_id: state.instance_id.clone(),
        files: files
            .iter()
            .filter_map(|path| {
                let name = if path == &snapshot {
                    "data.db".to_string()
                } else if path.file_name().is_some_and(|name| name == "settings.json") {
                    "settings.json".to_string()
                } else {
                    "config.json".to_string()
                };
                std::fs::metadata(path).ok().map(|metadata| BackupFile {
                    name,
                    size_bytes: metadata.len(),
                })
            })
            .collect(),
    };
    let temporary = directory.join(format!(".{id}.tmp"));
    let mut zip = ZipWriter::new(File::create(&temporary)?);
    let options: FileOptions<()> = FileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated)
        .compression_level(Some(6));
    zip.start_file("manifest.json", options)?;
    zip.write_all(serde_json::to_string_pretty(&manifest)?.as_bytes())?;
    for path in &files {
        let name = if path == &snapshot {
            "data.db"
        } else if path.file_name().is_some_and(|name| name == "settings.json") {
            "settings.json"
        } else {
            "config.json"
        };
        zip.start_file(name, options)?;
        let mut source = File::open(path)?;
        std::io::copy(&mut source, &mut zip)?;
    }
    zip.finish()?;
    std::fs::rename(&temporary, directory.join(&id))?;
    Ok(id)
}

pub fn validate_backup(data_dir: &Path, id: &str) -> Result<()> {
    let path = safe_backup_path(data_dir, id)?;
    let file = File::open(path)?;
    let mut zip = ZipArchive::new(file)?;
    let manifest: BackupManifest = {
        let mut entry = zip.by_name("manifest.json")?;
        let mut text = String::new();
        entry.read_to_string(&mut text)?;
        serde_json::from_str(&text)?
    };
    if manifest.format != BACKUP_FORMAT || !manifest.files.iter().any(|file| file.name == "data.db")
    {
        bail!("Backup manifest is not a Curator database backup.");
    }
    let temporary = tempfile::tempdir()?;
    let database = temporary.path().join("data.db");
    {
        let mut source = zip.by_name("data.db")?;
        let mut destination = File::create(&database)?;
        std::io::copy(&mut source, &mut destination)?;
    }
    let connection = Connection::open(&database)?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        bail!("SQLite integrity check failed: {integrity}");
    }
    if manifest
        .files
        .iter()
        .any(|file| file.name == "settings.json")
    {
        let mut entry = zip.by_name("settings.json")?;
        let mut text = String::new();
        entry.read_to_string(&mut text)?;
        let _: db::Settings = serde_json::from_str(&text)?;
    }
    Ok(())
}

fn stage_restore(state: &AppState, id: &str) -> Result<()> {
    validate_backup(&state.data_dir, id)?;
    let path = safe_backup_path(&state.data_dir, id)?;
    let mut zip = ZipArchive::new(File::open(path)?)?;
    extract_optional(
        &mut zip,
        "data.db",
        &state.data_dir.join("data.db.restore-pending"),
    )?
    .context("Backup did not include a database")?;
    let _ = extract_optional(
        &mut zip,
        "settings.json",
        &state.data_dir.join("settings.json.restore-pending"),
    )?;
    let _ = extract_optional(
        &mut zip,
        "config.json",
        &state.data_dir.join("config.json.restore-pending"),
    )?;
    write_pending(&state.data_dir, "restore")
}

fn extract_optional(
    zip: &mut ZipArchive<File>,
    name: &str,
    destination: &Path,
) -> Result<Option<()>> {
    match zip.by_name(name) {
        Ok(mut source) => {
            let mut output = File::create(destination)?;
            std::io::copy(&mut source, &mut output)?;
            Ok(Some(()))
        }
        Err(zip::result::ZipError::FileNotFound) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn stage_factory_reset(state: &AppState) -> Result<()> {
    let staging = tempfile::tempdir_in(&state.data_dir)?;
    let reset_root = staging.path().join("reset");
    std::fs::create_dir_all(&reset_root)?;
    let pool = db::init_pool(&reset_root)?;
    // init_pool enables WAL. Checkpoint before copying only data.db so the
    // staged reset contains every migration and default table on its own.
    pool.get()?
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(pool);
    db::save_settings(&reset_root, &db::Settings::default());
    std::fs::copy(
        reset_root.join("data.db"),
        state.data_dir.join("data.db.reset-pending"),
    )?;
    std::fs::copy(
        db::settings_path(&reset_root),
        state.data_dir.join("settings.json.reset-pending"),
    )?;
    write_pending(&state.data_dir, "factory_reset")
}

fn write_pending(data_dir: &Path, operation: &str) -> Result<()> {
    let content = serde_json::to_string_pretty(&PendingRestart {
        operation: operation.into(),
    })?;
    std::fs::write(data_dir.join(PENDING_RESTART), content)?;
    Ok(())
}

/// Run before SQLite opens. A staged restore/reset is reversible until every
/// replacement has succeeded, so an interrupted restart never silently
/// leaves a half-restored database behind.
pub fn apply_pending_restart(data_dir: &Path, scope: InstallScope) -> Result<()> {
    let pending_path = data_dir.join(PENDING_RESTART);
    if !pending_path.is_file() {
        return Ok(());
    }
    let pending: PendingRestart = serde_json::from_str(&std::fs::read_to_string(&pending_path)?)?;
    let suffix = match pending.operation.as_str() {
        "restore" => "restore-pending",
        "factory_reset" => "reset-pending",
        _ => bail!("Unknown pending Curator restart operation."),
    };
    let staged_db = data_dir.join(format!("data.db.{suffix}"));
    if !staged_db.is_file() {
        bail!("Pending restart is missing its staged database.");
    }
    let db_path = data_dir.join("data.db");
    let mut replacements = Vec::new();
    let mut sidecars = Vec::new();
    let operation = (|| -> Result<()> {
        replacements.push(replace_with_rollback(
            &db_path,
            &staged_db,
            &data_dir.join("data.db.pre-maintenance"),
        )?);
        for name in ["data.db-wal", "data.db-shm"] {
            let live = data_dir.join(name);
            let rollback = data_dir.join(format!("{name}.pre-maintenance"));
            sidecars.push(move_with_rollback(&live, &rollback)?);
        }

        let staged_settings = data_dir.join(format!("settings.json.{suffix}"));
        if staged_settings.is_file() {
            let settings = db::settings_path(data_dir);
            replacements.push(replace_with_rollback(
                &settings,
                &staged_settings,
                &data_dir.join("settings.json.pre-maintenance"),
            )?);
        }
        if pending.operation == "restore" {
            let staged_config = data_dir.join("config.json.restore-pending");
            if staged_config.is_file() {
                let config = crate::config::config_path_for(scope);
                if let Some(parent) = config.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                replacements.push(replace_with_rollback(
                    &config,
                    &staged_config,
                    &data_dir.join("config.json.pre-maintenance"),
                )?);
            }
        }
        Ok(())
    })();

    if let Err(error) = operation {
        for replacement in replacements.iter().rev() {
            rollback_replacement(replacement);
        }
        for sidecar in sidecars.iter().rev() {
            rollback_sidecar(sidecar);
        }
        return Err(error);
    }

    for replacement in replacements {
        remove_if_exists(&replacement.rollback);
    }
    for sidecar in sidecars {
        remove_if_exists(&sidecar.rollback);
    }
    remove_if_exists(&pending_path);
    Ok(())
}

#[derive(Debug)]
struct Replacement {
    live: PathBuf,
    staged: PathBuf,
    rollback: PathBuf,
    had_live: bool,
}

#[derive(Debug)]
struct SidecarMove {
    live: PathBuf,
    rollback: PathBuf,
    moved: bool,
}

fn replace_with_rollback(live: &Path, staged: &Path, rollback: &Path) -> Result<Replacement> {
    remove_if_exists(rollback);
    let had_live = live.exists();
    if had_live {
        std::fs::rename(live, rollback)?;
    }
    if let Err(error) = std::fs::rename(staged, live) {
        if had_live {
            let _ = std::fs::rename(rollback, live);
        }
        return Err(error.into());
    }
    Ok(Replacement {
        live: live.to_path_buf(),
        staged: staged.to_path_buf(),
        rollback: rollback.to_path_buf(),
        had_live,
    })
}

fn move_with_rollback(live: &Path, rollback: &Path) -> Result<SidecarMove> {
    remove_if_exists(rollback);
    let moved = live.is_file();
    if moved {
        std::fs::rename(live, rollback)?;
    }
    Ok(SidecarMove {
        live: live.to_path_buf(),
        rollback: rollback.to_path_buf(),
        moved,
    })
}

fn rollback_replacement(replacement: &Replacement) {
    if replacement.live.is_file() {
        if !replacement.staged.exists() {
            let _ = std::fs::rename(&replacement.live, &replacement.staged);
        } else {
            remove_if_exists(&replacement.live);
        }
    }
    if replacement.had_live && replacement.rollback.is_file() {
        let _ = std::fs::rename(&replacement.rollback, &replacement.live);
    }
}

fn rollback_sidecar(sidecar: &SidecarMove) {
    if sidecar.moved && sidecar.rollback.is_file() {
        let _ = std::fs::rename(&sidecar.rollback, &sidecar.live);
    }
}

fn remove_if_exists(path: &Path) {
    if path.is_file() {
        let _ = std::fs::remove_file(path);
    }
}

fn rebuild_caches(state: &AppState) -> Result<()> {
    if state.thumbs_dir.exists() {
        std::fs::remove_dir_all(&state.thumbs_dir)
            .with_context(|| format!("clearing thumbnail cache {}", state.thumbs_dir.display()))?;
    }
    std::fs::create_dir_all(&state.thumbs_dir)?;
    crate::thumb_worker::clear_failure_cache();
    Ok(())
}

fn remove_preserved_directory(target: &Path, data_dir: &Path) -> Result<()> {
    let root = dunce::canonicalize(data_dir)?;
    let target = dunce::canonicalize(target).unwrap_or_else(|_| target.to_path_buf());
    if !target.starts_with(&root) || target == root {
        bail!("Refusing to remove data outside Curator's preserved-data directory.");
    }
    if target.exists() {
        std::fs::remove_dir_all(&target)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destructive_actions_have_explicit_confirmation_phrases() {
        assert_eq!(
            MaintenanceKind::FactoryReset.confirmation_phrase(),
            Some("RESET CURATOR")
        );
        assert!(MaintenanceKind::DeleteGroups.is_destructive());
        assert!(!MaintenanceKind::CreateBackup.is_destructive());
    }

    #[test]
    fn backup_validation_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        assert!(safe_backup_path(dir.path(), "../data.db").is_err());
    }

    #[test]
    fn background_worker_leases_close_admission_and_drain() {
        let controller = Arc::new(MaintenanceController::new());
        let lease = controller
            .try_acquire_background_worker()
            .expect("inactive controller admits work");
        assert_eq!(controller.active_background_workers(), 1);

        controller.active.store(true, Ordering::Release);
        assert!(controller.try_acquire_background_worker().is_none());
        drop(lease);
        assert_eq!(controller.active_background_workers(), 0);

        controller.active.store(false, Ordering::Release);
        assert!(controller.try_acquire_background_worker().is_some());
    }

    #[tokio::test]
    async fn quiescence_waits_for_a_background_worker_lease() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let lease = state
            .maintenance
            .try_acquire_background_worker()
            .expect("inactive controller admits work");
        let state_for_wait = Arc::clone(&state);
        let mut wait = tokio::spawn(async move { wait_for_quiescence(&state_for_wait).await });
        assert!(tokio::time::timeout(Duration::from_millis(40), &mut wait)
            .await
            .is_err());

        drop(lease);
        assert!(tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .expect("worker should drain")
            .expect("wait task should complete"));
    }

    #[test]
    fn factory_reset_stage_contains_a_complete_checkpointed_database() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        stage_factory_reset(&state).unwrap();
        let staged = root.path().join("data.db.reset-pending");
        let connection = Connection::open(staged).unwrap();
        let integrity: String = connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        let media_table: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='media'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(integrity, "ok");
        assert_eq!(media_table, 1);
    }
}

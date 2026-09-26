use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

pub type DbPool = Pool<SqliteConnectionManager>;

// WAL permits readers alongside a writer, but SQLite still permits only one
// writer at a time. Downloads, filesystem indexing, and optional background
// work can all finish together, so a short default timeout turns a temporary
// writer handoff into a dropped update. Keep this comfortably below a request
// timeout while allowing a queued writer to make progress.
const SQLITE_BUSY_TIMEOUT_MS: u32 = 30_000;

// ─── Settings ────────────────────────────────────────────────────────────────

fn default_max_concurrent() -> u32 {
    6
}
fn default_slideshow_speed() -> f64 {
    3000.0
}
fn default_slideshow_loop() -> bool {
    true
}
fn default_slideshow_shuffle() -> bool {
    false
}
fn default_theme() -> String {
    "system".into()
}
fn default_export_reminder_days() -> u32 {
    30
}
fn default_ch_default_interval() -> f64 {
    5.0
}
fn default_ch_default_limit() -> u32 {
    200
}
fn default_ch_default_shuffle() -> bool {
    true
}
fn default_ch_default_media_type() -> String {
    "image".into()
}
fn default_max_clip_length_secs() -> u32 {
    60
}
fn default_last_play_mode() -> String {
    "slideshow".into()
}
fn default_keep_running_in_tray() -> bool {
    true
}
fn default_library_layout() -> String {
    "grid".into()
}
fn default_search_providers() -> Vec<String> {
    // Search is intentionally opt-in per provider. The only current remote
    // free-text adapter is Balbums; the other registry entries are exposed
    // honestly as direct-URL-only until they gain a real adapter.
    vec!["local".into(), "balbums".into()]
}
fn default_goon_persona() -> String {
    "neutral".into()
}
fn default_tts_rate() -> f64 {
    1.0
}
fn default_tts_pitch() -> f64 {
    1.0
}
fn default_tts_volume() -> f64 {
    1.0
}
fn default_metronome_volume() -> f64 {
    0.55
}
fn default_soundtrack_provider() -> String {
    "local".into()
}
fn default_automatic_cleanup_mode() -> String {
    "never".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    #[serde(default = "default_max_clip_length_secs")]
    pub max_clip_length_secs: u32,
    #[serde(default = "default_ch_default_limit")]
    pub goon_default_limit: u32,
    #[serde(default)]
    pub goon_log_sessions: bool,
    #[serde(default)]
    pub start_with_windows: bool,
    #[serde(default = "default_keep_running_in_tray")]
    pub keep_running_in_tray: bool,
    /// Opt-in LAN listener. Off by default so an accidentally shared network
    /// never silently becomes an unauthenticated admin surface; enabling it
    /// binds the configured port on all local interfaces.
    #[serde(default)]
    pub lan_access_enabled: bool,
    #[serde(default = "default_last_play_mode")]
    pub last_play_mode: String,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: u32,

    /// `None` deliberately means no remote file-size ceiling.  Keeping this
    /// optional is important for upgrades: an older settings.json must retain
    /// the historical unlimited behavior rather than unexpectedly skipping
    /// media on its first sync after an update.
    #[serde(default)]
    pub max_download_file_size_bytes: Option<u64>,
    /// A per-source ceiling applied before a remote source begins a sync.
    #[serde(default)]
    pub max_source_storage_bytes: Option<u64>,
    /// New syncs pause when the filesystem has less free space than this.
    #[serde(default)]
    pub minimum_free_disk_bytes: Option<u64>,
    /// Derived thumbnails are evicted least-recently-used once this ceiling is
    /// enabled. `None` preserves the pre-existing unbounded cache behavior.
    #[serde(default)]
    pub thumbnail_cache_max_bytes: Option<u64>,
    /// Remote limits intentionally do not affect a person's local-folder
    /// imports unless they explicitly opt in.
    #[serde(default)]
    pub apply_download_limits_to_local_imports: bool,
    /// `never`, `low_disk`, or `weekly`; destructive cleanup stays opt-in.
    #[serde(default = "default_automatic_cleanup_mode")]
    pub automatic_cleanup_mode: String,
    #[serde(default)]
    pub automatic_cleanup_low_disk_bytes: Option<u64>,
    #[serde(default)]
    pub archive_retention_days: Option<u32>,
    #[serde(default)]
    pub last_automatic_cleanup_at: Option<String>,

    #[serde(default = "default_slideshow_speed")]
    pub default_slideshow_speed: f64,

    #[serde(default = "default_slideshow_loop")]
    pub default_slideshow_loop: bool,

    #[serde(default = "default_slideshow_shuffle")]
    pub default_slideshow_shuffle: bool,

    #[serde(default = "default_theme")]
    pub theme: String,

    #[serde(default = "default_export_reminder_days")]
    pub export_reminder_days: u32,

    pub last_export_at: Option<String>,
    pub export_reminder_snoozed_until: Option<String>,

    // Tier 3 — Cock Hero settings
    #[serde(default)]
    pub ch_log_sessions: bool,

    #[serde(default = "default_ch_default_interval")]
    pub ch_default_interval: f64,

    #[serde(default = "default_ch_default_limit")]
    pub ch_default_limit: u32,

    #[serde(default = "default_ch_default_shuffle")]
    pub ch_default_shuffle: bool,

    #[serde(default = "default_ch_default_media_type")]
    pub ch_default_media_type: String,

    // NSFW auto-rating (opt-in, requires the Python worker's dependencies
    // to be installed — see nsfw_worker.py). Enabling/disabling takes effect
    // on next restart, since it decides whether the worker process gets
    // started at all. Recommendations and raw scores are retained separately;
    // automation changes the effective rating only until a human reviews it.
    #[serde(default)]
    pub nsfw_filter_enabled: bool,
    /// These flags survive closing/reopening Settings so a restart requirement
    /// is not reduced to a transient toast. They are cleared once a new
    /// backend process has started with the changed configuration.
    #[serde(default)]
    pub ffmpeg_restart_required: bool,
    #[serde(default)]
    pub nsfw_restart_required: bool,

    /// Library presentation is a preference, not a capability.  Grid is the
    /// default while Table remains useful for large collections and keyboard
    /// selection.
    #[serde(default = "default_library_layout")]
    pub library_layout: String,

    /// Explicit discovery providers.  An empty list means "local only";
    /// Curator never silently fans out to every extractor installed by
    /// gallery-dl.
    #[serde(default = "default_search_providers")]
    pub search_providers: Vec<String>,

    #[serde(default)]
    pub metronome_enabled: bool,
    #[serde(default = "default_metronome_volume")]
    pub metronome_volume: f64,
    #[serde(default = "default_goon_persona")]
    pub goon_persona: String,
    #[serde(default)]
    pub tts_voice: Option<String>,
    #[serde(default = "default_tts_rate")]
    pub tts_rate: f64,
    #[serde(default = "default_tts_pitch")]
    pub tts_pitch: f64,
    #[serde(default = "default_tts_volume")]
    pub tts_volume: f64,
    #[serde(default = "default_soundtrack_provider")]
    pub soundtrack_provider: String,

    // First-run OOBE (out-of-box setup wizard — see oobe.rs). `false` here
    // means "show the wizard instead of the normal UI". This field alone is
    // NOT the whole story for whether an existing installation gets forced
    // through it — see `load_settings` below and `oobe::existing_installation_has_data`
    // for the self-healing logic that protects upgraders.
    #[serde(default)]
    pub oobe_completed: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            max_clip_length_secs: default_max_clip_length_secs(),
            goon_default_limit: default_ch_default_limit(),
            goon_log_sessions: false,
            start_with_windows: false,
            keep_running_in_tray: default_keep_running_in_tray(),
            lan_access_enabled: false,
            last_play_mode: default_last_play_mode(),
            max_concurrent: default_max_concurrent(),
            max_download_file_size_bytes: None,
            max_source_storage_bytes: None,
            minimum_free_disk_bytes: None,
            thumbnail_cache_max_bytes: None,
            apply_download_limits_to_local_imports: false,
            automatic_cleanup_mode: default_automatic_cleanup_mode(),
            automatic_cleanup_low_disk_bytes: None,
            archive_retention_days: None,
            last_automatic_cleanup_at: None,
            default_slideshow_speed: default_slideshow_speed(),
            default_slideshow_loop: default_slideshow_loop(),
            default_slideshow_shuffle: default_slideshow_shuffle(),
            theme: default_theme(),
            export_reminder_days: default_export_reminder_days(),
            last_export_at: None,
            export_reminder_snoozed_until: None,
            ch_log_sessions: false,
            ch_default_interval: default_ch_default_interval(),
            ch_default_limit: default_ch_default_limit(),
            ch_default_shuffle: default_ch_default_shuffle(),
            ch_default_media_type: default_ch_default_media_type(),
            nsfw_filter_enabled: false,
            ffmpeg_restart_required: false,
            nsfw_restart_required: false,
            library_layout: default_library_layout(),
            search_providers: default_search_providers(),
            metronome_enabled: false,
            metronome_volume: default_metronome_volume(),
            goon_persona: default_goon_persona(),
            tts_voice: None,
            tts_rate: default_tts_rate(),
            tts_pitch: default_tts_pitch(),
            tts_volume: default_tts_volume(),
            soundtrack_provider: default_soundtrack_provider(),
            // A brand new Settings::default() (no settings.json on disk at
            // all) means a genuinely fresh install — OOBE should run. See
            // load_settings for how an *existing* settings.json that
            // predates this field is handled differently.
            oobe_completed: false,
        }
    }
}

pub fn settings_path(data_dir: &Path) -> PathBuf {
    data_dir.join("settings.json")
}

pub fn load_settings(data_dir: &Path) -> Settings {
    let path = settings_path(data_dir);
    if let Ok(text) = std::fs::read_to_string(&path) {
        if let Ok(mut s) = serde_json::from_str::<Settings>(&text) {
            // Self-heal: a settings.json that exists on disk but predates
            // the `oobe_completed` field belongs to an installation that
            // was already set up and running before OOBE existed in this
            // codebase. `#[serde(default)]` would otherwise silently give
            // it `false` and force a real, already-configured user through
            // first-run setup — which the spec explicitly forbids. Detect
            // that case by checking the raw JSON (not just the parsed
            // struct, since `false` is indistinguishable from "missing"
            // once deserialized) and repair the file once, the same way
            // db::run_migrations self-heals older databases.
            if !text.contains("\"oobe_completed\"") {
                s.oobe_completed = true;
                save_settings(data_dir, &s);
            }
            return s;
        }
    }
    Settings::default()
}

pub fn save_settings(data_dir: &Path, settings: &Settings) {
    let path = settings_path(data_dir);
    if let Ok(text) = serde_json::to_string_pretty(settings) {
        let _ = std::fs::write(&path, text);
    }
}

// ─── Pool init ────────────────────────────────────────────────────────────────

pub fn init_pool(data_dir: &Path) -> Result<DbPool> {
    let db_path = data_dir.join("data.db");
    let manager = SqliteConnectionManager::file(&db_path).with_init(|conn| {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
                 PRAGMA foreign_keys=ON;",
        )?;
        conn.busy_timeout(std::time::Duration::from_millis(u64::from(
            SQLITE_BUSY_TIMEOUT_MS,
        )))
    });
    let pool = r2d2::Pool::builder()
        .max_size(8)
        .build(manager)
        .context("building SQLite connection pool")?;

    let migration_conn = pool.get().context("getting migration connection")?;
    run_migrations(&migration_conn)?;
    Ok(pool)
}

// ─── Migrations ───────────────────────────────────────────────────────────────

pub fn run_migrations(conn: &Connection) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    let conn = &*tx;
    // ── groups ────────────────────────────────────────────────────────────────
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS groups (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT NOT NULL,
            parent_id INTEGER REFERENCES groups(id) ON DELETE SET NULL,
            added_at  TEXT NOT NULL
        );
    ",
    )?;

    // ALTER TABLE additions for existing DBs
    let group_cols: HashSet<String> = column_names(conn, "groups");
    if !group_cols.contains("parent_id") {
        conn.execute_batch("ALTER TABLE groups ADD COLUMN parent_id INTEGER REFERENCES groups(id) ON DELETE SET NULL;")?;
    }

    // ── sources ───────────────────────────────────────────────────────────────
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS sources (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            name          TEXT NOT NULL,
            url           TEXT NOT NULL,
            slug          TEXT NOT NULL,
            status        TEXT NOT NULL DEFAULT 'pending',
            item_count    INTEGER NOT NULL DEFAULT 0,
            included      INTEGER NOT NULL DEFAULT 1,
            group_id      INTEGER REFERENCES groups(id) ON DELETE SET NULL,
            error_message TEXT,
            log           TEXT,
            added_at      TEXT NOT NULL,
            synced_at     TEXT
        );
    ",
    )?;

    let src_cols: HashSet<String> = column_names(conn, "sources");
    if !src_cols.contains("group_id") {
        conn.execute_batch("ALTER TABLE sources ADD COLUMN group_id INTEGER REFERENCES groups(id) ON DELETE SET NULL;")?;
    }
    // Retry scheduling belongs to a source, rather than to an in-memory
    // task, so a background process can safely recover its queue after a
    // desktop-window or machine restart.
    if !src_cols.contains("retry_attempts") {
        conn.execute_batch(
            "ALTER TABLE sources ADD COLUMN retry_attempts INTEGER NOT NULL DEFAULT 0;",
        )?;
    }
    if !src_cols.contains("retry_at") {
        conn.execute_batch("ALTER TABLE sources ADD COLUMN retry_at INTEGER NOT NULL DEFAULT 0;")?;
    }
    // Source-level activity survives process restarts.  `known_total` stays
    // NULL until gallery-dl's placeholder listing can establish a count; a
    // NULL is intentionally displayed as indeterminate rather than a made-up
    // percentage.
    for (name, definition) in [
        ("known_total", "INTEGER CHECK(known_total >= 0)"),
        ("completed_count", "INTEGER NOT NULL DEFAULT 0"),
        ("current_filename", "TEXT"),
        ("queued_at", "TEXT"),
        ("started_at", "TEXT"),
        ("completed_at", "TEXT"),
        ("progress_updated_at", "TEXT"),
    ] {
        if !src_cols.contains(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE sources ADD COLUMN {name} {definition};"
            ))?;
        }
    }
    // Source retention and a one-time quota override are durable source
    // state, rather than process memory, so an intentional "permit once"
    // remains meaningful if Curator is restarted before the queued sync runs.
    for (name, definition) in [
        (
            "retention_keep_newest",
            "INTEGER CHECK(retention_keep_newest > 0)",
        ),
        ("storage_override_once", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !src_cols.contains(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE sources ADD COLUMN {name} {definition};"
            ))?;
        }
    }

    // ── media ─────────────────────────────────────────────────────────────────
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS media (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id  INTEGER NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
            filepath   TEXT NOT NULL UNIQUE,
            filename   TEXT NOT NULL,
            type       TEXT NOT NULL,
            added_at   TEXT NOT NULL,
            rating     INTEGER NOT NULL DEFAULT 0
        );
    ",
    )?;

    let media_cols: HashSet<String> = column_names(conn, "media");
    if !media_cols.contains("rating") {
        conn.execute_batch("ALTER TABLE media ADD COLUMN rating INTEGER NOT NULL DEFAULT 0;")?;
    }
    if !media_cols.contains("origin_url") {
        conn.execute_batch("ALTER TABLE media ADD COLUMN origin_url TEXT;")?;
    }
    if !media_cols.contains("downloaded") {
        // DEFAULT 1 so all pre-existing rows are treated as real files
        conn.execute_batch("ALTER TABLE media ADD COLUMN downloaded INTEGER NOT NULL DEFAULT 1;")?;
    }
    if !media_cols.contains("duration_secs") {
        // NULL = not a video, or a video whose duration isn't known yet
        // (ffprobe not installed, or not yet backfilled — see duration.rs).
        conn.execute_batch("ALTER TABLE media ADD COLUMN duration_secs REAL;")?;
    }

    for (name, definition) in [
        ("missing", "INTEGER NOT NULL DEFAULT 0"),
        ("file_stamp", "TEXT"),
        // Keep the filesystem facts independently from the ingest state.  In
        // particular, these fields let Activity distinguish a listed
        // placeholder from a completed, indexed file after a restart.
        ("file_size_bytes", "INTEGER CHECK(file_size_bytes >= 0)"),
        ("downloaded_at", "TEXT"),
        ("modified_at", "TEXT"),
        ("nsfw_state", "TEXT NOT NULL DEFAULT 'pending'"),
        ("nsfw_attempts", "INTEGER NOT NULL DEFAULT 0"),
        ("nsfw_retry_at", "INTEGER NOT NULL DEFAULT 0"),
        ("duration_attempted", "INTEGER NOT NULL DEFAULT 0"),
        (
            "clip_parent_id",
            "INTEGER REFERENCES media(id) ON DELETE SET NULL",
        ),
        ("clip_start_secs", "REAL CHECK(clip_start_secs >= 0)"),
        (
            "clip_end_secs",
            "REAL CHECK(clip_end_secs > clip_start_secs)",
        ),
        ("auto_rating", "INTEGER NOT NULL DEFAULT 0"),
        ("auto_rating_score", "REAL"),
        (
            "human_rating",
            "INTEGER CHECK(human_rating BETWEEN 1 AND 5)",
        ),
        ("rating_source", "TEXT NOT NULL DEFAULT 'none'"),
        ("rating_reviewed", "INTEGER NOT NULL DEFAULT 0"),
        ("rating_reviewed_at", "TEXT"),
        // NudeNet provenance is kept separately from the compatibility
        // `auto_rating_score` field so a reviewer can see exactly which model
        // and anatomical evidence produced a 1-3 suggestion.
        ("classifier_model", "TEXT"),
        ("classifier_version", "TEXT"),
        ("classifier_score", "REAL"),
        ("classifier_evidence", "TEXT"),
        // P-HAR is a separate, optional temporal model.  It can suggest
        // Fast (4) only; Cum is never inferred automatically.
        ("action_model", "TEXT"),
        ("action_model_version", "TEXT"),
        ("action_score", "REAL"),
        ("action_evidence", "TEXT"),
        (
            "action_rating",
            "INTEGER NOT NULL DEFAULT 0 CHECK(action_rating BETWEEN 0 AND 4)",
        ),
        (
            "classification_label",
            "TEXT NOT NULL DEFAULT 'unclassified'",
        ),
        ("manual_review_required", "INTEGER NOT NULL DEFAULT 0"),
        ("manual_review_reason", "TEXT"),
        ("classification_updated_at", "TEXT"),
        // A size-limited remote item remains a durable placeholder.  It is
        // not failed and it is never written to gallery-dl's archive, so a
        // later larger limit can download it normally.
        ("skip_reason", "TEXT"),
        ("skip_limit_bytes", "INTEGER CHECK(skip_limit_bytes >= 0)"),
        ("skipped_at", "TEXT"),
        // Retention never deletes a media row: annotations/provenance remain
        // visible as an unavailable placeholder and can be re-downloaded.
        ("retention_deleted", "INTEGER NOT NULL DEFAULT 0"),
        // There was no historical favorite control, but keeping this durable
        // guard makes cleanup safe for existing integrations and future UI.
        ("favorite", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        if !media_cols.contains(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE media ADD COLUMN {name} {definition};"
            ))?;
        }
    }
    conn.execute_batch("CREATE TABLE IF NOT EXISTS clip_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT, media_id INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
        seconds INTEGER NOT NULL, status TEXT NOT NULL, clip_count INTEGER NOT NULL DEFAULT 0,
        error TEXT, added_at TEXT NOT NULL);
        UPDATE clip_jobs SET status='failed',error='Interrupted by restart; original preserved' WHERE status='running';")?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_media_review ON media(rating_reviewed, auto_rating, id);
         CREATE INDEX IF NOT EXISTS idx_media_size ON media(file_size_bytes, id);",
    )?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS placeholder_scans (
        source_id INTEGER PRIMARY KEY REFERENCES sources(id) ON DELETE CASCADE,
        url TEXT NOT NULL, retry_at INTEGER NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_media_nsfw ON media(nsfw_state, downloaded, rating, type, nsfw_retry_at, id);
        CREATE INDEX IF NOT EXISTS idx_media_probe ON media(duration_attempted, downloaded, type, id);
        CREATE INDEX IF NOT EXISTS idx_media_filename ON media(filename COLLATE NOCASE, id);
        CREATE INDEX IF NOT EXISTS idx_media_rating_id ON media(rating DESC, id ASC);
        CREATE INDEX IF NOT EXISTS idx_media_manual_review ON media(manual_review_required, rating_reviewed, id);
        CREATE INDEX IF NOT EXISTS idx_sources_activity ON sources(status, queued_at, retry_at, id);
        CREATE TRIGGER IF NOT EXISTS media_count_insert AFTER INSERT ON media WHEN NEW.downloaded=1 BEGIN
          UPDATE sources SET item_count=item_count+1 WHERE id=NEW.source_id;
        END;
        CREATE TRIGGER IF NOT EXISTS media_count_delete AFTER DELETE ON media WHEN OLD.downloaded=1 BEGIN
          UPDATE sources SET item_count=MAX(0,item_count-1) WHERE id=OLD.source_id;
        END;
        CREATE TRIGGER IF NOT EXISTS media_count_update AFTER UPDATE OF downloaded,source_id ON media
        WHEN OLD.downloaded<>NEW.downloaded OR OLD.source_id<>NEW.source_id BEGIN
          UPDATE sources SET item_count=MAX(0,item_count-OLD.downloaded) WHERE id=OLD.source_id;
          UPDATE sources SET item_count=item_count+NEW.downloaded WHERE id=NEW.source_id;
        END;")?;

    // ── tags + junction tables ────────────────────────────────────────────────
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS tags (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            name     TEXT NOT NULL UNIQUE,
            added_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS media_tags (
            media_id INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
            tag_id   INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (media_id, tag_id)
        );
        -- A tag can be supplied by an import, an adapter, or a person.  Keep
        -- that provenance without changing the compact media_tags junction
        -- used by filtering queries.
        CREATE TABLE IF NOT EXISTS media_tag_provenance (
            media_id   INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
            tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            provenance TEXT NOT NULL DEFAULT 'legacy',
            added_at   TEXT NOT NULL,
            PRIMARY KEY (media_id, tag_id)
        );
        CREATE TABLE IF NOT EXISTS group_tags (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            tag_id   INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (group_id, tag_id)
        );
        -- A source's group is its default home. Individual media may also
        -- appear in other collections without moving the file or source.
        CREATE TABLE IF NOT EXISTS media_groups (
            media_id INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            added_at TEXT NOT NULL,
            PRIMARY KEY (media_id, group_id)
        );
        CREATE INDEX IF NOT EXISTS idx_media_groups_group ON media_groups(group_id, media_id);
    ",
    )?;

    // Extractor sidecars are evidence rather than automatic user-visible
    // tags. Keep their original shape so people can review or correct an
    // adapter's metadata without losing it on a later refresh.
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS source_metadata (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            media_id    INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
            provider    TEXT NOT NULL DEFAULT '',
            source_url  TEXT NOT NULL DEFAULT '',
            raw_json    TEXT NOT NULL,
            creator     TEXT,
            title       TEXT,
            captured_at TEXT NOT NULL,
            UNIQUE(media_id, provider, source_url)
        );
        CREATE TABLE IF NOT EXISTS source_tags (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            media_id            INTEGER NOT NULL REFERENCES media(id) ON DELETE CASCADE,
            source_metadata_id  INTEGER REFERENCES source_metadata(id) ON DELETE SET NULL,
            provider            TEXT NOT NULL DEFAULT '',
            raw_name            TEXT NOT NULL,
            normalized_name     TEXT,
            state               TEXT NOT NULL DEFAULT 'pending'
                                CHECK(state IN ('pending','approved','skipped')),
            reviewed_at         TEXT,
            added_at            TEXT NOT NULL,
            UNIQUE(media_id, provider, raw_name)
        );
        CREATE TABLE IF NOT EXISTS source_tag_rules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            provider        TEXT NOT NULL DEFAULT '',
            raw_name        TEXT NOT NULL COLLATE NOCASE,
            action          TEXT NOT NULL CHECK(action IN ('add','normalize','skip')),
            normalized_name TEXT,
            added_at        TEXT NOT NULL,
            updated_at      TEXT NOT NULL,
            UNIQUE(provider, raw_name)
        );
        CREATE INDEX IF NOT EXISTS idx_source_metadata_media ON source_metadata(media_id);
        CREATE INDEX IF NOT EXISTS idx_source_tags_review ON source_tags(state, provider, id);
        CREATE INDEX IF NOT EXISTS idx_source_tags_media ON source_tags(media_id);
        CREATE INDEX IF NOT EXISTS idx_media_tag_provenance_media ON media_tag_provenance(media_id, tag_id);
        CREATE INDEX IF NOT EXISTS idx_source_tag_rules_lookup ON source_tag_rules(provider, raw_name);
        ",
    )?;

    // ── Cock Hero session log (Tier 3, opt-in) ────────────────────────────────
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS ch_sessions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at  TEXT NOT NULL,
            duration_s  INTEGER NOT NULL,
            item_count  INTEGER NOT NULL,
            filters     TEXT,
            notes       TEXT
        );
    ",
    )?;
    // This is the durable cross-mode session log.  It predates the GOON
    // additions below, so create the base table before probing/adding its
    // optional columns on both fresh and upgraded databases.
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS interactive_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            duration_s INTEGER NOT NULL,
            item_count INTEGER NOT NULL,
            plan TEXT,
            events TEXT,
            ended_state TEXT NOT NULL DEFAULT 'completed'
        );",
    )?;
    let session_cols: HashSet<String> = column_names(conn, "interactive_sessions");
    for (name, definition) in [
        ("session_id", "TEXT"),
        ("soundtrack_provider", "TEXT"),
        ("bpm", "REAL"),
        ("beat_offset_secs", "REAL"),
        ("timing_corrections", "TEXT"),
        ("rating_phases", "TEXT"),
    ] {
        if !session_cols.contains(name) {
            conn.execute_batch(&format!(
                "ALTER TABLE interactive_sessions ADD COLUMN {name} {definition};"
            ))?;
        }
    }
    // Old GOON/Cock Hero rows intentionally have no native session identity.
    // Native terminal summaries use this partial key for retry-safe inserts.
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_interactive_sessions_session_id
         ON interactive_sessions(session_id) WHERE session_id IS NOT NULL;",
    )?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS goon_playlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            provider TEXT NOT NULL,
            source_url TEXT,
            tracks TEXT NOT NULL DEFAULT '[]',
            added_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
         );
         CREATE TABLE IF NOT EXISTS beat_maps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            playlist_id INTEGER REFERENCES goon_playlists(id) ON DELETE CASCADE,
            track_key TEXT NOT NULL,
            bpm REAL NOT NULL,
            beat_offset_secs REAL NOT NULL DEFAULT 0,
            confidence REAL NOT NULL DEFAULT 0,
            markers TEXT NOT NULL DEFAULT '[]',
            confirmed INTEGER NOT NULL DEFAULT 0,
            added_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(playlist_id,track_key)
         );
         CREATE INDEX IF NOT EXISTS idx_beat_maps_track ON beat_maps(track_key);",
    )?;

    // ── Indexes ───────────────────────────────────────────────────────────────
    conn.execute_batch(
        "
        CREATE INDEX IF NOT EXISTS idx_media_source    ON media(source_id);
        CREATE INDEX IF NOT EXISTS idx_media_rating    ON media(rating);
        CREATE INDEX IF NOT EXISTS idx_media_added_at  ON media(added_at);
        CREATE INDEX IF NOT EXISTS idx_media_duration  ON media(duration_secs);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_media_source_origin
            ON media(source_id, origin_url) WHERE origin_url IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_groups_parent   ON groups(parent_id);
        CREATE INDEX IF NOT EXISTS idx_sources_group   ON sources(group_id);
        CREATE INDEX IF NOT EXISTS idx_media_tags_tag  ON media_tags(tag_id);
        CREATE INDEX IF NOT EXISTS idx_group_tags_tag  ON group_tags(tag_id);
    ",
    )?;

    // ── Self-healing slug repair migration ────────────────────────────────────
    // Detects sources named by the old pre-skip-list logic (which collapsed every
    // bunkr /a/<code> and every kemono /<service>/user/<id> into a single shared
    // name) and renames them to what the current derive_name_from_url would produce.
    //
    // This mutates data rather than schema, so it's gated behind the _migrations
    // tracking table below and only ever runs once per database — not on every
    // startup. The content-comparison guard inside repair_slug_names (only touch
    // a row if its current name exactly matches the old bug's output) stays in
    // place too, as defense in depth: even if _migrations were ever lost or
    // tampered with, a re-run still can't clobber a name the user deliberately
    // set themselves.
    ensure_migrations_table(conn)?;
    run_migration_once(conn, "0001_repair_bunkr_kemono_slugs", |c| {
        repair_slug_names(c)
    })?;

    // One-time policy change: legacy effective ratings become provisional
    // automatic recommendations, but an install that already recorded an
    // explicit human review must keep that decision.  Older builds used the
    // overloaded `rating` column together with either `rating_reviewed` or a
    // human `rating_source`; preserve both signals until migration 0003 can
    // copy the value into `human_rating`.
    run_migration_once(conn, "0002_existing_ratings_are_automated", |c| {
        c.execute_batch(
            "UPDATE media SET
            auto_rating=CASE
                WHEN rating_reviewed=1 OR rating_source IN ('human','human_edited','manual')
                    THEN auto_rating
                WHEN rating>0 THEN rating
                ELSE auto_rating
            END,
            rating_source=CASE
                WHEN rating_reviewed=1 OR rating_source IN ('human','human_edited','manual') THEN 'human'
                WHEN rating>0 OR auto_rating>0 THEN 'auto'
                ELSE 'none'
            END,
            rating_reviewed=CASE
                WHEN rating_reviewed=1 OR rating_source IN ('human','human_edited','manual') THEN 1
                ELSE 0
            END,
            rating_reviewed_at=CASE
                WHEN rating_reviewed=1 OR rating_source IN ('human','human_edited','manual') THEN rating_reviewed_at
                ELSE NULL
            END;",
        )?;
        Ok(())
    })?;

    // Before `human_rating` existed, `rating` was an overloaded effective
    // cache. The reviewed bit is the only reliable signal that an old value
    // was a human decision. Preserve it exactly, including reviewed zero.
    run_migration_once(
        conn,
        "0003_separate_human_ratings_and_tag_provenance",
        |c| {
            c.execute_batch(
                "UPDATE media
             SET human_rating=CASE WHEN rating BETWEEN 1 AND 5 THEN rating ELSE NULL END
             WHERE rating_reviewed=1 AND human_rating IS NULL;
             UPDATE media
             SET rating=CASE
                 WHEN human_rating IS NOT NULL THEN human_rating
                 WHEN auto_rating BETWEEN 1 AND 5 THEN auto_rating
                 ELSE 0
             END,
             rating_source=CASE
                 WHEN human_rating IS NOT NULL THEN 'human'
                 WHEN auto_rating BETWEEN 1 AND 5 THEN 'auto'
                 ELSE 'none'
             END,
             rating_reviewed=CASE WHEN human_rating IS NULL THEN 0 ELSE 1 END;
             INSERT OR IGNORE INTO media_tag_provenance(media_id,tag_id,provenance,added_at)
             SELECT media_id,tag_id,'legacy',datetime('now') FROM media_tags;",
            )?;
            Ok(())
        },
    )?;

    // Preserve optional history created by an early experimental session
    // screen. It is deliberately copied rather than deleted so downgrading an
    // installation cannot destroy the person's own activity records.
    run_migration_once(conn, "0004_migrate_legacy_interactive_sessions", |c| {
        let has_legacy: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='ch_sessions')",
            [],
            |row| row.get(0),
        )?;
        if has_legacy {
            c.execute_batch(
                "INSERT OR IGNORE INTO interactive_sessions(id,started_at,duration_s,item_count,plan,events,ended_state)
                 SELECT id,started_at,duration_s,item_count,filters,notes,'completed' FROM ch_sessions;",
            )?;
        }
        Ok(())
    })?;

    // The old five-star scale used 1 for merely-safe media.  In the current
    // policy 1 is deliberately SFW and excluded from sexual playback, so
    // promote every existing non-5 decision before any new classifier result
    // is allowed to replace it.  Human values remain human values; automatic
    // values are retained as a provisional fallback and requeued even when a
    // human override sits above them.
    run_migration_once(conn, "0005_media_scale_and_classifier_provenance", |c| {
        c.execute_batch(
            "UPDATE media
                SET human_rating=CASE WHEN human_rating BETWEEN 1 AND 4 THEN human_rating+1 ELSE human_rating END,
                    auto_rating=CASE WHEN auto_rating BETWEEN 1 AND 4 THEN auto_rating+1 ELSE auto_rating END;
              UPDATE media
                SET rating=CASE
                    WHEN human_rating BETWEEN 1 AND 5 THEN human_rating
                    WHEN auto_rating BETWEEN 1 AND 5 THEN auto_rating
                    WHEN rating BETWEEN 1 AND 4 THEN rating+1
                    ELSE rating
                END,
                rating_source=CASE
                    WHEN human_rating BETWEEN 1 AND 5 THEN 'human'
                    WHEN auto_rating BETWEEN 1 AND 5 THEN 'auto'
                    ELSE 'none'
                END,
                rating_reviewed=CASE WHEN human_rating BETWEEN 1 AND 5 THEN 1 ELSE 0 END;
              UPDATE media
                SET nsfw_state='pending', nsfw_attempts=0, nsfw_retry_at=0,
                    classifier_model=NULL, classifier_version=NULL, classifier_score=NULL,
                    classifier_evidence=NULL, action_model=NULL, action_model_version=NULL,
                    action_score=NULL, action_evidence=NULL, action_rating=0,
                    classification_label='legacy_pending', manual_review_required=0,
                    manual_review_reason=NULL
                WHERE downloaded=1 AND missing=0
                  AND (auto_rating BETWEEN 1 AND 5 OR rating_source='auto');",
        )?;
        Ok(())
    })?;

    tx.commit()?;
    Ok(())
}

// ─── Migration tracking ────────────────────────────────────────────────────────

fn ensure_migrations_table(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS _migrations (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            name       TEXT NOT NULL UNIQUE,
            applied_at TEXT NOT NULL
        );
    ",
    )?;

    // CREATE TABLE IF NOT EXISTS is a no-op if a _migrations table already
    // exists from an older, differently-shaped version of this app — seen in
    // the wild: one missing the `name` column entirely, which then makes
    // every query below (and every startup) fail with "no column named
    // name" forever, since nothing here ever repairs an already-existing
    // table. Detect that and move the old table aside instead of touching it
    // further, then create a correctly-shaped one in its place. The one
    // migration this tracks (0001_repair_bunkr_kemono_slugs) only ever
    // touches rows that still exactly match the bug it's fixing, so
    // re-running it once more against a legacy database is safe regardless.
    let cols = column_names(conn, "_migrations");
    if !cols.contains("name") || !cols.contains("applied_at") {
        warn!(
            "_migrations table exists with an incompatible schema — moving it aside and recreating"
        );
        let mut legacy = "_migrations_legacy".to_string();
        while !column_names(conn, &legacy).is_empty() {
            legacy.push('_');
        }
        conn.execute_batch(&format!(
            "
            ALTER TABLE _migrations RENAME TO {legacy};
            CREATE TABLE _migrations (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                name       TEXT NOT NULL UNIQUE,
                applied_at TEXT NOT NULL
            );
        "
        ))?;
    }

    Ok(())
}

fn migration_applied(conn: &Connection, name: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM _migrations WHERE name=?1",
        params![name],
        |r| r.get::<_, i64>(0),
    )? > 0)
}

fn mark_migration_applied(conn: &Connection, name: &str) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO _migrations (name, applied_at) VALUES (?1, ?2)",
        params![name, now_iso()],
    )?;
    Ok(())
}

/// Runs a one-shot, non-schema data migration exactly once per database,
/// tracked by name in `_migrations`. Schema changes (CREATE TABLE IF NOT
/// EXISTS, the column-presence ALTER TABLEs above) are naturally idempotent
/// and don't need this — it's specifically for migrations that mutate rows,
/// where re-running on every startup would be wasteful or, for a less
/// carefully-guarded migration than this one, actively unsafe.
fn run_migration_once(
    conn: &Connection,
    name: &str,
    body: impl FnOnce(&Connection) -> Result<()>,
) -> Result<()> {
    if migration_applied(conn, name)? {
        return Ok(());
    }
    body(conn)?;
    mark_migration_applied(conn, name)?;
    info!("migration: applied {}", name);
    Ok(())
}

fn repair_slug_names(conn: &Connection) -> Result<()> {
    struct Row {
        id: i64,
        name: String,
        url: String,
    }
    let rows: Vec<Row> = {
        let mut stmt = conn.prepare("SELECT id, name, url FROM sources")?;
        let rows = stmt
            .query_map([], |r| {
                Ok(Row {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    url: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows
    };

    for row in rows {
        let old_guess = old_buggy_name(&row.url);
        if row.name != old_guess {
            continue; // doesn't match old bug's output — leave it alone
        }
        let new_name = crate::slug::derive_name_from_url(&row.url);
        if new_name != row.name {
            conn.execute(
                "UPDATE sources SET name=?1 WHERE id=?2",
                params![new_name, row.id],
            )?;
            info!(
                "repair: renamed source {} from {:?} to {:?}",
                row.id, row.name, new_name
            );
        }
    }
    Ok(())
}

/// Reconstructs what the pre-skip-list naming logic would have produced for a
/// given URL — used only by repair_slug_names to identify candidates for repair.
fn old_buggy_name(url: &str) -> String {
    // Manual URL parsing — no external url crate needed
    // Strip scheme: "https://host/path"
    let without_scheme = if let Some(idx) = url.find("://") {
        &url[idx + 3..]
    } else {
        url
    };

    // host = everything up to first '/'
    let (host_part, path_part) = if let Some(slash) = without_scheme.find('/') {
        (&without_scheme[..slash], &without_scheme[slash..])
    } else {
        (without_scheme, "")
    };

    let host = host_part.to_lowercase();
    let host = host.trim_start_matches("www.");
    let site = host.split('.').next().unwrap_or("site");

    let segments: Vec<&str> = path_part.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return host.to_string();
    }

    let handle = segments[0].trim_start_matches('@');
    if handle.is_empty() {
        return host.to_string();
    }
    format!("{} ({})", handle, site)
}

// ─── Schema helper ────────────────────────────────────────────────────────────

fn column_names(conn: &Connection, table: &str) -> HashSet<String> {
    let sql = format!("PRAGMA table_info({})", table);
    conn.prepare(&sql)
        .and_then(|mut stmt| {
            stmt.query_map([], |r| r.get::<_, String>(1))
                .map(|iter| iter.filter_map(|r| r.ok()).collect())
        })
        .unwrap_or_default()
}

// ─── Time helper ─────────────────────────────────────────────────────────────

pub fn now_iso() -> String {
    Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

// ─── Group tag cache helpers ──────────────────────────────────────────────────

/// group_id → [itself, parent, grandparent, ...] up to root
pub fn build_group_ancestry_map(conn: &Connection) -> rusqlite::Result<HashMap<i64, Vec<i64>>> {
    struct G {
        id: i64,
        parent_id: Option<i64>,
    }
    let rows: Vec<G> = {
        let mut stmt = conn.prepare("SELECT id, parent_id FROM groups")?;
        let rows = stmt.query_map([], |r| {
            Ok(G {
                id: r.get(0)?,
                parent_id: r.get(1)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let parents: HashMap<i64, Option<i64>> = rows.iter().map(|g| (g.id, g.parent_id)).collect();

    Ok(parents
        .keys()
        .map(|&gid| {
            let mut chain = vec![gid];
            let mut seen = HashSet::from([gid]);
            let mut cur = parents[&gid];
            while let Some(pid) = cur {
                if seen.contains(&pid) {
                    break;
                }
                chain.push(pid);
                seen.insert(pid);
                cur = parents.get(&pid).copied().flatten();
            }
            (gid, chain)
        })
        .collect())
}

/// group_id → set of tag names (group's own name + explicit tags + all ancestors' names+tags)
pub fn build_group_effective_tags_map(
    conn: &Connection,
) -> rusqlite::Result<HashMap<i64, HashSet<String>>> {
    let ancestry = build_group_ancestry_map(conn)?;

    let names: HashMap<i64, String> = {
        let mut stmt = conn.prepare("SELECT id, name FROM groups")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .map(|(id, name)| (id, name.trim().to_lowercase()))
            .collect()
    };

    let mut own_tags: HashMap<i64, HashSet<String>> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT gt.group_id, t.name FROM group_tags gt JOIN tags t ON t.id = gt.tag_id",
        )?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .for_each(|(gid, tag)| {
                own_tags.entry(gid).or_default().insert(tag);
            });
    }

    Ok(ancestry
        .into_iter()
        .map(|(gid, chain)| {
            let mut tags = HashSet::new();
            for ancestor_id in &chain {
                if let Some(name) = names.get(ancestor_id) {
                    if !name.is_empty() {
                        tags.insert(name.clone());
                    }
                }
                if let Some(t) = own_tags.get(ancestor_id) {
                    tags.extend(t.iter().cloned());
                }
            }
            (gid, tags)
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_connections_have_a_writer_handoff_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let pool = init_pool(dir.path()).unwrap();
        let conn = pool.get().unwrap();
        let timeout: u32 = conn
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(timeout, SQLITE_BUSY_TIMEOUT_MS);
    }

    #[test]
    fn repair_preserves_multiple_legacy_tables_and_is_repeatable() {
        for schema in [
            "version INTEGER",
            "name TEXT",
            "id INTEGER, applied_at TEXT",
        ] {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch(&format!("CREATE TABLE _migrations({schema}); CREATE TABLE _migrations_legacy(keep TEXT); INSERT INTO _migrations_legacy VALUES('preserve');")).unwrap();
            run_migrations(&conn).unwrap();
            run_migrations(&conn).unwrap();
            assert_eq!(
                conn.query_row("SELECT keep FROM _migrations_legacy", [], |r| r
                    .get::<_, String>(0))
                    .unwrap(),
                "preserve"
            );
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM _migrations", [], |r| r
                    .get::<_, i64>(0))
                    .unwrap(),
                // Current schema has five durable, one-time migrations. The
                // important part of this regression test is that a second
                // startup does not duplicate any of them.
                5
            );
        }
    }

    #[test]
    fn failed_migration_rolls_back_schema_and_tracker_repair() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE _migrations(version INTEGER); CREATE VIEW _migrations_legacy AS SELECT 1 AS preserved;").unwrap();
        // Force a failure after schema work starts, then verify nothing was committed.
        conn.execute_batch("CREATE VIEW sources AS SELECT 1 AS id;")
            .unwrap();
        assert!(run_migrations(&conn).is_err());
        assert!(column_names(&conn, "_migrations").contains("version"));
        assert!(column_names(&conn, "groups").is_empty());
    }

    /// Reproduces the exact failure seen in the wild: a pre-existing
    /// `_migrations` table from an older, differently-shaped version of the
    /// app (no `name` column) must not make `run_migrations` — and therefore
    /// the whole app's startup — fail with "no column named name".
    #[test]
    fn run_migrations_repairs_legacy_migrations_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE _migrations (id INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);",
        )
        .unwrap();

        run_migrations(&conn).expect("run_migrations should self-heal, not fail");

        assert!(column_names(&conn, "_migrations").contains("name"));
        // Old data preserved, not silently dropped.
        assert!(column_names(&conn, "_migrations_legacy").contains("applied_at"));
        // The migration still gets recorded in the new table.
        assert!(migration_applied(&conn, "0001_repair_bunkr_kemono_slugs").unwrap());
    }

    /// A normal, already-correct database (the common case) shouldn't be
    /// touched by the repair path at all.
    #[test]
    fn run_migrations_is_a_noop_on_a_healthy_db() {
        let conn = Connection::open_in_memory().unwrap();
        run_migrations(&conn).unwrap();
        run_migrations(&conn).expect("running migrations twice must stay safe");
        assert!(column_names(&conn, "_migrations").contains("name"));
        assert!(!column_names(&conn, "_migrations_legacy").contains("applied_at"));
        assert!(column_names(&conn, "sources").contains("retention_keep_newest"));
        assert!(column_names(&conn, "sources").contains("storage_override_once"));
        assert!(column_names(&conn, "media").contains("skip_reason"));
        assert!(column_names(&conn, "media").contains("retention_deleted"));
    }

    /// A legacy settings.json written before OOBE existed in this codebase
    /// must not force an already-configured installation through first-run
    /// setup just because the new field defaults to `false`.
    #[test]
    fn load_settings_self_heals_legacy_file_as_oobe_completed() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            settings_path(dir.path()),
            r#"{"max_concurrent":4,"theme":"dark"}"#,
        )
        .unwrap();

        let loaded = load_settings(dir.path());
        assert!(
            loaded.oobe_completed,
            "legacy settings.json should self-heal to completed"
        );
        // New optional storage controls are intentionally unlimited/disabled
        // when an older settings file omits them.
        assert_eq!(loaded.max_download_file_size_bytes, None);
        assert_eq!(loaded.max_source_storage_bytes, None);
        assert_eq!(loaded.minimum_free_disk_bytes, None);
        assert_eq!(loaded.thumbnail_cache_max_bytes, None);
        assert!(!loaded.apply_download_limits_to_local_imports);
        assert_eq!(loaded.automatic_cleanup_mode, "never");
        assert_eq!(loaded.automatic_cleanup_low_disk_bytes, None);
        assert_eq!(loaded.archive_retention_days, None);

        // And the repair should have been persisted, not just held in memory.
        let raw = std::fs::read_to_string(settings_path(dir.path())).unwrap();
        let repaired: Settings = serde_json::from_str(&raw).unwrap();
        assert!(repaired.oobe_completed);
    }

    /// A genuinely fresh install (no settings.json on disk at all) should
    /// need OOBE.
    #[test]
    fn load_settings_defaults_oobe_incomplete_for_fresh_install() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = load_settings(dir.path());
        assert!(!loaded.oobe_completed);
    }

    /// A settings.json that already has the field (any prior OOBE run,
    /// complete or reset) must be respected as-is, not re-healed.
    #[test]
    fn load_settings_respects_explicit_oobe_completed_false() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            settings_path(dir.path()),
            r#"{"max_concurrent":4,"theme":"dark","oobe_completed":false}"#,
        )
        .unwrap();
        let loaded = load_settings(dir.path());
        assert!(!loaded.oobe_completed);
    }
}

#[cfg(test)]
mod rating_migration_tests {
    #[test]
    fn existing_human_provenance_survives_legacy_normalization() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        super::run_migrations(&conn).unwrap();
        conn.execute_batch("DELETE FROM _migrations WHERE name='0002_existing_ratings_are_automated';
            INSERT INTO sources(id,name,url,slug,added_at) VALUES(1,'test','test','test','2026');
            INSERT INTO media(id,source_id,filepath,filename,type,added_at,rating,auto_rating,auto_rating_score,rating_source,rating_reviewed,rating_reviewed_at)
            VALUES(1,1,'a','a','image','2026',3,4,0.72,'human',1,'2026');").unwrap();
        super::run_migrations(&conn).unwrap();
        let row: (i64,i64,Option<f64>,String,bool,Option<String>) = conn.query_row("SELECT rating,auto_rating,auto_rating_score,rating_source,rating_reviewed,rating_reviewed_at FROM media", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).unwrap();
        assert_eq!(
            row,
            (3, 4, Some(0.72), "human".into(), true, Some("2026".into()))
        );
        conn.execute_batch("UPDATE media SET rating=2,rating_source='human',rating_reviewed=1,rating_reviewed_at='2027';").unwrap();
        super::run_migrations(&conn).unwrap();
        assert!(conn
            .query_row("SELECT rating_reviewed FROM media", [], |r| r
                .get::<_, bool>(0))
            .unwrap());
    }

    #[test]
    fn legacy_ratings_are_preserved_and_migration_is_idempotent() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE media(id INTEGER PRIMARY KEY, source_id INTEGER NOT NULL, filepath TEXT UNIQUE NOT NULL, filename TEXT NOT NULL, type TEXT NOT NULL, added_at TEXT NOT NULL, rating INTEGER NOT NULL DEFAULT 0);
            INSERT INTO media VALUES(1,1,'a','a','image','2026',3),(2,1,'b','b','image','2026',0);").unwrap();
        super::run_migrations(&conn).unwrap();
        let row: (i64,String,bool,Option<String>) = conn.query_row("SELECT rating,rating_source,rating_reviewed,rating_reviewed_at FROM media WHERE id=1", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).unwrap();
        // Legacy 3-star automatic content is promoted to 4 stars so it
        // cannot silently become the new SFW 1-star class.
        assert_eq!(row, (4, "auto".into(), false, None));
        conn.execute(
            "UPDATE media SET rating=4,auto_rating=3,rating_source='human',rating_reviewed=1,rating_reviewed_at='2026' WHERE id=2",
            [],
        )
        .unwrap();
        super::run_migrations(&conn).unwrap();
        assert!(conn
            .query_row("SELECT rating_reviewed FROM media WHERE id=2", [], |r| r
                .get::<_, bool>(
                0
            ))
            .unwrap());
    }

    #[test]
    fn scale_migration_promotes_human_and_nested_automatic_values_without_loss() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        super::run_migrations(&conn).unwrap();
        conn.execute_batch("DELETE FROM _migrations WHERE name='0005_media_scale_and_classifier_provenance';
            INSERT INTO sources(id,name,url,slug,added_at) VALUES(1,'test','test','test','2026');
            INSERT INTO media(id,source_id,filepath,filename,type,added_at,downloaded,rating,auto_rating,human_rating,rating_source,rating_reviewed)
            VALUES(1,1,'a','a','image','2026',1,1,1,1,'human',1),
                   (2,1,'b','b','image','2026',1,5,5,NULL,'auto',0);").unwrap();
        super::run_migrations(&conn).unwrap();
        let human: (i64, i64, i64, String, bool, String) = conn.query_row(
            "SELECT rating,auto_rating,human_rating,rating_source,rating_reviewed,nsfw_state FROM media WHERE id=1",
            [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
        ).unwrap();
        assert_eq!(human, (2, 2, 2, "human".into(), true, "pending".into()));
        let five: (i64, i64, Option<i64>) = conn
            .query_row(
                "SELECT rating,auto_rating,human_rating FROM media WHERE id=2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(five, (5, 5, None));
    }
}

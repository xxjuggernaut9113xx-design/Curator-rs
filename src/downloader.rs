use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{Stream, StreamExt};
use rusqlite::TransactionBehavior;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tracing::{info, warn};

use crate::db::now_iso;
use crate::slug::pending_filepath;
use crate::AppState;

// ─── Extension sets ──────────────────────────────────────────────────────────

pub fn image_exts() -> &'static [&'static str] {
    &[
        "jpg", "jpeg", "png", "gif", "webp", "bmp", "jfif", "avif", "tiff",
    ]
}

pub fn video_exts() -> &'static [&'static str] {
    &["mp4", "webm", "mov", "avi", "mkv", "m4v"]
}

fn is_image_path(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    image_exts().contains(&ext.as_str())
}

fn is_video_path(path: &Path) -> bool {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    video_exts().contains(&ext.as_str())
}

// ─── gallery-dl stderr filter ────────────────────────────────────────────────

pub fn filter_gdl_stderr(raw: &str) -> String {
    raw.lines()
        .filter(|l| !l.contains("RequestsDependencyWarning"))
        .collect::<Vec<_>>()
        .join("\n")
}

// ─── Kill a process by PID ───────────────────────────────────────────────────
//
// Windows taskkill must include /T; killing only the parent leaves ffmpeg children alive.
pub async fn kill_pid(pid: u32) {
    if pid == 0 {
        return;
    }
    #[cfg(target_os = "windows")]
    {
        // Use the shared process seam: it hides taskkill's helper window and
        // kills taskkill itself if Windows stalls while walking descendants.
        let mut taskkill = crate::process::command("taskkill");
        taskkill
            .args(["/F", "/T", "/PID", &pid.to_string()])
            .kill_on_drop(true);
        let result = tokio::time::timeout(Duration::from_secs(5), taskkill.output()).await;
        match result {
            Ok(Ok(output)) if output.status.success() => {}
            Ok(Ok(output)) => warn!(
                "Process-tree termination for PID {pid} returned {}: {}",
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ),
            Ok(Err(e)) => warn!("Could not terminate process tree for PID {pid}: {e}"),
            Err(_) => warn!("Process-tree termination for PID {pid} timed out"),
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        unsafe {
            libc::kill(pid as i32, libc::SIGTERM);
        }
    }
}

// ─── spawn_gallery_dl — shared subprocess-streaming primitive ────────────────
//
// Both `populate_placeholders` and `/api/preview/scan` (live browse) go
// through this single entry point, matching the Tier 1 contract: one place
// that spawns gallery-dl, registers/deregisters its PID, and drains stderr
// concurrently with stdout so a chatty extractor can't deadlock the pipe
// (stdout fills → gallery-dl blocks writing → if we're not *also* reading
// stderr at the same time and its OS pipe buffer fills, gallery-dl blocks
// on that too, and everything hangs forever).
//
// A note on the stream shape: `gallery-dl -j` does not emit newline-delimited
// JSON the way some other gallery-dl invocations do — it collects every
// discovered item internally and prints the whole thing as a single JSON
// array only once, at the end of the run. There is no way to get
// item-by-item output any earlier than that without patching gallery-dl
// itself. So this function reads stdout to completion, parses it as one
// JSON value, and yields the top-level array's elements one at a time —
// each element is one `[type, url, metadata]` entry, exactly the shape
// `preview_walk` already expects. This still gives real value on the SSE
// side (see routes/preview.rs): the HTTP connection stays open with
// keep-alive pings while gallery-dl runs, and once the result lands, items
// stream to the client one event at a time instead of one giant response
// the browser has to deserialize all at once.
pub fn spawn_gallery_dl(
    args: Vec<String>,
    source_id: Option<i64>,
    state: Arc<AppState>,
) -> impl Stream<Item = Result<Value>> {
    async_stream::stream! {
        let mut cmd = crate::process::command(&state.gallery_dl_bin);
        cmd.args(&args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped()).kill_on_drop(true);

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                yield Err(anyhow::anyhow!(
                    "gallery-dl could not be launched: {}. Is it on your PATH?", e
                ));
                return;
            }
        };

        let pid = child.id();
        if let (Some(id), Some(pid)) = (source_id, pid) {
            state.active_processes.lock().await.insert(id, pid);
        }

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        // Drain stderr concurrently on its own task so it can never back up
        // and block the process while we're reading stdout.
        let stderr_handle = tokio::spawn(async move {
            drain_tail(stderr).await
        });

        let output_task = tokio::spawn(async move {
            let mut raw=String::new();
            let result=BufReader::new(stdout).take(16*1024*1024+1).read_to_string(&mut raw).await;
            (raw,result)
        });
        let stopped = tokio::select! {
            _ = state.shutdown.cancelled() => true,
            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => true,
            _ = child.wait() => false,
        };
        if stopped {
            if let Some(pid)=child.id() { kill_pid(pid).await; }
            let _=child.kill().await;
            let _=child.wait().await;
        }
        let (raw,_) = output_task.await.unwrap_or_else(|_| (String::new(), Ok(0)));
        if stopped || raw.len()>16*1024*1024 {
            let _=stderr_handle.await;
            yield Err(anyhow::anyhow!("Listing stopped (30s deadline, cancellation, or 16 MiB output limit)"));
            return;
        }
        if let (Some(id), Some(_)) = (source_id, pid) {
            state.active_processes.lock().await.remove(&id);
        }

        let stderr_text = stderr_handle.await.unwrap_or_default();
        let raw = raw.trim();

        if raw.is_empty() {
            let err = filter_gdl_stderr(&stderr_text);
            let err = if err.is_empty() {
                "gallery-dl returned nothing. Is the URL supported / does the extractor need login cookies?".to_string()
            } else {
                let tail: String = err.chars().rev().take(1500).collect::<String>().chars().rev().collect();
                tail
            };
            yield Err(anyhow::anyhow!(err));
            return;
        }

        match serde_json::from_str::<Value>(raw) {
            Ok(Value::Array(items)) => {
                for item in items {
                    yield Ok(item);
                }
            }
            Ok(other) => {
                yield Ok(other);
            }
            Err(e) => {
                yield Err(anyhow::anyhow!("Could not parse gallery-dl output: {}", e));
            }
        }
    }
}

// ─── Preview walk — extract file entries from gallery-dl -j output ───────────

#[derive(serde::Serialize, Debug, Clone)]
pub struct PreviewItem {
    pub url: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub creator: String,
    pub title: String,
    pub poster: Option<String>,
    pub source: String,
    /// gallery-dl exposes a size for some extractors during `-j` listing.
    /// It lets Curator mark an oversized item before a download is attempted;
    /// absent metadata still falls back to gallery-dl's --filesize-max guard.
    pub remote_size_bytes: Option<u64>,
}

fn preview_size_bytes(meta: Option<&serde_json::Map<String, Value>>) -> Option<u64> {
    let value = meta?
        .get("filesize")
        .or_else(|| meta?.get("file_size"))
        .or_else(|| meta?.get("size"))?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        .or_else(|| {
            value
                .as_str()
                .and_then(|number| number.trim().parse::<u64>().ok())
        })
}

pub fn preview_ext_from(meta: &Value, url: &str) -> String {
    if let Some(ext) = meta.get("extension").and_then(|v| v.as_str()) {
        if !ext.is_empty() {
            return ext.to_lowercase();
        }
    }
    let tail = url.split('?').next().unwrap_or(url);
    let tail = tail.rsplit('/').next().unwrap_or("");
    if tail.contains('.') {
        tail.rsplit('.').next().unwrap_or("").to_lowercase()
    } else {
        String::new()
    }
}

pub fn preview_walk(
    node: &Value,
    source_url: &str,
    results: &mut Vec<PreviewItem>,
    seen: &mut std::collections::HashSet<String>,
) {
    let arr = match node.as_array() {
        Some(a) => a,
        None => return,
    };

    // gallery-dl -j produces arrays: [type, url, metadata_dict]
    if arr.len() >= 2 {
        if let Some(url_str) = arr[1].as_str() {
            if url_str.starts_with("http") {
                let meta = arr.last().and_then(|v| v.as_object());
                let meta_val = arr.last().cloned().unwrap_or(Value::Null);
                let ext = preview_ext_from(&meta_val, url_str);

                let kind = if image_exts().contains(&ext.as_str()) {
                    Some("image")
                } else if video_exts().contains(&ext.as_str()) {
                    Some("video")
                } else {
                    None
                };

                if let Some(kind) = kind {
                    if !seen.contains(url_str) {
                        seen.insert(url_str.to_string());

                        let creator = meta
                            .and_then(|m| {
                                m.get("username")
                                    .or_else(|| m.get("author"))
                                    .or_else(|| m.get("user"))
                                    .or_else(|| m.get("artist"))
                            })
                            .and_then(|v| v.as_str())
                            .unwrap_or(source_url)
                            .to_string();

                        let title = meta
                            .and_then(|m| {
                                m.get("title")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string())
                                    .or_else(|| m.get("id").map(|v| v.to_string()))
                            })
                            .unwrap_or_default();

                        let poster = meta
                            .and_then(|m| m.get("thumbnail").or_else(|| m.get("preview")))
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let remote_size_bytes = preview_size_bytes(meta);

                        results.push(PreviewItem {
                            url: url_str.to_string(),
                            kind: kind.to_string(),
                            creator,
                            title,
                            poster,
                            source: source_url.to_string(),
                            remote_size_bytes,
                        });
                    }
                }
            }
        }
    }

    // Recurse into nested arrays
    for item in arr {
        if item.is_array() {
            preview_walk(item, source_url, results, seen);
        }
    }
}

// ─── collect_gallery_dl_items ─────────────────────────────────────────────────
// Non-streaming convenience wrapper over spawn_gallery_dl for callers (namely
// populate_placeholders) that just want the final Vec<PreviewItem> with a
// timeout, and don't care about incremental delivery the way the SSE preview
// endpoint does.

pub async fn collect_gallery_dl_items(
    url: &str,
    state: Arc<AppState>,
) -> std::result::Result<Vec<PreviewItem>, String> {
    let args = vec![
        "-j".into(),
        "--no-download".into(),
        "--range".into(),
        "1-1000".into(),
        url.to_string(),
    ];
    let url_owned = url.to_string();

    let collect_fut = async move {
        let stream = spawn_gallery_dl(args, None, state);
        futures::pin_mut!(stream);

        let mut items: Vec<PreviewItem> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut first_err: Option<String> = None;

        while let Some(node_result) = stream.next().await {
            match node_result {
                Ok(node) => preview_walk(&node, &url_owned, &mut items, &mut seen),
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e.to_string());
                    }
                }
            }
        }

        (items, first_err)
    };

    let (items, first_err) = collect_fut.await;
    if items.is_empty() {
        if let Some(e) = first_err {
            return Err(e);
        }
    }
    Ok(items)
}

// ─── scan_and_index ───────────────────────────────────────────────────────────
/// Probe on a blocking thread with a five-second deadline. Indexing itself does
/// not probe; the bounded duration backfill persists the result or failure.
pub(crate) fn probe_video_duration(ffprobe_bin: &str, path: &Path) -> Option<f64> {
    if !path.is_file() {
        return None;
    }
    let output = crate::process::output_timeout(
        crate::process::blocking_command(ffprobe_bin)
            .args([
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
            ])
            .arg(path),
        std::time::Duration::from_secs(5),
    )
    .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|d| d.is_finite() && *d >= 0.0)
}

pub fn index_file(state: &AppState, source_id: i64, path: &Path) -> Result<bool> {
    use rusqlite::OptionalExtension;
    let path = dunce::simplified(path);
    if !path.is_file() || (!is_image_path(path) && !is_video_path(path)) {
        return Ok(false);
    }
    // gallery-dl writes .part files and atomically renames them on completion.
    let mut part = path.as_os_str().to_os_string();
    part.push(".part");
    if Path::new(&part).exists() {
        return Ok(false);
    }
    let rel = path
        .strip_prefix(dunce::simplified(&state.library_dir))?
        .to_string_lossy()
        .replace('\\', "/");
    let stamp = crate::media_files::stamp(path);
    let mut sidecar = path.as_os_str().to_os_string();
    sidecar.push(".json");
    // A file can arrive before gallery-dl completes its sidecar. Retain the
    // whole document, rather than only its URL, so a later sidecar event can
    // atomically add provenance without a second independent database write.
    let sidecar_metadata = std::fs::read_to_string(Path::new(&sidecar))
        .ok()
        .and_then(|raw| serde_json::from_str::<Value>(&raw).ok());
    let origin: Option<String> = sidecar_metadata
        .as_ref()
        .and_then(|v| v.get("url").and_then(Value::as_str).map(str::to_owned));
    let mut conn = state.pool.get()?;
    let existing: Option<(i64, Option<String>, Option<String>, bool)> = conn
        .query_row(
            "SELECT id, file_stamp, origin_url, downloaded FROM media WHERE filepath=?1",
            [&rel],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    if existing
        .as_ref()
        .is_some_and(|r| r.1 == stamp && r.3 && (origin.is_none() || r.2 == origin))
    {
        // The file row itself is current, but metadata-only filesystem events
        // must still be useful. BEGIN IMMEDIATE obtains the sole SQLite writer
        // lease before the read/modify/write work below, which avoids the
        // immediate SQLITE_BUSY failure a deferred transaction can get while
        // trying to upgrade a stale read snapshot.
        if let (Some(metadata), Some((media_id, _, _, _))) =
            (sidecar_metadata.as_ref(), existing.as_ref())
        {
            let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if let Err(error) =
                capture_sidecar_metadata(&tx, *media_id, origin.as_deref(), metadata)
            {
                // Sidecar evidence is supplementary: a malformed extractor
                // document must not make a completed file disappear.
                warn!("Could not retain late source metadata for media {media_id}: {error}");
            }
            tx.commit()?;
        }
        return Ok(false);
    }
    // Claim the writer before inspecting possible placeholder rows. A
    // deferred transaction can read while another indexer owns the writer,
    // then fail immediately when it tries to upgrade; the pool's busy timeout
    // is honored when BEGIN IMMEDIATE waits for that handoff instead.
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // A metadata event can follow the file event. Merge an early real row into
    // its placeholder, retaining the placeholder ID, ratings and both tag sets.
    if let (Some((real_id, _, _, _)), Some(url)) = (&existing, &origin) {
        let placeholder: Option<i64> = tx
            .query_row(
                "SELECT id FROM media WHERE source_id=?1 AND origin_url=?2 AND id<>?3",
                rusqlite::params![source_id, url, real_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(id) = placeholder {
            tx.execute("INSERT OR IGNORE INTO media_tags SELECT ?1, tag_id FROM media_tags WHERE media_id=?2", rusqlite::params![id,real_id])?;
            tx.execute("UPDATE media SET (rating,rating_source,rating_reviewed,rating_reviewed_at)=
                (SELECT rating,rating_source,rating_reviewed,rating_reviewed_at FROM media WHERE id=?2)
                WHERE id=?1 AND rating_reviewed=0 AND (rating=0 OR (SELECT rating_reviewed FROM media WHERE id=?2)=1)", rusqlite::params![id,real_id])?;
            tx.execute(
                "UPDATE media SET (auto_rating,auto_rating_score)=
                (SELECT auto_rating,auto_rating_score FROM media WHERE id=?2)
                WHERE id=?1 AND auto_rating=0",
                rusqlite::params![id, real_id],
            )?;
            tx.execute("DELETE FROM media WHERE id=?1", [real_id])?;
        }
    }
    let kind = if is_video_path(path) {
        "video"
    } else {
        "image"
    };
    tx.execute("INSERT INTO media(source_id,filepath,filename,type,added_at,origin_url,downloaded,file_stamp)
        VALUES(?1,?2,?3,?4,?5,?6,1,?7)
        ON CONFLICT(source_id,origin_url) WHERE origin_url IS NOT NULL DO UPDATE SET
          filepath=excluded.filepath, filename=excluded.filename, downloaded=1, missing=0,
          file_stamp=excluded.file_stamp, modified_at=COALESCE(excluded.modified_at,media.modified_at),
          downloaded_at=CASE WHEN media.downloaded=0 OR media.missing=1 THEN excluded.downloaded_at ELSE media.downloaded_at END,
          skip_reason=NULL, skip_limit_bytes=NULL, skipped_at=NULL, retention_deleted=0,
          nsfw_state='pending', nsfw_attempts=0, nsfw_retry_at=0, duration_attempted=0,
          action_rating=0, action_model=NULL, action_model_version=NULL, action_score=NULL, action_evidence=NULL,
          classifier_model=NULL, classifier_version=NULL, classifier_score=NULL, classifier_evidence=NULL,
          classification_label='unclassified', manual_review_required=0, manual_review_reason=NULL,
          duration_secs=CASE WHEN media.file_stamp=excluded.file_stamp THEN media.duration_secs ELSE NULL END
        ON CONFLICT(filepath) DO UPDATE SET downloaded=1, missing=0, file_stamp=excluded.file_stamp,
          origin_url=COALESCE(excluded.origin_url,media.origin_url), nsfw_state='pending', nsfw_attempts=0,
          skip_reason=NULL, skip_limit_bytes=NULL, skipped_at=NULL, retention_deleted=0,
          action_rating=0, action_model=NULL, action_model_version=NULL, action_score=NULL, action_evidence=NULL,
          classifier_model=NULL, classifier_version=NULL, classifier_score=NULL, classifier_evidence=NULL,
          classification_label='unclassified', manual_review_required=0, manual_review_reason=NULL,
          nsfw_retry_at=0, duration_attempted=0, duration_secs=CASE WHEN media.file_stamp=excluded.file_stamp THEN media.duration_secs ELSE NULL END",
        rusqlite::params![source_id,rel,path.file_name().unwrap_or_default().to_string_lossy(),kind,now_iso(),origin,stamp])?;
    if let Some(metadata) = sidecar_metadata.as_ref() {
        let media_id: i64 =
            tx.query_row("SELECT id FROM media WHERE filepath=?1", [&rel], |row| {
                row.get(0)
            })?;
        if let Err(error) = capture_sidecar_metadata(&tx, media_id, origin.as_deref(), metadata) {
            // Preserve a successfully indexed file if an optional extractor
            // payload is malformed. The helper rolls back just its savepoint.
            warn!("Could not retain source metadata for media {media_id}: {error}");
        }
    }
    tx.commit()?;
    // Completion derives from real indexed files, not parser/log guesses.
    // A source with no successful placeholder listing keeps known_total NULL,
    // which is intentionally rendered as indeterminate progress.
    let _ = conn.execute(
        "UPDATE sources SET completed_count=(SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=1 AND missing=0),current_filename=?2,progress_updated_at=?3 WHERE id=?1",
        rusqlite::params![source_id, path.file_name().unwrap_or_default().to_string_lossy(), now_iso()],
    );
    // Keep sidecars: restart recovery and late metadata events need their URL.
    Ok(true)
}

/// Capture optional extractor evidence without allowing one malformed sidecar
/// to roll back the media row that was just indexed. Keeping it inside the
/// caller's immediate transaction also makes the media upsert, provenance,
/// and source-tag candidates visible together.
fn capture_sidecar_metadata(
    tx: &rusqlite::Transaction<'_>,
    media_id: i64,
    source_url: Option<&str>,
    metadata: &Value,
) -> Result<()> {
    tx.execute_batch("SAVEPOINT capture_sidecar_metadata")?;
    match crate::provenance::capture_source_metadata(tx, media_id, source_url, metadata) {
        Ok(()) => tx.execute_batch("RELEASE SAVEPOINT capture_sidecar_metadata")?,
        Err(error) => {
            // Roll back every statement from the optional capture (including
            // any partially inserted tag candidate) before committing the
            // primary media transaction.
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT capture_sidecar_metadata;
                 RELEASE SAVEPOINT capture_sidecar_metadata;",
            )?;
            return Err(error);
        }
    }
    Ok(())
}

pub fn scan_and_index(state: &AppState, source_id: i64, dest: &Path) -> Result<(i64, i64)> {
    let mut added = 0;
    for entry in walkdir::WalkDir::new(dest).into_iter() {
        let entry = entry?;
        if entry.file_type().is_file() && index_file(state, source_id, entry.path())? {
            added += 1;
        }
    }
    let conn = state.pool.get()?;
    let total = conn.query_row(
        "SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=1",
        [source_id],
        |r| r.get(0),
    )?;
    conn.execute(
        "UPDATE sources SET item_count=?1,completed_count=?1,progress_updated_at=?3 WHERE id=?2",
        rusqlite::params![total, source_id, now_iso()],
    )?;
    Ok((total, added))
}

async fn pause_after_active_storage_limit(
    state: &Arc<AppState>,
    source_id: i64,
    destination: &Path,
) {
    if state.storage_pauses.lock().await.contains_key(&source_id) {
        return;
    }
    let Some(slug) = destination
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_owned)
    else {
        return;
    };
    let settings = state.settings.read().await.clone();
    let check_state = state.clone();
    let pause = tokio::task::spawn_blocking(move || {
        crate::storage::active_sync_pause(
            &settings,
            &check_state.data_dir,
            &check_state.library_dir,
            &slug,
            true,
        )
    })
    .await
    .ok()
    .flatten();
    let Some(pause) = pause else { return };
    state
        .storage_pauses
        .lock()
        .await
        .insert(source_id, pause.clone());
    if let Some(pid) = state.active_processes.lock().await.get(&source_id).copied() {
        kill_pid(pid).await;
    }
}

/// A single consumer serializes event batches, recovery scans and the final scan.
async fn index_download(
    state: Arc<AppState>,
    source_id: i64,
    dest: PathBuf,
    done: tokio_util::sync::CancellationToken,
) {
    use notify::Watcher;
    use std::sync::atomic::{AtomicBool, Ordering};
    let (tx, mut rx) = tokio::sync::mpsc::channel::<PathBuf>(1024);
    let dirty = Arc::new(AtomicBool::new(false));
    let dirty_event = dirty.clone();
    let mut watcher =
        notify::recommended_watcher(move |event: notify::Result<notify::Event>| match event {
            Ok(e) => {
                if e.need_rescan() {
                    dirty_event.store(true, Ordering::Relaxed);
                }
                if matches!(e.kind, notify::EventKind::Access(_)) {
                    return;
                }
                for path in e.paths {
                    if tx.try_send(path).is_err() {
                        dirty_event.store(true, Ordering::Relaxed);
                    }
                }
            }
            Err(_) => dirty_event.store(true, Ordering::Relaxed),
        })
        .and_then(|mut w| {
            w.watch(&dest, notify::RecursiveMode::Recursive)?;
            Ok(w)
        })
        .ok();
    if watcher.is_none() {
        warn!(
            "File notifications unavailable for source {source_id}; using 5-minute recovery scans"
        );
    }
    let mut recovery = tokio::time::interval(std::time::Duration::from_secs(300));
    loop {
        let mut paths = std::collections::HashSet::new();
        let final_scan;
        let scan;
        tokio::select! {
            _ = done.cancelled() => { final_scan=true; scan=true; }
            _ = recovery.tick() => { final_scan=false; scan=true; }
            path = rx.recv(), if watcher.is_some() => {
                final_scan=false; scan=false;
                if let Some(p)=path { paths.insert(p); }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                for _ in 0..1024 { if let Ok(p)=rx.try_recv() { paths.insert(p); } else { break; } }
            }
        }
        // Overflow recovers on the next scheduled scan, never a continuous scan storm.
        if scan && dirty.swap(false, Ordering::Relaxed) {
            info!("Recovering missed filesystem events for source {source_id}");
        }
        // Each filesystem batch can write media/source rows. Move the lease
        // into the blocking closure so it remains held even if the async
        // watcher is cancelled while that closure is still finishing.
        let Some(worker_lease) = state.maintenance.try_acquire_background_worker() else {
            if final_scan {
                watcher.take();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        let s = state.clone();
        let d = dest.clone();
        let settings = state.settings.read().await.clone();
        let result=tokio::task::spawn_blocking(move || -> Result<(bool, Option<crate::storage::SyncPause>)> {
            let _worker_lease = worker_lease;
            // Check directly after each newly indexed remote file instead of
            // after an event batch. This keeps a source quota bounded to one
            // file beyond its allowance even when the watcher coalesces many
            // filesystem notifications.
            let candidates: Vec<PathBuf> = if scan {
                walkdir::WalkDir::new(&d)
                    .into_iter()
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| entry.file_type().is_file())
                    .map(|entry| entry.path().to_path_buf())
                    .collect()
            } else {
                paths.into_iter().collect()
            };
            let mut indexed_any = false;
            for mut path in candidates {
                if path.extension().is_some_and(|e| e=="json") { path.set_extension(""); }
                if path.is_file() {
                    let indexed = index_file(&s,source_id,&path)?;
                    indexed_any |= indexed;
                    if indexed {
                        let slug = d.file_name().and_then(|value| value.to_str()).unwrap_or_default();
                        if let Some(pause) = crate::storage::active_sync_pause(
                            &settings, &s.data_dir, &s.library_dir, slug, true,
                        ) {
                            return Ok((indexed_any, Some(pause)));
                        }
                    }
                } else if let Ok(rel)=path.strip_prefix(&s.library_dir) {
                    let conn=s.pool.get()?;
                    conn.execute("UPDATE media SET missing=1,downloaded=0,nsfw_state='missing' WHERE filepath=?1 AND downloaded=1",[rel.to_string_lossy().replace('\\',"/")])?;
                }
            }
            Ok((indexed_any, None))
        }).await;
        match result {
            Ok(Ok((indexed_any, forced_pause))) => {
                if let Some(pause) = forced_pause {
                    state.storage_pauses.lock().await.insert(source_id, pause);
                    if let Some(pid) = state.active_processes.lock().await.get(&source_id).copied()
                    {
                        kill_pid(pid).await;
                    }
                } else if indexed_any {
                    pause_after_active_storage_limit(&state, source_id, &dest).await;
                }
            }
            Ok(Err(e)) => warn!("Indexing source {source_id} failed: {e}"),
            Err(e) => warn!("Indexing source {source_id} worker failed: {e}"),
        }
        if scan {
            // Even a scan that takes longer than the interval must leave a quiet gap.
            recovery.reset_after(std::time::Duration::from_secs(300));
        }
        if final_scan {
            watcher.take();
            break;
        }
    }
}

// ─── populate_placeholders ───────────────────────────────────────────────────
//
// Pre-scans a source via `gallery-dl -j` and inserts downloaded=0 placeholder
// rows so the UI can show "coming soon" tiles before the real download
// finishes. Its origin-URL upsert refreshes only an undownloaded placeholder:
// if a real download has already retired that URL into a downloaded row, the
// `WHERE media.downloaded=0` guard leaves it untouched. This lets a later
// larger size limit clear an old skip reason without ever regressing a real
// file back to a placeholder.

pub async fn populate_placeholders(state: Arc<AppState>, source_id: i64) {
    let (url, status) = {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };
        match conn.query_row(
            "SELECT url, status FROM sources WHERE id=?1",
            [source_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        ) {
            Ok(v) => v,
            Err(_) => return,
        }
    };

    if status == "done" {
        return;
    }

    // Guard: if any real file predates origin_url tracking, skip to avoid duplicates
    let unmatched_real: i64 = {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.query_row(
            "SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=1 AND origin_url IS NULL",
            [source_id],
            |r| r.get(0),
        )
        .unwrap_or(0)
    };
    if unmatched_real > 0 {
        return;
    }

    // Rate-limit placeholder scans (3 concurrent max)
    let _permit = tokio::select! {
        _=state.shutdown.cancelled()=>return,
        p=state.placeholder_semaphore.acquire()=>match p {Ok(p)=>p,Err(_)=>return},
    };

    if state.shutdown.is_cancelled() {
        return;
    }
    // Claiming a listing slot is a durable write. The long gallery-dl call
    // below is deliberately outside this short lease so maintenance can
    // cancel the owning download rather than wait for a network listing.
    let Some(claim_lease) = state.maintenance.try_acquire_background_worker() else {
        return;
    };
    let claimed = {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };
        conn.execute(
            "INSERT INTO placeholder_scans(source_id,url,retry_at) VALUES(?1,?2,unixepoch()+21600)
          ON CONFLICT(source_id) DO UPDATE SET url=excluded.url,retry_at=excluded.retry_at
          WHERE placeholder_scans.url<>excluded.url OR placeholder_scans.retry_at<=unixepoch()",
            rusqlite::params![source_id, url],
        )
        .unwrap_or(0)
            > 0
    };
    drop(claim_lease);
    if !claimed {
        return;
    }
    let items = match collect_gallery_dl_items(&url, Arc::clone(&state)).await {
        Ok(items) => items,
        Err(err) => {
            info!(
                "Placeholder pre-scan skipped for source {}: {}",
                source_id, err
            );
            return;
        }
    };
    if items.is_empty() {
        return;
    }
    let file_size_limit = state.settings.read().await.max_download_file_size_bytes;

    // The listing finished outside maintenance. Re-admit its transactional
    // result only if no Admin job has begun in the meantime.
    let Some(_persist_lease) = state.maintenance.try_acquire_background_worker() else {
        return;
    };
    let mut conn = match state.pool.get() {
        Ok(c) => c,
        Err(_) => return,
    };
    let now = now_iso();

    let tx = match conn.transaction_with_behavior(TransactionBehavior::Immediate) {
        Ok(tx) => tx,
        Err(error) => {
            warn!("Placeholder pre-scan for source {source_id} could not start a transaction: {error}");
            return;
        }
    };
    {
        let mut stmt = match tx.prepare(
            "INSERT INTO media (source_id, filepath, filename, type, added_at, origin_url, downloaded,file_size_bytes,skip_reason,skip_limit_bytes,skipped_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?9, ?10)
             ON CONFLICT(source_id,origin_url) WHERE origin_url IS NOT NULL DO UPDATE SET
               file_size_bytes=COALESCE(excluded.file_size_bytes,media.file_size_bytes),
               skip_reason=excluded.skip_reason,skip_limit_bytes=excluded.skip_limit_bytes,
               skipped_at=excluded.skipped_at
             WHERE media.downloaded=0"
        ) {
            Ok(statement) => statement,
            Err(error) => {
                warn!("Placeholder pre-scan for source {source_id} could not prepare inserts: {error}");
                return;
            }
        };

        for item in &items {
            let fname = item
                .url
                .split('?')
                .next()
                .unwrap_or(&item.url)
                .rsplit('/')
                .next()
                .unwrap_or("item")
                .to_string();
            let fname = if fname.is_empty() {
                "item".to_string()
            } else {
                fname
            };
            let fp = pending_filepath(source_id, &item.url);
            let oversized = file_size_limit
                .zip(item.remote_size_bytes)
                .filter(|(limit, size)| size > limit);
            let skip_reason = oversized.map(|(limit, size)| {
                format!(
                    "Skipped: {} is larger than the {} download-size limit.",
                    crate::storage::human_bytes(size),
                    crate::storage::human_bytes(limit)
                )
            });
            if let Err(error) = stmt.execute(rusqlite::params![
                source_id,
                fp,
                fname,
                item.kind,
                now,
                item.url,
                item.remote_size_bytes,
                skip_reason,
                oversized.map(|(limit, _)| limit),
                if oversized.is_some() {
                    Some(now.as_str())
                } else {
                    None
                }
            ]) {
                // This scan is intentionally best-effort; one malformed
                // listing item must not hide every other placeholder.
                warn!("Placeholder pre-scan for source {source_id} skipped one item: {error}");
            }
        }
    }
    if let Err(error) = tx.commit() {
        warn!("Placeholder pre-scan for source {source_id} could not commit: {error}");
        return;
    }
    if let Ok(conn) = state.pool.get() {
        let _ = conn.execute(
            "UPDATE sources SET known_total=?1,progress_updated_at=?2 WHERE id=?3",
            rusqlite::params![items.len() as i64, now_iso(), source_id],
        );
    }

    info!(
        "Placeholder pre-scan for source {}: {} candidate item(s)",
        source_id,
        items.len()
    );
}

// ─── run_download ─────────────────────────────────────────────────────────────

/// Keep the gallery-dl contract in one testable place. Passing no size limit
/// preserves every existing installation's historical unlimited behavior.
pub fn gallery_dl_download_args(
    url: &str,
    destination: &Path,
    archive: &Path,
    max_file_size_bytes: Option<u64>,
) -> Vec<String> {
    let mut args = vec![
        url.to_string(),
        "-D".into(),
        destination.to_string_lossy().to_string(),
        "--download-archive".into(),
        archive.to_string_lossy().to_string(),
        "--write-metadata".into(),
    ];
    if let Some(maximum) = max_file_size_bytes.filter(|maximum| *maximum > 0) {
        args.push("--filesize-max".into());
        args.push(maximum.to_string());
    }
    args
}

pub fn run_download(
    state: Arc<AppState>,
    source_id: i64,
) -> impl std::future::Future<Output = ()> + Send {
    let token = state.download_tasks.token();
    async move {
        let _token = token;
        // A source owns both files and database rows for its whole run. The
        // lease closes the gap between a queued task waking and maintenance
        // pausing it; once maintenance flips active, newly queued sources do
        // not begin mutating state.
        let Some(_maintenance_lease) = state.maintenance.try_acquire_background_worker() else {
            return;
        };
        if state.shutdown.is_cancelled() || !state.running_sources.lock().await.insert(source_id) {
            return;
        }
        let cancel = tokio_util::sync::CancellationToken::new();
        state
            .source_cancellations
            .lock()
            .await
            .insert(source_id, cancel.clone());
        run_download_impl(state.clone(), source_id, cancel).await;
        state.source_cancellations.lock().await.remove(&source_id);
        state.running_sources.lock().await.remove(&source_id);
    }
}

async fn run_download_impl(
    state: Arc<AppState>,
    source_id: i64,
    cancel: tokio_util::sync::CancellationToken,
) {
    // Fire placeholder scan concurrently — never gates the real download
    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2",
                rusqlite::params![now_iso(), source_id],
            );
        }
        return;
    }

    // Rate-limited providers remain queued without taking a gallery-dl permit.
    // Local folder imports do not use gallery-dl and therefore have no remote
    // provider cooldown to observe.
    let provider = state.pool.get().ok().and_then(|conn| {
        conn.query_row(
            "SELECT url,slug,storage_override_once FROM sources WHERE id=?1",
            [source_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .ok()
    });
    let Some((provider_url, provider_slug, permit_once)) = provider else {
        return;
    };
    let is_local_import = provider_url.starts_with("local:");
    let settings = state.settings.read().await.clone();
    let apply_remote_limits = !is_local_import || settings.apply_download_limits_to_local_imports;
    let admission_state = state.clone();
    let admission_settings = settings.clone();
    let admission_slug = provider_slug.clone();
    let admission = tokio::task::spawn_blocking(move || {
        crate::storage::source_sync_admission(
            &admission_settings,
            &admission_state.data_dir,
            &admission_state.library_dir,
            &admission_slug,
            apply_remote_limits,
        )
    })
    .await
    .ok()
    .flatten();
    let permit_can_bypass = permit_once
        && matches!(
            &admission,
            Some(crate::storage::SyncPause::SourceQuota { .. })
        );
    if let Some(pause) = admission {
        if !permit_can_bypass {
            if let Ok(conn) = state.pool.get() {
                let _ = conn.execute(
                    "UPDATE sources SET status=?1,error_message=?2,progress_updated_at=?3,current_filename=NULL WHERE id=?4",
                    rusqlite::params![pause.status(), pause.message(), now_iso(), source_id],
                );
            }
            return;
        }
    }
    if permit_can_bypass {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET storage_override_once=0 WHERE id=?1",
                [source_id],
            );
        }
    }
    state.storage_pauses.lock().await.remove(&source_id);

    // A remote pre-scan records any known oversized items as durable
    // placeholders before gallery-dl starts. Local imports do not invoke it.
    if !is_local_import {
        let placeholder_state = Arc::clone(&state);
        state
            .download_tasks
            .spawn(async move { populate_placeholders(placeholder_state, source_id).await });
    }
    if !is_local_import {
        let provider = provider_key(&provider_url);
        if !wait_for_provider_cooldown(&state, &provider, &cancel).await {
            if let Ok(conn) = state.pool.get() {
                let _ = conn.execute(
                    "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2",
                    rusqlite::params![now_iso(), source_id],
                );
            }
            return;
        }
    }

    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2",
                rusqlite::params![now_iso(), source_id],
            );
        }
        return;
    }

    // Claim status NOW (before the semaphore wait) to avoid concurrent duplicate downloads
    {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET status='downloading', error_message=NULL,started_at=COALESCE(started_at,?1),progress_updated_at=?1,current_filename=NULL WHERE id=?2",
                rusqlite::params![now_iso(), source_id],
            );
        } else {
            warn!("Source {source_id} could not claim downloading status: database unavailable");
            return;
        }
    }

    let sem = {
        let guard = state.download_semaphore.lock().await;
        Arc::clone(&*guard)
    };
    let _permit = tokio::select! {
        _ = state.shutdown.cancelled() => {
            if let Ok(conn)=state.pool.get() { let _=conn.execute("UPDATE sources SET status='paused' WHERE id=?1",[source_id]); }
            return;
        }
        _ = cancel.cancelled() => return,
        permit = sem.acquire() => match permit { Ok(p)=>p,Err(_)=>return },
    };

    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2",
                rusqlite::params![now_iso(), source_id],
            );
        }
        return;
    }

    run_download_inner(Arc::clone(&state), source_id, cancel).await;
}

async fn run_download_inner(
    state: Arc<AppState>,
    source_id: i64,
    cancel: tokio_util::sync::CancellationToken,
) {
    let (url, slug, name, retry_attempts) = {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };
        match conn.query_row(
            "SELECT url, slug, name, retry_attempts FROM sources WHERE id=?1",
            [source_id],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            },
        ) {
            Ok(v) => v,
            Err(_) => return,
        }
    };

    let dest = dunce::simplified(&state.library_dir.join(&slug)).to_path_buf();
    let archive_path =
        dunce::simplified(&state.archives_dir.join(format!("{}.sqlite3", slug))).to_path_buf();
    let _ = std::fs::create_dir_all(&dest);

    // Local folder sources use Curator's importer directly.  Route them
    // before spawning gallery-dl so a missing gallery-dl binary cannot block
    // local imports and no unused child process is created (or briefly
    // orphaned) for a source that never needs it.
    if let Some(folder) = url.strip_prefix("local:") {
        let folder = PathBuf::from(folder);
        let local_state = state.clone();
        let local_cancel = cancel.clone();
        let result = tokio::task::spawn_blocking(move || {
            crate::local_import::sync_folder_with_cancel(
                &local_state,
                source_id,
                &folder,
                Some(local_cancel),
            )
        })
        .await;
        if cancel.is_cancelled()
            || state
                .downloads_paused
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            state.paused_source_ids.lock().await.insert(source_id);
            if let Ok(conn) = state.pool.get() {
                let _ = conn.execute(
                    "UPDATE sources SET status='paused',error_message='Import paused; resume to continue',progress_updated_at=?1 WHERE id=?2",
                    rusqlite::params![now_iso(), source_id],
                );
            }
        } else {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!("Local import task for source {source_id} did not complete: {error}");
                }
                Err(error) => {
                    warn!("Local import task for source {source_id} failed to join: {error}");
                }
            }
        }
        return;
    }

    let initial_count: i64 = state
        .pool
        .get()
        .ok()
        .and_then(|c| {
            c.query_row(
                "SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=1",
                [source_id],
                |r| r.get(0),
            )
            .ok()
        })
        .unwrap_or(0);
    let initial_size_skip_count = oversized_skip_count(&state, source_id);
    info!("Starting sync for source {} ({}): {}", source_id, name, url);

    let max_file_size_bytes = state.settings.read().await.max_download_file_size_bytes;
    let args = gallery_dl_download_args(&url, &dest, &archive_path, max_file_size_bytes);

    let mut child = match crate::process::command(&state.gallery_dl_bin)
        .args(&args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            let msg = format!(
                "gallery-dl could not be launched: {}. Is it on your PATH?",
                e
            );
            let conn = state.pool.get().unwrap();
            let _ = conn.execute(
                "UPDATE sources SET status='error', error_message=?1, synced_at=?2 WHERE id=?3",
                rusqlite::params![msg, now_iso(), source_id],
            );
            return;
        }
    };

    // Keep the job guard alive for the whole download. On Windows it makes
    // every gallery-dl helper/ffmpeg descendant die with the source process,
    // including when the app is closed mid-transfer.
    #[cfg(windows)]
    let _process_tree_guard = child.id().and_then(crate::process::guard_process_tree);

    if let Some(pid) = child.id() {
        state.active_processes.lock().await.insert(source_id, pid);
    }
    if state
        .downloads_paused
        .load(std::sync::atomic::Ordering::SeqCst)
    {
        state.paused_source_ids.lock().await.insert(source_id);
        if let Some(pid) = child.id() {
            kill_pid(pid).await;
        }
    }
    let index_done = tokio_util::sync::CancellationToken::new();
    let mut idx_task = tokio::spawn(index_download(
        state.clone(),
        source_id,
        dest.clone(),
        index_done.clone(),
    ));

    // Drain stdout (keep last 500 lines)
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let mut lines_buf: std::collections::VecDeque<String> = std::collections::VecDeque::new();
    let mut stdout_reader = BufReader::new(stdout).lines();

    let mut stderr_task = tokio::spawn(async move { drain_tail(stderr).await });

    let mut stdout_task = tokio::spawn(async move {
        while let Ok(Some(line)) = stdout_reader.next_line().await {
            lines_buf.push_back(line);
            if lines_buf.len() > 500 {
                lines_buf.pop_front();
            }
        }
        lines_buf
    });
    let interrupted;
    let returncode;
    tokio::select! {
        _ = async { tokio::select! { _=state.shutdown.cancelled()=>{}, _=cancel.cancelled()=>{} } } => {
            interrupted=true;
            if let Some(pid)=child.id() { kill_pid(pid).await; }
            let _=child.kill().await;
            returncode=match tokio::time::timeout(Duration::from_secs(3), child.wait()).await {
                Ok(Ok(status)) => status.code().unwrap_or(-1),
                Ok(Err(_)) | Err(_) => -1,
            };
        }
        status=child.wait() => { interrupted=false; returncode=status.map(|s|s.code().unwrap_or(-1)).unwrap_or(-1); }
    }
    index_done.cancel();
    // A killed gallery-dl can leave pipe handles inherited by a descendant,
    // and a filesystem watcher can be mid-scan.  Shutdown must remain
    // bounded: recovery on the next run is safe because indexing is
    // idempotent and annotations live in the database.
    if interrupted {
        idx_task.abort();
        stdout_task.abort();
        stderr_task.abort();
    }
    let _ = tokio::time::timeout(Duration::from_secs(3), &mut idx_task).await;
    if !idx_task.is_finished() {
        idx_task.abort();
    }
    let lines_buf = match tokio::time::timeout(Duration::from_secs(3), &mut stdout_task).await {
        Ok(Ok(lines)) => lines,
        _ => {
            stdout_task.abort();
            std::collections::VecDeque::new()
        }
    };
    let log_text = lines_buf.into_iter().collect::<Vec<_>>().join("\n");
    let stderr_text = match tokio::time::timeout(Duration::from_secs(3), &mut stderr_task).await {
        Ok(Ok(text)) => text,
        _ => {
            stderr_task.abort();
            String::new()
        }
    };

    // Keep both diagnostic streams; cancellation comes from application state.
    let combined_text = if stderr_text.trim().is_empty() {
        log_text.clone()
    } else if log_text.is_empty() {
        stderr_text.clone()
    } else {
        format!("{}\n{}", log_text, stderr_text)
    };

    {
        let mut procs = state.active_processes.lock().await;
        procs.remove(&source_id);
    }

    let total: i64 = state
        .pool
        .get()
        .ok()
        .and_then(|c| {
            c.query_row(
                "SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=1",
                [source_id],
                |r| r.get(0),
            )
            .ok()
        })
        .unwrap_or(0);
    let new_count = (total - initial_count).max(0);

    let was_paused = {
        let ids = state.paused_source_ids.lock().await;
        ids.contains(&source_id)
    };
    let storage_pause = state.storage_pauses.lock().await.remove(&source_id);
    let skipped_for_size = oversized_skip_count(&state, source_id);
    let new_size_skips = (skipped_for_size - initial_size_skip_count).max(0);

    let mut status = if returncode == 0 { "done" } else { "error" };
    let mut delayed_retry: Option<Duration> = None;
    let mut error_msg: Option<String> = None;

    if let Some(pause) = storage_pause {
        status = pause.status();
        error_msg = Some(pause.message());
        info!(
            "Source {} ({}) paused by {} after {} new item(s), {} total",
            source_id, name, status, new_count, total
        );
    } else if was_paused {
        status = "paused";
        info!(
            "Source {} ({}) paused after {} new item(s), {} total",
            source_id, name, new_count, total
        );
    } else if interrupted {
        status = "paused";
        error_msg = Some(
            if cancel.is_cancelled() {
                "Cancelled by user; resync to continue."
            } else {
                "Interrupted by Curator shutdown; resync to continue."
            }
            .to_string(),
        );
        info!(
            "Source {} ({}) was interrupted after {} new item(s), {} total",
            source_id, name, new_count, total
        );
    } else if status == "error"
        && (new_size_skips > 0 || skipped_for_size > 0 && filesize_skip_output(&combined_text))
        && (filesize_skip_output(&combined_text)
            || (new_count == 0 && !retryable_failure(returncode, &combined_text)))
    {
        // gallery-dl generally exits successfully for filtered files, but
        // extractors differ. A size skip is a completed decision, never a
        // source failure or an infinite retry loop.
        status = "done";
        error_msg = Some(format!(
            "{} item(s) skipped by the download-size limit; raise or remove the limit and re-sync to download them.",
            skipped_for_size
        ));
    } else if status == "error" {
        let summary = short_error_summary(&combined_text);
        let detail = if summary.is_empty() {
            combined_text
                .chars()
                .rev()
                .take(4000)
                .collect::<String>()
                .chars()
                .rev()
                .collect()
        } else {
            summary
        };
        let is_rate_limited = rate_limited(&combined_text);
        if retryable_failure(returncode, &combined_text) && retry_attempts < MAX_TRANSIENT_RETRIES {
            let next_attempt = retry_attempts + 1;
            let delay = retry_delay(next_attempt, is_rate_limited);
            status = "retrying";
            error_msg = Some(format!(
                "Temporary gallery-dl failure (exit {returncode}); retry {next_attempt}/{MAX_TRANSIENT_RETRIES} in {}s. {detail}",
                delay.as_secs()
            ));
            if is_rate_limited {
                extend_provider_cooldown(&state, provider_key(&url), delay).await;
            }
            delayed_retry = Some(delay);
            warn!(
                "Source {} ({}) hit a temporary gallery-dl failure; retry {}/{} in {}s: {}",
                source_id,
                name,
                next_attempt,
                MAX_TRANSIENT_RETRIES,
                delay.as_secs(),
                detail
            );
        } else {
            warn!(
                "Source {} ({}) failed to sync (gallery-dl exit {}): {}",
                source_id, name, returncode, detail
            );
            error_msg = Some(detail);
        }
    } else if new_count > 0 {
        info!(
            "Finished syncing source {} ({}): {} new item(s), {} total",
            source_id, name, new_count, total
        );
    } else {
        info!(
            "Finished syncing source {} ({}): nothing new ({} total)",
            source_id, name, total
        );
    }

    let log_tail: String = {
        let chars: Vec<char> = combined_text.chars().collect();
        chars
            .iter()
            .rev()
            .take(4000)
            .collect::<String>()
            .chars()
            .rev()
            .collect()
    };

    if let Ok(conn) = state.pool.get() {
        let saved = if status == "retrying" {
            let delay_secs = delayed_retry.map_or(0_i64, |delay| delay.as_secs() as i64);
            conn.execute(
                "UPDATE sources SET status=?1, item_count=?2, completed_count=?2, error_message=?3, log=?4, synced_at=?5,
                    progress_updated_at=?5,current_filename=NULL,retry_attempts=retry_attempts+1, retry_at=unixepoch()+?6 WHERE id=?7",
                rusqlite::params![status, total, error_msg, log_tail, now_iso(), delay_secs, source_id],
            )
        } else {
            // A completed run or a terminal failure starts a future manual
            // retry with a fresh budget.  Paused work deliberately retains
            // its state so Resume continues the same archive-backed job.
            conn.execute(
                "UPDATE sources SET status=?1, item_count=?2, completed_count=?2, error_message=?3, log=?4, synced_at=?5,
                    progress_updated_at=?5,current_filename=NULL,completed_at=CASE WHEN ?1 IN ('done','error') THEN ?5 ELSE completed_at END,
                    retry_attempts=CASE WHEN ?1 IN ('done','error') THEN 0 ELSE retry_attempts END,
                    retry_at=CASE WHEN ?1 IN ('done','error') THEN 0 ELSE retry_at END WHERE id=?6",
                rusqlite::params![status, total, error_msg, log_tail, now_iso(), source_id],
            )
        };
        if saved.is_ok() {
            if let Some(delay) = delayed_retry {
                schedule_retry(Arc::clone(&state), source_id, delay);
            }
        } else {
            warn!("Source {source_id} retry state could not be persisted");
        }
    } else {
        warn!("Source {source_id} finished but final status could not be persisted: database unavailable");
    }
}

fn schedule_retry(state: Arc<AppState>, source_id: i64, delay: Duration) {
    // `retry_at` is durable for restart recovery; this timer is only the fast
    // path for a running app. It checks the stored state before launching so
    // a manual resync, pause, removal, or another recovery can win safely.
    let retry_state = Arc::clone(&state);
    state.download_tasks.spawn(async move {
        let state = retry_state;
        tokio::select! {
            _ = state.shutdown.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        // A retry timer can wake long after its source worker ended. Acquire
        // its own short lease before it changes durable retry state or queues
        // another downloader.
        let Some(_retry_lease) = state.maintenance.try_acquire_background_worker() else {
            return;
        };

        if state
            .downloads_paused
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            if let Ok(conn) = state.pool.get() {
                let _ = conn.execute(
                    "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2 AND status='retrying'",
                    rusqlite::params![now_iso(), source_id],
                );
            }
            return;
        }

        let due = state
            .pool
            .get()
            .ok()
            .and_then(|conn| {
                conn.query_row(
                    "SELECT status='retrying' AND retry_at<=unixepoch() FROM sources WHERE id=?1",
                    [source_id],
                    |row| row.get::<_, bool>(0),
                )
                .ok()
            })
            .unwrap_or(false);
        if due {
            state
                .download_tasks
                .spawn(run_download(Arc::clone(&state), source_id));
        }
    });
}

// gallery-dl already retries individual HTTP requests. A source-level retry
// keeps a brief provider outage from becoming a terminal error while the
// download archive remains the authority for work that has finished.
const MAX_TRANSIENT_RETRIES: i64 = 6;

fn provider_key(url: &str) -> String {
    url.split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("unknown")
        .split('@')
        .next_back()
        .unwrap_or("unknown")
        .split(':')
        .next()
        .unwrap_or("unknown")
        .trim()
        .to_ascii_lowercase()
}

fn rate_limited(log_text: &str) -> bool {
    let lower = log_text.to_ascii_lowercase();
    lower.contains("429")
        || lower.contains("too many requests")
        || lower.contains("rate limit")
        || lower.contains("rate-limit")
}

fn retryable_failure(exit_code: i32, log_text: &str) -> bool {
    if filesize_skip_output(log_text) {
        return false;
    }
    if exit_code == 4 || rate_limited(log_text) {
        return true;
    }
    let lower = log_text.to_ascii_lowercase();
    [
        "incompleteread",
        "connection broken",
        "connection reset",
        "connection aborted",
        "timed out",
        "timeout",
        "temporary failure",
        "temporarily unavailable",
        "network is unreachable",
        "name or service not known",
        "dns",
        "http 500",
        "http 502",
        "http 503",
        "http 504",
        "service unavailable",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn filesize_skip_output(log_text: &str) -> bool {
    let lower = log_text.to_ascii_lowercase();
    (lower.contains("filesize") || lower.contains("file size"))
        && (lower.contains("skip") || lower.contains("larger") || lower.contains("exceed"))
}

fn oversized_skip_count(state: &AppState, source_id: i64) -> i64 {
    state
        .pool
        .get()
        .ok()
        .and_then(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM media WHERE source_id=?1 AND downloaded=0 AND skip_limit_bytes IS NOT NULL",
                [source_id],
                |row| row.get::<_, i64>(0),
            )
            .ok()
        })
        .unwrap_or(0)
}

fn retry_delay(attempt: i64, is_rate_limited: bool) -> Duration {
    let base = if is_rate_limited { 60_u64 } else { 10_u64 };
    let cap = if is_rate_limited { 30 * 60 } else { 5 * 60 };
    let exponent = attempt.saturating_sub(1).clamp(0, 8) as u32;
    Duration::from_secs(base.saturating_mul(1_u64 << exponent).min(cap))
}

async fn wait_for_provider_cooldown(
    state: &AppState,
    provider: &str,
    cancel: &tokio_util::sync::CancellationToken,
) -> bool {
    loop {
        let wait = {
            let cooldowns = state.download_cooldowns.lock().await;
            cooldowns
                .get(provider)
                .copied()
                .and_then(|until| until.checked_duration_since(Instant::now()))
        };
        let Some(wait) = wait else { return true };
        tokio::select! {
            _ = state.shutdown.cancelled() => return false,
            _ = cancel.cancelled() => return false,
            _ = tokio::time::sleep(wait) => {}
        }
    }
}

async fn extend_provider_cooldown(state: &AppState, provider: String, delay: Duration) {
    let requested = Instant::now() + delay;
    let mut cooldowns = state.download_cooldowns.lock().await;
    cooldowns
        .entry(provider)
        .and_modify(|current| *current = (*current).max(requested))
        .or_insert(requested);
}

fn short_error_summary(log_text: &str) -> String {
    let filtered: Vec<&str> = log_text
        .lines()
        .filter(|l| !l.contains("RequestsDependencyWarning"))
        .collect();
    let joined = filtered.join("\n");
    let tail: String = joined
        .chars()
        .rev()
        .take(500)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    tail.trim().to_string()
}

async fn drain_tail(mut reader: impl tokio::io::AsyncRead + Unpin) -> String {
    let mut tail = std::collections::VecDeque::new();
    let mut bytes = [0u8; 4096];
    while let Ok(n) = reader.read(&mut bytes).await {
        if n == 0 {
            break;
        }
        tail.extend(&bytes[..n]);
        while tail.len() > 65536 {
            tail.pop_front();
        }
    }
    filter_gdl_stderr(&String::from_utf8_lossy(
        &tail.into_iter().collect::<Vec<_>>(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    use tokio::process::Command;

    #[test]
    fn transient_failure_detection_is_conservative_and_backoff_is_bounded() {
        assert!(retryable_failure(1, "Connection broken: IncompleteRead"));
        assert!(retryable_failure(1, "HTTP 503 service unavailable"));
        assert!(retryable_failure(4, "gallery-dl extractor exited"));
        assert!(retryable_failure(1, "429 Too Many Requests"));
        assert!(!retryable_failure(1, "unsupported URL / deleted resource"));
        assert_eq!(retry_delay(1, false), Duration::from_secs(10));
        assert_eq!(retry_delay(1, true), Duration::from_secs(60));
        assert!(retry_delay(99, false) <= Duration::from_secs(5 * 60));
        assert!(retry_delay(99, true) <= Duration::from_secs(30 * 60));
    }

    #[test]
    fn provider_key_does_not_include_credentials_or_paths() {
        assert_eq!(
            provider_key("https://user:pass@example.test/a"),
            "example.test"
        );
        assert_eq!(provider_key("https://example.test/a"), "example.test");
        assert_eq!(provider_key("local:C:\\media"), "local");
    }

    #[test]
    fn gallery_dl_size_limit_argument_is_conditional_and_unlimited_is_unchanged() {
        let destination = Path::new("library/source");
        let archive = Path::new("library/source/.archive");
        let unlimited =
            gallery_dl_download_args("https://example.test/source", destination, archive, None);
        assert!(!unlimited.iter().any(|arg| arg == "--filesize-max"));
        assert!(!unlimited.iter().any(|arg| arg == "0"));

        let limited = gallery_dl_download_args(
            "https://example.test/source",
            destination,
            archive,
            Some(50 * 1024 * 1024),
        );
        let index = limited
            .iter()
            .position(|arg| arg == "--filesize-max")
            .expect("configured limit must be passed to gallery-dl");
        assert_eq!(
            limited.get(index + 1),
            Some(&((50 * 1024 * 1024) as u64).to_string())
        );

        // Zero is the JSON/UI normalization for Unlimited and must never
        // accidentally become a gallery-dl ceiling.
        let zero = gallery_dl_download_args("url", destination, archive, Some(0));
        assert!(!zero.iter().any(|arg| arg == "--filesize-max"));
    }

    #[test]
    fn size_skips_are_terminal_placeholders_and_never_retryable() {
        let output = "gallery-dl: file size 500000000 exceeds the configured filesize limit; skip";
        assert!(filesize_skip_output(output));
        assert!(!retryable_failure(1, output));
        assert!(!retryable_failure(4, output));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn an_oversized_listing_stays_a_placeholder_and_a_later_limit_allows_it() {
        let root = tempfile::tempdir().unwrap();
        let mut state = crate::test_support::state(root.path());
        Arc::get_mut(&mut state).unwrap().gallery_dl_bin = crate::test_support::fake_downloader();
        crate::test_support::source(&state);
        state
            .pool
            .get()
            .unwrap()
            .execute(
                "UPDATE sources SET url='https://example.test/size-test' WHERE id=1",
                [],
            )
            .unwrap();
        state.settings.write().await.max_download_file_size_bytes = Some(50);

        populate_placeholders(state.clone(), 1).await;
        let skipped: (i64, Option<String>, Option<i64>) = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT downloaded,skip_reason,skip_limit_bytes FROM media WHERE source_id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(skipped.0, 0, "an oversize is not a completed download");
        assert!(skipped.1.unwrap_or_default().contains("larger"));
        assert_eq!(skipped.2, Some(50));
        assert!(
            !state.archives_dir.join("test.sqlite3").exists(),
            "a listing-only size skip must not create a successful download archive"
        );

        // Make the listing eligible immediately, then raise its ceiling. The
        // same origin row is refreshed rather than kept in an endless retry
        // or permanent skipped state, and gallery-dl's new ceiling permits
        // the known 100-byte item.
        state.settings.write().await.max_download_file_size_bytes = Some(200);
        state
            .pool
            .get()
            .unwrap()
            .execute(
                "UPDATE placeholder_scans SET retry_at=0 WHERE source_id=1",
                [],
            )
            .unwrap();
        populate_placeholders(state.clone(), 1).await;
        let cleared: (Option<String>, Option<i64>) = state
            .pool
            .get()
            .unwrap()
            .query_row(
                "SELECT skip_reason,skip_limit_bytes FROM media WHERE source_id=1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(cleared, (None, None));
        let args = gallery_dl_download_args(
            "https://example.test/size-test",
            &state.library_dir.join("test"),
            &state.archives_dir.join("test.sqlite3"),
            Some(200),
        );
        assert!(args
            .windows(2)
            .any(|pair| pair == ["--filesize-max", "200"]));
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn placeholder_cache_survives_repeated_calls_and_refreshes_changed_url() {
        let root = tempfile::tempdir().unwrap();
        let mut state = crate::test_support::state(root.path());
        Arc::get_mut(&mut state).unwrap().gallery_dl_bin = crate::test_support::fake_downloader();
        crate::test_support::source(&state);
        populate_placeholders(state.clone(), 1).await;
        state
            .pool
            .get()
            .unwrap()
            .execute(
                "UPDATE placeholder_scans SET retry_at=unixepoch()+100000",
                [],
            )
            .unwrap();
        let before: i64 = state
            .pool
            .get()
            .unwrap()
            .query_row("SELECT retry_at FROM placeholder_scans", [], |r| r.get(0))
            .unwrap();
        populate_placeholders(state.clone(), 1).await;
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT retry_at FROM placeholder_scans", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            before
        );
        state
            .pool
            .get()
            .unwrap()
            .execute("UPDATE sources SET url='https://example.test/changed'", [])
            .unwrap();
        populate_placeholders(state.clone(), 1).await;
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT url FROM placeholder_scans", [], |r| r
                    .get::<_, String>(0))
                .unwrap(),
            "https://example.test/changed"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn explicit_shutdown_and_real_failure_are_distinct() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = crate::test_support::state(dir.path());
        Arc::get_mut(&mut state).unwrap().gallery_dl_bin = crate::test_support::fake_downloader();
        crate::test_support::source(&state);
        for (url, expected) in [
            ("https://fixture/success", "done"),
            // HTTP 503 is a transient failure and is therefore surfaced as a
            // durable retry state instead of being treated as terminal.
            ("https://fixture/failure", "retrying"),
        ] {
            state
                .pool
                .get()
                .unwrap()
                .execute("UPDATE sources SET url=?1", [url])
                .unwrap();
            run_download(state.clone(), 1).await;
            assert_eq!(
                state
                    .pool
                    .get()
                    .unwrap()
                    .query_row("SELECT status FROM sources", [], |r| r.get::<_, String>(0))
                    .unwrap(),
                expected
            );
        }
        state
            .pool
            .get()
            .unwrap()
            .execute("UPDATE sources SET url='https://fixture/wait'", [])
            .unwrap();
        let task = tokio::spawn(run_download(state.clone(), 1));
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !state.library_dir.join("test/child.pid").is_file() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        state.shutdown.cancel();
        tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        assert!(state.active_processes.lock().await.is_empty());
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT status FROM sources", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "paused"
        );
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn pause_kills_windows_descendants_and_resume_requeues() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = crate::test_support::state(dir.path());
        Arc::get_mut(&mut state).unwrap().gallery_dl_bin = crate::test_support::fake_downloader();
        crate::test_support::source(&state);
        state
            .pool
            .get()
            .unwrap()
            .execute("UPDATE sources SET url='https://fixture/wait'", [])
            .unwrap();
        let task = tokio::spawn(run_download(state.clone(), 1));
        let pid_path = state.library_dir.join("test/child.pid");
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !pid_path.is_file() {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        let pid: u32 = std::fs::read_to_string(pid_path).unwrap().parse().unwrap();
        let _ = crate::services::downloads::pause(&state).await;
        tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        let output = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                &format!("if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ exit 1 }}"),
            ])
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "grandchild must be terminated too");
        state
            .pool
            .get()
            .unwrap()
            .execute("UPDATE sources SET url='https://fixture/success'", [])
            .unwrap();
        let result = crate::services::downloads::resume(state.clone()).await;
        assert_eq!(result["requeued"], 1);
        state.download_tasks.close();
        state.download_tasks.wait().await;
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT status FROM sources", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "done"
        );
        kill_pid(0).await;
    }

    #[test]
    fn late_metadata_merges_placeholder_without_losing_ratings_or_tags() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        crate::test_support::source(&state);
        let conn = state.pool.get().unwrap();
        conn.execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at,downloaded,origin_url,rating) VALUES(10,1,'pending','pending','image','2026',0,'https://example.test/item.jpg',4);
            INSERT INTO tags(id,name,added_at) VALUES(1,'keep','2026'),(2,'also keep','2026'); INSERT INTO media_tags VALUES(10,1);").unwrap();
        conn.execute("UPDATE media SET rating=0,rating_source='human',rating_reviewed=1,rating_reviewed_at='2026' WHERE id=10", []).unwrap();
        let path = state.library_dir.join("test/item.jpg");
        std::fs::write(&path, b"downloaded").unwrap();
        assert!(index_file(&state, 1, &path).unwrap());
        let real_id: i64 = conn
            .query_row("SELECT id FROM media WHERE downloaded=1", [], |r| r.get(0))
            .unwrap();
        crate::nsfw::persist_score(&conn, real_id, 0.72).unwrap();
        conn.execute("INSERT INTO media_tags VALUES(?1,2)", [real_id])
            .unwrap();
        std::fs::write(
            state.library_dir.join("test/item.jpg.json"),
            r#"{"url":"https://example.test/item.jpg","creator":"artist","tags":["tag one"]}"#,
        )
        .unwrap();
        assert!(index_file(&state, 1, &path).unwrap());
        assert!(!index_file(&state, 1, &path).unwrap());
        assert_eq!(
            conn.query_row("SELECT id,rating,downloaded FROM media", [], |r| Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?
            )))
            .unwrap(),
            (10, 0, 1)
        );
        let provenance: (String, bool, i64) = conn
            .query_row(
                "SELECT rating_source,rating_reviewed,auto_rating FROM media",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        // NudeNet compatibility results stop at Medium (3); only the
        // temporal action model or a human can create Fast (4).
        assert_eq!(provenance, ("human".into(), true, 3));
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM media_tags WHERE media_id=10",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM media", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert_eq!(
            conn.query_row("SELECT creator FROM source_metadata", [], |r| {
                r.get::<_, Option<String>>(0)
            })
            .unwrap(),
            Some("artist".into())
        );
        assert_eq!(
            conn.query_row("SELECT raw_name FROM source_tags", [], |r| {
                r.get::<_, String>(0)
            })
            .unwrap(),
            "tag one"
        );
    }

    #[test]
    fn index_file_waits_for_a_competing_writer() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        crate::test_support::source(&state);
        let path = state.library_dir.join("test/writer-race.jpg");
        std::fs::write(&path, b"first version").unwrap();
        std::fs::write(
            state.library_dir.join("test/writer-race.jpg.json"),
            r#"{"url":"https://example.test/writer-race.jpg"}"#,
        )
        .unwrap();
        assert!(index_file(&state, 1, &path).unwrap());

        // Make the row stale so indexing has to read a placeholder candidate
        // and then write. This is the deferred-transaction upgrade race that
        // produced the "database is locked" stream in the supplied log.
        std::fs::write(&path, b"second version with a different size").unwrap();
        let blocker = rusqlite::Connection::open(dir.path().join("data.db")).unwrap();
        blocker
            .execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=30000; BEGIN IMMEDIATE;")
            .unwrap();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = std::sync::mpsc::channel();
        let worker_state = state.clone();
        let worker_path = path.clone();
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _ = finished_tx.send(index_file(&worker_state, 1, &worker_path));
        });
        started_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(
            finished_rx.try_recv().is_err(),
            "indexing must wait for the writer instead of failing its update"
        );
        blocker.execute_batch("COMMIT").unwrap();
        assert!(finished_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap()
            .unwrap());
        worker.join().unwrap();
    }

    #[test]
    fn partial_files_are_not_indexed() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        crate::test_support::source(&state);
        let path = state.library_dir.join("test/item.jpg");
        std::fs::write(&path, b"incomplete").unwrap();
        std::fs::write(state.library_dir.join("test/item.jpg.part"), b"partial").unwrap();
        assert!(!index_file(&state, 1, &path).unwrap());
    }

    #[tokio::test]
    async fn native_events_index_completed_file_before_final_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        crate::test_support::source(&state);
        let done = tokio_util::sync::CancellationToken::new();
        let task = tokio::spawn(index_download(
            state.clone(),
            1,
            state.library_dir.join("test"),
            done.clone(),
        ));
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        let path = state.library_dir.join("test/event.jpg");
        std::fs::write(&path, b"complete").unwrap();
        let observed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let count: i64 = state
                    .pool
                    .get()
                    .unwrap()
                    .query_row(
                        "SELECT COUNT(*) FROM media WHERE filepath='test/event.jpg'",
                        [],
                        |r| r.get(0),
                    )
                    .unwrap();
                if count == 1 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        })
        .await;
        done.cancel();
        task.await.unwrap();
        assert!(
            observed.is_ok(),
            "native filesystem event should index without a recovery scan"
        );
    }

    #[test]
    fn probe_video_duration_fails_soft_on_a_missing_binary() {
        // Never a panic or Err — just None, same as "duration not known yet".
        assert_eq!(
            probe_video_duration(
                "definitely-not-a-real-binary-xyz",
                Path::new("/nonexistent.mp4")
            ),
            None
        );
    }

    #[test]
    fn probe_video_duration_fails_soft_on_a_missing_file() {
        assert_eq!(
            probe_video_duration("ffprobe", Path::new("/nonexistent.mp4")),
            None
        );
    }

    #[test]
    fn probe_video_duration_reads_a_real_file_correctly() {
        // Skips (doesn't fail) on machines without ffmpeg/ffprobe installed —
        // this checks the parsing logic is correct, not that ffmpeg exists.
        if !std::process::Command::new("ffmpeg")
            .arg("-version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            eprintln!("skipping: ffmpeg not available on this machine");
            return;
        }

        let dir = std::env::temp_dir().join(format!("curator_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("test.mp4");

        let status = std::process::Command::new("ffmpeg")
            .args([
                "-y",
                "-f",
                "lavfi",
                "-i",
                "testsrc=duration=3:size=64x64:rate=5",
            ])
            .arg(&path)
            .output()
            .unwrap();
        assert!(
            status.status.success(),
            "ffmpeg failed to generate the test fixture"
        );

        let duration = probe_video_duration("ffprobe", &path);
        let _ = std::fs::remove_dir_all(&dir);

        let d =
            duration.expect("ffprobe should have reported a duration for a file ffmpeg just made");
        assert!((d - 3.0).abs() < 0.5, "expected ~3s, got {}", d);
    }
}

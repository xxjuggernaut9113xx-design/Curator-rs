use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    Json,
};
use serde_json::{json, Value};

use crate::AppState;

// ─── GET /api/downloads/status ───────────────────────────────────────────────

pub async fn status(
    State(state): State<Arc<AppState>>,
) -> Json<crate::services::downloads::DownloadStatus> {
    Json(crate::services::downloads::status(&state).await)
}

// POST /api/downloads/pause
pub async fn pause(State(state): State<Arc<AppState>>) -> Json<Value> {
    let _control = state.download_control.lock().await;
    state.downloads_paused.store(true, Ordering::SeqCst);

    // Delayed retry timers have no child PID to kill. Persist their paused
    // state now so Resume can claim them immediately and a restart cannot
    // lose them.
    if let Ok(conn) = state.pool.get() {
        let _ = conn.execute(
            "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE status IN ('pending','retrying')",
            [crate::db::now_iso()],
        );
    }

    // Kill all running gallery-dl processes and mark their sources as paused
    let procs: Vec<(i64, u32)> = {
        let guard = state.active_processes.lock().await;
        guard.iter().map(|(&sid, &pid)| (sid, pid)).collect()
    };

    for &(source_id, pid) in &procs {
        state.paused_source_ids.lock().await.insert(source_id);
        // The owning task also has to leave its select loop.  Killing a PID
        // alone is not sufficient on Windows when a managed environment
        // rejects taskkill's tree walk, and it cannot wake a queued source.
        if let Some(cancel) = state
            .source_cancellations
            .lock()
            .await
            .get(&source_id)
            .cloned()
        {
            cancel.cancel();
        }
        crate::downloader::kill_pid(pid).await;
    }

    // Local-folder imports and queued workers do not have a gallery-dl PID,
    // but they still register a source cancellation token.  Cancel those
    // tokens as part of the same global transition so pause has one meaning
    // for every source type.
    let cancellations: Vec<(i64, tokio_util::sync::CancellationToken)> = state
        .source_cancellations
        .lock()
        .await
        .iter()
        .filter(|(source_id, _)| !procs.iter().any(|(id, _)| id == *source_id))
        .map(|(source_id, token)| (*source_id, token.clone()))
        .collect();
    for (source_id, cancel) in cancellations {
        state.paused_source_ids.lock().await.insert(source_id);
        cancel.cancel();
        if let Ok(conn) = state.pool.get() {
            let _ = conn.execute(
                "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2 AND status IN ('downloading','indexing')",
                rusqlite::params![crate::db::now_iso(), source_id],
            );
        }
    }

    Json(json!({ "paused": true }))
}

/// Pause one source without stopping unrelated work.  The owning downloader
/// task reaps its process before a later Resume requeues it.
pub async fn pause_source(State(state): State<Arc<AppState>>, Path(id): Path<i64>) -> Json<Value> {
    let _control = state.download_control.lock().await;
    let exists = state
        .pool
        .get()
        .ok()
        .and_then(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sources WHERE id=?1)",
                [id],
                |row| row.get::<_, bool>(0),
            )
            .ok()
        })
        .unwrap_or(false);
    if !exists {
        return Json(json!({"error":"Source not found"}));
    }
    state.paused_source_ids.lock().await.insert(id);
    if let Some(cancel) = state.source_cancellations.lock().await.get(&id).cloned() {
        cancel.cancel();
    }
    if let Some(pid) = state.active_processes.lock().await.get(&id).copied() {
        crate::downloader::kill_pid(pid).await;
    }
    if let Ok(conn) = state.pool.get() {
        let _ = conn.execute(
            "UPDATE sources SET status='paused',progress_updated_at=?1 WHERE id=?2",
            rusqlite::params![crate::db::now_iso(), id],
        );
    }
    Json(json!({"id":id,"paused":true}))
}

/// Resume only one paused source.  Global pause still wins so this endpoint
/// cannot accidentally restart downloads behind the user's back.
pub async fn resume_source(State(state): State<Arc<AppState>>, Path(id): Path<i64>) -> Json<Value> {
    if state.maintenance.is_active() {
        return Json(json!({"id":id,"error":"A local maintenance job is active"}));
    }
    // Serialize a source-level resume with global pause/resume.  Without the
    // same transition lock, a double-click or a simultaneous global resume
    // could both claim the row and enqueue duplicate downloader tasks.
    let _control = state.download_control.lock().await;
    if state.downloads_paused.load(Ordering::SeqCst) {
        return Json(json!({"id":id,"error":"Downloads are globally paused"}));
    }
    let changed = state.pool.get().ok().and_then(|conn| conn.execute(
        "UPDATE sources SET status='pending',queued_at=?1,progress_updated_at=?1,current_filename=NULL WHERE id=?2 AND status IN ('paused','storage_limit','low_disk','error','done','retrying')",
        rusqlite::params![crate::db::now_iso(),id],
    ).ok()).unwrap_or(0);
    if changed == 0 {
        return Json(json!({"id":id,"error":"Source is not resumable"}));
    }
    state.paused_source_ids.lock().await.remove(&id);
    state
        .download_tasks
        .spawn(crate::downloader::run_download(Arc::clone(&state), id));
    Json(json!({"id":id,"paused":false,"status":"queued"}))
}

// ─── POST /api/downloads/resume ──────────────────────────────────────────────

pub async fn resume(State(state): State<Arc<AppState>>) -> Json<Value> {
    if state.maintenance.is_active() {
        return Json(json!({"error":"A local maintenance job is active"}));
    }
    resume_unchecked(state).await
}

/// The maintenance controller pauses downloads itself. Once its transaction
/// and cache invalidation are complete it reopens worker admission, then uses
/// this internal path to restore the prior download state without routing a
/// synthetic HTTP request through the public control endpoint.
pub(crate) async fn resume_after_maintenance(state: Arc<AppState>) -> Json<Value> {
    resume_unchecked(state).await
}

async fn resume_unchecked(state: Arc<AppState>) -> Json<Value> {
    let _control = state.download_control.lock().await;
    // Wait for killed children and their final index pass before requeueing.
    while state.downloads_paused.load(Ordering::SeqCst) {
        if state.running_sources.lock().await.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let paused_ids: Vec<i64> = {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return Json(json!({"error":"Database unavailable"})),
        };
        let mut stmt = match conn.prepare("SELECT id FROM sources WHERE status='paused'") {
            Ok(s) => s,
            Err(_) => return Json(json!({"error":"Database unavailable"})),
        };
        let ids = stmt
            .query_map([], |r| r.get(0))
            .map(|rows| rows.filter_map(Result::ok).collect())
            .unwrap_or_default();
        ids
    };
    // Claim paused rows before spawning. A second Resume call then sees zero
    // paused rows instead of launching duplicate downloads.
    if !paused_ids.is_empty() {
        let conn = match state.pool.get() {
            Ok(c) => c,
            Err(_) => return Json(json!({"error":"Database unavailable"})),
        };
        let tx = match conn.unchecked_transaction() {
            Ok(tx) => tx,
            Err(_) => return Json(json!({"error":"Database unavailable"})),
        };
        for id in &paused_ids {
            if tx
                .execute(
                    "UPDATE sources SET status='pending',queued_at=?1,progress_updated_at=?1,current_filename=NULL WHERE id=?2 AND status='paused'",
                    rusqlite::params![crate::db::now_iso(), id],
                )
                .is_err()
            {
                return Json(json!({"error":"Database unavailable"}));
            }
        }
        if tx.commit().is_err() {
            return Json(json!({"error":"Database unavailable"}));
        }
    }
    state.paused_source_ids.lock().await.clear();
    state.downloads_paused.store(false, Ordering::SeqCst);

    for id in &paused_ids {
        state
            .download_tasks
            .spawn(crate::downloader::run_download(Arc::clone(&state), *id));
    }

    Json(json!({ "paused": false, "requeued": paused_ids.len() }))
}

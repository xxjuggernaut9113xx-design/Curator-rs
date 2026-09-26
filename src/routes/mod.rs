pub mod admin;
pub mod ch;
pub mod clips;
pub mod downloads;
pub mod export;
pub mod goon;
pub mod groups;
pub mod library;
pub mod media;
pub mod misc;
pub mod oobe;
pub mod remote;
pub mod search;
pub mod session;
pub mod settings;
pub mod source_tags;
pub mod sources;
pub mod storage;
pub mod system;
pub mod tags;
pub mod thumb;

use crate::AppState;
use axum::{
    extract::{ConnectInfo, Request, State},
    http::{Method, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::{delete, get, patch, post, put},
    Json, Router,
};
use serde_json::json;
use std::{net::SocketAddr, sync::Arc};

pub(crate) fn actor_for_peer(
    peer: Option<ConnectInfo<SocketAddr>>,
) -> crate::services::access::Actor {
    if peer.is_some_and(|peer| !peer.0.ip().is_loopback()) {
        crate::services::access::Actor::RemoteViewer
    } else {
        crate::services::access::Actor::LocalOwner
    }
}

/// Maintenance owns the library exclusively while it takes a safety backup
/// and applies a recovery transaction.  Individual download routes also
/// cooperate with that mode, but this router-level gate keeps tags, groups,
/// ratings, settings, and every other mutation from racing a backup or
/// rollback through a route that was added later.
async fn maintenance_write_guard(
    State(state): State<Arc<AppState>>,
    request: Request,
    next: Next,
) -> Response {
    let mutating_method = matches!(
        *request.method(),
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    );
    if mutating_method {
        // The capability handshake currently grants Viewer read access only.
        // The TCP peer supplied by axum::serve is the authority here; a
        // caller-provided header cannot turn a Tailnet request into Host.
        if request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .is_some_and(|peer| !peer.0.ip().is_loopback())
        {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error":"Viewer role does not permit this operation"})),
            )
                .into_response();
        }
        // Hold a lease for the full request, rather than merely checking the
        // flag once.  This closes the otherwise unavoidable race where a
        // maintenance job starts between a middleware check and a handler's
        // SQLite write.
        let Some(_request_lease) = state.maintenance.try_acquire_background_worker() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "A local maintenance job is active. Try this change again when it completes."
                })),
            )
                .into_response();
        };
        return next.run(request).await;
    }
    next.run(request).await
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/system/info", get(system::info))
        .route(
            "/api/admin/jobs",
            get(admin::list_jobs).post(admin::start_job),
        )
        .route("/api/admin/jobs/:id", get(admin::get_job))
        .route(
            "/api/admin/backups",
            get(admin::list_backups).post(admin::create_backup),
        )
        .route("/api/admin/backups/:id", get(admin::download_backup))
        .route(
            "/api/admin/backups/:id/validate",
            post(admin::validate_backup),
        )
        .route(
            "/api/admin/backups/:id/restore",
            post(admin::restore_backup),
        )
        .route(
            "/api/admin/phar",
            get(admin::phar_status).post(admin::phar_intent),
        )
        .route("/api/admin/phar/install", post(admin::phar_install))
        .route("/api/admin/phar/cancel", post(admin::phar_cancel))
        .route("/api/admin/phar/repair", post(admin::phar_repair))
        .route("/api/admin/phar/self-test", post(admin::phar_self_test))
        .route("/api/library/summary", get(crate::hierarchy::endpoint))
        // Shared session service: native Slint and remote/recovery clients use
        // these same typed operations, rather than owning competing clocks.
        .route("/api/session", get(session::current))
        .route("/api/session/start", post(session::start))
        .route("/api/session/command", post(session::control))
        // ── First-run OOBE ─────────────────────────────────────────────────
        // Explicit routes on "/" and "/index.html" take priority over the
        // static-file fallback_service registered in main.rs, so a
        // not-yet-configured install is handed oobe.html instead of the
        // normal app shell without needing any change to app.js's own
        // startup sequence.
        .route("/", get(oobe::serve_root))
        .route("/index.html", get(oobe::serve_root))
        .route("/api/oobe/status", get(oobe::status))
        .route("/api/oobe/validate", post(oobe::validate))
        .route("/api/oobe/settings", post(oobe::save_settings))
        .route("/api/oobe/complete", post(oobe::complete))
        .route("/api/oobe/reset", post(oobe::reset))
        // ── Media ──────────────────────────────────────────────────────────
        .route("/api/media", get(media::list))
        .route(
            "/api/media/:id/stream",
            get(media::stream).head(media::stream),
        )
        .route("/api/media/bulk", post(media::bulk))
        .route("/api/media/:id/clips", post(clips::create))
        .route("/api/clip-jobs/:id", get(clips::status))
        .route("/api/media/:id/rating", put(media::set_rating))
        .route("/api/media/:id/rating/approve", post(media::approve_rating))
        .route("/api/media/:id/rating/undo", post(media::undo_rating))
        .route("/api/media/:id/duration", put(media::set_duration))
        .route("/api/media/:id/tags", post(media::add_tag))
        .route("/api/media/:id/tags/:tag_id", delete(media::remove_tag))
        // ── Thumbnails ─────────────────────────────────────────────────────
        .route("/api/thumb/:id", get(thumb::get_thumbnail))
        // ── Tags ───────────────────────────────────────────────────────────
        .route("/api/tags", get(tags::list))
        .route("/api/tags/quick", get(tags::quick))
        .route("/api/tags/:id", delete(tags::delete_tag))
        .route(
            "/api/source-tags/review",
            get(source_tags::review_list).post(source_tags::review),
        )
        .route(
            "/api/source-tag-rules",
            get(source_tags::list_rules).post(source_tags::save_rule),
        )
        .route(
            "/api/source-tag-rules/:id",
            delete(source_tags::delete_rule),
        )
        // ── Sources ────────────────────────────────────────────────────────
        .route("/api/sources", get(sources::list).post(sources::add))
        .route("/api/sources/resync-all", post(sources::resync_all))
        .route(
            "/api/sources/:id",
            get(sources::get)
                .patch(sources::patch)
                .delete(sources::delete),
        )
        .route("/api/sources/:id/group", patch(sources::set_group))
        .route("/api/sources/:id/resync", post(sources::resync))
        .route("/api/sources/:id/log", get(misc::source_log))
        // ── Unified discovery ───────────────────────────────────────────────
        .route("/api/search/providers", get(search::providers))
        .route("/api/search", get(search::search))
        .route("/api/search/download", post(search::download_selected))
        // ── Groups ─────────────────────────────────────────────────────────
        .route("/api/groups", get(groups::list).post(groups::create))
        .route(
            "/api/groups/:id",
            patch(groups::update).delete(groups::delete),
        )
        .route("/api/groups/:id/tags", post(groups::add_tag))
        .route("/api/groups/:id/tags/:tag_id", delete(groups::remove_tag))
        // ── Downloads ──────────────────────────────────────────────────────
        .route("/api/downloads/status", get(downloads::status))
        .route("/api/downloads/pause", post(downloads::pause))
        .route("/api/downloads/resume", post(downloads::resume))
        .route(
            "/api/downloads/sources/:id/pause",
            post(downloads::pause_source),
        )
        .route(
            "/api/downloads/sources/:id/resume",
            post(downloads::resume_source),
        )
        // ── Settings ───────────────────────────────────────────────────────
        .route("/api/settings", get(settings::get).patch(settings::patch))
        .route("/api/remote-access", get(remote::status))
        .route("/api/storage", get(storage::dashboard))
        .route(
            "/api/storage/sources/:id/permit-once",
            post(storage::permit_one_sync),
        )
        .route(
            "/api/storage/sources/:id/cleanup",
            post(storage::cleanup_source),
        )
        .route(
            "/api/storage/thumbnails/clear",
            post(storage::clear_thumbnails),
        )
        .route(
            "/api/storage/archives/cleanup",
            post(storage::cleanup_archives),
        )
        // ── Export / Import ────────────────────────────────────────────────
        .route("/api/export", get(export::export_sources))
        .route("/api/export/chpack", post(export::export_chpack))
        .route("/api/import", post(export::import_sources))
        // ── Stats / Log ────────────────────────────────────────────────────
        .route("/api/stats", get(misc::stats))
        .route("/api/log", get(misc::get_log))
        // ── Curator interactive sessions ────────────────────────────────────
        .route("/api/goon/session", post(goon::start))
        .route("/api/goon/session/complete", post(goon::complete))
        .route("/api/goon/connectors", get(goon::connector_status))
        .route(
            "/api/goon/playlists",
            get(goon::list_playlists).post(goon::save_playlist),
        )
        .route("/api/goon/beat-maps/analyze", post(goon::analyze_beat_map))
        .route("/api/goon/beat-maps/:id", patch(goon::update_beat_map))
        .route("/api/goon/oauth/callback", post(goon::oauth_callback))
        .layer(axum::middleware::from_fn_with_state(
            Arc::clone(&state),
            maintenance_write_guard,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use tower::ServiceExt;

    #[tokio::test]
    async fn tailnet_viewer_cannot_mutate_through_any_http_method() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let app = build_router(state);
        let peer = ConnectInfo(SocketAddr::from(([100, 64, 1, 2], 49152)));
        for (method, path) in [
            (Method::POST, "/api/sources"),
            (Method::PUT, "/api/media/1/rating"),
            (Method::PATCH, "/api/settings"),
            (Method::DELETE, "/api/groups/1"),
            (Method::POST, "/api/session/start"),
            (Method::POST, "/api/admin/jobs"),
        ] {
            let mut request = axum::http::Request::builder()
                .method(method)
                .uri(path)
                .body(Body::empty())
                .unwrap();
            request.extensions_mut().insert(peer);
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
        }
        let mut request = axum::http::Request::builder()
            .uri("/api/system/info")
            .body(Body::empty())
            .unwrap();
        request.extensions_mut().insert(peer);
        assert_eq!(
            app.clone().oneshot(request).await.unwrap().status(),
            StatusCode::OK
        );

        let mut local = axum::http::Request::builder()
            .method(Method::PATCH)
            .uri("/api/settings")
            .header("x-forwarded-for", "100.64.1.2")
            .body(Body::empty())
            .unwrap();
        local
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 49153))));
        assert_ne!(
            app.oneshot(local).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );
    }

    #[tokio::test]
    async fn maintenance_mode_rejects_all_normal_mutations() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        // Hold quiescence just long enough to prove the router cannot race a
        // queued job. The task itself uses the direct pause function, so it
        // is not blocked by this HTTP-only guard.
        state.running_sources.lock().await.insert(1);
        state
            .maintenance
            .start(
                Arc::clone(&state),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    confirmation: String::new(),
                    backup_id: None,
                },
            )
            .await
            .unwrap();
        assert!(state.maintenance.is_active());

        let response = build_router(Arc::clone(&state))
            .oneshot(
                axum::http::Request::builder()
                    .method(Method::POST)
                    .uri("/api/groups")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"must wait"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        state.running_sources.lock().await.clear();
        for _ in 0..100 {
            if !state.maintenance.is_active() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!state.maintenance.is_active());
        assert!(
            !state
                .downloads_paused
                .load(std::sync::atomic::Ordering::SeqCst),
            "a completed job must restore the prior unpaused download state"
        );
        state.server_tasks.close();
        state.server_tasks.wait().await;
    }
}

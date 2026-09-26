//! Shared media review mutations and validated native streaming ranges.

use super::access::Actor;
use crate::{db::now_iso, provenance, AppState};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RatingReview {
    pub id: i64,
    pub rating: i64,
    pub pace_label: &'static str,
    pub human_rating: Option<i64>,
    pub auto_rating: i64,
    pub auto_rating_score: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_rating: Option<i64>,
    pub rating_source: String,
    pub rating_reviewed: bool,
    pub rating_reviewed_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaError {
    Forbidden,
    ShuttingDown,
    Maintenance,
    InvalidRating,
    InvalidSelection,
    InvalidTag,
    NoAutomatedRating,
    Missing,
    ChangedReview,
    Database(String),
}

impl MediaError {
    pub fn message(&self) -> &str {
        match self {
            Self::Forbidden => "Viewer cannot edit a local library",
            Self::ShuttingDown => "Curator is shutting down",
            Self::Maintenance => "A local maintenance job is active",
            Self::InvalidRating => "Rating must be between 1 and 5",
            Self::InvalidSelection => "Select between one and 500 valid media items",
            Self::InvalidTag => "A valid tag is required",
            Self::NoAutomatedRating => "No automated rating to approve",
            Self::Missing => "Media not found",
            Self::ChangedReview => "Rating changed since this review; cannot undo it",
            Self::Database(message) => message,
        }
    }
}

fn db_err(error: impl std::fmt::Display) -> MediaError {
    MediaError::Database(error.to_string())
}

fn edit_lease(
    state: &AppState,
    actor: Actor,
) -> Result<crate::maintenance::BackgroundWorkerLease, MediaError> {
    if !actor.can_edit_library() || !state.edition.owns_library() {
        return Err(MediaError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(MediaError::ShuttingDown);
    }
    let lease = state
        .maintenance
        .try_acquire_background_worker()
        .ok_or(MediaError::Maintenance)?;
    if state.shutdown.is_cancelled() {
        return Err(MediaError::ShuttingDown);
    }
    Ok(lease)
}

pub fn review(
    state: &AppState,
    actor: Actor,
    id: i64,
    rating: Option<i64>,
) -> Result<RatingReview, MediaError> {
    let _lease = edit_lease(state, actor)?;
    if rating.is_some_and(|rating| !(1..=5).contains(&rating)) {
        return Err(MediaError::InvalidRating);
    }
    let conn = state.pool.get().map_err(db_err)?;
    let result = conn.query_row(
        "UPDATE media SET human_rating=COALESCE(?1,NULLIF(action_rating,0),NULLIF(auto_rating,0)),
         rating=COALESCE(?1,NULLIF(action_rating,0),NULLIF(auto_rating,0)), rating_source='human',
         rating_reviewed=1, rating_reviewed_at=?2 WHERE id=?3 AND (?1 IS NOT NULL OR action_rating=4 OR auto_rating BETWEEN 1 AND 5)
         RETURNING human_rating,auto_rating,auto_rating_score,action_rating,rating_source,rating_reviewed,rating_reviewed_at",
        rusqlite::params![rating, now_iso(), id],
        |row| {
            let rating = row.get::<_, i64>(0)?;
            Ok(RatingReview {
                id,
                rating,
                pace_label: crate::nsfw::pace_label(rating),
                human_rating: row.get(0)?,
                auto_rating: row.get(1)?,
                auto_rating_score: row.get(2)?,
                action_rating: Some(row.get(3)?),
                rating_source: row.get(4)?,
                rating_reviewed: row.get(5)?,
                rating_reviewed_at: row.get(6)?,
            })
        },
    );
    match result {
        Ok(review) => Ok(review),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM media WHERE id=?1)",
                    [id],
                    |row| row.get(0),
                )
                .map_err(db_err)?;
            Err(if exists {
                MediaError::NoAutomatedRating
            } else {
                MediaError::Missing
            })
        }
        Err(error) => Err(db_err(error)),
    }
}

pub fn undo_review(
    state: &AppState,
    actor: Actor,
    id: i64,
    reviewed_at: &str,
) -> Result<RatingReview, MediaError> {
    let _lease = edit_lease(state, actor)?;
    let conn = state.pool.get().map_err(db_err)?;
    conn.query_row("UPDATE media SET human_rating=NULL,rating=COALESCE(NULLIF(action_rating,0),auto_rating),
        rating_source=CASE WHEN action_rating=4 THEN 'auto_action' WHEN auto_rating>0 THEN 'auto' ELSE 'none' END,rating_reviewed=0,rating_reviewed_at=NULL
        WHERE id=?1 AND human_rating IS NOT NULL AND rating_reviewed_at=?2
        RETURNING rating,auto_rating,auto_rating_score,rating_source", rusqlite::params![id,reviewed_at], |row| {
            let rating = row.get::<_, i64>(0)?;
            Ok(RatingReview {
                id,
                rating,
                pace_label: crate::nsfw::pace_label(rating),
                human_rating: None,
                auto_rating: row.get(1)?,
                auto_rating_score: row.get(2)?,
                action_rating: None,
                rating_source: row.get(3)?,
                rating_reviewed: false,
                rating_reviewed_at: None,
            })
        })
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => MediaError::ChangedReview,
            other => db_err(other),
        })
}

pub fn rate_many(
    state: &AppState,
    actor: Actor,
    ids: &[i64],
    rating: i64,
) -> Result<usize, MediaError> {
    let _lease = edit_lease(state, actor)?;
    if ids.is_empty() || ids.len() > 500 || ids.iter().any(|id| *id <= 0) {
        return Err(MediaError::InvalidSelection);
    }
    if !(1..=5).contains(&rating) {
        return Err(MediaError::InvalidRating);
    }
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let mut conn = state.pool.get().map_err(db_err)?;
    let tx = conn.transaction().map_err(db_err)?;
    let reviewed_at = now_iso();
    let mut updated = 0;
    for id in &ids {
        updated += tx.execute(
            "UPDATE media SET human_rating=?1,rating=?1,rating_source='human',rating_reviewed=1,rating_reviewed_at=?2 WHERE id=?3",
            rusqlite::params![rating, reviewed_at, id],
        ).map_err(db_err)?;
    }
    tx.commit().map_err(db_err)?;
    Ok(updated)
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct BulkTagResult {
    pub updated: usize,
    pub failed: Vec<Value>,
}

pub fn add_tag_many(
    state: &AppState,
    actor: Actor,
    ids: &[i64],
    name: &str,
) -> Result<BulkTagResult, MediaError> {
    let _lease = edit_lease(state, actor)?;
    if ids.is_empty() || ids.len() > 500 || ids.iter().any(|id| *id <= 0) {
        return Err(MediaError::InvalidSelection);
    }
    let name = provenance::normalize_tag(name).ok_or(MediaError::InvalidTag)?;
    let mut ids = ids.to_vec();
    ids.sort_unstable();
    ids.dedup();
    let mut conn = state.pool.get().map_err(db_err)?;
    let tx = conn.transaction().map_err(db_err)?;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let exists: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM media WHERE id=?1)",
                [id],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        if exists {
            provenance::attach_tag(&tx, id, &name, provenance::HUMAN, None).map_err(db_err)?;
            updated += 1;
        } else {
            failed.push(json!({"id":id,"error":"Media not found"}));
        }
    }
    tx.commit().map_err(db_err)?;
    Ok(BulkTagResult { updated, failed })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamPlan {
    pub start: u64,
    pub length: u64,
    pub total: u64,
    pub partial: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidRange;

impl StreamPlan {
    pub fn end(self) -> u64 {
        self.start + self.length.saturating_sub(1)
    }
}

/// Accept one RFC 9110 byte range. Multipart ranges are deliberately rejected
/// because the native player requests one contiguous region at a time.
pub fn plan_range(total: u64, range: Option<&str>) -> Result<StreamPlan, InvalidRange> {
    let Some(range) = range else {
        return Ok(StreamPlan {
            start: 0,
            length: total,
            total,
            partial: false,
        });
    };
    let spec = range.strip_prefix("bytes=").ok_or(InvalidRange)?;
    if total == 0 || spec.contains(',') || spec.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(InvalidRange);
    }
    let (start, end) = spec.split_once('-').ok_or(InvalidRange)?;
    if !start.bytes().all(|byte| byte.is_ascii_digit())
        || !end.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(InvalidRange);
    }
    let (start, end) = if start.is_empty() {
        let suffix: u64 = end.parse().map_err(|_| InvalidRange)?;
        if suffix == 0 {
            return Err(InvalidRange);
        }
        (total.saturating_sub(suffix), total - 1)
    } else {
        let start: u64 = start.parse().map_err(|_| InvalidRange)?;
        if start >= total {
            return Err(InvalidRange);
        }
        let end = if end.is_empty() {
            total - 1
        } else {
            end.parse::<u64>().map_err(|_| InvalidRange)?.min(total - 1)
        };
        (start, end)
    };
    if end < start {
        return Err(InvalidRange);
    }
    Ok(StreamPlan {
        start,
        length: end - start + 1,
        total,
        partial: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{header, Request, StatusCode},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn review_service_matches_http_and_rejects_stale_undo() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state
            .pool
            .get()
            .unwrap()
            .execute_batch(
                "INSERT INTO media(id,source_id,filepath,filename,type,added_at,auto_rating)
             VALUES(1,1,'one.jpg','one.jpg','image','now',3);",
            )
            .unwrap();

        let direct = review(&state, Actor::LocalOwner, 1, Some(4)).unwrap();
        let response = crate::routes::media::set_rating(
            axum::extract::State(state.clone()),
            None,
            axum::extract::Path(1),
            axum::Json(crate::routes::media::RatingBody { rating: 4 }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(
            serde_json::to_value(direct).unwrap()["rating"],
            response["rating"]
        );
        assert_eq!(response["pace_label"], "fast");
        assert_eq!(
            undo_review(&state, Actor::LocalOwner, 1, "stale"),
            Err(MediaError::ChangedReview)
        );
        let current_token = response["rating_reviewed_at"].as_str().unwrap();
        assert!(
            !undo_review(&state, Actor::LocalOwner, 1, current_token)
                .unwrap()
                .rating_reviewed
        );
        assert_eq!(rate_many(&state, Actor::LocalOwner, &[1, 1], 5).unwrap(), 1);
    }

    #[test]
    fn review_service_denies_viewer_and_shutdown() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(
            review(&viewer, Actor::LocalOwner, 1, Some(4)),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            undo_review(&viewer, Actor::LocalOwner, 1, "token"),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            rate_many(&viewer, Actor::LocalOwner, &[1], 4),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            review(&state, Actor::RemoteViewer, 1, Some(4)),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            undo_review(&state, Actor::RemoteViewer, 1, "token"),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            rate_many(&state, Actor::RemoteViewer, &[1], 4),
            Err(MediaError::Forbidden)
        );
        state.shutdown.cancel();
        assert_eq!(
            review(&state, Actor::LocalOwner, 1, Some(4)),
            Err(MediaError::ShuttingDown)
        );
    }

    #[tokio::test]
    async fn review_service_waits_for_maintenance() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let lease = state.maintenance.try_acquire_background_worker().unwrap();
        state
            .maintenance
            .start(
                state.clone(),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    confirmation: String::new(),
                    backup_id: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            review(&state, Actor::LocalOwner, 1, Some(4)),
            Err(MediaError::Maintenance)
        );
        assert_eq!(
            undo_review(&state, Actor::LocalOwner, 1, "token"),
            Err(MediaError::Maintenance)
        );
        assert_eq!(
            rate_many(&state, Actor::LocalOwner, &[1], 4),
            Err(MediaError::Maintenance)
        );
        drop(lease);
        state.server_tasks.close();
        state.server_tasks.wait().await;
    }

    #[tokio::test]
    async fn bulk_tags_share_service_and_http_results() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state
            .pool
            .get()
            .unwrap()
            .execute_batch(
                "INSERT INTO media(id,source_id,filepath,filename,type,added_at)
             VALUES(1,1,'one.jpg','one.jpg','image','now');",
            )
            .unwrap();
        let direct = add_tag_many(&state, Actor::LocalOwner, &[1, 1, 2], "reviewed").unwrap();
        let http = crate::routes::media::bulk(
            axum::extract::State(state.clone()),
            None,
            axum::Json(crate::routes::media::BulkMediaBody {
                ids: vec![1, 1, 2],
                action: "add_tag".into(),
                group_id: None,
                tag: Some("reviewed".into()),
                rating: None,
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(http["updated"], direct.updated);
        assert_eq!(http["failed"], json!(direct.failed));
        assert!(matches!(
            add_tag_many(&state, Actor::LocalOwner, &[1], "  "),
            Err(MediaError::InvalidTag)
        ));
        let mut viewer = (*state).clone();
        viewer.edition = crate::edition::Edition::Viewer;
        assert_eq!(
            add_tag_many(&viewer, Actor::LocalOwner, &[1], "reviewed"),
            Err(MediaError::Forbidden)
        );
        assert_eq!(
            add_tag_many(&state, Actor::RemoteViewer, &[1], "reviewed"),
            Err(MediaError::Forbidden)
        );
    }

    #[tokio::test]
    async fn tailnet_peer_is_denied_by_the_media_service_even_without_router_guard() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        let peer = axum::extract::ConnectInfo(std::net::SocketAddr::from(([100, 64, 1, 2], 50000)));
        let error = crate::routes::media::set_rating(
            axum::extract::State(state.clone()),
            Some(peer.clone()),
            axum::extract::Path(1),
            axum::Json(crate::routes::media::RatingBody { rating: 4 }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        let error = crate::routes::media::bulk(
            axum::extract::State(state),
            Some(peer),
            axum::Json(crate::routes::media::BulkMediaBody {
                ids: vec![1],
                action: "add_tag".into(),
                group_id: None,
                tag: Some("reviewed".into()),
                rating: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
    }

    #[test]
    fn byte_ranges_are_clamped_and_malformed_ranges_are_rejected() {
        assert_eq!(plan_range(10, Some("bytes=2-5")).unwrap().length, 4);
        assert_eq!(plan_range(10, Some("bytes=8-")).unwrap().end(), 9);
        assert_eq!(plan_range(10, Some("bytes=-3")).unwrap().start, 7);
        assert_eq!(plan_range(10, Some("bytes=8-999")).unwrap().end(), 9);
        for invalid in [
            "bytes=10-",
            "bytes=5-4",
            "bytes=-0",
            "bytes=0-1,3-4",
            "items=0-1",
            "bytes= 0-1",
            "bytes=0-18446744073709551616",
        ] {
            assert!(plan_range(10, Some(invalid)).is_err(), "{invalid}");
        }
        assert_eq!(plan_range(0, None).unwrap().length, 0);
        assert!(plan_range(0, Some("bytes=0-")).is_err());
    }

    #[tokio::test]
    async fn server_stream_matches_validated_range_plan() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        std::fs::write(state.library_dir.join("test/clip.mp4"), b"abcdefghij").unwrap();
        state.pool.get().unwrap().execute(
            "INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES(1,1,'test/clip.mp4','clip.mp4','video','now')",
            [],
        ).unwrap();

        for (method, range, expected_status, expected_body, expected_range) in [
            ("GET", None, StatusCode::OK, "abcdefghij", None),
            (
                "GET",
                Some("bytes=2-5"),
                StatusCode::PARTIAL_CONTENT,
                "cdef",
                Some("bytes 2-5/10"),
            ),
            (
                "GET",
                Some("bytes=-3"),
                StatusCode::PARTIAL_CONTENT,
                "hij",
                Some("bytes 7-9/10"),
            ),
            (
                "HEAD",
                Some("bytes=2-5"),
                StatusCode::PARTIAL_CONTENT,
                "",
                Some("bytes 2-5/10"),
            ),
        ] {
            let mut builder = Request::builder().method(method).uri("/api/media/1/stream");
            if let Some(range) = range {
                builder = builder.header(header::RANGE, range);
            }
            let response = crate::router((*state).clone())
                .oneshot(builder.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers().get(header::ACCEPT_RANGES).unwrap(),
                "bytes"
            );
            assert_eq!(
                response
                    .headers()
                    .get(header::CONTENT_RANGE)
                    .map(|value| value.to_str().unwrap()),
                expected_range
            );
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            assert_eq!(body.as_ref(), expected_body.as_bytes());
        }

        let response = crate::router((*state).clone())
            .oneshot(
                Request::builder()
                    .uri("/api/media/1/stream")
                    .header(header::RANGE, "bytes=10-")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            response.headers().get(header::CONTENT_RANGE).unwrap(),
            "bytes */10"
        );

        let missing = crate::router((*state).clone())
            .oneshot(
                Request::builder()
                    .uri("/api/media/999/stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }
}

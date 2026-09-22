//! Validated byte ranges for native media streaming.

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

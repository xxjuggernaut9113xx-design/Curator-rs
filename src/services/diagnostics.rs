//! Bounded, redacted diagnostic logging shared by native and HTTP clients.

use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::OnceLock,
};

use regex::Regex;
use tracing_appender::non_blocking::NonBlocking;

const MAX_TAIL_BYTES: u64 = 1024 * 1024;

pub fn rotating_file_writer(
    log_path: &Path,
) -> Result<tracing_appender::rolling::RollingFileAppender, tracing_appender::rolling::InitError> {
    tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix("curator")
        .filename_suffix("log")
        .max_log_files(8)
        .build(log_path.parent().unwrap_or(Path::new(".")))
}

fn secret_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"(?i)\b(cookie|set-cookie|password|passwd|token|access_token|refresh_token|secret|client_secret|api[_-]?key|session|signature)\s*[:=]\s*[^\s,;\r\n&]+").unwrap())
}

fn authorization_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)\b(authorization)\s*[:=]\s*[^\s,;\r\n&]+(?:\s+[^\s,;\r\n&]+)?").unwrap()
    })
}

fn bearer_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/-]+=*").unwrap())
}

fn query_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(r"(?i)([?&](?:token|access_token|refresh_token|key|api_key|password|secret|client_secret|session|auth|signature)=)[^&\s]+").unwrap()
    })
}

fn userinfo_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| Regex::new(r"(?i)(https?://)[^/@\s]+@").unwrap())
}

pub fn redact(input: &str) -> String {
    let text = userinfo_pattern().replace_all(input, "${1}[redacted]@");
    let text = query_pattern().replace_all(&text, "${1}[redacted]");
    let text = bearer_pattern().replace_all(&text, "${1} [redacted]");
    let text = authorization_pattern().replace_all(&text, "${1}=[redacted]");
    secret_pattern()
        .replace_all(&text, "${1}=[redacted]")
        .into_owned()
}

#[derive(Clone)]
pub struct RedactingMakeWriter(pub NonBlocking);

pub struct RedactingWriter {
    sink: NonBlocking,
    buffer: Vec<u8>,
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RedactingMakeWriter {
    type Writer = RedactingWriter;

    fn make_writer(&'a self) -> Self::Writer {
        RedactingWriter {
            sink: self.0.clone(),
            buffer: Vec::new(),
        }
    }
}

impl Write for RedactingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Drop for RedactingWriter {
    fn drop(&mut self) {
        let text = String::from_utf8_lossy(&self.buffer);
        let _ = self.sink.write_all(redact(&text).as_bytes());
    }
}

/// Find the current daily log, falling back to a pre-rotation curator.log.
pub fn latest_log_path(legacy_path: &Path) -> Option<PathBuf> {
    let directory = legacy_path.parent()?;
    let mut files = std::fs::read_dir(directory)
        .ok()?
        .filter_map(Result::ok)
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            name.starts_with("curator.") && name.ends_with(".log") && name != "curator.log"
        })
        .filter_map(|entry| {
            let metadata = entry.metadata().ok()?;
            metadata
                .is_file()
                .then(|| (metadata.modified().ok(), entry.path()))
        })
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    files
        .pop()
        .map(|(_, path)| path)
        .or_else(|| legacy_path.exists().then(|| legacy_path.to_path_buf()))
}

pub fn location(legacy_path: &Path) -> PathBuf {
    latest_log_path(legacy_path).unwrap_or_else(|| legacy_path.to_path_buf())
}

pub fn read_tail(legacy_path: &Path, max_lines: usize) -> io::Result<String> {
    let path = location(legacy_path);
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    file.seek(SeekFrom::Start(size.saturating_sub(MAX_TAIL_BYTES)))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let text = String::from_utf8_lossy(&bytes);
    let lines = text.lines().collect::<Vec<_>>();
    let first = lines.len().saturating_sub(max_lines);
    Ok(redact(&lines[first..].join("\n")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    use tracing_subscriber::fmt::MakeWriter;

    #[test]
    fn hides_secrets_in_urls_headers_and_legacy_logs() {
        let text = "Authorization: Bearer abc Cookie: session=xyz https://alice:pass@example.com/a?token=s3cr3t&safe=1 password=hunter2";
        let redacted = redact(text);
        for secret in [
            "Bearer abc",
            "session=xyz",
            "alice:pass",
            "token=s3cr3t",
            "password=hunter2",
        ] {
            assert!(!redacted.contains(secret), "leaked {secret}: {redacted}");
        }
        assert!(redacted.contains("safe=1"));
    }

    #[test]
    fn file_writer_redacts_before_persisting() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("writer.log");
        let file = std::fs::File::create(&path).unwrap();
        let (sink, guard) = tracing_appender::non_blocking(file);
        let make_writer = RedactingMakeWriter(sink);
        {
            let mut writer = make_writer.make_writer();
            writer.write_all(b"token=private-value\n").unwrap();
        }
        drop(make_writer);
        drop(guard);
        let text = std::fs::read_to_string(path).unwrap();
        assert_eq!(text, "token=[redacted]\n");
    }

    #[test]
    fn production_writer_creates_discoverable_daily_log() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("curator.log");
        let mut writer = rotating_file_writer(&legacy).unwrap();
        writer.write_all(b"rotation probe\n").unwrap();
        writer.flush().unwrap();
        let path = location(&legacy);
        assert_ne!(path, legacy);
        assert!(path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("curator."));
        assert!(path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".log"));
        assert!(std::fs::read_to_string(path)
            .unwrap()
            .contains("rotation probe"));
    }

    #[test]
    fn selects_rotated_log_and_bounds_tail() {
        let dir = tempfile::tempdir().unwrap();
        let legacy = dir.path().join("curator.log");
        std::fs::write(&legacy, "old\n").unwrap();
        let rotated = dir.path().join("curator.2026-09-22.log");
        std::fs::write(&rotated, "first\npassword=hidden\nlast\n").unwrap();
        assert_eq!(location(&legacy), rotated);
        assert_eq!(read_tail(&legacy, 2).unwrap(), "password=[redacted]\nlast");
    }

    #[tokio::test]
    async fn native_and_http_log_views_share_redacted_tail() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        std::fs::write(&state.log_path, "started\ntoken=private-value\n").unwrap();
        let client = crate::native::Client::Local(
            crate::native::LocalClient::new((*state).clone()).unwrap(),
        );
        let direct = client.diagnostic_log().await.unwrap();
        let response = crate::router((*state).clone())
            .oneshot(
                Request::builder()
                    .uri("/api/log")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "text/plain; charset=utf-8"
        );
        let http = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(direct.as_bytes(), http.as_ref());
        assert!(!direct.contains("private-value"));
    }

    #[tokio::test]
    async fn viewer_log_access_is_denied_before_network() {
        let client = crate::native::Client::Remote(
            crate::native::RemoteClient::from_validated_peer("http://100.64.1.2:42168").unwrap(),
        );
        assert!(client
            .diagnostic_log()
            .await
            .unwrap_err()
            .contains("only on Host"));
    }

    #[tokio::test]
    async fn source_log_adapter_redacts_stored_errors() {
        let dir = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(dir.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute(
            "UPDATE sources SET log='token=source-secret', error_message='password=error-secret' WHERE id=1",
            [],
        ).unwrap();
        let response = crate::router((*state).clone())
            .oneshot(
                Request::builder()
                    .uri("/api/sources/1/log")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["log"], "token=[redacted]");
        assert_eq!(value["error_message"], "password=[redacted]");
    }
}

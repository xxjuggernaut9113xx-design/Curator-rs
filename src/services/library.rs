//! Library reads shared by Host and the Server HTTP adapter.
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::AppState;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MediaPage {
    pub media: Vec<HashMap<String, Value>>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
    pub next_after_id: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LibraryError {
    BadRequest(String),
    Internal(String),
    Forbidden,
    ShuttingDown,
    Maintenance,
}

impl LibraryError {
    pub fn message(&self) -> &str {
        match self {
            Self::BadRequest(message) | Self::Internal(message) => message,
            Self::Forbidden => "Viewer cannot open a local library",
            Self::ShuttingDown => "Curator is shutting down",
            Self::Maintenance => "A local maintenance job is active",
        }
    }
}

fn db_err(error: impl std::fmt::Display) -> LibraryError {
    LibraryError::Internal(error.to_string())
}

/// Keep the operational rating definition in one SQL expression. `rating`
/// remains a backwards-compatible cache for older clients, but every new
/// decision path uses this precedence: human star, reserved human `pace:*`
/// tag, temporal P-HAR Fast suggestion, then NudeNet 1-3 evidence.
pub const EFFECTIVE_RATING_SQL: &str = "COALESCE(m.human_rating, NULLIF((SELECT MAX(CASE LOWER(t.name) WHEN 'pace:slow' THEN 2 WHEN 'pace:medium' THEN 3 WHEN 'pace:fast' THEN 4 WHEN 'pace:cum' THEN 5 ELSE 0 END) FROM media_tags pmt JOIN tags t ON t.id=pmt.tag_id JOIN media_tag_provenance mtp ON mtp.media_id=pmt.media_id AND mtp.tag_id=pmt.tag_id WHERE pmt.media_id=m.id AND mtp.provenance IN ('human','human_edited')), 0), NULLIF(m.action_rating, 0), NULLIF(m.auto_rating, 0), 0)";

const HUMAN_PACE_TAG_SQL: &str = "EXISTS(SELECT 1 FROM media_tags pmt JOIN tags t ON t.id=pmt.tag_id JOIN media_tag_provenance mtp ON mtp.media_id=pmt.media_id AND mtp.tag_id=pmt.tag_id WHERE pmt.media_id=m.id AND LOWER(t.name) IN ('pace:slow','pace:medium','pace:fast','pace:cum') AND mtp.provenance IN ('human','human_edited'))";

fn pace_label_sql() -> String {
    format!(
        "CASE {EFFECTIVE_RATING_SQL} WHEN 1 THEN 'sfw' WHEN 2 THEN 'slow' WHEN 3 THEN 'medium' WHEN 4 THEN 'fast' WHEN 5 THEN 'cum' ELSE 'unrated' END"
    )
}

// ─── Sort orders ─────────────────────────────────────────────────────────────

fn sort_order(sort: &str) -> &'static str {
    match sort {
        "size_desc" => "COALESCE(m.file_size_bytes,-1) DESC, m.id ASC",
        "size_asc" => "COALESCE(m.file_size_bytes,9223372036854775807) ASC, m.id ASC",
        // Rating order is expanded in `list` so the tag/action precedence in
        // `EFFECTIVE_RATING_SQL` is retained. These arms remain for callers
        // that only need a valid deterministic fallback.
        "rating_desc" => "m.id DESC",
        "rating_asc" => "m.id ASC",
        "date_desc" => "m.added_at DESC, m.id DESC",
        "date_asc" => "m.added_at ASC, m.id ASC",
        "filename_asc" => "m.filename COLLATE NOCASE ASC, m.id ASC",
        "filename_desc" => "m.filename COLLATE NOCASE DESC, m.id ASC",
        _ => "m.id ASC",
    }
}

// ─── GET /api/media ───────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MediaQuery {
    pub search: Option<String>,
    pub creator: Option<String>,
    pub limit: Option<usize>,
    pub after_id: Option<i64>,
    pub cursor: Option<String>,
    pub shuffle_seed: Option<i64>,
    pub media_type: Option<String>,
    pub tags: Option<String>,
    pub any_tags: Option<String>,
    pub exclude_tags: Option<String>,
    pub source_id: Option<i64>,
    pub group_id: Option<i64>,
    pub only_included: Option<bool>,
    pub tag: Option<String>,
    pub sort: Option<String>,
    /// Hide anything rated above this (0 = unrated is always shown
    /// regardless, since it hasn't been rated — manually or by the NSFW
    /// auto-rater — yet).
    pub max_rating: Option<i64>,
    pub rating_status: Option<String>,
    pub include_long_videos: Option<bool>,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
    pub unknown_size: Option<bool>,
}

pub async fn list(state: &AppState, q: MediaQuery) -> Result<MediaPage, LibraryError> {
    if !state.edition.owns_library() {
        return Err(LibraryError::Forbidden);
    }
    if state.shutdown.is_cancelled() {
        return Err(LibraryError::ShuttingDown);
    }
    let _lease = state
        .maintenance
        .try_acquire_background_worker()
        .ok_or(LibraryError::Maintenance)?;
    if state.shutdown.is_cancelled() {
        return Err(LibraryError::ShuttingDown);
    }
    let max_clip_length_secs = state.settings.read().await.max_clip_length_secs;
    let limit = q.limit.unwrap_or(100).clamp(1, 500);
    let seed = q.shuffle_seed.unwrap_or(1).clamp(1, 2147483646);
    let sort = q.sort.as_deref().unwrap_or("default");
    let (key, descending, id_desc) = match sort {
        // A cursor has to use precisely the expression used by ORDER BY.
        // `rating` is only a compatibility cache, while the visible value
        // honors human decisions, pace tags, P-HAR, then NudeNet.
        "rating_desc" => (EFFECTIVE_RATING_SQL.to_string(), true, false),
        "rating_asc" => (EFFECTIVE_RATING_SQL.to_string(), false, false),
        "date_desc" => ("m.added_at".to_string(), true, true),
        "date_asc" => ("m.added_at".to_string(), false, false),
        "filename_asc" => ("m.filename COLLATE NOCASE".to_string(), false, false),
        "filename_desc" => ("m.filename COLLATE NOCASE".to_string(), true, false),
        "size_desc" => ("COALESCE(m.file_size_bytes,-1)".to_string(), true, false),
        "size_asc" => (
            "COALESCE(m.file_size_bytes,9223372036854775807)".to_string(),
            false,
            false,
        ),
        "shuffle" => (format!("((m.id * {seed}) % 2147483647)"), false, false),
        _ => ("m.id".to_string(), false, false),
    };
    let base_order = if sort == "shuffle" {
        format!("{key}, m.id")
    } else if sort == "rating_desc" {
        format!("{EFFECTIVE_RATING_SQL} DESC, m.id ASC")
    } else if sort == "rating_asc" {
        format!("{EFFECTIVE_RATING_SQL} ASC, m.id ASC")
    } else {
        sort_order(sort).to_string()
    };
    // Manual-review clips (including intentionally skipped long videos) are
    // always surfaced before ordinary automatic suggestions.
    let order = if q.rating_status.as_deref() == Some("needs_review") {
        format!("m.manual_review_required DESC, {base_order}")
    } else {
        base_order
    };

    // Get group effective tags — read from cache or rebuild
    let group_effective_tags = effective_tags(state).await?;

    let mut params: Vec<rusqlite::types::Value> = Vec::new();
    let scope_sql = if let Some(sid) = q.source_id {
        params.push(sid.into());
        "m.source_id=?1"
    } else if let Some(gid) = q.group_id {
        if gid == 0 {
            "s.group_id IS NULL"
        } else {
            params.push(gid.into());
            "s.group_id IN (WITH RECURSIVE subtree(id) AS (SELECT ?1 UNION SELECT g.id FROM groups g JOIN subtree st ON g.parent_id=st.id) SELECT id FROM subtree)"
        }
    } else if q.only_included.unwrap_or(false) {
        "s.included=1"
    } else {
        "1=1"
    };
    let rating_clause = if let Some(max) = q.max_rating {
        params.push(max.into());
        format!(
            " AND ({EFFECTIVE_RATING_SQL}=0 OR {EFFECTIVE_RATING_SQL}<=?{})",
            params.len()
        )
    } else {
        String::new()
    };

    // Retention and size-limit decisions keep a durable media row so users
    // can see why an original is unavailable. Ordinary missing files remain
    // hidden, preserving the historical library behavior.
    let mut extra = String::from(
        " AND (m.missing=0 OR COALESCE(m.retention_deleted,0)=1 OR m.skip_reason IS NOT NULL)",
    );
    if let Some(search) = q.search.as_deref().filter(|s| !s.trim().is_empty()) {
        // Literal substring matching avoids interpreting user-entered SQL
        // wildcard characters. Apply before keyset pagination.
        params.push(search.trim().to_lowercase().into());
        let n = params.len();
        extra.push_str(&format!(" AND (instr(lower(m.filename),?{n})>0 OR instr(lower(s.name),?{n})>0 OR EXISTS(SELECT 1 FROM source_metadata sm WHERE sm.media_id=m.id AND instr(lower(sm.creator),?{n})>0))"));
    }
    if let Some(creator) = q.creator.as_deref().filter(|s| !s.trim().is_empty()) {
        params.push(creator.trim().to_lowercase().into());
        let n = params.len();
        extra.push_str(&format!(" AND EXISTS(SELECT 1 FROM source_metadata sm WHERE sm.media_id=m.id AND instr(lower(sm.creator),?{n})>0)"));
    }
    let rating_status_clause = match q.rating_status.as_deref().unwrap_or("") {
        "" | "all" => String::new(),
        "unrated" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND m.auto_rating=0 AND m.action_rating=0"),
        "auto" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND (m.auto_rating>0 OR m.action_rating>0)"),
        "needs_review" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND (m.manual_review_required=1 OR m.auto_rating>0 OR m.action_rating>0)"),
        "reviewed" => format!(" AND (m.human_rating IS NOT NULL OR {HUMAN_PACE_TAG_SQL})"),
        _ => {
            return Err(LibraryError::BadRequest("Invalid rating_status".into()))
        }
    };
    extra.push_str(&rating_status_clause);
    if let Some(kind) = q.media_type.as_deref() {
        match kind {
            "image" => extra.push_str(" AND m.type='image'"),
            "clip" => {
                params.push((max_clip_length_secs as i64).into());
                extra.push_str(&format!(
                    " AND m.type='video' AND m.duration_secs IS NOT NULL AND m.duration_secs<=?{}",
                    params.len()
                ));
            }
            "video" => {
                params.push((max_clip_length_secs as i64).into());
                extra.push_str(&format!(
                    " AND m.type='video' AND (m.duration_secs IS NULL OR m.duration_secs>?{})",
                    params.len()
                ));
            }
            _ => {}
        }
    }
    if q.rating_status.as_deref() == Some("needs_review") && q.include_long_videos != Some(true) {
        params.push((max_clip_length_secs as i64).into());
        extra.push_str(&format!(
            " AND (m.manual_review_required=1 OR m.type<>'video' OR m.duration_secs IS NULL OR m.duration_secs<=?{})",
            params.len()
        ));
    }
    if q.unknown_size.unwrap_or(false) {
        if q.min_size.is_some() || q.max_size.is_some() {
            return Err(LibraryError::BadRequest(
                "unknown_size cannot be combined with min_size or max_size".into(),
            ));
        }
        extra.push_str(" AND m.file_size_bytes IS NULL");
    } else {
        if let Some(minimum) = q.min_size {
            if minimum < 0 {
                return Err(LibraryError::BadRequest(
                    "min_size must be non-negative".into(),
                ));
            }
            params.push(minimum.into());
            extra.push_str(&format!(" AND m.file_size_bytes>=?{}", params.len()));
        }
        if let Some(maximum) = q.max_size {
            if maximum < 0 {
                return Err(LibraryError::BadRequest(
                    "max_size must be non-negative".into(),
                ));
            }
            if q.min_size.is_some_and(|minimum| minimum > maximum) {
                return Err(LibraryError::BadRequest(
                    "min_size must not exceed max_size".into(),
                ));
            }
            params.push(maximum.into());
            extra.push_str(&format!(" AND m.file_size_bytes<=?{}", params.len()));
        }
    }
    if let Some(tag) = q.tag.as_deref() {
        extra.push_str(" AND ");
        extra.push_str(&tag_predicate(
            tag.trim().to_lowercase().as_str(),
            &group_effective_tags,
            &mut params,
        ));
    }
    for (raw, mode) in [
        (&q.tags, "AND"),
        (&q.any_tags, "OR"),
        (&q.exclude_tags, "NOT"),
    ] {
        let predicates: Vec<String> = raw
            .as_deref()
            .unwrap_or("")
            .split(',')
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(|t| tag_predicate(&t.to_lowercase(), &group_effective_tags, &mut params))
            .collect();
        if !predicates.is_empty() {
            extra.push_str(&format!(
                " AND {}({})",
                if mode == "NOT" { "NOT " } else { "" },
                predicates.join(if mode == "AND" { " AND " } else { " OR " })
            ));
        }
    }
    let anchor: Option<(Value, i64)> = if let Some(cursor) = q.cursor.as_ref() {
        Some(
            hex::decode(cursor)
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .ok_or_else(|| LibraryError::BadRequest("Invalid media cursor".into()))?,
        )
    } else if let Some(id) = q.after_id {
        let conn = state.pool.get().map_err(db_err)?;
        let sql = format!("SELECT {key} FROM media m WHERE m.id=?1");
        let value = conn
            .query_row(&sql, [id], |r| Ok(sql_value(r.get_ref(0)?)))
            .map_err(|_| {
                LibraryError::BadRequest("Cursor media no longer exists; use next_cursor".into())
            })?;
        Some((value, id))
    } else {
        None
    };
    if let Some((value, id)) = anchor {
        let v = match value {
            Value::String(s) => rusqlite::types::Value::Text(s),
            Value::Number(n) => rusqlite::types::Value::Integer(
                n.as_i64().ok_or_else(|| db_err("Invalid cursor value"))?,
            ),
            _ => return Err(db_err("Invalid cursor value")),
        };
        params.push(v);
        let n = params.len();
        params.push(id.into());
        let i = params.len();
        if key == "m.id" {
            extra.push_str(&format!(" AND m.id>?{i} AND m.id>=?{n}"));
        } else {
            extra.push_str(&format!(
                " AND ({key} {} ?{n} OR ({key} = ?{n} AND m.id {} ?{i}))",
                if descending { "<" } else { ">" },
                if id_desc { "<" } else { ">" }
            ));
        }
    }
    params.push(((limit + 1) as i64).into());
    let limit_param = params.len();
    let pace_label = pace_label_sql();
    let query = format!(
        "SELECT m.id,m.source_id,m.filepath,m.filename,m.type,m.added_at,m.downloaded_at,m.modified_at,{EFFECTIVE_RATING_SQL} AS rating,{pace_label} AS pace_label,m.human_rating,m.auto_rating,m.auto_rating_score,m.action_rating,m.rating_source,m.rating_reviewed,m.rating_reviewed_at,m.origin_url,m.downloaded,m.duration_secs,m.clip_parent_id,m.file_size_bytes,m.clip_start_secs,m.clip_end_secs,m.classifier_model,m.classifier_version,m.classifier_score,m.classifier_evidence,m.action_model,m.action_model_version,m.action_score,m.action_evidence,m.classification_label,m.manual_review_required,m.manual_review_reason,m.classification_updated_at,m.skip_reason,m.skip_limit_bytes,m.skipped_at,m.retention_deleted,CASE WHEN m.clip_start_secs IS NOT NULL THEN (SELECT filepath FROM media parent WHERE parent.id=m.clip_parent_id) ELSE m.filepath END AS playback_filepath, {key} AS _cursor_key, s.group_id AS _source_group_id, (SELECT GROUP_CONCAT(mg.group_id, ',') FROM media_groups mg WHERE mg.media_id=m.id) AS _media_group_ids, s.name AS source, s.url AS source_url, (SELECT sm.creator FROM source_metadata sm WHERE sm.media_id=m.id ORDER BY sm.id DESC LIMIT 1) AS creator, \
            (SELECT GROUP_CONCAT(t.name, ',') FROM media_tags mt \
             JOIN tags t ON t.id = mt.tag_id WHERE mt.media_id = m.id) AS tags_csv \
         FROM media m \
         JOIN sources s ON s.id = m.source_id \
         WHERE ({}){}{extra} \
         ORDER BY {} LIMIT ?{limit_param}",
        scope_sql, rating_clause, order
    );

    let mut rows: Vec<HashMap<String, Value>> = {
        let conn = state.pool.get().map_err(db_err)?;
        let mut stmt = conn.prepare(&query).map_err(db_err)?;

        let params_refs: Vec<&dyn rusqlite::ToSql> =
            params.iter().map(|b| b as &dyn rusqlite::ToSql).collect();

        let out = stmt
            .query_map(params_refs.as_slice(), |row| {
                let col_count = row.as_ref().column_count();
                let col_names: Vec<String> = (0..col_count)
                    .map(|i| row.as_ref().column_name(i).unwrap_or("?").to_string())
                    .collect();

                let mut map = HashMap::new();
                for (i, name) in col_names.iter().enumerate() {
                    let val: Value = match row.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Null) => Value::Null,
                        Ok(rusqlite::types::ValueRef::Integer(n)) => json!(n),
                        Ok(rusqlite::types::ValueRef::Real(f)) => json!(f),
                        Ok(rusqlite::types::ValueRef::Text(s)) => {
                            json!(std::str::from_utf8(s).unwrap_or(""))
                        }
                        Ok(rusqlite::types::ValueRef::Blob(b)) => {
                            json!(std::str::from_utf8(b).unwrap_or(""))
                        }
                        Err(_) => Value::Null,
                    };
                    map.insert(name.clone(), val);
                }
                Ok(map)
            })
            .map_err(db_err)?
            .filter_map(|r| r.ok())
            .collect();
        out
    };

    let has_more = rows.len() > limit;
    rows.truncate(limit);
    let next_cursor = if has_more {
        rows.last().map(|r| {
            hex::encode(
                serde_json::to_vec(&(r["_cursor_key"].clone(), r["id"].as_i64().unwrap_or(0)))
                    .unwrap_or_default(),
            )
        })
    } else {
        None
    };
    let next_after_id = if has_more {
        rows.last().and_then(|r| r["id"].as_i64())
    } else {
        None
    };

    let mut media = Vec::new();
    for mut r in rows {
        r.remove("_cursor_key");
        r.insert("rating_reviewed".into(), json!(r["rating_reviewed"] == 1));
        r.insert(
            "manual_review_required".into(),
            json!(r["manual_review_required"] == 1),
        );
        let source_group_id: Option<i64> = r.remove("_source_group_id").and_then(|v| v.as_i64());
        let tags_csv = r
            .remove("tags_csv")
            .and_then(|v| v.as_str().map(|s| s.to_string()));

        let own_tags: HashSet<String> = tags_csv
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| s.split(',').map(|t| t.to_string()).collect())
            .unwrap_or_default();

        let inherited: HashSet<String> = source_group_id
            .and_then(|gid| group_effective_tags.get(&gid).cloned())
            .unwrap_or_default();

        let effective_tags = own_tags.union(&inherited).cloned().collect::<HashSet<_>>();

        let mut own_sorted: Vec<String> = own_tags.iter().cloned().collect();
        own_sorted.sort();
        let mut inh_sorted: Vec<String> = (effective_tags.difference(&own_tags)).cloned().collect();
        inh_sorted.sort();

        r.insert("tags".into(), json!(own_sorted));
        r.insert("inherited_tags".into(), json!(inh_sorted));
        media.push(r);
    }

    Ok(MediaPage {
        media,
        has_more,
        next_cursor,
        next_after_id,
        limit,
    })
}

pub async fn effective_tags(
    state: &AppState,
) -> Result<Arc<HashMap<i64, HashSet<String>>>, LibraryError> {
    if let Some(cached) = state.group_tag_cache.read().await.as_ref() {
        return Ok(cached.clone());
    }
    let mut cache = state.group_tag_cache.write().await;
    if let Some(cached) = cache.as_ref() {
        return Ok(cached.clone());
    }
    let rebuilt = {
        let conn = state.pool.get().map_err(db_err)?;
        Arc::new(crate::db::build_group_effective_tags_map(&conn).map_err(db_err)?)
    };
    *cache = Some(rebuilt.clone());
    Ok(rebuilt)
}

pub fn tag_predicate(
    tag: &str,
    groups: &HashMap<i64, HashSet<String>>,
    params: &mut Vec<rusqlite::types::Value>,
) -> String {
    params.push(tag.to_string().into());
    let t = params.len();
    let ids: Vec<i64> = groups
        .iter()
        .filter(|(_, tags)| tags.contains(tag))
        .map(|(id, _)| *id)
        .collect();
    params.push(
        serde_json::to_string(&ids)
            .unwrap_or_else(|_| "[]".into())
            .into(),
    );
    let g = params.len();
    format!("(EXISTS(SELECT 1 FROM media_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.media_id=m.id AND t.name=?{t}) OR s.group_id IN (SELECT value FROM json_each(?{g})))")
}

fn sql_value(value: rusqlite::types::ValueRef<'_>) -> Value {
    match value {
        rusqlite::types::ValueRef::Integer(i) => json!(i),
        rusqlite::types::ValueRef::Text(s) => json!(String::from_utf8_lossy(s)),
        rusqlite::types::ValueRef::Real(f) => json!(f),
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use tower::ServiceExt;

    async fn http_list(state: &AppState, path: &str) -> (u16, Value) {
        let response = crate::router(state.clone())
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status().as_u16();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (status, serde_json::from_slice(&bytes).unwrap())
    }

    #[tokio::test]
    async fn direct_native_and_http_library_reads_agree() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at,file_size_bytes) VALUES (1,1,'a','a.jpg','image','2026',10),(2,1,'b','b.jpg','image','2026',20);").unwrap();

        let query = MediaQuery {
            search: Some(".jpg".into()),
            sort: Some("size_desc".into()),
            limit: Some(1),
            ..Default::default()
        };
        let first = list(&state, query.clone()).await.unwrap();
        let (status, from_http) =
            http_list(&state, "/api/media?search=.jpg&sort=size_desc&limit=1").await;
        assert_eq!(status, 200);
        assert_eq!(serde_json::to_value(&first).unwrap(), from_http);
        assert_eq!(first.media[0]["id"], 2);

        let cursor = first.next_cursor.clone().unwrap();
        let second = list(
            &state,
            MediaQuery {
                cursor: Some(cursor.clone()),
                ..query
            },
        )
        .await
        .unwrap();
        let (status, from_http) = http_list(
            &state,
            &format!("/api/media?search=.jpg&sort=size_desc&limit=1&cursor={cursor}"),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(serde_json::to_value(&second).unwrap(), from_http);
        assert_eq!(second.media[0]["id"], 1);

        let native = crate::native::LocalClient::new((*state).clone()).unwrap();
        let page = native
            .library(crate::native::LibraryQuery {
                search: Some(".jpg".into()),
                sort: "size_desc".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page.media.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[tokio::test]
    async fn invalid_query_and_admission_are_shared() {
        let root = tempfile::tempdir().unwrap();
        let mut state = crate::test_support::state(root.path());
        assert_eq!(
            list(
                &state,
                MediaQuery {
                    min_size: Some(-1),
                    ..Default::default()
                }
            )
            .await,
            Err(LibraryError::BadRequest(
                "min_size must be non-negative".into()
            ))
        );
        let (status, body) = http_list(&state, "/api/media?min_size=-1").await;
        assert_eq!(status, 400);
        assert_eq!(body["error"], "min_size must be non-negative");

        Arc::get_mut(&mut state).unwrap().edition = crate::edition::Edition::Viewer;
        assert_eq!(
            list(&state, MediaQuery::default()).await,
            Err(LibraryError::Forbidden)
        );
        Arc::get_mut(&mut state).unwrap().edition = crate::edition::Edition::Host;
        state.shutdown.cancel();
        assert_eq!(
            list(&state, MediaQuery::default()).await,
            Err(LibraryError::ShuttingDown)
        );
        let (status, body) = http_list(&state, "/api/media").await;
        assert_eq!(status, 503);
        assert_eq!(body["error"], "Curator is shutting down");
    }

    #[tokio::test]
    async fn creator_and_size_filters_match_http_and_native() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at,file_size_bytes) VALUES (1,1,'a','a.jpg','image','2026',10),(2,1,'b','b.jpg','image','2026',20); INSERT INTO source_metadata(media_id,provider,source_url,raw_json,creator,captured_at) VALUES (1,'test','https://example.com/a','{}','Ada','2026'),(2,'test','https://example.com/b','{}','Bea','2026');").unwrap();
        let query = MediaQuery {
            creator: Some("ADA".into()),
            min_size: Some(5),
            max_size: Some(15),
            ..Default::default()
        };
        let direct = list(&state, query).await.unwrap();
        let (status, http) =
            http_list(&state, "/api/media?creator=ADA&min_size=5&max_size=15").await;
        assert_eq!(status, 200);
        assert_eq!(serde_json::to_value(&direct).unwrap(), http);
        assert_eq!(direct.media.len(), 1);
        assert_eq!(direct.media[0]["id"], 1);

        let native = crate::native::LocalClient::new((*state).clone()).unwrap();
        let page = native
            .library(crate::native::LibraryQuery {
                creator: Some("ADA".into()),
                min_size: Some(5),
                max_size: Some(15),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page.media.iter().map(|item| item.id).collect::<Vec<_>>(),
            vec![1]
        );
    }

    #[tokio::test]
    async fn maintenance_denies_direct_and_http_library_reads() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        state.running_sources.lock().await.insert(1);
        state
            .maintenance
            .start(
                state.clone(),
                crate::maintenance::MaintenanceRequest {
                    kind: crate::maintenance::MaintenanceKind::CreateBackup,
                    backup_id: None,
                    confirmation: String::new(),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            list(&state, MediaQuery::default()).await,
            Err(LibraryError::Maintenance)
        );
        let (status, body) = http_list(&state, "/api/media").await;
        assert_eq!(status, 409);
        assert_eq!(body["error"], "A local maintenance job is active");
        state.running_sources.lock().await.clear();
        for _ in 0..100 {
            if !state.maintenance.is_active() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(!state.maintenance.is_active());
        state.server_tasks.close();
        state.server_tasks.wait().await;
    }
}

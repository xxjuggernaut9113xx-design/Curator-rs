use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::db::now_iso;
use crate::provenance;
use crate::AppState;

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

#[derive(Deserialize, Default)]
pub struct MediaQuery {
    search: Option<String>,
    limit: Option<usize>,
    after_id: Option<i64>,
    cursor: Option<String>,
    shuffle_seed: Option<i64>,
    media_type: Option<String>,
    tags: Option<String>,
    any_tags: Option<String>,
    exclude_tags: Option<String>,
    source_id: Option<i64>,
    group_id: Option<i64>,
    only_included: Option<bool>,
    tag: Option<String>,
    sort: Option<String>,
    /// Hide anything rated above this (0 = unrated is always shown
    /// regardless, since it hasn't been rated — manually or by the NSFW
    /// auto-rater — yet).
    max_rating: Option<i64>,
    rating_status: Option<String>,
    include_long_videos: Option<bool>,
    min_size: Option<i64>,
    max_size: Option<i64>,
    unknown_size: Option<bool>,
}

pub async fn list(
    State(state): State<Arc<AppState>>,
    Query(q): Query<MediaQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
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
    let group_effective_tags = effective_tags(&state).await?;

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
    let rating_status_clause = match q.rating_status.as_deref().unwrap_or("") {
        "" | "all" => String::new(),
        "unrated" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND m.auto_rating=0 AND m.action_rating=0"),
        "auto" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND (m.auto_rating>0 OR m.action_rating>0)"),
        "needs_review" => format!(" AND m.human_rating IS NULL AND NOT ({HUMAN_PACE_TAG_SQL}) AND (m.manual_review_required=1 OR m.auto_rating>0 OR m.action_rating>0)"),
        "reviewed" => format!(" AND (m.human_rating IS NOT NULL OR {HUMAN_PACE_TAG_SQL})"),
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Invalid rating_status"})),
            ))
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
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"unknown_size cannot be combined with min_size or max_size"})),
            ));
        }
        extra.push_str(" AND m.file_size_bytes IS NULL");
    } else {
        if let Some(minimum) = q.min_size {
            if minimum < 0 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"min_size must be non-negative"})),
                ));
            }
            params.push(minimum.into());
            extra.push_str(&format!(" AND m.file_size_bytes>=?{}", params.len()));
        }
        if let Some(maximum) = q.max_size {
            if maximum < 0 {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"max_size must be non-negative"})),
                ));
            }
            if q.min_size.is_some_and(|minimum| minimum > maximum) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"min_size must not exceed max_size"})),
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
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error":"Invalid media cursor"})),
                    )
                })?,
        )
    } else if let Some(id) = q.after_id {
        let conn = state.pool.get().map_err(db_err)?;
        let sql = format!("SELECT {key} FROM media m WHERE m.id=?1");
        let value = conn
            .query_row(&sql, [id], |r| Ok(sql_value(r.get_ref(0)?)))
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Cursor media no longer exists; use next_cursor"})),
                )
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
        let conn = state.pool.get().map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;
        let mut stmt = conn.prepare(&query).map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

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
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": e.to_string()})),
                )
            })?
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

    Ok(Json(
        json!({ "media": media, "has_more":has_more, "next_cursor":next_cursor, "next_after_id":next_after_id, "limit":limit }),
    ))
}

// ─── PUT /api/media/:id/rating ───────────────────────────────────────────────

#[derive(Deserialize)]
pub struct RatingBody {
    pub rating: i64,
}

pub async fn set_rating(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<RatingBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !(1..=5).contains(&body.rating) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Rating must be between 1 and 5"})),
        ));
    }
    save_review(&state, id, Some(body.rating))
}

pub async fn approve_rating(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    save_review(&state, id, None)
}

fn save_review(
    state: &AppState,
    id: i64,
    rating: Option<i64>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let result = conn.query_row(
        "UPDATE media SET human_rating=COALESCE(?1,NULLIF(action_rating,0),NULLIF(auto_rating,0)),
         rating=COALESCE(?1,NULLIF(action_rating,0),NULLIF(auto_rating,0)), rating_source='human',
         rating_reviewed=1, rating_reviewed_at=?2 WHERE id=?3 AND (?1 IS NOT NULL OR action_rating=4 OR auto_rating BETWEEN 1 AND 5)
         RETURNING human_rating,auto_rating,auto_rating_score,action_rating,rating_source,rating_reviewed,rating_reviewed_at",
        rusqlite::params![rating, now_iso(), id], |r| Ok(json!({
            "id":id, "rating":r.get::<_,i64>(0)?, "pace_label":crate::nsfw::pace_label(r.get::<_,i64>(0)?), "human_rating":r.get::<_,Option<i64>>(0)?, "auto_rating":r.get::<_,i64>(1)?,
            "auto_rating_score":r.get::<_,Option<f64>>(2)?, "action_rating":r.get::<_,i64>(3)?, "rating_source":r.get::<_,String>(4)?,
            "rating_reviewed":r.get::<_,bool>(5)?, "rating_reviewed_at":r.get::<_,Option<String>>(6)?
        })));
    match result {
        Ok(value) => Ok(Json(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            let exists = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM media WHERE id=?1)",
                    [id],
                    |r| r.get::<_, bool>(0),
                )
                .map_err(db_err)?;
            Err((
                if exists {
                    StatusCode::CONFLICT
                } else {
                    StatusCode::NOT_FOUND
                },
                Json(
                    json!({"error":if exists {"No automated rating to approve"} else {"Media not found"}}),
                ),
            ))
        }
        Err(e) => Err(db_err(e)),
    }
}

#[derive(Deserialize)]
pub struct DurationBody {
    pub duration_secs: f64,
}

pub async fn set_duration(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<DurationBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if !body.duration_secs.is_finite() || body.duration_secs <= 0.0 || body.duration_secs > 604800.0
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid video duration"})),
        ));
    }
    let conn = state.pool.get().map_err(db_err)?;
    conn.execute("UPDATE media SET duration_secs=?1,duration_attempted=1 WHERE id=?2 AND type='video' AND duration_secs IS NULL", rusqlite::params![body.duration_secs,id]).map_err(db_err)?;
    Ok(Json(json!({"id":id})))
}

#[derive(Deserialize)]
pub struct UndoRatingBody {
    pub rating_reviewed_at: String,
}

pub async fn undo_rating(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<UndoRatingBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let result = conn.query_row("UPDATE media SET human_rating=NULL,rating=COALESCE(NULLIF(action_rating,0),auto_rating),
        rating_source=CASE WHEN action_rating=4 THEN 'auto_action' WHEN auto_rating>0 THEN 'auto' ELSE 'none' END,rating_reviewed=0,rating_reviewed_at=NULL
        WHERE id=?1 AND human_rating IS NOT NULL AND rating_reviewed_at=?2
        RETURNING rating,auto_rating,auto_rating_score,rating_source", rusqlite::params![id,body.rating_reviewed_at], |r| Ok(json!({
            "id":id,"rating":r.get::<_,i64>(0)?,"pace_label":crate::nsfw::pace_label(r.get::<_,i64>(0)?),"auto_rating":r.get::<_,i64>(1)?,"auto_rating_score":r.get::<_,Option<f64>>(2)?,
            "human_rating":null,"rating_source":r.get::<_,String>(3)?,"rating_reviewed":false,"rating_reviewed_at":null
        })));
    match result {
        Ok(value) => Ok(Json(value)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Err((
            StatusCode::CONFLICT,
            Json(json!({"error":"Rating changed since this review; cannot undo it"})),
        )),
        Err(e) => Err(db_err(e)),
    }
}

#[derive(Deserialize)]
pub struct TagBody {
    pub name: String,
}

pub async fn add_tag(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Json(body): Json<TagBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let exists: bool = conn
        .query_row("SELECT COUNT(*) FROM media WHERE id=?1", [id], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap_or(0)
        > 0;
    if !exists {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": "Media not found"})),
        ));
    }

    provenance::attach_tag(&conn, id, &body.name, provenance::HUMAN, None).map_err(db_err)?;

    let tags: Vec<String> = {
        let mut stmt = conn.prepare("SELECT t.name FROM media_tags mt JOIN tags t ON t.id=mt.tag_id WHERE mt.media_id=?1 ORDER BY t.name COLLATE NOCASE").map_err(db_err)?;
        let out = stmt
            .query_map([id], |r| r.get(0))
            .map_err(db_err)?
            .filter_map(|r| r.ok())
            .collect();
        out
    };

    Ok(Json(json!({ "media_id": id, "tags": tags })))
}

// ─── DELETE /api/media/:id/tags/:tag_id ──────────────────────────────────────

pub async fn remove_tag(
    State(state): State<Arc<AppState>>,
    Path((id, tag_id)): Path<(i64, i64)>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    conn.execute(
        "DELETE FROM media_tags WHERE media_id=?1 AND tag_id=?2",
        rusqlite::params![id, tag_id],
    )
    .map_err(db_err)?;
    Ok(Json(json!({ "status": "removed" })))
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct BulkMediaBody {
    #[serde(default)]
    pub ids: Vec<i64>,
    pub action: String,
    pub group_id: Option<i64>,
    pub tag: Option<String>,
    pub rating: Option<i64>,
}

/// Server-side bulk organization for Explorer multi-select. The action names
/// intentionally mirror the UI and reuse the existing media/tag/metadata
/// models instead of creating a separate library store.
pub async fn bulk(
    State(state): State<Arc<AppState>>,
    Json(body): Json<BulkMediaBody>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut ids = body.ids.clone();
    ids.sort_unstable();
    ids.dedup();
    if ids.is_empty() || ids.len() > 500 || ids.iter().any(|id| *id <= 0) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Select between one and 500 valid media items"})),
        ));
    }
    let (updated, failed) = match body.action.as_str() {
        "add_group" | "move" => bulk_groups(&state, &ids, &body).await?,
        "add_tag" => bulk_add_tag(&state, &ids, body.tag.as_deref())?,
        "set_rating" => bulk_set_rating(&state, &ids, body.rating)?,
        "refresh_metadata" => {
            let worker_state = Arc::clone(&state);
            let worker_ids = ids.clone();
            tokio::task::spawn_blocking(move || bulk_refresh_metadata(&worker_state, &worker_ids))
                .await
                .map_err(db_err)??
        }
        "delete" => {
            let worker_state = Arc::clone(&state);
            let worker_ids = ids.clone();
            tokio::task::spawn_blocking(move || bulk_delete_files(&worker_state, &worker_ids))
                .await
                .map_err(db_err)??
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"Unknown bulk action"})),
            ))
        }
    };
    Ok(Json(
        json!({"action":body.action,"updated":updated,"failed":failed}),
    ))
}

async fn bulk_groups(
    state: &AppState,
    ids: &[i64],
    body: &BulkMediaBody,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    if body.action == "add_group" && body.group_id.is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A group is required"})),
        ));
    }
    let updated = {
        let conn = state.pool.get().map_err(db_err)?;
        if let Some(group_id) = body.group_id {
            let exists: bool = conn
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM groups WHERE id=?1)",
                    [group_id],
                    |row| row.get(0),
                )
                .map_err(db_err)?;
            if !exists {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({"error":"Group not found"})),
                ));
            }
        }
        let tx = conn.unchecked_transaction().map_err(db_err)?;
        let mut changed = 0;
        for id in ids {
            if body.action == "move" {
                tx.execute("DELETE FROM media_groups WHERE media_id=?1", [id])
                    .map_err(db_err)?;
            }
            if let Some(group_id) = body.group_id {
                changed += tx.execute(
                    "INSERT OR IGNORE INTO media_groups(media_id,group_id,added_at) VALUES(?1,?2,?3)",
                    rusqlite::params![id, group_id, now_iso()],
                ).map_err(db_err)?;
            } else if body.action == "move" {
                changed += 1;
            }
        }
        tx.commit().map_err(db_err)?;
        changed
    };
    *state.group_tag_cache.write().await = None;
    Ok((updated, Vec::new()))
}

fn bulk_add_tag(
    state: &AppState,
    ids: &[i64],
    name: Option<&str>,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let name = name.and_then(provenance::normalize_tag).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A valid tag is required"})),
        )
    })?;
    let conn = state.pool.get().map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let tx_conn = &*tx;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let exists: bool = tx_conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM media WHERE id=?1)",
                [id],
                |row| row.get(0),
            )
            .map_err(db_err)?;
        if exists {
            provenance::attach_tag(tx_conn, *id, &name, provenance::HUMAN, None).map_err(db_err)?;
            updated += 1;
        } else {
            failed.push(json!({"id":id,"error":"Media not found"}));
        }
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, failed))
}

fn bulk_set_rating(
    state: &AppState,
    ids: &[i64],
    rating: Option<i64>,
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let rating = rating.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"A rating is required"})),
        )
    })?;
    if !(1..=5).contains(&rating) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Rating must be between 1 and 5"})),
        ));
    }
    let conn = state.pool.get().map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let mut updated = 0;
    for id in ids {
        updated += tx.execute("UPDATE media SET human_rating=?1,rating=?1,rating_source='human',rating_reviewed=1,rating_reviewed_at=?2 WHERE id=?3", rusqlite::params![rating, now_iso(), id]).map_err(db_err)?;
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, Vec::new()))
}

fn bulk_refresh_metadata(
    state: &AppState,
    ids: &[i64],
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let root = dunce::canonicalize(&state.library_dir).map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let tx_conn = &*tx;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let row: Option<(String, Option<String>)> = tx_conn
            .query_row(
                "SELECT filepath,origin_url FROM media WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();
        let Some((filepath, origin_url)) = row else {
            failed.push(json!({"id":id,"error":"Media not found"}));
            continue;
        };
        let file = root.join(filepath);
        let sidecar = file.with_file_name(format!(
            "{}.json",
            file.file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default()
        ));
        match std::fs::read_to_string(sidecar)
            .ok()
            .and_then(|raw| serde_json::from_str::<Value>(&raw).ok())
        {
            Some(metadata) => match provenance::capture_source_metadata(
                tx_conn,
                *id,
                origin_url.as_deref(),
                &metadata,
            ) {
                Ok(()) => {
                    tx_conn.execute("UPDATE media SET nsfw_state='pending',nsfw_attempts=0,nsfw_retry_at=0 WHERE id=?1", [id]).map_err(db_err)?;
                    updated += 1;
                }
                Err(error) => failed.push(json!({"id":id,"error":error.to_string()})),
            },
            None => failed.push(json!({"id":id,"error":"No valid gallery-dl metadata sidecar"})),
        }
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, failed))
}

fn bulk_delete_files(
    state: &AppState,
    ids: &[i64],
) -> Result<(usize, Vec<Value>), (StatusCode, Json<Value>)> {
    let conn = state.pool.get().map_err(db_err)?;
    let root = dunce::canonicalize(&state.library_dir).map_err(db_err)?;
    let tx = conn.unchecked_transaction().map_err(db_err)?;
    let tx_conn = &*tx;
    let mut updated = 0;
    let mut failed = Vec::new();
    for id in ids {
        let row: Option<(String, bool)> = tx_conn
            .query_row(
                "SELECT filepath,clip_start_secs IS NOT NULL FROM media WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .ok();
        let Some((filepath, virtual_clip)) = row else {
            failed.push(json!({"id":id,"error":"Media not found"}));
            continue;
        };
        if virtual_clip {
            tx_conn
                .execute("UPDATE media SET downloaded=0,missing=1 WHERE id=?1", [id])
                .map_err(db_err)?;
            updated += 1;
            continue;
        }
        let Ok(path) = dunce::canonicalize(root.join(filepath)) else {
            failed.push(json!({"id":id,"error":"Local file is already unavailable"}));
            continue;
        };
        if !path.starts_with(&root) {
            failed.push(
                json!({"id":id,"error":"Refusing to delete a file outside Curator's library"}),
            );
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                tx_conn
                    .execute(
                        "UPDATE media SET downloaded=0,missing=1,file_size_bytes=NULL WHERE id=?1",
                        [id],
                    )
                    .map_err(db_err)?;
                updated += 1;
            }
            Err(error) => failed.push(json!({"id":id,"error":error.to_string()})),
        }
    }
    tx.commit().map_err(db_err)?;
    Ok((updated, failed))
}

pub fn get_or_create_tag(conn: &rusqlite::Connection, name: &str) -> rusqlite::Result<i64> {
    let name = name.trim().to_lowercase();
    if name.is_empty() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    if let Ok(id) = conn.query_row("SELECT id FROM tags WHERE name=?1", [&name], |r| {
        r.get::<_, i64>(0)
    }) {
        return Ok(id);
    }
    conn.execute(
        "INSERT INTO tags (name, added_at) VALUES (?1, ?2)",
        rusqlite::params![name, now_iso()],
    )?;
    Ok(conn.last_insert_rowid())
}

pub fn db_err(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": e.to_string()})),
    )
}

pub async fn effective_tags(
    state: &AppState,
) -> Result<Arc<HashMap<i64, HashSet<String>>>, (StatusCode, Json<Value>)> {
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

    #[tokio::test]
    async fn unknown_videos_stay_visible_and_browser_duration_moves_clips() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES(1,1,'v','v','video','2026');").unwrap();
        let videos = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("video".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(videos["media"].as_array().unwrap().len(), 1);
        let _ = set_duration(
            State(state.clone()),
            Path(1),
            Json(DurationBody {
                duration_secs: 45.0,
            }),
        )
        .await
        .unwrap();
        let clips = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("clip".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(clips["media"].as_array().unwrap().len(), 1);
        let videos = list(
            State(state.clone()),
            Query(MediaQuery {
                media_type: Some("video".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(videos["media"].as_array().unwrap().is_empty());
        assert_eq!(
            set_duration(
                State(state),
                Path(1),
                Json(DurationBody {
                    duration_secs: -1.0
                })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn undo_review_restores_auto_queue_and_rejects_stale_undo() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at,auto_rating,rating,rating_source) VALUES(1,1,'a','a','image','2026',4,4,'auto');").unwrap();
        let saved = set_rating(
            State(state.clone()),
            Path(1),
            Json(RatingBody { rating: 3 }),
        )
        .await
        .unwrap()
        .0;
        let token = saved["rating_reviewed_at"].as_str().unwrap().to_string();
        let undone = undo_rating(
            State(state.clone()),
            Path(1),
            Json(UndoRatingBody {
                rating_reviewed_at: token.clone(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(undone["rating"], 4);
        assert_eq!(undone["rating_reviewed"], false);
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(queue["media"].as_array().unwrap().len(), 1);
        state.pool.get().unwrap().execute_batch("UPDATE media SET rating=2,rating_reviewed=1,rating_source='human',rating_reviewed_at='future';").unwrap();
        assert_eq!(
            undo_rating(
                State(state.clone()),
                Path(1),
                Json(UndoRatingBody {
                    rating_reviewed_at: token
                })
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            state
                .pool
                .get()
                .unwrap()
                .query_row("SELECT rating FROM media WHERE id=1", [], |r| r
                    .get::<_, i64>(0))
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn automated_and_human_rating_lifecycle() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let conn = state.pool.get().unwrap();
        conn.execute_batch("INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES
            (1,1,'a','a','image','2026'),(2,1,'b','b','image','2026'),(3,1,'c','c','image','2026'),(4,1,'d','d','image','2026');").unwrap();
        crate::nsfw::persist_score(&conn, 1, 0.72).unwrap();
        crate::nsfw::persist_score(&conn, 2, 0.4).unwrap();
        crate::nsfw::persist_score(&conn, 4, 0.9).unwrap();
        conn.execute("UPDATE media SET missing=1 WHERE id=4", [])
            .unwrap();
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(queue["media"][0]["auto_rating"], 3);
        assert_eq!(queue["media"][0]["rating"], 3);
        assert_eq!(queue["media"][0]["rating_source"], "auto");
        assert_eq!(queue["media"][0]["rating_reviewed"], false);
        assert!((queue["media"][0]["auto_rating_score"].as_f64().unwrap() - 0.72).abs() < 0.00001);
        assert_eq!(queue["has_more"], true);
        // Approving an automatic recommendation is intentionally a separate
        // endpoint; star-rating requests themselves accept only 1..=5.
        let manual = approve_rating(State(state.clone()), Path(1))
            .await
            .unwrap()
            .0;
        assert_eq!(manual["rating_source"], "human");
        assert_eq!(manual["rating_reviewed"], true);
        assert!(manual["rating_reviewed_at"].is_string());
        crate::nsfw::persist_score(&conn, 1, 0.99).unwrap();
        let row: (i64, i64, String) = conn
            .query_row(
                "SELECT rating,auto_rating,rating_source FROM media WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        // A later automated score is retained, but never replaces the
        // reviewer’s effective rating.
        assert_eq!(row, (3, 3, "human".into()));
        let next = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                cursor: queue["next_cursor"].as_str().map(str::to_owned),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(next["media"][0]["id"], 2);
        assert_eq!(next["has_more"], false);
        let approved = approve_rating(State(state.clone()), Path(2))
            .await
            .unwrap()
            .0;
        assert_eq!(approved["rating"], approved["auto_rating"]);
        assert_eq!(approved["auto_rating"], 2);
        assert_eq!(approved["rating_source"], "human");
        assert_eq!(approved["rating_reviewed"], true);
        let queue = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(queue["media"].as_array().unwrap().is_empty());
        // Rating APIs intentionally accept only 1-5. Undo restores the
        // independently stored automatic recommendation for review.
        let cleared = undo_rating(
            State(state.clone()),
            Path(1),
            Json(UndoRatingBody {
                rating_reviewed_at: manual["rating_reviewed_at"].as_str().unwrap().to_owned(),
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(cleared["rating"], 3);
        assert!(cleared["human_rating"].is_null());
        assert_eq!(cleared["rating_source"], "auto");
        assert_eq!(cleared["rating_reviewed"], false);
        let reopened = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(reopened["media"][0]["id"], 1);
        assert_eq!(
            approve_rating(State(state.clone()), Path(3))
                .await
                .unwrap_err()
                .0,
            StatusCode::CONFLICT
        );
        assert_eq!(
            approve_rating(State(state.clone()), Path(999))
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            set_rating(State(state), Path(1), Json(RatingBody { rating: 6 }))
                .await
                .unwrap_err()
                .0,
            StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn human_pace_tag_is_reviewed_and_controls_effective_rating() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        let conn = state.pool.get().unwrap();
        conn.execute(
            "INSERT INTO media(id,source_id,filepath,filename,type,added_at,auto_rating,rating,rating_source)
             VALUES(1,1,'a','a','image','2026',3,3,'auto')",
            [],
        )
        .unwrap();
        crate::provenance::attach_tag(&conn, 1, "pace:cum", crate::provenance::HUMAN, None)
            .unwrap();

        let needs_review = list(
            State(state.clone()),
            Query(MediaQuery {
                rating_status: Some("needs_review".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert!(needs_review["media"].as_array().unwrap().is_empty());

        let reviewed = list(
            State(state),
            Query(MediaQuery {
                rating_status: Some("reviewed".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(reviewed["media"][0]["rating"], 5);
        assert_eq!(reviewed["media"][0]["pace_label"], "cum");
    }

    #[tokio::test]
    async fn page_limit_is_capped_and_missing_media_excluded() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        state.pool.get().unwrap().execute_batch("WITH RECURSIVE nums(n) AS(SELECT 1 UNION ALL SELECT n+1 FROM nums WHERE n<600)
            INSERT INTO media(id,source_id,filepath,filename,type,added_at) SELECT n,1,'test/'||n,'file','image','2026' FROM nums;
            UPDATE media SET missing=1,downloaded=0 WHERE id=1;").unwrap();
        let result = list(
            State(state),
            Query(MediaQuery {
                limit: Some(usize::MAX),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"].as_array().unwrap().len(), 500);
        assert_eq!(result["media"][0]["id"], 2);
        assert_eq!(result["has_more"], true);
    }

    #[tokio::test]
    async fn keyset_pages_preserve_every_sort_and_survive_deleted_anchor() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        {
            let conn = state.pool.get().unwrap();
            for id in 1..=21 {
                conn.execute("INSERT INTO media(id,source_id,filepath,filename,type,added_at,rating) VALUES(?1,1,?2,?3,'image',?4,?5)",
                    rusqlite::params![id,format!("test/{id}.jpg"),if id%2==0 {"Same"}else{"same"},format!("2026-01-{:02}",id%3+1),id%5]).unwrap();
            }
        }
        for sort in [
            "default",
            "rating_desc",
            "rating_asc",
            "date_desc",
            "date_asc",
            "filename_asc",
            "filename_desc",
            "shuffle",
        ] {
            let whole = list(
                State(state.clone()),
                Query(MediaQuery {
                    sort: Some(sort.into()),
                    limit: Some(500),
                    shuffle_seed: Some(1234567),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .0;
            let expected: Vec<i64> = whole["media"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["id"].as_i64().unwrap())
                .collect();
            let mut actual = Vec::new();
            let mut cursor = None;
            loop {
                let page = list(
                    State(state.clone()),
                    Query(MediaQuery {
                        sort: Some(sort.into()),
                        limit: Some(4),
                        shuffle_seed: Some(1234567),
                        cursor,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .0;
                actual.extend(
                    page["media"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|m| m["id"].as_i64().unwrap()),
                );
                cursor = page["next_cursor"].as_str().map(str::to_owned);
                if cursor.is_none() {
                    break;
                }
                assert!(actual.len() <= 21, "cursor must advance");
            }
            assert_eq!(actual, expected, "sort {sort}");
        }
        let first = list(
            State(state.clone()),
            Query(MediaQuery {
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        state
            .pool
            .get()
            .unwrap()
            .execute("DELETE FROM media WHERE id=1", [])
            .unwrap();
        let next = list(
            State(state),
            Query(MediaQuery {
                cursor: first["next_cursor"].as_str().map(str::to_owned),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(next["media"][0]["id"], 2);
    }

    #[tokio::test]
    async fn sql_filters_inherited_and_or_exclusion_before_pagination() {
        let root = tempfile::tempdir().unwrap();
        let state = crate::test_support::state(root.path());
        crate::test_support::source(&state);
        {
            let conn = state.pool.get().unwrap();
            conn.execute_batch("INSERT INTO groups(id,name,added_at) VALUES(1,'Parent','2026');
                INSERT INTO groups(id,name,parent_id,added_at) VALUES(2,'Child',1,'2026');
                UPDATE sources SET group_id=2;
                INSERT INTO tags(id,name,added_at) VALUES(1,'own','2026'),(2,'exclude','2026');
                INSERT INTO media(id,source_id,filepath,filename,type,added_at) VALUES
                (1,1,'test/1','1','image','2026'),(2,1,'test/2','2','image','2026'),(3,1,'test/3','3','image','2026');
                INSERT INTO media_tags VALUES(2,1),(3,1),(3,2);").unwrap();
        }
        let result = list(
            State(state.clone()),
            Query(MediaQuery {
                tags: Some("parent,own".into()),
                exclude_tags: Some("exclude".into()),
                limit: Some(1),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"][0]["id"], 2);
        assert_eq!(result["has_more"], false);
        let result = list(
            State(state.clone()),
            Query(MediaQuery {
                any_tags: Some("absent,own".into()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .0;
        assert_eq!(result["media"].as_array().unwrap().len(), 2);
        let cache1 = effective_tags(&state).await.unwrap();
        let cache2 = effective_tags(&state).await.unwrap();
        assert!(Arc::ptr_eq(&cache1, &cache2));
    }
}

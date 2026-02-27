use std::collections::HashMap;
use std::sync::Arc;

use crate::error::EsError;
use crate::model::search::{AggBucket, AggregationResult, Hit, HitsEnvelope, HitsTotal};
use crate::query::translator::TranslatedQuery;
use crate::storage::registry::IndexHandle;

pub struct DocRecord {
    pub id: String,
    pub version: i64,
    pub seq_no: i64,
    pub source: String,
}

/// Get a single document by ID.
pub async fn get_document(
    handle: &Arc<IndexHandle>,
    id: &str,
) -> Result<Option<DocRecord>, EsError> {
    let id = id.to_string();
    let result = handle
        .conn
        .call(move |conn| {
            let record = conn
                .query_row(
                    "SELECT _id, _version, _seq_no, _source FROM _source WHERE _id = ?1",
                    rusqlite::params![&id],
                    |row| {
                        Ok(DocRecord {
                            id: row.get(0)?,
                            version: row.get(1)?,
                            seq_no: row.get(2)?,
                            source: row.get(3)?,
                        })
                    },
                )
                .ok();
            Ok(record)
        })
        .await?;
    Ok(result)
}

/// Execute a search query and return hits.
pub async fn search(
    handle: &Arc<IndexHandle>,
    translated: TranslatedQuery,
    from: usize,
    size: usize,
    index_name: String,
) -> Result<(HitsEnvelope, u64), EsError> {
    let result = handle
        .conn
        .call(move |conn| {
            // Check if _fts table exists
            let mut translated = translated;
            if translated.has_fts {
                let fts_exists: bool = conn
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_fts'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|c| c > 0)
                    .unwrap_or(false);

                if !fts_exists {
                    // Drop FTS entirely - rely on WHERE clause for filtering.
                    // Extract simple terms from fts_match for a basic LIKE fallback
                    // only if there's no WHERE clause already.
                    translated.has_fts = false;
                    if translated.where_clause.is_empty() && !translated.fts_match.is_empty() {
                        // Extract alphanumeric terms from FTS expression for a basic LIKE
                        let terms: Vec<&str> = translated
                            .fts_match
                            .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
                            .filter(|s| !s.is_empty() && s.len() > 1)
                            .filter(|s| !["AND", "OR", "NOT", "NEAR"].contains(s))
                            .collect();
                        if !terms.is_empty() {
                            let conditions: Vec<String> = terms
                                .iter()
                                .map(|t| format!("_source LIKE '%{}%'", t.replace('\'', "''")))
                                .collect();
                            translated.where_clause = conditions.join(" AND ");
                        }
                    }
                    translated.fts_match.clear();
                }
            }

            tracing::debug!(
                "Translated query: fts={:?}, where={:?}, has_fts={}",
                translated.fts_match,
                translated.where_clause,
                translated.has_fts
            );

            // Build the full SQL query
            let (sql, count_sql, params) = build_search_sql(&translated, from, size);
            tracing::debug!("Search SQL: {}", sql);

            // Execute count query - with FTS error fallback
            let (total, use_fallback) = match conn.prepare(&count_sql) {
                Ok(mut stmt) => {
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                        .iter()
                        .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                        .collect();
                    match stmt.query_row(param_refs.as_slice(), |row| row.get::<_, i64>(0)) {
                        Ok(total) => (total, false),
                        Err(e) => {
                            tracing::debug!("FTS query failed, falling back to LIKE: {}", e);
                            (0i64, true)
                        }
                    }
                }
                Err(e) => {
                    tracing::debug!("FTS query prepare failed, falling back to LIKE: {}", e);
                    (0i64, true)
                }
            };

            // If FTS failed (e.g., column not in FTS table), fall back to LIKE
            if use_fallback && translated.has_fts {
                fts_to_like_fallback(&mut translated);
                let (fallback_sql, fallback_count_sql, fallback_params) =
                    build_search_sql(&translated, from, size);
                tracing::debug!("Fallback SQL: {}", fallback_sql);
                let total: i64 = {
                    let mut stmt = conn.prepare(&fallback_count_sql)?;
                    let param_refs: Vec<&dyn rusqlite::types::ToSql> = fallback_params
                        .iter()
                        .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                        .collect();
                    stmt.query_row(param_refs.as_slice(), |row| row.get(0))?
                };
                let mut stmt = conn.prepare(&fallback_sql)?;
                let param_refs: Vec<&dyn rusqlite::types::ToSql> = fallback_params
                    .iter()
                    .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                    .collect();
                let mut rows = stmt.query(param_refs.as_slice())?;
                let mut hits = Vec::new();
                let mut max_score: Option<f64> = None;
                while let Some(row) = rows.next()? {
                    let id: String = row.get("_id")?;
                    let source_str: String = row.get("_source")?;
                    let score: f64 = row.get("_score")?;
                    let normalized_score = if score < 0.0 { -score } else { score };
                    let source: serde_json::Value =
                        serde_json::from_str(&source_str).unwrap_or(serde_json::Value::Null);
                    max_score = Some(match max_score {
                        Some(m) => f64::max(m, normalized_score),
                        None => normalized_score,
                    });
                    hits.push(Hit {
                        _index: index_name.clone(),
                        _id: id,
                        _score: Some(normalized_score),
                        _source: source,
                        sort: None,
                    });
                }
                let envelope = HitsEnvelope {
                    total: HitsTotal {
                        value: total as u64,
                        relation: "eq".to_string(),
                    },
                    max_score,
                    hits,
                };
                return Ok((envelope, 0u64));
            }

            // Execute search query
            let mut stmt = conn.prepare(&sql)?;
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                .iter()
                .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                .collect();
            let mut rows = stmt.query(param_refs.as_slice())?;

            let mut hits = Vec::new();
            let mut max_score: Option<f64> = None;

            while let Some(row) = rows.next()? {
                let id: String = row.get("_id")?;
                let source_str: String = row.get("_source")?;
                let score: f64 = row.get("_score")?;

                // FTS5 bm25 returns negative values; negate for ES-style positive scores
                let normalized_score = if score < 0.0 { -score } else { score };

                let source: serde_json::Value =
                    serde_json::from_str(&source_str).unwrap_or(serde_json::Value::Null);

                max_score = Some(match max_score {
                    Some(m) => f64::max(m, normalized_score),
                    None => normalized_score,
                });

                // Extract sort values if sort clauses were specified
                let sort_values = if !translated.sort_clauses.is_empty() {
                    let mut vals = Vec::new();
                    for (sort_ref, _) in &translated.sort_clauses {
                        // Extract the field path from the sort ref
                        let field_path = if sort_ref.starts_with("json_extract") {
                            // json_extract(_source, '$.field') -> extract field path
                            sort_ref
                                .split("'$.")
                                .nth(1)
                                .and_then(|s| s.strip_suffix("')"))
                                .unwrap_or("")
                        } else {
                            sort_ref.trim_matches('"')
                        };
                        if let Some(v) =
                            source.pointer(&format!("/{}", field_path.replace('.', "/")))
                        {
                            vals.push(v.clone());
                        } else {
                            vals.push(serde_json::Value::Null);
                        }
                    }
                    Some(vals)
                } else {
                    None
                };

                hits.push(Hit {
                    _index: index_name.clone(),
                    _id: id,
                    _score: if translated.sort_clauses.is_empty() {
                        Some(normalized_score)
                    } else {
                        None
                    },
                    _source: source,
                    sort: sort_values,
                });
            }

            let envelope = HitsEnvelope {
                total: HitsTotal {
                    value: total as u64,
                    relation: "eq".to_string(),
                },
                max_score,
                hits,
            };

            Ok((envelope, 0u64))
        })
        .await?;

    Ok(result)
}

/// Count documents matching a query.
pub async fn count(handle: &Arc<IndexHandle>, translated: TranslatedQuery) -> Result<u64, EsError> {
    let result = handle
        .conn
        .call(move |conn| {
            let mut translated = translated;
            if translated.has_fts {
                let fts_exists: bool = conn
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_fts'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|c| c > 0)
                    .unwrap_or(false);

                if !fts_exists {
                    translated.has_fts = false;
                    if translated.where_clause.is_empty() && !translated.fts_match.is_empty() {
                        let terms: Vec<&str> = translated
                            .fts_match
                            .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
                            .filter(|s| !s.is_empty() && s.len() > 1)
                            .filter(|s| !["AND", "OR", "NOT", "NEAR"].contains(s))
                            .collect();
                        if !terms.is_empty() {
                            let conditions: Vec<String> = terms
                                .iter()
                                .map(|t| format!("_source LIKE '%{}%'", t.replace('\'', "''")))
                                .collect();
                            translated.where_clause = conditions.join(" AND ");
                        }
                    }
                    translated.fts_match.clear();
                }
            }

            let (_, count_sql, params) = build_search_sql(&translated, 0, 0);
            let mut stmt = conn.prepare(&count_sql)?;
            let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                .iter()
                .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                .collect();
            let total: i64 = stmt.query_row(param_refs.as_slice(), |row| row.get(0))?;
            Ok(total as u64)
        })
        .await?;
    Ok(result)
}

/// Execute aggregations and return results.
/// Supports "terms" aggregations with field and size parameters.
pub async fn aggregate(
    handle: &Arc<IndexHandle>,
    translated: TranslatedQuery,
    aggs: HashMap<String, AggDef>,
) -> Result<HashMap<String, AggregationResult>, EsError> {
    let result = handle
        .conn
        .call(move |conn| {
            let mut translated = translated;
            // Same FTS fallback logic as search
            if translated.has_fts {
                let fts_exists: bool = conn
                    .query_row(
                        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='_fts'",
                        [],
                        |row| row.get::<_, i64>(0),
                    )
                    .map(|c| c > 0)
                    .unwrap_or(false);

                if !fts_exists {
                    translated.has_fts = false;
                    if translated.where_clause.is_empty() && !translated.fts_match.is_empty() {
                        let terms: Vec<&str> = translated
                            .fts_match
                            .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
                            .filter(|s| !s.is_empty() && s.len() > 1)
                            .filter(|s| !["AND", "OR", "NOT", "NEAR"].contains(s))
                            .collect();
                        if !terms.is_empty() {
                            let conditions: Vec<String> = terms
                                .iter()
                                .map(|t| format!("_source LIKE '%{}%'", t.replace('\'', "''")))
                                .collect();
                            translated.where_clause = conditions.join(" AND ");
                        }
                    }
                    translated.fts_match.clear();
                }
            }

            let mut results = HashMap::new();

            for (agg_name, agg_def) in &aggs {
                match agg_def {
                    AggDef::Terms { field, size } => {
                        let buckets = execute_terms_agg(conn, &translated, field, *size)?;
                        let total_count: u64 = buckets.iter().map(|b| b.doc_count).sum();
                        let shown_count: u64 = buckets.iter().map(|b| b.doc_count).sum();
                        results.insert(
                            agg_name.clone(),
                            AggregationResult {
                                doc_count_error_upper_bound: Some(0),
                                sum_other_doc_count: Some((total_count - shown_count) as i64),
                                buckets: Some(buckets),
                                value: None,
                            },
                        );
                    }
                }
            }

            Ok(results)
        })
        .await?;
    Ok(result)
}

/// Parsed aggregation definition.
#[derive(Debug, Clone)]
pub enum AggDef {
    Terms { field: String, size: usize },
}

/// Parse aggregation definitions from the request JSON.
pub fn parse_aggs(aggs_value: &serde_json::Value) -> HashMap<String, AggDef> {
    let mut result = HashMap::new();
    if let Some(obj) = aggs_value.as_object() {
        for (name, def) in obj {
            if let Some(terms_def) = def.get("terms").or_else(|| def.get("term")) {
                let field = terms_def
                    .get("field")
                    .and_then(|f| f.as_str())
                    .unwrap_or("")
                    .to_string();
                // Strip .keyword suffix
                let field = field.strip_suffix(".keyword").unwrap_or(&field).to_string();
                let size = terms_def.get("size").and_then(|s| s.as_u64()).unwrap_or(10) as usize;
                if !field.is_empty() {
                    result.insert(name.clone(), AggDef::Terms { field, size });
                }
            }
        }
    }
    result
}

fn execute_terms_agg(
    conn: &rusqlite::Connection,
    translated: &TranslatedQuery,
    field: &str,
    size: usize,
) -> Result<Vec<AggBucket>, rusqlite::Error> {
    let field_expr = format!("json_extract(_source, '$.{}')", field);

    let (from_clause, base_where) = if translated.has_fts {
        let where_extra = if translated.where_clause.is_empty() {
            String::new()
        } else {
            format!(" AND {}", translated.where_clause)
        };
        (
            "_source s JOIN _fts ON s.rowid = _fts.rowid".to_string(),
            format!(
                "WHERE _fts MATCH '{}'{}",
                translated.fts_match.replace('\'', "''"),
                where_extra,
            ),
        )
    } else {
        let where_clause = if translated.where_clause.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", translated.where_clause)
        };
        ("_source".to_string(), where_clause)
    };

    let where_prefix = if base_where.is_empty() {
        "WHERE 1=1"
    } else {
        &base_where
    };

    // Try simple GROUP BY first (works for scalar fields - most common case)
    let simple_sql = format!(
        "SELECT {fe} as _key, COUNT(*) as _count \
         FROM {from} {where} AND {fe} IS NOT NULL \
         GROUP BY _key \
         ORDER BY _count DESC \
         LIMIT {size}",
        fe = field_expr,
        from = from_clause,
        where = where_prefix,
        size = size,
    );

    tracing::debug!("Aggregation SQL for '{}': {}", field, simple_sql);

    let mut stmt = conn.prepare(&simple_sql)?;
    let mut rows = stmt.query([])?;
    let mut buckets = Vec::new();
    let mut has_array_values = false;

    while let Some(row) = rows.next()? {
        let key: rusqlite::types::Value = row.get(0)?;
        let count: u64 = row.get(1)?;
        let key_json = match key {
            rusqlite::types::Value::Text(ref s) => {
                // Check if this is a JSON array — if so, we need json_each expansion
                if s.starts_with('[') {
                    has_array_values = true;
                    break;
                }
                serde_json::Value::String(s.clone())
            }
            rusqlite::types::Value::Integer(i) => serde_json::json!(i),
            rusqlite::types::Value::Real(f) => serde_json::json!(f),
            _ => continue,
        };
        buckets.push(AggBucket {
            key: key_json,
            doc_count: count,
        });
    }
    drop(rows);
    drop(stmt);

    // If we detected array values, use json_each to expand arrays
    if has_array_values {
        let array_sql = format!(
            "SELECT je.value as _key, COUNT(*) as _count \
             FROM {from}, json_each({fe}) je \
             {where} AND je.value IS NOT NULL \
             GROUP BY je.value \
             ORDER BY _count DESC \
             LIMIT {size}",
            fe = field_expr,
            from = from_clause,
            where = where_prefix,
            size = size,
        );

        tracing::debug!("Aggregation SQL (array) for '{}': {}", field, array_sql);

        let mut stmt = conn.prepare(&array_sql)?;
        let mut rows = stmt.query([])?;
        buckets.clear();
        while let Some(row) = rows.next()? {
            let key: rusqlite::types::Value = row.get(0)?;
            let count: u64 = row.get(1)?;
            let key_json = match key {
                rusqlite::types::Value::Text(s) => serde_json::Value::String(s),
                rusqlite::types::Value::Integer(i) => serde_json::json!(i),
                rusqlite::types::Value::Real(f) => serde_json::json!(f),
                _ => continue,
            };
            buckets.push(AggBucket {
                key: key_json,
                doc_count: count,
            });
        }
    }

    Ok(buckets)
}

/// Convert FTS query to LIKE-based fallback when FTS fails
/// (e.g., column referenced in FTS MATCH doesn't exist in FTS table).
fn fts_to_like_fallback(translated: &mut TranslatedQuery) {
    if !translated.has_fts || translated.fts_match.is_empty() {
        return;
    }
    // Extract only the actual search terms from the FTS expression,
    // skipping column specifiers like {name description columnNamesFuzzy}
    let fts_copy = translated.fts_match.clone();

    // Remove column specifiers: everything between { and } followed by :
    // e.g. "({name description}: ("act"* "hi"*))" -> "("act"* "hi"*)"
    let mut cleaned = fts_copy.clone();
    while let Some(start) = cleaned.find('{') {
        if let Some(end) = cleaned[start..].find('}') {
            let after_brace = start + end + 1;
            // Skip optional ": " after the closing brace
            let skip_to = cleaned[after_brace..]
                .find(':')
                .map(|i| after_brace + i + 1)
                .unwrap_or(after_brace);
            cleaned = format!("{}{}", &cleaned[..start], &cleaned[skip_to..]);
        } else {
            break;
        }
    }

    let terms: Vec<&str> = cleaned
        .split(|c: char| !c.is_alphanumeric() && c != '_' && c != '-')
        .filter(|s| !s.is_empty() && s.len() > 1)
        .filter(|s| !["AND", "OR", "NOT", "NEAR"].contains(s))
        .collect();
    translated.has_fts = false;
    translated.fts_match.clear();
    if !terms.is_empty() {
        let conditions: Vec<String> = terms
            .iter()
            .map(|t| format!("_source LIKE '%{}%'", t.replace('\'', "''")))
            .collect();
        let like_clause = conditions.join(" AND ");
        if translated.where_clause.is_empty() {
            translated.where_clause = like_clause;
        } else {
            translated.where_clause = format!("{} AND {}", translated.where_clause, like_clause);
        }
    }
}

fn build_search_sql(
    translated: &TranslatedQuery,
    from: usize,
    size: usize,
) -> (String, String, Vec<Box<dyn rusqlite::types::ToSql + Send>>) {
    let params: Vec<Box<dyn rusqlite::types::ToSql + Send>> = Vec::new();

    // Build ORDER BY clause from sort_clauses, falling back to default
    let order_by = if !translated.sort_clauses.is_empty() {
        let clauses: Vec<String> = translated
            .sort_clauses
            .iter()
            .map(|(field, dir)| format!("{} {}", field, dir))
            .collect();
        format!("ORDER BY {}", clauses.join(", "))
    } else if translated.has_fts {
        "ORDER BY _score".to_string()
    } else {
        "ORDER BY _id".to_string()
    };

    if translated.has_fts {
        let where_clause = if translated.where_clause.is_empty() {
            String::new()
        } else {
            format!(" AND {}", translated.where_clause)
        };

        let sql = format!(
            "SELECT s._id, s._source, bm25(_fts{}) as _score \
             FROM _source s \
             JOIN _fts ON s.rowid = _fts.rowid \
             WHERE _fts MATCH '{}'{} \
             {} \
             LIMIT {} OFFSET {}",
            translated.bm25_weights,
            translated.fts_match.replace('\'', "''"),
            where_clause,
            order_by,
            size,
            from,
        );

        let count_sql = format!(
            "SELECT COUNT(*) \
             FROM _source s \
             JOIN _fts ON s.rowid = _fts.rowid \
             WHERE _fts MATCH '{}'{}",
            translated.fts_match.replace('\'', "''"),
            where_clause,
        );

        (sql, count_sql, params)
    } else {
        let where_clause = if translated.where_clause.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", translated.where_clause)
        };

        let sql = format!(
            "SELECT _id, _source, 1.0 as _score \
             FROM _source{} \
             {} \
             LIMIT {} OFFSET {}",
            where_clause, order_by, size, from,
        );

        let count_sql = format!("SELECT COUNT(*) FROM _source{}", where_clause,);

        (sql, count_sql, params)
    }
}

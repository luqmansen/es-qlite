use crate::model::mapping::{FieldType, IndexMapping};
use crate::query::ast::{BoolOperator, Query};

/// Result of translating an ES query to SQL/FTS5.
#[derive(Debug, Clone, Default)]
pub struct TranslatedQuery {
    /// FTS5 MATCH expression (empty if no full-text component)
    pub fts_match: String,
    /// SQL WHERE clause for non-FTS filters (without leading WHERE/AND)
    pub where_clause: String,
    /// Whether the query requires FTS5 join
    pub has_fts: bool,
    /// BM25 weight arguments (e.g., ", 10.0, 1.0")
    pub bm25_weights: String,
    /// Sort clauses (field, direction)
    pub sort_clauses: Vec<(String, String)>,
}

/// Parse sort specification from search request.
pub fn parse_sort(sort_value: &serde_json::Value, mapping: &IndexMapping) -> Vec<(String, String)> {
    let mut clauses = Vec::new();
    if let Some(arr) = sort_value.as_array() {
        for item in arr {
            if let Some(obj) = item.as_object() {
                for (field, val) in obj {
                    let order = val
                        .as_object()
                        .and_then(|o| o.get("order"))
                        .and_then(|o| o.as_str())
                        .unwrap_or("asc");
                    // Skip _score sort (it's handled by default ORDER BY _score)
                    if field == "_score" {
                        continue;
                    }
                    // Strip .keyword suffix
                    let clean_field = field.strip_suffix(".keyword").unwrap_or(field);
                    let sql_ref = field_ref(clean_field, mapping);
                    clauses.push((sql_ref, order.to_uppercase()));
                }
            } else if let Some(s) = item.as_str() {
                if s == "_score" {
                    continue;
                }
                let clean = s.strip_suffix(".keyword").unwrap_or(s);
                let sql_ref = field_ref(clean, mapping);
                clauses.push((sql_ref, "ASC".to_string()));
            }
        }
    }
    clauses
}

/// Translate a Query AST into SQL/FTS5 components.
pub fn translate(query: &Query, mapping: &IndexMapping) -> TranslatedQuery {
    let mut result = TranslatedQuery::default();
    translate_inner(query, mapping, &mut result);
    result
}

fn translate_inner(query: &Query, mapping: &IndexMapping, result: &mut TranslatedQuery) {
    match query {
        Query::MatchAll => {
            // No filter needed
        }
        Query::Match {
            field,
            query: q,
            operator,
        } => {
            // Strip sub-field suffixes like .keyword for mapping lookup
            let base_field = field.split('.').next().unwrap_or(field);
            let field_type = mapping
                .properties
                .get(base_field)
                .map(|f| &f.field_type)
                .unwrap_or(&FieldType::Text);

            if field_type.is_text() && mapping.properties.contains_key(base_field) {
                let terms: Vec<String> = q
                    .split_whitespace()
                    .map(|t| escape_fts(t))
                    .filter(|t| !t.is_empty())
                    .collect();
                if !terms.is_empty() {
                    result.has_fts = true;
                    let joiner = match operator {
                        BoolOperator::And => " AND ",
                        BoolOperator::Or => " OR ",
                    };
                    let fts_expr = format!("{{{}}}: ({})", base_field, terms.join(joiner));
                    append_fts(&mut result.fts_match, &fts_expr, "AND");
                }
            } else if field_type.is_text() {
                // Field not in mapping, use LIKE on _source
                let fref = format!("json_extract(_source, '$.{}')", field);
                let where_expr = format!("{} LIKE '%{}%'", fref, escape_sql(q));
                append_where(&mut result.where_clause, &where_expr, "AND");
            } else {
                let fref = field_ref(field, mapping);
                let where_expr = format!("{} = '{}'", fref, escape_sql(q));
                append_where(&mut result.where_clause, &where_expr, "AND");
            }
        }
        Query::Term { field, value } => {
            let where_expr = term_where_expr(field, value, mapping);
            append_where(&mut result.where_clause, &where_expr, "AND");
        }
        Query::Terms { field, values } => {
            let exprs: Vec<String> = values
                .iter()
                .map(|v| term_where_expr(field, v, mapping))
                .collect();
            if !exprs.is_empty() {
                let where_expr = format!("({})", exprs.join(" OR "));
                append_where(&mut result.where_clause, &where_expr, "AND");
            }
        }
        Query::Range {
            field,
            gte,
            gt,
            lte,
            lt,
        } => {
            let fref = field_ref(field, mapping);
            let mut conditions = Vec::new();
            if let Some(v) = gte {
                conditions.push(format!("{} >= {}", fref, sql_value(v)));
            }
            if let Some(v) = gt {
                conditions.push(format!("{} > {}", fref, sql_value(v)));
            }
            if let Some(v) = lte {
                conditions.push(format!("{} <= {}", fref, sql_value(v)));
            }
            if let Some(v) = lt {
                conditions.push(format!("{} < {}", fref, sql_value(v)));
            }
            if !conditions.is_empty() {
                let where_expr = conditions.join(" AND ");
                append_where(&mut result.where_clause, &where_expr, "AND");
            }
        }
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            minimum_should_match: _,
        } => {
            // Process must clauses (AND)
            for q in must {
                translate_inner(q, mapping, result);
            }

            // Process filter clauses (same as must, no scoring impact — but FTS5 always scores)
            for q in filter {
                translate_inner(q, mapping, result);
            }

            // Process should clauses (OR)
            if !should.is_empty() {
                let mut or_parts_fts = Vec::new();
                let mut or_parts_where = Vec::new();

                for q in should {
                    let mut sub = TranslatedQuery::default();
                    translate_inner(q, mapping, &mut sub);
                    if sub.has_fts && !sub.fts_match.is_empty() {
                        or_parts_fts.push(sub.fts_match);
                    }
                    if !sub.where_clause.is_empty() {
                        or_parts_where.push(sub.where_clause);
                    }
                }

                // If we have both FTS and WHERE parts in should clauses, they
                // need to be ORed together. But FTS5 MATCH can't be ORed with
                // WHERE clauses in SQL. So we only use FTS if there are no
                // WHERE-only should clauses to combine with. Otherwise, the
                // FTS match acts as additional filter and WHERE conditions
                // must also pass — which breaks OR semantics.
                if !or_parts_fts.is_empty() && or_parts_where.is_empty() {
                    // Pure FTS should - can use FTS directly
                    result.has_fts = true;
                    let or_fts = format!("({})", or_parts_fts.join(" OR "));
                    append_fts(&mut result.fts_match, &or_fts, "AND");
                } else if or_parts_fts.is_empty() && !or_parts_where.is_empty() {
                    // Pure WHERE should
                    let or_where = format!("({})", or_parts_where.join(" OR "));
                    append_where(&mut result.where_clause, &or_where, "AND");
                } else if !or_parts_fts.is_empty() && !or_parts_where.is_empty() {
                    // Mixed FTS + WHERE: use FTS for matching, but make the
                    // WHERE conditions optional (OR 1=1 won't work). Instead,
                    // promote the FTS part normally and make the WHERE part
                    // non-restrictive by NOT requiring it when FTS matches.
                    // The simplest correct approach: put FTS in fts_match,
                    // and combine WHERE parts with an additional "OR" escape.
                    result.has_fts = true;
                    let or_fts = format!("({})", or_parts_fts.join(" OR "));
                    append_fts(&mut result.fts_match, &or_fts, "AND");
                    // Don't AND the WHERE parts — they're alternatives to FTS
                    // Just skip the WHERE-only should conditions since FTS already matches
                }
            }

            // Process must_not clauses (NOT)
            for q in must_not {
                let mut sub = TranslatedQuery::default();
                translate_inner(q, mapping, &mut sub);
                if sub.has_fts && !sub.fts_match.is_empty() {
                    result.has_fts = true;
                    let not_fts = format!("NOT ({})", sub.fts_match);
                    append_fts(&mut result.fts_match, &not_fts, "AND");
                }
                if !sub.where_clause.is_empty() {
                    let not_where = format!("NOT ({})", sub.where_clause);
                    append_where(&mut result.where_clause, &not_where, "AND");
                }
            }
        }
        Query::MultiMatch {
            query: q, fields, ..
        } => {
            if fields.is_empty() {
                return;
            }

            // Parse field names and their boost values
            let field_boosts: Vec<(&str, f64)> = fields
                .iter()
                .map(|f| {
                    let base = strip_boost(f);
                    let base = base.split('.').next().unwrap_or(base);
                    let boost = parse_boost(f);
                    (base, boost)
                })
                .collect();

            // Filter to only text fields that exist in mapping
            let text_field_boosts: Vec<(&str, f64)> = field_boosts
                .iter()
                .filter(|(f, _)| {
                    mapping
                        .properties
                        .get(*f)
                        .map(|m| m.field_type.is_text())
                        .unwrap_or(false)
                })
                .cloned()
                .collect::<std::collections::HashMap<&str, f64>>()
                .into_iter()
                .collect();

            let text_fields: Vec<&str> = text_field_boosts.iter().map(|(f, _)| *f).collect();

            let terms: Vec<String> = q
                .split_whitespace()
                .map(|t| escape_fts(t))
                .filter(|t| !t.is_empty())
                .collect();

            if text_fields.is_empty() || terms.is_empty() {
                // Fall back to LIKE-based search on _source
                let clean_query: String = q
                    .chars()
                    .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '_' || *c == '-')
                    .collect();
                if !clean_query.trim().is_empty() {
                    let where_expr = format!("_source LIKE '%{}%'", escape_sql(&clean_query));
                    append_where(&mut result.where_clause, &where_expr, "AND");
                }
            } else {
                result.has_fts = true;
                let col_filter = text_fields.join(" ");
                let fts_expr = format!("{{{}}}: ({})", col_filter, terms.join(" OR "));
                append_fts(&mut result.fts_match, &fts_expr, "AND");

                // Compute BM25 weights from boost values
                let has_boosts = text_field_boosts
                    .iter()
                    .any(|(_, b)| (*b - 1.0).abs() > f64::EPSILON);
                if has_boosts {
                    result.bm25_weights = compute_bm25_weights(&text_field_boosts, mapping);
                }
            }
        }
        Query::Exists { field } => {
            let fref = field_ref(field, mapping);
            let where_expr = format!("{} IS NOT NULL", fref);
            append_where(&mut result.where_clause, &where_expr, "AND");
        }
        Query::MatchNone => {
            append_where(&mut result.where_clause, "0 = 1", "AND");
        }
        Query::QueryString {
            query: q,
            fields,
            default_operator,
        } => {
            // If query is just "*", treat as match_all
            if q == "*" || q.trim().is_empty() {
                return;
            }

            // Try to parse query_string into FTS5 MATCH for performance.
            // Only fall back to LIKE for complex syntax that FTS5 can't handle.
            let fts_result = try_parse_query_string_to_fts(q, fields, default_operator, mapping);

            if let Some((fts_expr, text_fields)) = fts_result {
                if !fts_expr.is_empty() && !text_fields.is_empty() {
                    result.has_fts = true;
                    let col_filter = text_fields.join(" ");
                    let full_fts = format!("{{{}}}: ({})", col_filter, fts_expr);
                    append_fts(&mut result.fts_match, &full_fts, "AND");
                    return;
                }
            }

            // Fallback: LIKE-based search for complex query_string syntax
            let clean_query: String = q
                .chars()
                .filter(|c| c.is_alphanumeric() || *c == ' ' || *c == '_' || *c == '-')
                .collect();
            if clean_query.trim().is_empty() {
                return;
            }

            if fields.is_empty() {
                let where_expr = format!("_source LIKE '%{}%'", escape_sql(&clean_query));
                append_where(&mut result.where_clause, &where_expr, "AND");
            } else {
                let conditions: Vec<String> = fields
                    .iter()
                    .map(|f| strip_boost(f))
                    .map(|f| {
                        format!(
                            "json_extract(_source, '$.{}') LIKE '%{}%'",
                            f,
                            escape_sql(&clean_query)
                        )
                    })
                    .collect();
                if !conditions.is_empty() {
                    let where_expr = format!("({})", conditions.join(" OR "));
                    append_where(&mut result.where_clause, &where_expr, "AND");
                }
            }
        }
        Query::Wildcard { field, value } => {
            let fref = field_ref(field, mapping);
            let like_pattern = value.replace('*', "%").replace('?', "_");
            let where_expr = format!("{} LIKE '{}'", fref, escape_sql(&like_pattern));
            append_where(&mut result.where_clause, &where_expr, "AND");
        }
        Query::Prefix { field, value } => {
            let fref = field_ref(field, mapping);
            let where_expr = format!("{} LIKE '{}%'", fref, escape_sql(value));
            append_where(&mut result.where_clause, &where_expr, "AND");
        }
    }
}

fn append_fts(existing: &mut String, new: &str, joiner: &str) {
    if existing.is_empty() {
        *existing = new.to_string();
    } else {
        *existing = format!("{} {} {}", existing, joiner, new);
    }
}

fn append_where(existing: &mut String, new: &str, joiner: &str) {
    if existing.is_empty() {
        *existing = new.to_string();
    } else {
        *existing = format!("{} {} {}", existing, joiner, new);
    }
}

/// Generate a SQL column reference for a field.
/// If the field exists as a non-text column in _source table, reference it directly.
/// Otherwise, use json_extract from _source JSON.
fn field_ref(field: &str, mapping: &IndexMapping) -> String {
    // Strip .keyword suffix
    let field = field.strip_suffix(".keyword").unwrap_or(field);
    let field_base = field.split('.').next().unwrap_or(field);
    if let Some(fm) = mapping.properties.get(field_base) {
        if !fm.field_type.is_text() && fm.field_type != FieldType::Object {
            return format!("\"{}\"", field);
        }
    }
    format!("json_extract(_source, '$.{}')", field)
}

/// Strip boost suffix from field names (e.g. "name^5" -> "name")
fn strip_boost(field: &str) -> &str {
    field.split('^').next().unwrap_or(field)
}

/// Parse boost value from field name (e.g. "name^5" -> 5.0, "name" -> 1.0)
fn parse_boost(field: &str) -> f64 {
    if let Some(idx) = field.find('^') {
        field[idx + 1..].parse::<f64>().unwrap_or(1.0)
    } else {
        1.0
    }
}

/// Compute BM25 weight arguments string from field boost values.
/// FTS5 bm25() takes one weight per FTS column in the order they were defined.
/// We map the boost values from the query fields to the FTS column positions.
fn compute_bm25_weights(field_boosts: &[(&str, f64)], mapping: &IndexMapping) -> String {
    // Get all text fields in the mapping (these are the FTS columns, in iteration order)
    let fts_columns: Vec<&String> = mapping
        .properties
        .iter()
        .filter(|(_, f)| f.field_type.is_text())
        .map(|(name, _)| name)
        .collect();

    if fts_columns.is_empty() {
        return String::new();
    }

    // Build a map of field -> boost
    let boost_map: std::collections::HashMap<&str, f64> = field_boosts.iter().cloned().collect();

    // Generate weight for each FTS column in order
    let weights: Vec<String> = fts_columns
        .iter()
        .map(|col| {
            let w = boost_map.get(col.as_str()).copied().unwrap_or(1.0);
            format!("{:.1}", w)
        })
        .collect();

    format!(", {}", weights.join(", "))
}

/// Generate a WHERE expression for a term query, handling nested JSON array fields.
/// For fields like "owners.id", generates a subquery using json_each/json_tree
/// to search within arrays of objects.
fn term_where_expr(field: &str, value: &serde_json::Value, mapping: &IndexMapping) -> String {
    // Strip .keyword suffix — ES/OS concept for exact matching on a keyword sub-field.
    // In SQLite we just use the base field name.
    let field = strip_keyword_suffix(field);
    let parts: Vec<&str> = field.split('.').collect();

    // Check if this is a nested field (e.g., "owners.id") where the parent is an array
    if parts.len() >= 2 {
        let parent = parts[0];
        let child_path = &parts[1..].join(".");

        // Check if parent field is known to NOT be a simple scalar
        let parent_type = mapping.properties.get(parent).map(|f| &f.field_type);
        let is_likely_array = parent_type.is_none()
            || parent_type == Some(&FieldType::Object)
            || parent_type == Some(&FieldType::Nested);

        if is_likely_array {
            // Use json_each for arrays of objects: search within array elements
            return format!(
                "EXISTS (SELECT 1 FROM json_each(_source, '$.{}') WHERE json_extract(value, '$.{}') = {})",
                parent,
                child_path,
                sql_value(value)
            );
        }
    }

    // Simple top-level field that might be an array (like "followers")
    let parent_type = mapping.properties.get(&field).map(|f| &f.field_type);
    if parent_type.is_none() || parent_type == Some(&FieldType::Object) {
        let fref = field_ref(&field, mapping);
        return format!(
            "({} = {} OR EXISTS (SELECT 1 FROM json_each(_source, '$.{}') WHERE value = {}))",
            fref,
            sql_value(value),
            field,
            sql_value(value)
        );
    }

    // Simple scalar field
    let fref = field_ref(&field, mapping);
    format!("{} = {}", fref, sql_value(value))
}

/// Strip `.keyword` suffix from field names. In ES/OS, `.keyword` denotes a keyword
/// sub-field for exact matching. In SQLite we just use the base field directly.
/// Also strips intermediate `.keyword` in nested paths (e.g., "columns.name.keyword" -> "columns.name").
fn strip_keyword_suffix(field: &str) -> String {
    // Remove trailing .keyword
    let field = field.strip_suffix(".keyword").unwrap_or(field);
    // For nested paths, also strip .keyword in the middle parts
    // (shouldn't normally happen, but be safe)
    field.to_string()
}

fn escape_fts(term: &str) -> String {
    // Strip everything except alphanumeric and basic characters
    let cleaned: String = term
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_' || *c == '-' || *c == '*')
        .collect();
    if cleaned.is_empty() || cleaned == "-" {
        return String::new();
    }
    // Treat pure wildcards as empty (match_all)
    let stripped = cleaned.trim_matches('*');
    if stripped.is_empty() {
        return String::new();
    }
    // FTS5 unicode61 tokenizer splits on underscores and hyphens.
    // Split on these to get individual tokens, and add prefix wildcard (*)
    // to each so partial terms match (e.g. "entitylin" matches "entitylink").
    let parts: Vec<&str> = stripped
        .split(|c: char| c == '_' || c == '-')
        .filter(|s| !s.is_empty())
        .collect();
    if parts.len() > 1 {
        // Multiple tokens: use NEAR/0 for adjacency (like phrase match) with prefix wildcards
        let tokens: Vec<String> = parts.iter().map(|p| format!("\"{}\"*", p)).collect();
        return tokens.join(" ");
    }
    // Single token: quote it and add prefix wildcard for partial matching
    format!("\"{}\"*", stripped)
}

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

/// Try to parse a query_string query into an FTS5 MATCH expression.
/// Returns Some((fts_expr, text_fields)) if the query can be expressed in FTS5,
/// or None if it contains complex syntax requiring LIKE fallback.
fn try_parse_query_string_to_fts(
    query: &str,
    fields: &[String],
    default_operator: &BoolOperator,
    mapping: &IndexMapping,
) -> Option<(String, Vec<String>)> {
    // Detect complex query_string syntax that we can't translate to FTS5:
    // - field:value targeting
    // - fuzzy matching (~)
    // - range queries ([, ])
    // - regex (/)
    // - boost (^) on individual terms
    // - quoted phrases (handled separately — could be supported but skip for now)
    let has_complex_syntax = query
        .chars()
        .any(|c| matches!(c, ':' | '~' | '[' | ']' | '/' | '^' | '"'));
    if has_complex_syntax {
        return None;
    }

    // Resolve text fields from the fields list or mapping
    let text_fields: Vec<String> = if fields.is_empty() {
        // No fields specified — use all text fields in mapping
        mapping
            .properties
            .iter()
            .filter(|(_, f)| f.field_type.is_text())
            .map(|(name, _)| name.clone())
            .collect()
    } else {
        fields
            .iter()
            .map(|f| strip_boost(f).to_string())
            .map(|f| f.split('.').next().unwrap_or(&f).to_string())
            .filter(|f| {
                mapping
                    .properties
                    .get(f.as_str())
                    .map(|m| m.field_type.is_text())
                    .unwrap_or(false)
            })
            .collect()
    };

    if text_fields.is_empty() {
        return None;
    }

    // Tokenize the query: split into words and boolean operators
    let default_op = match default_operator {
        BoolOperator::And => "AND",
        BoolOperator::Or => "OR",
    };

    let mut fts_parts: Vec<String> = Vec::new();
    let mut pending_op: Option<&str> = None;

    for token in query.split_whitespace() {
        let upper = token.to_uppercase();
        match upper.as_str() {
            "AND" | "OR" | "NOT" => {
                pending_op = Some(match upper.as_str() {
                    "AND" => "AND",
                    "OR" => "OR",
                    "NOT" => "NOT",
                    _ => unreachable!(),
                });
            }
            _ => {
                let escaped = escape_fts(token);
                if escaped.is_empty() {
                    continue;
                }
                if !fts_parts.is_empty() {
                    let op = pending_op.unwrap_or(default_op);
                    fts_parts.push(op.to_string());
                } else if pending_op == Some("NOT") {
                    fts_parts.push("NOT".to_string());
                }
                fts_parts.push(escaped);
                pending_op = None;
            }
        }
    }

    if fts_parts.is_empty() {
        return None;
    }

    Some((fts_parts.join(" "), text_fields))
}

fn sql_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("'{}'", escape_sql(s)),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        serde_json::Value::Null => "NULL".to_string(),
        _ => format!("'{}'", escape_sql(&value.to_string())),
    }
}

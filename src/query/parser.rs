use crate::error::EsError;
use crate::query::ast::{BoolOperator, MultiMatchType, Query};

/// Parse an ES query DSL JSON value into a Query AST.
pub fn parse_query(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("query must be an object".into()))?;

    if obj.is_empty() || obj.contains_key("match_all") {
        return Ok(Query::MatchAll);
    }

    if let Some(match_val) = obj.get("match") {
        return parse_match(match_val);
    }

    if let Some(term_val) = obj.get("term") {
        return parse_term(term_val);
    }

    if let Some(terms_val) = obj.get("terms") {
        return parse_terms(terms_val);
    }

    if let Some(range_val) = obj.get("range") {
        return parse_range(range_val);
    }

    if let Some(bool_val) = obj.get("bool") {
        return parse_bool(bool_val);
    }

    if let Some(multi_val) = obj.get("multi_match") {
        return parse_multi_match(multi_val);
    }

    if let Some(exists_val) = obj.get("exists") {
        return parse_exists(exists_val);
    }

    if let Some(qs_val) = obj.get("query_string") {
        return parse_query_string(qs_val);
    }

    if let Some(wildcard_val) = obj.get("wildcard") {
        return parse_wildcard(wildcard_val);
    }

    if let Some(prefix_val) = obj.get("prefix") {
        return parse_prefix(prefix_val);
    }

    if let Some(nested_val) = obj.get("nested") {
        return parse_nested(nested_val);
    }

    if let Some(fs_val) = obj.get("function_score") {
        return parse_function_score(fs_val);
    }

    if let Some(wrapper_val) = obj.get("wrapper") {
        return parse_wrapper(wrapper_val);
    }

    if obj.contains_key("match_none") {
        return Ok(Query::MatchNone);
    }

    // For unsupported queries, return MatchAll rather than error
    tracing::warn!(
        "Unsupported query type treated as match_all: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
    Ok(Query::MatchAll)
}

fn parse_match(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("match query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("match query must have a field".into()))?;

    let (query, operator) = if let Some(s) = field_value.as_str() {
        (s.to_string(), BoolOperator::Or)
    } else if let Some(obj) = field_value.as_object() {
        let query = obj
            .get("query")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let operator = match obj.get("operator").and_then(|v| v.as_str()) {
            Some("and") | Some("AND") => BoolOperator::And,
            _ => BoolOperator::Or,
        };
        (query, operator)
    } else {
        (field_value.to_string(), BoolOperator::Or)
    };

    Ok(Query::Match {
        field: field.clone(),
        query,
        operator,
    })
}

fn parse_term(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("term query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("term query must have a field".into()))?;

    let val = if let Some(obj) = field_value.as_object() {
        obj.get("value").cloned().unwrap_or(serde_json::Value::Null)
    } else {
        field_value.clone()
    };

    Ok(Query::Term {
        field: field.clone(),
        value: val,
    })
}

fn parse_terms(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("terms query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("terms query must have a field".into()))?;

    let values = field_value
        .as_array()
        .ok_or_else(|| EsError::ParsingError("terms values must be an array".into()))?
        .clone();

    Ok(Query::Terms {
        field: field.clone(),
        values,
    })
}

fn parse_range(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("range query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("range query must have a field".into()))?;

    let range_obj = field_value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("range field value must be an object".into()))?;

    Ok(Query::Range {
        field: field.clone(),
        gte: range_obj.get("gte").cloned(),
        gt: range_obj.get("gt").cloned(),
        lte: range_obj.get("lte").cloned(),
        lt: range_obj.get("lt").cloned(),
    })
}

fn parse_bool(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("bool query must be an object".into()))?;

    let parse_clause = |key: &str| -> Result<Vec<Query>, EsError> {
        match obj.get(key) {
            Some(serde_json::Value::Array(arr)) => arr.iter().map(parse_query).collect(),
            Some(val) if val.is_object() => Ok(vec![parse_query(val)?]),
            Some(_) => Err(EsError::ParsingError(format!(
                "bool.{key} must be an array or object"
            ))),
            None => Ok(vec![]),
        }
    };

    let must = parse_clause("must")?;
    let should = parse_clause("should")?;
    let must_not = parse_clause("must_not")?;
    let filter = parse_clause("filter")?;

    let minimum_should_match = obj
        .get("minimum_should_match")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    Ok(Query::Bool {
        must,
        should,
        must_not,
        filter,
        minimum_should_match,
    })
}

fn parse_multi_match(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("multi_match must be an object".into()))?;

    let query = obj
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let fields = obj
        .get("fields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let match_type = match obj.get("type").and_then(|v| v.as_str()) {
        Some("most_fields") => MultiMatchType::MostFields,
        Some("cross_fields") => MultiMatchType::CrossFields,
        Some("phrase") => MultiMatchType::Phrase,
        Some("phrase_prefix") => MultiMatchType::PhrasePrefix,
        _ => MultiMatchType::BestFields,
    };

    Ok(Query::MultiMatch {
        query,
        fields,
        match_type,
    })
}

fn parse_query_string(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("query_string must be an object".into()))?;

    let query = obj
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("*")
        .to_string();

    let fields = obj
        .get("fields")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .or_else(|| {
            obj.get("default_field")
                .and_then(|v| v.as_str())
                .map(|f| vec![f.to_string()])
        })
        .unwrap_or_default();

    let default_operator = match obj.get("default_operator").and_then(|v| v.as_str()) {
        Some("AND") | Some("and") => BoolOperator::And,
        _ => BoolOperator::Or,
    };

    Ok(Query::QueryString {
        query,
        fields,
        default_operator,
    })
}

fn parse_wildcard(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("wildcard query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("wildcard query must have a field".into()))?;

    let val = if let Some(s) = field_value.as_str() {
        s.to_string()
    } else if let Some(obj) = field_value.as_object() {
        obj.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("*")
            .to_string()
    } else {
        "*".to_string()
    };

    Ok(Query::Wildcard {
        field: field.clone(),
        value: val,
    })
}

fn parse_prefix(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("prefix query must be an object".into()))?;

    let (field, field_value) = obj
        .iter()
        .next()
        .ok_or_else(|| EsError::ParsingError("prefix query must have a field".into()))?;

    let val = if let Some(s) = field_value.as_str() {
        s.to_string()
    } else if let Some(obj) = field_value.as_object() {
        obj.get("value")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    } else {
        field_value.to_string()
    };

    Ok(Query::Prefix {
        field: field.clone(),
        value: val,
    })
}

fn parse_nested(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("nested query must be an object".into()))?;

    // For nested queries, just parse the inner query - we don't have true nested doc support
    if let Some(inner_query) = obj.get("query") {
        parse_query(inner_query)
    } else {
        Ok(Query::MatchAll)
    }
}

fn parse_function_score(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("function_score must be an object".into()))?;

    // Extract the inner query, ignoring score functions
    if let Some(inner_query) = obj.get("query") {
        parse_query(inner_query)
    } else {
        Ok(Query::MatchAll)
    }
}

fn parse_wrapper(value: &serde_json::Value) -> Result<Query, EsError> {
    // wrapper query contains a base64-encoded query JSON
    let encoded = if let Some(s) = value.as_str() {
        s.to_string()
    } else if let Some(obj) = value.as_object() {
        obj.get("query")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    } else {
        return Ok(Query::MatchAll);
    };

    if encoded.is_empty() {
        return Ok(Query::MatchAll);
    }

    // Decode base64
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.as_bytes())
        .map_err(|e| EsError::ParsingError(format!("wrapper query base64 decode error: {e}")))?;

    let inner: serde_json::Value = serde_json::from_slice(&decoded)
        .map_err(|e| EsError::ParsingError(format!("wrapper query JSON parse error: {e}")))?;

    parse_query(&inner)
}

fn parse_exists(value: &serde_json::Value) -> Result<Query, EsError> {
    let obj = value
        .as_object()
        .ok_or_else(|| EsError::ParsingError("exists query must be an object".into()))?;

    let field = obj
        .get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| EsError::ParsingError("exists query must have a 'field'".into()))?
        .to_string();

    Ok(Query::Exists { field })
}

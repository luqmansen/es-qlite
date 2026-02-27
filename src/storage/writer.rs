use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::EsError;
use crate::model::mapping::{FieldMapping, FieldType};
use crate::storage::registry::IndexHandle;

pub struct WriteResult {
    pub id: String,
    pub version: i64,
    pub seq_no: i64,
    pub created: bool,
}

/// Index (insert or replace) a document.
pub async fn index_document(
    handle: &Arc<IndexHandle>,
    id: String,
    source: serde_json::Value,
) -> Result<WriteResult, EsError> {
    // Detect and update mapping for new fields (lock scope limited to avoid holding across await)
    let new_fields = {
        let mut new_fields = Vec::new();
        if let serde_json::Value::Object(ref map) = source {
            let mut mapping = handle.mapping.write();
            for (key, value) in map {
                if !mapping.properties.contains_key(key) {
                    let field_type = FieldType::detect(value);
                    let field_mapping = FieldMapping {
                        field_type: field_type.clone(),
                        fields: if field_type == FieldType::Text {
                            let mut sub = std::collections::HashMap::new();
                            sub.insert(
                                "keyword".to_string(),
                                FieldMapping {
                                    field_type: FieldType::Keyword,
                                    fields: None,
                                    properties: None,
                                    extra: Default::default(),
                                },
                            );
                            Some(sub)
                        } else {
                            None
                        },
                        properties: None,
                        extra: Default::default(),
                    };
                    new_fields.push((key.clone(), field_type));
                    mapping.properties.insert(key.clone(), field_mapping);
                }
            }
            // Lock dropped here at end of block
        }
        new_fields
    };

    if !new_fields.is_empty() {
        let mapping_json = serde_json::to_string(&*handle.mapping.read()).unwrap_or_default();
        let mapping_clone = handle.mapping.read().clone();
        let new_fields_clone = new_fields;
        handle
            .conn
            .call(move |conn| {
                conn.execute(
                    "INSERT OR REPLACE INTO _meta (key, value) VALUES ('mappings', ?1)",
                    rusqlite::params![mapping_json],
                )?;
                for (name, field_type) in &new_fields_clone {
                    crate::storage::schema::add_column(conn, name, field_type, &mapping_clone)?;
                }
                Ok(())
            })
            .await?;
    }

    let source_json = serde_json::to_string(&source)?;
    let mapping = handle.mapping.read().clone();

    // Build column names and values for non-text fields
    let mut extra_cols = Vec::new();
    let mut extra_vals: Vec<Box<dyn rusqlite::types::ToSql + Send + 'static>> = Vec::new();
    if let serde_json::Value::Object(ref map) = source {
        for (key, field) in &mapping.properties {
            if !field.field_type.is_text() && field.field_type != FieldType::Object {
                if let Some(value) = map.get(key) {
                    extra_cols.push(format!("\"{key}\""));
                    match &field.field_type {
                        FieldType::Boolean => {
                            let v = value.as_bool().unwrap_or(false) as i64;
                            extra_vals.push(Box::new(v));
                        }
                        FieldType::Long
                        | FieldType::Integer
                        | FieldType::Short
                        | FieldType::Byte => {
                            let v = value.as_i64().unwrap_or(0);
                            extra_vals.push(Box::new(v));
                        }
                        FieldType::Float | FieldType::Double | FieldType::HalfFloat => {
                            let v = value.as_f64().unwrap_or(0.0);
                            extra_vals.push(Box::new(v));
                        }
                        FieldType::Date | FieldType::Keyword => {
                            let v = value.as_str().unwrap_or("").to_string();
                            extra_vals.push(Box::new(v));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let id_clone = id.clone();
    let result = handle
        .conn
        .call(move |conn| {
            let existing: Option<(i64, i64)> = conn
                .query_row(
                    "SELECT _version, _seq_no FROM _source WHERE _id = ?1",
                    rusqlite::params![&id_clone],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            let (version, seq_no, created) = if let Some((v, s)) = existing {
                (v + 1, s + 1, false)
            } else {
                (1i64, 0i64, true)
            };

            let mut cols = vec!["_id", "_version", "_seq_no", "_source"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>();
            cols.extend(extra_cols.iter().cloned());

            let placeholders: Vec<String> = (1..=cols.len()).map(|i| format!("?{i}")).collect();

            let sql = format!(
                "INSERT OR REPLACE INTO _source ({}) VALUES ({})",
                cols.join(", "),
                placeholders.join(", ")
            );

            let mut params: Vec<Box<dyn rusqlite::types::ToSql + Send>> = vec![
                Box::new(id_clone.clone()),
                Box::new(version),
                Box::new(seq_no),
                Box::new(source_json),
            ];
            params.extend(extra_vals);

            let param_refs: Vec<&dyn rusqlite::types::ToSql> = params
                .iter()
                .map(|p| p.as_ref() as &dyn rusqlite::types::ToSql)
                .collect();
            conn.execute(&sql, param_refs.as_slice())?;

            Ok(WriteResult {
                id: id_clone,
                version,
                seq_no,
                created,
            })
        })
        .await?;

    if result.created {
        handle.doc_count.fetch_add(1, Ordering::Relaxed);
    }

    Ok(result)
}

/// Delete a document by ID.
pub async fn delete_document(handle: &Arc<IndexHandle>, id: &str) -> Result<WriteResult, EsError> {
    let id_owned = id.to_string();
    let id_for_err = id.to_string();
    let index_name = handle.name.clone();

    let result = handle
        .conn
        .call(move |conn| {
            let existing: Option<(i64, i64)> = conn
                .query_row(
                    "SELECT _version, _seq_no FROM _source WHERE _id = ?1",
                    rusqlite::params![&id_owned],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok();

            match existing {
                Some((version, seq_no)) => {
                    conn.execute(
                        "DELETE FROM _source WHERE _id = ?1",
                        rusqlite::params![&id_owned],
                    )?;
                    Ok(Some(WriteResult {
                        id: id_owned,
                        version: version + 1,
                        seq_no: seq_no + 1,
                        created: false,
                    }))
                }
                None => Ok(None),
            }
        })
        .await?;

    match result {
        Some(r) => {
            handle.doc_count.fetch_sub(1, Ordering::Relaxed);
            Ok(r)
        }
        None => Err(EsError::DocumentNotFound(id_for_err, index_name)),
    }
}

/// Partial update: merge new fields into existing _source.
pub async fn update_document(
    handle: &Arc<IndexHandle>,
    id: &str,
    doc: serde_json::Value,
) -> Result<WriteResult, EsError> {
    let id = id.to_string();
    let index_name = handle.name.clone();

    let existing_source = handle
        .conn
        .call({
            let id = id.clone();
            move |conn| {
                let source: Option<String> = conn
                    .query_row(
                        "SELECT _source FROM _source WHERE _id = ?1",
                        rusqlite::params![&id],
                        |row| row.get(0),
                    )
                    .ok();
                Ok(source)
            }
        })
        .await?;

    let existing_source =
        existing_source.ok_or_else(|| EsError::DocumentNotFound(id.clone(), index_name))?;

    let mut merged: serde_json::Value = serde_json::from_str(&existing_source)?;
    if let (serde_json::Value::Object(ref mut base), serde_json::Value::Object(updates)) =
        (&mut merged, doc)
    {
        for (k, v) in updates {
            base.insert(k, v);
        }
    }

    index_document(handle, id, merged).await
}

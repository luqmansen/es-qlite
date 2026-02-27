use rusqlite::Connection;

use crate::model::mapping::{FieldType, IndexMapping};

/// Create all tables for a new index based on its mapping.
pub fn create_index_tables(
    conn: &Connection,
    mapping: &IndexMapping,
) -> Result<(), rusqlite::Error> {
    // 1. Create metadata table
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS _meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );",
    )?;

    // 2. Build _source table with extracted columns for non-text fields
    let mut source_columns = vec![
        "_id TEXT PRIMARY KEY".to_string(),
        "_version INTEGER NOT NULL DEFAULT 1".to_string(),
        "_seq_no INTEGER NOT NULL DEFAULT 0".to_string(),
        "_source TEXT NOT NULL".to_string(),
        "_indexed_at TEXT NOT NULL DEFAULT (datetime('now'))".to_string(),
    ];

    for (name, field) in &mapping.properties {
        if !field.field_type.is_text() {
            source_columns.push(format!("\"{}\" {}", name, field.field_type.sqlite_type()));
        }
        // Text fields with keyword sub-field get a keyword column
        if field.field_type.is_text() {
            if let Some(fields) = &field.fields {
                if fields.contains_key("keyword") {
                    source_columns.push(format!("\"{}.keyword\" TEXT", name));
                }
            }
        }
    }

    let create_source = format!(
        "CREATE TABLE IF NOT EXISTS _source ({});",
        source_columns.join(", ")
    );
    conn.execute_batch(&create_source)?;

    // 3. Build FTS5 virtual table for text fields
    let text_columns: Vec<&String> = mapping
        .properties
        .iter()
        .filter(|(_, f)| f.field_type.is_text())
        .map(|(name, _)| name)
        .collect();

    if !text_columns.is_empty() {
        let fts_cols: Vec<String> = text_columns.iter().map(|c| format!("\"{}\"", c)).collect();
        let create_fts = format!(
            "CREATE VIRTUAL TABLE IF NOT EXISTS _fts USING fts5({}, content='_source', content_rowid='rowid');",
            fts_cols.join(", ")
        );
        conn.execute_batch(&create_fts)?;

        // 4. Create triggers for external content sync
        create_fts_triggers(conn, &text_columns, mapping)?;
    }

    Ok(())
}

fn create_fts_triggers(
    conn: &Connection,
    text_columns: &[&String],
    _mapping: &IndexMapping,
) -> Result<(), rusqlite::Error> {
    let col_list: String = text_columns
        .iter()
        .map(|c| format!("\"{}\"", c))
        .collect::<Vec<_>>()
        .join(", ");
    let new_json_extracts: String = text_columns
        .iter()
        .map(|c| format!("json_extract(new._source, '$.{}')", c))
        .collect::<Vec<_>>()
        .join(", ");
    let old_json_extracts: String = text_columns
        .iter()
        .map(|c| format!("json_extract(old._source, '$.{}')", c))
        .collect::<Vec<_>>()
        .join(", ");

    // After INSERT
    let trigger_ai = format!(
        "CREATE TRIGGER IF NOT EXISTS _fts_ai AFTER INSERT ON _source BEGIN
            INSERT INTO _fts(rowid, {col_list})
            VALUES (new.rowid, {new_json_extracts});
        END;"
    );
    conn.execute_batch(&trigger_ai)?;

    // After DELETE
    let trigger_ad = format!(
        "CREATE TRIGGER IF NOT EXISTS _fts_ad AFTER DELETE ON _source BEGIN
            INSERT INTO _fts(_fts, rowid, {col_list})
            VALUES ('delete', old.rowid, {old_json_extracts});
        END;"
    );
    conn.execute_batch(&trigger_ad)?;

    // After UPDATE
    let trigger_au = format!(
        "CREATE TRIGGER IF NOT EXISTS _fts_au AFTER UPDATE ON _source BEGIN
            INSERT INTO _fts(_fts, rowid, {col_list})
            VALUES ('delete', old.rowid, {old_json_extracts});
            INSERT INTO _fts(rowid, {col_list})
            VALUES (new.rowid, {new_json_extracts});
        END;"
    );
    conn.execute_batch(&trigger_au)?;

    Ok(())
}

/// Add a new column to _source table and rebuild FTS triggers if needed.
pub fn add_column(
    conn: &Connection,
    name: &str,
    field_type: &FieldType,
    mapping: &IndexMapping,
) -> Result<(), rusqlite::Error> {
    if field_type.is_text() {
        // Add to FTS5 — unfortunately FTS5 doesn't support ALTER TABLE ADD COLUMN.
        // We'd need to rebuild the FTS table. For now, we'll skip dynamic text field addition.
        // Non-text fields can be added to _source directly.
        tracing::warn!(
            "Dynamic text field addition to FTS5 not yet supported: {}",
            name
        );
    } else {
        let sql = format!(
            "ALTER TABLE _source ADD COLUMN \"{}\" {};",
            name,
            field_type.sqlite_type()
        );
        conn.execute_batch(&sql)?;
    }
    let _ = mapping; // Will be used for trigger rebuild in the future
    Ok(())
}

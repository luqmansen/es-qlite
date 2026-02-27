use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio_rusqlite::Connection as AsyncConnection;

use crate::error::EsError;
use crate::model::mapping::{IndexMapping, IndexSettings};
use crate::storage::{connection, schema};

pub struct IndexHandle {
    pub name: String,
    pub conn: AsyncConnection,
    pub mapping: RwLock<IndexMapping>,
    pub settings: IndexSettings,
    pub doc_count: AtomicU64,
}

pub struct IndexRegistry {
    data_dir: PathBuf,
    indices: RwLock<HashMap<String, Arc<IndexHandle>>>,
    /// Maps alias names to sets of index names
    aliases: RwLock<HashMap<String, Vec<String>>>,
}

impl IndexRegistry {
    pub fn new(data_dir: PathBuf) -> Self {
        IndexRegistry {
            data_dir,
            indices: RwLock::new(HashMap::new()),
            aliases: RwLock::new(HashMap::new()),
        }
    }

    /// Scan data directory for existing .db files and open them.
    pub async fn load_existing(&self) -> Result<(), EsError> {
        let read_dir =
            std::fs::read_dir(&self.data_dir).map_err(|e| EsError::Internal(e.to_string()))?;

        for entry in read_dir {
            let entry = entry.map_err(|e| EsError::Internal(e.to_string()))?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("db") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    let name = name.to_string();
                    tracing::info!("Loading existing index: {}", name);
                    let conn = connection::open_or_create(&path)
                        .await
                        .map_err(|e| EsError::Internal(e.to_string()))?;

                    // Load mapping and aliases from _meta
                    let (mapping, saved_aliases) = conn
                        .call(|conn| {
                            let mapping_json: Option<String> = conn
                                .query_row(
                                    "SELECT value FROM _meta WHERE key = 'mappings'",
                                    [],
                                    |row| row.get(0),
                                )
                                .ok();
                            let mapping = mapping_json
                                .and_then(|j| serde_json::from_str::<IndexMapping>(&j).ok())
                                .unwrap_or_default();
                            let aliases_json: Option<String> = conn
                                .query_row(
                                    "SELECT value FROM _meta WHERE key = 'aliases'",
                                    [],
                                    |row| row.get(0),
                                )
                                .ok();
                            let aliases: Vec<String> = aliases_json
                                .and_then(|j| serde_json::from_str(&j).ok())
                                .unwrap_or_default();
                            Ok((mapping, aliases))
                        })
                        .await
                        .map_err(|e| EsError::Internal(e.to_string()))?;

                    // Count documents
                    let count = conn
                        .call(|conn| {
                            let count: i64 = conn
                                .query_row("SELECT COUNT(*) FROM _source", [], |row| row.get(0))
                                .unwrap_or(0);
                            Ok(count as u64)
                        })
                        .await
                        .map_err(|e| EsError::Internal(e.to_string()))?;

                    let handle = Arc::new(IndexHandle {
                        name: name.clone(),
                        conn,
                        mapping: RwLock::new(mapping),
                        settings: IndexSettings::default(),
                        doc_count: AtomicU64::new(count),
                    });

                    self.indices.write().insert(name.clone(), handle);

                    // Restore aliases
                    if !saved_aliases.is_empty() {
                        let mut aliases = self.aliases.write();
                        for alias in &saved_aliases {
                            let entry = aliases.entry(alias.clone()).or_default();
                            if !entry.contains(&name) {
                                entry.push(name.clone());
                            }
                        }
                        tracing::info!("Restored aliases for {}: {:?}", name, saved_aliases);
                    }
                }
            }
        }
        Ok(())
    }

    pub fn get(&self, name: &str) -> Result<Arc<IndexHandle>, EsError> {
        self.indices
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| EsError::IndexNotFound(name.to_string()))
    }

    pub fn exists(&self, name: &str) -> bool {
        self.indices.read().contains_key(name)
    }

    pub fn list(&self) -> Vec<Arc<IndexHandle>> {
        self.indices.read().values().cloned().collect()
    }

    /// Resolve an index pattern (possibly with wildcards or comma-separated names)
    /// into a list of matching index handles.
    /// Patterns like "di-data-assets-*" or "index1,index2" are supported.
    pub fn resolve_pattern(&self, pattern: &str) -> Vec<Arc<IndexHandle>> {
        let indices = self.indices.read();
        let mut results = Vec::new();

        for sub_pattern in pattern.split(',') {
            let sub_pattern = sub_pattern.trim();
            if sub_pattern.contains('*') || sub_pattern.contains('?') {
                // Glob-style matching
                for (name, handle) in indices.iter() {
                    if glob_match(sub_pattern, name) {
                        results.push(handle.clone());
                    }
                }
            } else if let Some(handle) = indices.get(sub_pattern) {
                results.push(handle.clone());
            }
        }

        // Deduplicate by name
        results.sort_by(|a, b| a.name.cmp(&b.name));
        results.dedup_by(|a, b| a.name == b.name);
        results
    }

    /// Check if a pattern contains wildcards.
    pub fn is_pattern(name: &str) -> bool {
        name.contains('*') || name.contains('?') || name.contains(',')
    }

    /// Add an alias pointing to an index (and persist to _meta).
    pub fn add_alias(&self, index: &str, alias: &str) {
        let mut aliases = self.aliases.write();
        let entry = aliases.entry(alias.to_string()).or_default();
        if !entry.contains(&index.to_string()) {
            entry.push(index.to_string());
        }
        // Persist: collect all aliases for this index
        let index_aliases: Vec<String> = aliases
            .iter()
            .filter(|(_, indices)| indices.contains(&index.to_string()))
            .map(|(alias, _)| alias.clone())
            .collect();
        drop(aliases);
        self.persist_aliases(index, &index_aliases);
    }

    /// Remove an alias from an index (and persist to _meta).
    pub fn remove_alias(&self, index: &str, alias: &str) {
        let mut aliases = self.aliases.write();
        if let Some(entry) = aliases.get_mut(alias) {
            entry.retain(|n| n != index);
            if entry.is_empty() {
                aliases.remove(alias);
            }
        }
        // Persist: collect remaining aliases for this index
        let index_aliases: Vec<String> = aliases
            .iter()
            .filter(|(_, indices)| indices.contains(&index.to_string()))
            .map(|(alias, _)| alias.clone())
            .collect();
        drop(aliases);
        self.persist_aliases(index, &index_aliases);
    }

    /// Persist aliases for an index into its _meta table.
    fn persist_aliases(&self, index: &str, aliases: &[String]) {
        let handle = match self.get(index) {
            Ok(h) => h,
            Err(_) => return,
        };
        let aliases_json = serde_json::to_string(aliases).unwrap_or_else(|_| "[]".to_string());
        let conn = handle.conn.clone();
        tokio::spawn(async move {
            let _ = conn
                .call(move |conn| {
                    conn.execute(
                        "INSERT OR REPLACE INTO _meta (key, value) VALUES ('aliases', ?1)",
                        rusqlite::params![&aliases_json],
                    )?;
                    Ok(())
                })
                .await;
        });
    }

    /// Resolve a name that could be an index name, alias, or wildcard pattern.
    /// Returns matching IndexHandles.
    pub fn resolve_name(&self, name: &str) -> Vec<Arc<IndexHandle>> {
        // First try exact index match
        if let Ok(handle) = self.get(name) {
            return vec![handle];
        }

        // Try alias
        let alias_indices = {
            let aliases = self.aliases.read();
            aliases.get(name).cloned()
        };
        if let Some(index_names) = alias_indices {
            let indices = self.indices.read();
            return index_names
                .iter()
                .filter_map(|n| indices.get(n).cloned())
                .collect();
        }

        // Try wildcard pattern
        if Self::is_pattern(name) {
            return self.resolve_pattern(name);
        }

        vec![]
    }

    /// Get all aliases for a specific index.
    pub fn get_aliases_for_index(&self, index: &str) -> Vec<String> {
        let aliases = self.aliases.read();
        aliases
            .iter()
            .filter(|(_, indices)| indices.contains(&index.to_string()))
            .map(|(alias, _)| alias.clone())
            .collect()
    }

    /// Get all aliases.
    pub fn get_all_aliases(&self) -> HashMap<String, Vec<String>> {
        self.aliases.read().clone()
    }

    pub async fn create(
        &self,
        name: String,
        mapping: IndexMapping,
        settings: IndexSettings,
    ) -> Result<Arc<IndexHandle>, EsError> {
        validate_index_name(&name)?;

        if self.exists(&name) {
            return Err(EsError::IndexAlreadyExists(name));
        }

        let db_path = self.data_dir.join(format!("{}.db", name));
        let conn = connection::open_or_create(&db_path)
            .await
            .map_err(|e| EsError::Internal(e.to_string()))?;

        // Create tables
        let mapping_clone = mapping.clone();
        conn.call(move |conn| {
            schema::create_index_tables(conn, &mapping_clone)?;

            // Store mapping in _meta
            let mapping_json = serde_json::to_string(&mapping_clone).unwrap_or_default();
            conn.execute(
                "INSERT INTO _meta (key, value) VALUES ('mappings', ?1)",
                rusqlite::params![mapping_json],
            )?;
            Ok(())
        })
        .await
        .map_err(|e| EsError::Internal(e.to_string()))?;

        let handle = Arc::new(IndexHandle {
            name: name.clone(),
            conn,
            mapping: RwLock::new(mapping),
            settings,
            doc_count: AtomicU64::new(0),
        });

        self.indices.write().insert(name, handle.clone());
        Ok(handle)
    }

    /// Get an existing index or auto-create it with default (empty) mappings.
    pub async fn get_or_create(&self, name: &str) -> Result<Arc<IndexHandle>, EsError> {
        if let Ok(handle) = self.get(name) {
            return Ok(handle);
        }
        // Auto-create with empty mapping
        tracing::info!("Auto-creating index: {}", name);
        self.create(
            name.to_string(),
            IndexMapping::default(),
            IndexSettings::default(),
        )
        .await
    }

    pub async fn delete(&self, name: &str) -> Result<(), EsError> {
        let handle = self
            .indices
            .write()
            .remove(name)
            .ok_or_else(|| EsError::IndexNotFound(name.to_string()))?;

        // Close the connection by dropping the handle
        drop(handle);

        // Remove the database file
        let db_path = self.data_dir.join(format!("{}.db", name));
        if db_path.exists() {
            std::fs::remove_file(&db_path).map_err(|e| EsError::Internal(e.to_string()))?;
        }
        // Also remove WAL and SHM files
        let wal_path = self.data_dir.join(format!("{}.db-wal", name));
        let shm_path = self.data_dir.join(format!("{}.db-shm", name));
        let _ = std::fs::remove_file(wal_path);
        let _ = std::fs::remove_file(shm_path);

        Ok(())
    }

    pub fn increment_doc_count(&self, name: &str, delta: u64) {
        if let Ok(handle) = self.get(name) {
            handle.doc_count.fetch_add(delta, Ordering::Relaxed);
        }
    }

    pub fn decrement_doc_count(&self, name: &str, delta: u64) {
        if let Ok(handle) = self.get(name) {
            handle.doc_count.fetch_sub(delta, Ordering::Relaxed);
        }
    }
}

/// Simple glob matching supporting `*` (any chars) and `?` (single char).
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();
    let mut dp = vec![vec![false; text.len() + 1]; pattern.len() + 1];
    dp[0][0] = true;

    for i in 1..=pattern.len() {
        if pattern[i - 1] == '*' {
            dp[i][0] = dp[i - 1][0];
        }
    }

    for i in 1..=pattern.len() {
        for j in 1..=text.len() {
            if pattern[i - 1] == '*' {
                dp[i][j] = dp[i - 1][j] || dp[i][j - 1];
            } else if pattern[i - 1] == '?' || pattern[i - 1] == text[j - 1] {
                dp[i][j] = dp[i - 1][j - 1];
            }
        }
    }

    dp[pattern.len()][text.len()]
}

fn validate_index_name(name: &str) -> Result<(), EsError> {
    if name.is_empty() {
        return Err(EsError::InvalidIndexName(
            "index name cannot be empty".to_string(),
        ));
    }
    if name.starts_with('_') || name.starts_with('-') || name.starts_with('+') {
        return Err(EsError::InvalidIndexName(format!(
            "index name [{}] must not start with '_', '-', or '+'",
            name
        )));
    }
    if name != name.to_lowercase() {
        return Err(EsError::InvalidIndexName(format!(
            "index name [{}] must be lowercase",
            name
        )));
    }
    if name.contains(['/', '*', '?', '"', '<', '>', '|', ' ', ',', '#', ':']) {
        return Err(EsError::InvalidIndexName(format!(
            "index name [{}] contains invalid characters",
            name
        )));
    }
    Ok(())
}

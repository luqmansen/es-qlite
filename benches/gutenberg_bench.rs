//! Performance benchmark: es-sqlite vs OpenSearch with real Gutenberg books.
//!
//! Downloads ~1500 books from Project Gutenberg (cached after first run),
//! then benchmarks both engines at increasing document body sizes:
//!   1K, 5K, 10K, 50K, 100K chars per document.
//!
//! Run with:
//!   cargo bench --bench gutenberg_bench
//!
//! Requires Docker for OpenSearch. If Docker is unavailable, only es-sqlite
//! is benchmarked.
#![allow(clippy::type_complexity)]

use futures::stream::{self, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const CONTAINER_NAME: &str = "es-sqlite-bench-opensearch";
const OPENSEARCH_IMAGE: &str = "opensearchproject/opensearch:2.17.1";
const CACHE_DIR: &str = "tests/.cache/gutenberg_books";

// ─── Server Management ──────────────────────────────────────────────────────

fn start_es_sqlite() -> String {
    // Kill any existing es-sqlite on port 19222
    let _ = Command::new("pkill")
        .args(["-f", "es-sqlite.*19222"])
        .output();
    std::thread::sleep(Duration::from_millis(500));

    // Build first
    let build = Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo build");
    assert!(build.status.success(), "cargo build failed");

    // Start server
    let data_dir = format!("{}/target/bench-data", env!("CARGO_MANIFEST_DIR"));
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).ok();

    let binary = format!("{}/target/release/es-sqlite", env!("CARGO_MANIFEST_DIR"));
    let mut child = Command::new(&binary)
        .args(["--port", "19222", "--data-dir", &data_dir])
        .spawn()
        .expect("start es-sqlite");
    // Detach so cleanup_servers() handles termination via pkill
    std::thread::spawn(move || {
        let _ = child.wait();
    });

    // Wait for it to be ready
    let url = "http://127.0.0.1:19222";
    for _ in 0..30 {
        if let Ok(output) = Command::new("curl")
            .args(["-sf", "--max-time", "1", url])
            .output()
        {
            if output.status.success() {
                return url.to_string();
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    panic!("es-sqlite failed to start");
}

fn start_opensearch() -> Option<String> {
    let docker_check = Command::new("docker").arg("info").output();
    if docker_check.is_err() || !docker_check.unwrap().status.success() {
        eprintln!("Docker not available, will only benchmark es-sqlite");
        return None;
    }

    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let result = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{port}:9200"),
            "-e",
            "discovery.type=single-node",
            "-e",
            "DISABLE_SECURITY_PLUGIN=true",
            "-e",
            "OPENSEARCH_INITIAL_ADMIN_PASSWORD=Admin123!",
            "-e",
            "OPENSEARCH_JAVA_OPTS=-Xms512m -Xmx512m",
            OPENSEARCH_IMAGE,
        ])
        .output();

    match result {
        Ok(output) if output.status.success() => {
            eprintln!("Started OpenSearch on port {port}");
        }
        _ => {
            eprintln!("Failed to start OpenSearch container");
            return None;
        }
    }

    let url = format!("http://127.0.0.1:{port}");
    for i in 0..120 {
        let check = Command::new("curl")
            .args(["-sf", "--max-time", "2", &url])
            .output();
        if let Ok(output) = check {
            if output.status.success() {
                eprintln!("OpenSearch ready after ~{i}s");
                return Some(url);
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }

    eprintln!("OpenSearch failed to start within 120s");
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
    None
}

fn cleanup_servers() {
    let _ = Command::new("pkill")
        .args(["-f", "es-sqlite.*19222"])
        .output();
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
    // Clean up bench data
    let data_dir = format!("{}/target/bench-data", env!("CARGO_MANIFEST_DIR"));
    let _ = std::fs::remove_dir_all(&data_dir);
}

// ─── HTTP Helpers ────────────────────────────────────────────────────────────

fn http() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .unwrap()
}

async fn create_index(base: &str, index: &str, mappings: Value) {
    let client = http();
    let _ = client.delete(format!("{base}/{index}")).send().await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let resp = client
        .put(format!("{base}/{index}"))
        .json(&json!({ "mappings": mappings }))
        .send()
        .await
        .expect("create index");
    assert!(resp.status().is_success(), "create index failed on {base}");
}

async fn refresh(base: &str, index: &str) {
    let client = http();
    client
        .post(format!("{base}/{index}/_refresh"))
        .send()
        .await
        .expect("refresh");
}

async fn search(base: &str, index: &str, body: Value) -> Value {
    let client = http();
    let resp = client
        .post(format!("{base}/{index}/_search"))
        .json(&body)
        .send()
        .await
        .expect("search");
    assert!(resp.status().is_success(), "search failed on {base}");
    resp.json().await.expect("parse")
}

async fn cleanup(base: &str, index: &str) {
    let client = http();
    let _ = client.delete(format!("{base}/{index}")).send().await;
}

async fn bulk_index(base: &str, index: &str, docs: &[(String, Value)]) {
    let client = http();
    // Use smaller chunks for larger payloads to avoid request size limits
    let chunk_size = 10;
    for chunk in docs.chunks(chunk_size) {
        let mut ndjson = String::new();
        for (id, doc) in chunk {
            ndjson.push_str(&format!(
                "{{\"index\":{{\"_index\":\"{index}\",\"_id\":\"{id}\"}}}}\n"
            ));
            ndjson.push_str(&serde_json::to_string(doc).unwrap());
            ndjson.push('\n');
        }
        let resp = client
            .post(format!("{base}/_bulk"))
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await
            .expect("bulk send failed");
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            panic!(
                "bulk failed on {} with status {}: {}",
                base,
                status,
                &body[..body.len().min(500)]
            );
        }
    }
}

// ─── Corpus Download ─────────────────────────────────────────────────────────

async fn load_or_download_corpus() -> Vec<Value> {
    let cache_dir = Path::new(CACHE_DIR);
    if cache_dir.exists() {
        let files: Vec<PathBuf> = std::fs::read_dir(cache_dir)
            .expect("read cache dir")
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|e| e == "json").unwrap_or(false))
            .collect();
        if !files.is_empty() {
            eprintln!(
                "Loading cached corpus from {} ({} files)",
                CACHE_DIR,
                files.len()
            );
            let docs: Vec<Value> = files
                .iter()
                .filter_map(|f| {
                    let data = std::fs::read_to_string(f).ok()?;
                    serde_json::from_str(&data).ok()
                })
                .collect();
            return docs;
        }
    }

    eprintln!("Downloading Gutenberg corpus (first run only)...");
    download_gutenberg_corpus().await
}

async fn fetch_metadata_page(client: &Client, page: i32) -> Vec<Value> {
    let url =
        format!("https://gutendex.com/books/?page={page}&languages=en&mime_type=text%2Fplain");

    let max_retries = 3;
    let mut last_err = String::new();

    for attempt in 1..=max_retries {
        let resp = match client.get(&url).send().await {
            Ok(r) => {
                if !r.status().is_success() {
                    last_err = format!("HTTP {}", r.status());
                    eprintln!(
                        "  Page {page} attempt {attempt}/{max_retries}: HTTP {}",
                        r.status()
                    );
                    tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                    continue;
                }
                r
            }
            Err(e) => {
                last_err = format!("{e:#}");
                eprintln!("  Page {page} attempt {attempt}/{max_retries}: {e:#}");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                continue;
            }
        };

        let body: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                last_err = format!("JSON parse: {e:#}");
                eprintln!("  Page {page} attempt {attempt}/{max_retries}: JSON parse error: {e:#}");
                tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
                continue;
            }
        };

        let results = match body["results"].as_array() {
            Some(r) => r,
            None => {
                eprintln!("  Page {page}: no 'results' array in response");
                return Vec::new();
            }
        };

        let mut books = Vec::new();
        for book in results {
            let title = book["title"].as_str().unwrap_or("").to_string();
            if title.is_empty() {
                continue;
            }
            let author = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["name"].as_str())
                .unwrap_or("Unknown")
                .to_string();
            let birth_year = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["birth_year"].as_i64())
                .unwrap_or(0);
            let death_year = book["authors"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|a| a["death_year"].as_i64())
                .unwrap_or(0);
            let subject = book["subjects"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string();
            let bookshelf = book["bookshelves"]
                .as_array()
                .and_then(|a| a.first())
                .and_then(|s| s.as_str())
                .unwrap_or("")
                .to_string();
            let download_count = book["download_count"].as_i64().unwrap_or(0);

            let text_url = book["formats"]
                .as_object()
                .and_then(|f| {
                    f.get("text/plain; charset=utf-8")
                        .or_else(|| f.get("text/plain; charset=us-ascii"))
                        .or_else(|| f.get("text/plain"))
                })
                .and_then(|v| v.as_str())
                .map(String::from);

            books.push(json!({
                "title": title,
                "author": author,
                "birth_year": birth_year,
                "death_year": death_year,
                "subject": subject,
                "bookshelf": bookshelf,
                "download_count": download_count,
                "text_url": text_url,
            }));
        }
        return books;
    }

    eprintln!("  Page {page} FAILED after {max_retries} retries: {last_err}");
    Vec::new()
}

async fn download_gutenberg_corpus() -> Vec<Value> {
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();

    // Phase 1: Fetch metadata (parallel — each page is independent)
    let pages = 50;
    eprintln!("  Fetching {pages} pages of metadata (3 concurrent)...");

    let all_books: Vec<Value> = stream::iter(1..=pages)
        .map(|page| {
            let client = &client;
            async move { fetch_metadata_page(client, page).await }
        })
        .buffer_unordered(3)
        .collect::<Vec<Vec<Value>>>()
        .await
        .into_iter()
        .flatten()
        .collect();

    eprintln!(
        "  Fetched metadata from {pages} pages ({} books)",
        all_books.len()
    );

    if all_books.is_empty() {
        eprintln!("  ERROR: No metadata fetched from Gutendex API");
        return Vec::new();
    }

    let total = all_books.len();
    eprintln!("  Downloading texts (5 workers)...");

    // Phase 2: Download texts with 10 concurrent workers, writing each to disk immediately
    std::fs::create_dir_all(CACHE_DIR).expect("create cache dir");

    let completed = AtomicUsize::new(0);

    let docs: Vec<Value> = stream::iter(all_books.into_iter().enumerate())
        .map(|(i, book)| {
            let client = &client;
            let completed = &completed;
            async move {
                let text_url = match book["text_url"].as_str() {
                    Some(u) if !u.is_empty() => u,
                    _ => return None,
                };

                let body_text = match client.get(text_url).send().await {
                    Ok(resp) => {
                        if !resp.status().is_success() {
                            eprintln!("  Book {i}: HTTP {} from {text_url}", resp.status());
                            return None;
                        }
                        match resp.text().await {
                            Ok(text) => {
                                let cleaned = strip_gutenberg_boilerplate(&text);
                                if cleaned.len() < 500 {
                                    return None;
                                }
                                truncate_to_char_boundary(&cleaned, 100_000).to_string()
                            }
                            Err(e) => {
                                eprintln!("  Book {i}: body read error: {e:#}");
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("  Book {i}: download failed: {e:#}");
                        return None;
                    }
                };

                let doc = json!({
                    "title": book["title"],
                    "author": book["author"],
                    "body": body_text,
                    "subject": book["subject"],
                    "bookshelf": book["bookshelf"],
                    "download_count": book["download_count"],
                    "birth_year": book["birth_year"],
                    "death_year": book["death_year"],
                });

                // Write to disk immediately
                let path = Path::new(CACHE_DIR).join(format!("book_{i:04}.json"));
                if let Ok(data) = serde_json::to_string(&doc) {
                    let _ = std::fs::write(&path, data);
                }

                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 50 == 0 {
                    eprintln!("  Downloaded {done}/{total}...");
                }

                Some(doc)
            }
        })
        .buffer_unordered(5)
        .filter_map(|x| async { x })
        .collect()
        .await;

    eprintln!("  Downloaded {} books with text", docs.len());
    docs
}

fn truncate_to_char_boundary(s: &str, max_bytes: usize) -> &str {
    if max_bytes >= s.len() {
        return s;
    }
    // Walk backwards from max_bytes to find a char boundary
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

fn strip_gutenberg_boilerplate(text: &str) -> String {
    let start_markers = [
        "*** START OF THIS PROJECT GUTENBERG",
        "*** START OF THE PROJECT GUTENBERG",
        "***START OF THIS PROJECT GUTENBERG",
        "***START OF THE PROJECT GUTENBERG",
    ];
    let end_markers = [
        "*** END OF THIS PROJECT GUTENBERG",
        "*** END OF THE PROJECT GUTENBERG",
        "***END OF THIS PROJECT GUTENBERG",
        "***END OF THE PROJECT GUTENBERG",
        "End of the Project Gutenberg",
        "End of Project Gutenberg",
    ];

    let start = start_markers
        .iter()
        .filter_map(|m| text.find(m))
        .min()
        .map(|pos| text[pos..].find('\n').map(|nl| pos + nl + 1).unwrap_or(pos))
        .unwrap_or(0);

    let end = end_markers
        .iter()
        .filter_map(|m| text[start..].find(m))
        .min()
        .map(|pos| start + pos)
        .unwrap_or(text.len());

    text[start..end].trim().to_string()
}

// ─── Benchmark Core ──────────────────────────────────────────────────────────

fn gutenberg_mappings() -> Value {
    json!({
        "properties": {
            "title": {"type": "text"},
            "author": {"type": "keyword"},
            "body": {"type": "text"},
            "subject": {"type": "keyword"},
            "bookshelf": {"type": "keyword"},
            "download_count": {"type": "integer"},
            "birth_year": {"type": "integer"},
            "death_year": {"type": "integer"}
        }
    })
}

/// Truncate the body field of each document to `max_chars`.
fn truncate_corpus(corpus: &[Value], max_chars: usize) -> Vec<(String, Value)> {
    corpus
        .iter()
        .enumerate()
        .map(|(i, doc)| {
            let mut d = doc.clone();
            if let Some(body) = d["body"].as_str() {
                let truncated = if body.len() > max_chars {
                    // Truncate at char boundary
                    let end = body
                        .char_indices()
                        .take_while(|(idx, _)| *idx < max_chars)
                        .last()
                        .map(|(idx, c)| idx + c.len_utf8())
                        .unwrap_or(max_chars.min(body.len()));
                    &body[..end]
                } else {
                    body
                };
                d["body"] = Value::String(truncated.to_string());
            }
            ((i + 1).to_string(), d)
        })
        .collect()
}

struct BenchResult {
    query_name: String,
    es_avg_us: u128,
    os_avg_us: Option<u128>,
}

/// Benchmark queries against a single engine. Returns (query_name, avg_us) pairs.
async fn bench_queries_single(
    base: &str,
    index: &str,
    queries: &[(&str, Value)],
    iterations: usize,
) -> Vec<(String, u128)> {
    let mut results = Vec::new();

    for (name, query) in queries {
        // Warm up
        let _ = search(base, index, query.clone()).await;

        let start = Instant::now();
        for _ in 0..iterations {
            let _ = search(base, index, query.clone()).await;
        }
        let avg = start.elapsed().as_micros() / iterations as u128;
        results.push((name.to_string(), avg));
    }
    results
}

fn stop_opensearch() {
    let _ = Command::new("docker")
        .args(["rm", "-f", CONTAINER_NAME])
        .output();
    // Give Docker a moment to release the port
    std::thread::sleep(Duration::from_secs(1));
}

fn print_step_header(body_size: &str, doc_count: usize) {
    eprintln!();
    eprintln!("┌───────────────────────────────────────────────────────────────────────────┐");
    eprintln!(
        "│  Body size: {body_size:>6}  |  Documents: {doc_count:>4}                                     │"
    );
    eprintln!("├───────────────────────┬──────────────┬──────────────┬────────────────────┤");
    eprintln!("│ Query                 │ es-sqlite    │ OpenSearch   │ Ratio              │");
    eprintln!("├───────────────────────┼──────────────┼──────────────┼────────────────────┤");
}

fn print_result(r: &BenchResult) {
    let os_str = match r.os_avg_us {
        Some(us) => format!("{us:>8} µs"),
        None => "      N/A".to_string(),
    };
    let ratio_str = match r.os_avg_us {
        Some(os_us) if os_us > 0 => {
            let ratio = r.es_avg_us as f64 / os_us as f64;
            if ratio < 1.0 {
                format!("{:>5.1}x faster ⚡", 1.0 / ratio)
            } else {
                format!("{ratio:>5.1}x slower   ")
            }
        }
        _ => "                ".to_string(),
    };
    eprintln!(
        "│ {:<21} │ {:>8} µs  │ {}  │ {} │",
        r.query_name, r.es_avg_us, os_str, ratio_str
    );
}

fn print_index_time(label: &str, es_ms: u128, os_ms: Option<u128>) {
    let os_str = match os_ms {
        Some(ms) => format!("{ms:>8} ms"),
        None => "      N/A".to_string(),
    };
    let ratio_str = match os_ms {
        Some(os) if os > 0 => {
            let ratio = es_ms as f64 / os as f64;
            if ratio < 1.0 {
                format!("{:>5.1}x faster ⚡", 1.0 / ratio)
            } else {
                format!("{ratio:>5.1}x slower   ")
            }
        }
        _ => "                ".to_string(),
    };
    eprintln!("├───────────────────────┼──────────────┼──────────────┼────────────────────┤");
    eprintln!("│ {label:<21} │ {es_ms:>8} ms  │ {os_str}  │ {ratio_str} │");
}

fn print_step_footer() {
    eprintln!("└───────────────────────┴──────────────┴──────────────┴────────────────────┘");
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        eprintln!("═══════════════════════════════════════════════════════════════");
        eprintln!("  es-sqlite vs OpenSearch — Gutenberg Corpus Benchmark");
        eprintln!("═══════════════════════════════════════════════════════════════");

        // Load corpus
        let corpus = load_or_download_corpus().await;
        if corpus.is_empty() {
            eprintln!("ERROR: No corpus available, cannot benchmark");
            std::process::exit(1);
        }
        eprintln!("Corpus: {} documents loaded", corpus.len());

        // Define queries used at every step
        let queries: Vec<(&str, Value)> = vec![
            (
                "match_all",
                json!({"query": {"match_all": {}}, "size": 20}),
            ),
            (
                "match_single",
                json!({"query": {"match": {"title": "adventure"}}, "size": 20}),
            ),
            (
                "match_body",
                json!({"query": {"match": {"body": "love and marriage"}}, "size": 20}),
            ),
            (
                "multi_match",
                json!({
                    "query": {"multi_match": {"query": "war peace", "fields": ["title^3", "body"]}},
                    "size": 20
                }),
            ),
            (
                "bool_must_filter",
                json!({
                    "query": {"bool": {
                        "must": [{"match": {"body": "science"}}],
                        "filter": [{"range": {"download_count": {"gte": 100}}}]
                    }},
                    "size": 20
                }),
            ),
            (
                "terms_agg",
                json!({"size": 0, "aggs": {"by_author": {"terms": {"field": "author", "size": 20}}}}),
            ),
            (
                "sort_popularity",
                json!({"query": {"match_all": {}}, "sort": [{"download_count": {"order": "desc"}}], "size": 20}),
            ),
            (
                "sort_multi",
                json!({"query": {"match_all": {}}, "sort": [{"author": {"order": "asc"}}, {"download_count": {"order": "desc"}}], "size": 20}),
            ),
            (
                "query_string",
                json!({"query": {"query_string": {"query": "adventure OR mystery", "default_field": "body"}}, "size": 20}),
            ),
            (
                "filtered_agg",
                json!({"size": 0, "query": {"match": {"body": "history"}}, "aggs": {"by_subject": {"terms": {"field": "subject", "size": 20}}}}),
            ),
        ];

        let iterations = 10;

        let steps: Vec<(&str, usize)> = vec![
            ("1K", 1_000),
            ("5K", 5_000),
            ("10K", 10_000),
            ("50K", 50_000),
            ("100K", 100_000),
        ];

        // ── Phase 1: es-sqlite (all steps) ──────────────────────────────
        eprintln!();
        eprintln!("═══════════════════════════════════════════════════════════════");
        eprintln!("  Phase 1: Benchmarking es-sqlite");
        eprintln!("═══════════════════════════════════════════════════════════════");

        let es_url = start_es_sqlite();
        eprintln!("es-sqlite ready at {es_url}");

        // es_results[step_idx] = (label, index_ms, [(query_name, avg_us)])
        let mut es_results: Vec<(String, u128, Vec<(String, u128)>)> = Vec::new();

        for (label, max_chars) in &steps {
            let docs = truncate_corpus(&corpus, *max_chars);
            let idx = format!("bench-{}", label.to_lowercase());

            let avg_body: usize = docs
                .iter()
                .map(|(_, d)| d["body"].as_str().map(|s| s.len()).unwrap_or(0))
                .sum::<usize>()
                / docs.len().max(1);

            eprintln!();
            eprintln!(
                "  [es-sqlite] Step {} (avg {}, {} docs)",
                label,
                format_bytes(avg_body),
                docs.len()
            );

            cleanup(&es_url, &idx).await;
            create_index(&es_url, &idx, gutenberg_mappings()).await;

            let index_start = Instant::now();
            bulk_index(&es_url, &idx, &docs).await;
            refresh(&es_url, &idx).await;
            let index_ms = index_start.elapsed().as_millis();

            let query_results =
                bench_queries_single(&es_url, &idx, &queries, iterations).await;

            for (name, avg) in &query_results {
                eprintln!("    {name:<21} {avg:>8} µs");
            }
            eprintln!("    {:<21} {:>8} ms", "bulk_index", index_ms);

            cleanup(&es_url, &idx).await;
            es_results.push((label.to_string(), index_ms, query_results));
        }

        // Kill es-sqlite
        let _ = Command::new("pkill")
            .args(["-f", "es-sqlite.*19222"])
            .output();

        eprintln!();
        eprintln!("  es-sqlite phase complete.");

        // ── Phase 2: OpenSearch (fresh container per step) ───────────────
        let docker_check = Command::new("docker").arg("info").output();
        let docker_available =
            docker_check.map(|o| o.status.success()).unwrap_or(false);

        // os_results[step_idx] = Option<(index_ms, [(query_name, avg_us)])>
        let mut os_results: Vec<Option<(u128, Vec<(String, u128)>)>> = Vec::new();

        if docker_available {
            eprintln!();
            eprintln!("═══════════════════════════════════════════════════════════════");
            eprintln!("  Phase 2: Benchmarking OpenSearch (fresh container per step)");
            eprintln!("═══════════════════════════════════════════════════════════════");

            for (label, max_chars) in &steps {
                let docs = truncate_corpus(&corpus, *max_chars);
                let idx = format!("bench-{}", label.to_lowercase());

                let avg_body: usize = docs
                    .iter()
                    .map(|(_, d)| d["body"].as_str().map(|s| s.len()).unwrap_or(0))
                    .sum::<usize>()
                    / docs.len().max(1);

                eprintln!();
                eprintln!(
                    "  [OpenSearch] Step {} (avg {}, {} docs) — starting fresh container...",
                    label,
                    format_bytes(avg_body),
                    docs.len()
                );

                // Kill any previous container and start fresh
                stop_opensearch();

                let os_url = match start_opensearch() {
                    Some(url) => url,
                    None => {
                        eprintln!("    Failed to start OpenSearch, skipping step {label}");
                        os_results.push(None);
                        continue;
                    }
                };

                create_index(&os_url, &idx, gutenberg_mappings()).await;

                let index_start = Instant::now();
                bulk_index(&os_url, &idx, &docs).await;
                refresh(&os_url, &idx).await;
                let index_ms = index_start.elapsed().as_millis();

                // Give OpenSearch time to settle after indexing
                tokio::time::sleep(Duration::from_secs(3)).await;

                let query_results =
                    bench_queries_single(&os_url, &idx, &queries, iterations).await;

                for (name, avg) in &query_results {
                    eprintln!("    {name:<21} {avg:>8} µs");
                }
                eprintln!("    {:<21} {:>8} ms", "bulk_index", index_ms);

                // Stop container immediately — next step gets a clean one
                stop_opensearch();

                os_results.push(Some((index_ms, query_results)));
            }

            eprintln!();
            eprintln!("  OpenSearch phase complete.");
        } else {
            eprintln!();
            eprintln!("Docker not available, skipping OpenSearch benchmarks.");
            for _ in &steps {
                os_results.push(None);
            }
        }

        // ── Combined Results ─────────────────────────────────────────────
        eprintln!();
        eprintln!("═══════════════════════════════════════════════════════════════════════════");
        eprintln!("  COMBINED RESULTS");
        eprintln!("═══════════════════════════════════════════════════════════════════════════");

        // Build summary for the table
        let mut summary: Vec<(String, Vec<(String, u128, Option<u128>)>)> = Vec::new();

        for (si, (label, es_index_ms, es_queries)) in es_results.iter().enumerate() {
            let os_data = os_results.get(si).and_then(|o| o.as_ref());

            let docs = truncate_corpus(&corpus, steps[si].1);
            let avg_body: usize = docs
                .iter()
                .map(|(_, d)| d["body"].as_str().map(|s| s.len()).unwrap_or(0))
                .sum::<usize>()
                / docs.len().max(1);

            let os_index_ms = os_data.map(|(ms, _)| *ms);

            print_step_header(
                &format!("{} (avg {})", label, format_bytes(avg_body)),
                docs.len(),
            );

            let mut step_results: Vec<(String, u128, Option<u128>)> = Vec::new();

            for (qi, (qname, es_avg)) in es_queries.iter().enumerate() {
                let os_avg = os_data.and_then(|(_, oq)| oq.get(qi)).map(|(_, v)| *v);

                let r = BenchResult {
                    query_name: qname.clone(),
                    es_avg_us: *es_avg,
                    os_avg_us: os_avg,
                };
                print_result(&r);
                step_results.push((qname.clone(), *es_avg, os_avg));
            }

            print_index_time(
                &format!("Bulk index ({label})"),
                *es_index_ms,
                os_index_ms,
            );
            print_step_footer();

            step_results.push(("bulk_index_ms".to_string(), *es_index_ms, os_index_ms));
            summary.push((label.clone(), step_results));
        }

        // Print scaling summary
        eprintln!();
        eprintln!("═══════════════════════════════════════════════════════════════════════════");
        eprintln!("  SCALING SUMMARY — How latency changes with document size");
        eprintln!("═══════════════════════════════════════════════════════════════════════════");
        eprintln!();

        // Header row
        eprint!("  {:>18}", "");
        for (label, _) in &summary {
            eprint!(" │ {label:^23}");
        }
        eprintln!();

        eprint!("  {:>18}", "Query");
        for _ in &summary {
            eprint!(" │ {:>8}  {:>8}  {:>3}", "sqlite", "OS", "x");
        }
        eprintln!();

        eprint!("  {:─>18}", "");
        for _ in &summary {
            eprint!("─┼─{:─>23}", "");
        }
        eprintln!();

        // Data rows
        if let Some((_, first_results)) = summary.first() {
            for (qi, (qname, _, _)) in first_results.iter().enumerate() {
                eprint!("  {qname:>18}");
                for (_, step_results) in &summary {
                    if let Some((_, es, os)) = step_results.get(qi) {
                        let unit = if qname == "bulk_index_ms" {
                            "ms"
                        } else {
                            "µs"
                        };
                        let os_str = match os {
                            Some(v) => format!("{v:>6}{unit}"),
                            None => "     N/A".to_string(),
                        };
                        let ratio = match os {
                            Some(ov) if *ov > 0 => {
                                let r = *es as f64 / *ov as f64;
                                if r < 1.0 {
                                    format!("{:.0}x", 1.0 / r)
                                } else {
                                    format!("{r:.0}x")
                                }
                            }
                            _ => " - ".to_string(),
                        };
                        eprint!(" │ {es:>6}{unit} {os_str} {ratio:>3}");
                    }
                }
                eprintln!();
            }
        }

        eprintln!();
        eprintln!("Done. {} documents benchmarked at 5 body size steps.", corpus.len());

        cleanup_servers();
    });
}

fn format_bytes(bytes: usize) -> String {
    if bytes >= 1_000_000 {
        format!("{:.1}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes}B")
    }
}

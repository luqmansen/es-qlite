//! Shared test infrastructure: auto-starts an es-sqlite server for integration tests.

use std::net::TcpListener;
use std::sync::{Arc, OnceLock};

static SERVER_URL: OnceLock<String> = OnceLock::new();

/// Returns the base URL (e.g. "http://127.0.0.1:12345") of a running es-sqlite server.
/// The server is started once and shared across all tests in the process.
/// Uses a dedicated background thread with its own tokio runtime so the server
/// survives across the separate per-test runtimes created by `#[tokio::test]`.
pub async fn ensure_server() -> &'static str {
    SERVER_URL.get_or_init(|| {
        let port = find_available_port();
        let data_dir = std::env::temp_dir().join(format!("es-sqlite-test-{port}"));
        let _ = std::fs::remove_dir_all(&data_dir);
        std::fs::create_dir_all(&data_dir).expect("create temp data dir");

        let data_dir_clone = data_dir.clone();

        // Spawn the server on a dedicated background thread with its own runtime
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().expect("create test server runtime");
            rt.block_on(async move {
                let registry = Arc::new(
                    es_sqlite::storage::registry::IndexRegistry::new(data_dir_clone),
                );
                registry.load_existing().await.expect("load existing indices");
                let app = es_sqlite::server::build_router(registry);

                let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{port}"))
                    .await
                    .expect("bind test server");

                axum::serve(listener, app).await.ok();
            });
        });

        // Wait until the server is accepting connections (blocking — runs once during init)
        for _ in 0..100 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                return format!("http://127.0.0.1:{port}");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        panic!("Test server failed to start on port {port}");
    })
}

fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind to find free port");
    listener.local_addr().unwrap().port()
}

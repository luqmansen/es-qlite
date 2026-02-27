mod config;
mod error;
mod model;
mod query;
mod routes;
mod server;
mod storage;

use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;

use config::Config;
use storage::registry::IndexRegistry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "es_qlite=info".into()),
        )
        .init();

    let config = Config::parse();

    // Ensure data directory exists
    std::fs::create_dir_all(&config.data_dir)?;

    let registry = Arc::new(IndexRegistry::new(config.data_dir.clone()));

    // Load existing indices
    registry.load_existing().await?;

    let app = server::build_router(registry);

    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("es-qlite listening on {}", addr);

    let listener = TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

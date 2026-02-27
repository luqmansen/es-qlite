use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug, Clone)]
#[command(name = "es-sqlite", about = "Elasticsearch-compatible API over SQLite FTS5")]
pub struct Config {
    #[arg(short, long, default_value = "9200")]
    pub port: u16,

    #[arg(short, long, default_value = "./data")]
    pub data_dir: PathBuf,

    #[arg(long, default_value = "0.0.0.0")]
    pub host: String,
}

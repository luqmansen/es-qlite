use rusqlite::Connection;
use std::path::Path;
use tokio_rusqlite::Connection as AsyncConnection;

pub async fn open_or_create(path: &Path) -> Result<AsyncConnection, tokio_rusqlite::Error> {
    let path = path.to_path_buf();
    let conn = AsyncConnection::open(path).await?;
    conn.call(|conn| {
        init_pragmas(conn)?;
        Ok(())
    })
    .await?;
    Ok(conn)
}

fn init_pragmas(conn: &Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA foreign_keys = ON;
         PRAGMA busy_timeout = 5000;",
    )?;
    Ok(())
}

use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;

use booldb_core::db::Database;

use crate::session;

/// Start the BoolDB TCP server.
pub async fn start(bind_addr: &str, data_dir: &str) -> std::io::Result<()> {
    let db = Database::open(data_dir).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
    })?;
    let db = Arc::new(Mutex::new(db));

    let listener = TcpListener::bind(bind_addr).await?;
    eprintln!("BoolDB server listening on {}", bind_addr);
    eprintln!("Data directory: {}", data_dir);

    loop {
        let (stream, addr) = listener.accept().await?;
        eprintln!("[server] Accepted connection from {}", addr);
        let db = Arc::clone(&db);

        tokio::spawn(async move {
            session::handle_connection(stream, db).await;
        });
    }
}

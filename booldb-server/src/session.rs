use tokio::net::TcpStream;

use booldb_core::sql::executor::ExecResult;
use booldb_core::types::Value;

use crate::protocol::{self, Request, Response};

/// Handle a single client connection.
pub async fn handle_connection(
    mut stream: TcpStream,
    db: std::sync::Arc<std::sync::Mutex<booldb_core::db::Database>>,
) {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    eprintln!("[session] Client connected: {}", peer);

    loop {
        let msg = match protocol::read_message(&mut stream).await {
            Ok(Some(data)) => data,
            Ok(None) => {
                eprintln!("[session] Client disconnected: {}", peer);
                break;
            }
            Err(e) => {
                eprintln!("[session] Read error from {}: {}", peer, e);
                break;
            }
        };

        let request: Request = match serde_json::from_slice(&msg) {
            Ok(req) => req,
            Err(e) => {
                let resp = Response::error(format!("Invalid request: {}", e));
                let _ = send_response(&mut stream, &resp).await;
                continue;
            }
        };

        let response = {
            let mut db = db.lock().unwrap();
            match db.execute(&request.sql) {
                Ok(result) => match result {
                    ExecResult::Ok { message } => Response::ok(message),
                    ExecResult::RowsAffected { count } => Response::rows_affected(count),
                    ExecResult::Rows { columns, rows } => {
                        let string_rows: Vec<Vec<String>> = rows
                            .iter()
                            .map(|row| row.iter().map(format_value).collect())
                            .collect();
                        Response::query_result(columns, string_rows)
                    }
                },
                Err(e) => Response::error(e.to_string()),
            }
        };

        if send_response(&mut stream, &response).await.is_err() {
            break;
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Text(v) => v.clone(),
        Value::Boolean(v) => v.to_string(),
    }
}

async fn send_response(
    stream: &mut TcpStream,
    response: &Response,
) -> Result<(), std::io::Error> {
    let data = serde_json::to_vec(response).unwrap();
    protocol::write_message(stream, &data).await
}

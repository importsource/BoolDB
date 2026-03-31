use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Client request.
#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub sql: String,
}

/// Server response.
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<String>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_affected: Option<usize>,
}

impl Response {
    pub fn ok(message: String) -> Self {
        Response {
            status: "ok".to_string(),
            message: Some(message),
            columns: None,
            rows: None,
            rows_affected: None,
        }
    }

    pub fn error(message: String) -> Self {
        Response {
            status: "error".to_string(),
            message: Some(message),
            columns: None,
            rows: None,
            rows_affected: None,
        }
    }

    pub fn rows_affected(count: usize) -> Self {
        Response {
            status: "ok".to_string(),
            message: Some(format!("{} row(s) affected", count)),
            columns: None,
            rows: None,
            rows_affected: Some(count),
        }
    }

    pub fn query_result(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Response {
            status: "ok".to_string(),
            message: None,
            columns: Some(columns),
            rows: Some(rows),
            rows_affected: None,
        }
    }
}

/// Wire protocol: length-prefixed JSON messages.
/// Format: [4 bytes big-endian length][JSON payload]

pub async fn read_message(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > 16 * 1024 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Message too large",
        ));
    }
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

pub async fn write_message(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = data.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

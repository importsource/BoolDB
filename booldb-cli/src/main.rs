use std::io::{Read, Write};
use std::net::TcpStream;

use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde::{Deserialize, Serialize};

const DEFAULT_ADDR: &str = "127.0.0.1:5433";

#[derive(Serialize)]
struct Request {
    sql: String,
}

#[derive(Deserialize, Debug)]
struct Response {
    status: String,
    message: Option<String>,
    columns: Option<Vec<String>>,
    rows: Option<Vec<Vec<String>>>,
    #[allow(dead_code)]
    rows_affected: Option<usize>,
}

fn main() {
    let addr = std::env::var("BOOLDB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());

    println!("BoolDB CLI - Connecting to {}...", addr);

    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => {
            println!("Connected. Type SQL statements or \\q to quit.\n");
            s
        }
        Err(e) => {
            eprintln!("Failed to connect to {}: {}", addr, e);
            eprintln!("Is the BoolDB server running?");
            std::process::exit(1);
        }
    };

    let mut rl = DefaultEditor::new().expect("Failed to create editor");
    let history_path = dirs_history_path();
    let _ = rl.load_history(&history_path);

    loop {
        let readline = rl.readline("booldb> ");
        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(trimmed);

                match trimmed {
                    "\\q" | "quit" | "exit" => {
                        println!("Bye!");
                        break;
                    }
                    "\\dt" => {
                        send_and_display(&mut stream, "SHOW TABLES");
                    }
                    "\\di" => {
                        send_and_display(&mut stream, "SHOW INDEXES");
                    }
                    cmd if cmd.starts_with("\\d ") => {
                        let table = cmd.trim_start_matches("\\d ").trim();
                        send_and_display(
                            &mut stream,
                            &format!("DESCRIBE {}", table),
                        );
                    }
                    "\\help" | "\\?" => {
                        print_help();
                    }
                    _ => {
                        send_and_display(&mut stream, trimmed);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
                break;
            }
        }
    }

    let _ = rl.save_history(&history_path);
}

fn send_and_display(stream: &mut TcpStream, sql: &str) {
    let request = Request {
        sql: sql.to_string(),
    };
    let payload = serde_json::to_vec(&request).unwrap();

    // Send: [4 bytes big-endian length][payload]
    let len = payload.len() as u32;
    if stream.write_all(&len.to_be_bytes()).is_err()
        || stream.write_all(&payload).is_err()
        || stream.flush().is_err()
    {
        eprintln!("Error: Lost connection to server");
        std::process::exit(1);
    }

    // Read response
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).is_err() {
        eprintln!("Error: Lost connection to server");
        std::process::exit(1);
    }
    let resp_len = u32::from_be_bytes(len_buf) as usize;
    let mut resp_buf = vec![0u8; resp_len];
    if stream.read_exact(&mut resp_buf).is_err() {
        eprintln!("Error: Lost connection to server");
        std::process::exit(1);
    }

    let response: Response = match serde_json::from_slice(&resp_buf) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error parsing response: {}", e);
            return;
        }
    };

    display_response(&response);
}

fn display_response(resp: &Response) {
    if resp.status == "error" {
        eprintln!(
            "ERROR: {}",
            resp.message.as_deref().unwrap_or("Unknown error")
        );
        return;
    }

    if let (Some(columns), Some(rows)) = (&resp.columns, &resp.rows) {
        print_table(columns, rows);
        println!("({} row(s))\n", rows.len());
    } else if let Some(msg) = &resp.message {
        println!("{}\n", msg);
    }
}

fn print_table(columns: &[String], rows: &[Vec<String>]) {
    if columns.is_empty() {
        return;
    }

    // Calculate column widths
    let mut widths: Vec<usize> = columns.iter().map(|c| c.len()).collect();
    for row in rows {
        for (i, cell) in row.iter().enumerate() {
            if i < widths.len() && cell.len() > widths[i] {
                widths[i] = cell.len();
            }
        }
    }

    // Print header
    let header: Vec<String> = columns
        .iter()
        .enumerate()
        .map(|(i, c)| format!(" {:width$} ", c, width = widths[i]))
        .collect();
    let separator: Vec<String> = widths.iter().map(|&w| "-".repeat(w + 2)).collect();

    println!("{}", header.join("|"));
    println!("{}", separator.join("+"));

    // Print rows
    for row in rows {
        let cells: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, cell)| {
                let w = if i < widths.len() { widths[i] } else { cell.len() };
                format!(" {:width$} ", cell, width = w)
            })
            .collect();
        println!("{}", cells.join("|"));
    }
}

fn print_help() {
    println!("BoolDB CLI Commands:");
    println!("  \\q            Quit");
    println!("  \\dt           List tables");
    println!("  \\di           List indexes");
    println!("  \\d TABLE      Describe table columns");
    println!("  \\help         Show this help");
    println!();
    println!("SQL Commands:");
    println!("  CREATE TABLE name (col TYPE, ...)");
    println!("  DROP TABLE name");
    println!("  INSERT INTO name VALUES (...)");
    println!("  SELECT ... FROM name [WHERE ...] [JOIN ...]");
    println!("  UPDATE name SET col = val [WHERE ...]");
    println!("  DELETE FROM name [WHERE ...]");
    println!("  CREATE INDEX name ON table (column)");
    println!("  DROP INDEX name");
    println!("  SHOW TABLES");
    println!("  SHOW INDEXES [ON table]");
    println!("  DESCRIBE table");
    println!("  EXPLAIN SELECT ...");
    println!();
    println!("Types: INTEGER, FLOAT, TEXT, BOOLEAN");
    println!();
}

fn dirs_history_path() -> String {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    format!("{}/.booldb_history", home)
}

mod protocol;
mod server;
mod session;

const DEFAULT_ADDR: &str = "127.0.0.1:5433";
const DEFAULT_DATA_DIR: &str = "./booldb_data";

#[tokio::main]
async fn main() {
    let addr = std::env::var("BOOLDB_ADDR").unwrap_or_else(|_| DEFAULT_ADDR.to_string());
    let data_dir =
        std::env::var("BOOLDB_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());

    eprintln!("Starting BoolDB server...");
    if let Err(e) = server::start(&addr, &data_dir).await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

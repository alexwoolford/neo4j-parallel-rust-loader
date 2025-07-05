use neo4j_parallel_rust_loader::{connect, load_parquet_parallel, Neo4jConfig};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    let cfg = Neo4jConfig::from_env()?;
    let mut args = env::args();
    let _bin = args.next();
    let path = match args.next() {
        Some(p) => p,
        None => {
            eprintln!("Usage: cargo run -- <path> <label> [concurrency]");
            std::process::exit(1);
        }
    };
    let label = match args.next() {
        Some(l) => l,
        None => {
            eprintln!("Usage: cargo run -- <path> <label> [concurrency]");
            std::process::exit(1);
        }
    };
    let concurrency: usize = args.next().unwrap_or_else(|| "4".to_string()).parse().unwrap_or(4);
    let graph = connect(&cfg).await?;
    load_parquet_parallel(graph, path, &label, concurrency).await?;
    Ok(())
}

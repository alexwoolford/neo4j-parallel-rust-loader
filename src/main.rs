use neo4j_parallel_rust_loader::{
    connect,
    load_parquet_nodes_parallel,
    load_parquet_relationships_parallel,
    Neo4jConfig,
};
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenvy::dotenv().ok();
    let cfg = Neo4jConfig::from_env()?;
    let mut args = env::args();
    let _bin = args.next();
    let mode = match args.next() {
        Some(m) => m,
        None => {
            eprintln!(
                "Usage:\n  cargo run -- nodes <path> <label> [concurrency]\n  cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]"
            );
            std::process::exit(1);
        }
    };
    let graph = connect(&cfg).await?;
    if mode == "nodes" {
        let path = match args.next() {
            Some(p) => p,
            None => {
                eprintln!("Usage: cargo run -- nodes <path> <label> [concurrency]");
                std::process::exit(1);
            }
        };
        let label = match args.next() {
            Some(l) => l,
            None => {
                eprintln!("Usage: cargo run -- nodes <path> <label> [concurrency]");
                std::process::exit(1);
            }
        };
        let concurrency: usize = args
            .next()
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .unwrap_or(4);
        load_parquet_nodes_parallel(graph, path, &label, concurrency).await?;
    } else if mode == "rels" {
        let path = match args.next() {
            Some(p) => p,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let rel_type = match args.next() {
            Some(l) => l,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let start_label = match args.next() {
            Some(l) => l,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let start_col = match args.next() {
            Some(c) => c,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let end_label = match args.next() {
            Some(l) => l,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let end_col = match args.next() {
            Some(c) => c,
            None => {
                eprintln!("Usage: cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]");
                std::process::exit(1);
            }
        };
        let concurrency: usize = args
            .next()
            .unwrap_or_else(|| "4".to_string())
            .parse()
            .unwrap_or(4);
        load_parquet_relationships_parallel(
            graph,
            path,
            &rel_type,
            &start_label,
            &start_col,
            &end_label,
            &end_col,
            concurrency,
        )
        .await?;
    } else {
        eprintln!(
            "Usage:\n  cargo run -- nodes <path> <label> [concurrency]\n  cargo run -- rels <path> <rel-type> <start-label> <start-col> <end-label> <end-col> [concurrency]"
        );
        std::process::exit(1);
    }
    Ok(())
}

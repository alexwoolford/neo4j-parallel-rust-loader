pub mod config;
pub mod loader;
pub mod neo4j;

pub use config::Neo4jConfig;
pub use loader::{
    load_parquet_nodes_parallel, load_parquet_parallel, load_parquet_relationships_parallel,
};
pub use neo4j::connect;

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use tokio::runtime::Runtime;

    #[test]
    fn test_env_config() {
        dotenv().ok();
        unsafe {
            std::env::set_var("NEO4J_URI", "bolt://example.com:7687");
            std::env::set_var("NEO4J_USERNAME", "neo4j");
            std::env::set_var("NEO4J_PASSWORD", "pass");
            std::env::set_var("NEO4J_DATABASE", "neo4j");
        }
        let cfg = Neo4jConfig::from_env().unwrap();
        assert_eq!(cfg.uri, "bolt://example.com:7687");
        assert_eq!(cfg.username, "neo4j");
        assert_eq!(cfg.password, "pass");
        assert_eq!(cfg.database.as_deref(), Some("neo4j"));
    }

    #[test]
    fn connection_requires_env() {
        dotenv().ok();
        if Neo4jConfig::from_env().is_ok() {
            // attempt to connect only when env vars present
            let cfg = Neo4jConfig::from_env().unwrap();
            let rt = Runtime::new().unwrap();
            let res = rt.block_on(async { connect(&cfg).await });
            // connection may fail if server is not reachable
            if let Err(e) = &res {
                eprintln!("failed to connect: {}", e);
            }
        }
    }
}

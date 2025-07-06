use crate::config::Neo4jConfig;
use neo4rs::{ConfigBuilder, Graph};
use rustls::crypto::ring;

/// Create a [`Graph`] connection using the provided config.
pub async fn connect(config: &Neo4jConfig) -> Result<Graph, neo4rs::Error> {
    // Install the rustls crypto provider so neo4rs can build TLS configs.
    let _ = ring::default_provider().install_default();
    let mut builder = ConfigBuilder::default()
        .uri(&config.uri)
        .user(&config.username)
        .password(&config.password);
    if let Some(db) = &config.database {
        builder = builder.db(db.as_str());
    }
    let cfg = builder.build().map_err(neo4rs::Error::from)?;
    Graph::connect(cfg)
}

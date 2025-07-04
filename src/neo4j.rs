use crate::config::Neo4jConfig;
use neo4rs::{Graph, ConfigBuilder};

/// Create a [`Graph`] connection using the provided config.
pub async fn connect(config: &Neo4jConfig) -> Result<Graph, neo4rs::Error> {
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

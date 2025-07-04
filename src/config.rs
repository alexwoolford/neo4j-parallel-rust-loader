use std::env;

/// Configuration for connecting to a Neo4j instance.
#[derive(Debug, Clone)]
pub struct Neo4jConfig {
    pub uri: String,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

impl Neo4jConfig {
    /// Load configuration from environment variables.
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            uri: env::var("NEO4J_URI")?,
            username: env::var("NEO4J_USERNAME")?,
            password: env::var("NEO4J_PASSWORD")?,
            database: env::var("NEO4J_DATABASE").ok(),
        })
    }
}

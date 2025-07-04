use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::{stream::FuturesUnordered, StreamExt};
use neo4rs::{query, BoltType, Graph};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::Row;
use tokio::sync::Semaphore;

/// Load Parquet data into Neo4j in parallel.
/// Each row in the Parquet file is mapped to properties of a node with the given label.
pub async fn load_parquet_parallel<P: AsRef<Path>>(
    graph: Graph,
    path: P,
    label: &str,
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let path_buf = path.as_ref().to_path_buf();
    // Read all rows from parquet in a blocking task
    let rows: Vec<Row> = tokio::task::spawn_blocking(move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
        let file = std::fs::File::open(path_buf)?;
        let reader = SerializedFileReader::new(file)?;
        let iter = reader.get_row_iter(None)?;
        let mut rows = Vec::new();
        for row in iter {
            rows.push(row?);
        }
        Ok(rows)
    })
    .await??;

    let semaphore = Arc::new(Semaphore::new(concurrency));
    let mut tasks = FuturesUnordered::new();

    for row in rows {
        let label = label.to_owned();
        let graph = graph.clone();
        let permit = semaphore.clone().acquire_owned().await?;
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let mut map: HashMap<String, BoltType> = HashMap::new();
            for (key, field) in row.get_column_iter() {
                let json = field.to_json_value();
                let bolt: BoltType = json.try_into()?;
                map.insert(key.clone(), bolt);
            }
            let q = query(format!("CREATE (n:{label} $props)").as_str()).param("props", map);
            graph.run(q).await
        }));
    }

    while let Some(res) = tasks.next().await {
        res??;
    }
    Ok(())
}

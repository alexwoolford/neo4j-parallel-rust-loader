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
pub async fn load_parquet_nodes_parallel<P: AsRef<Path>>(
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

/// Alias kept for backwards compatibility
pub async fn load_parquet_parallel<P: AsRef<Path>>(
    graph: Graph,
    path: P,
    label: &str,
    concurrency: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    load_parquet_nodes_parallel(graph, path, label, concurrency).await
}

/// Load relationships from a Parquet file in parallel.
///
/// Each row must contain identifiers for the start and end nodes as well as any
/// relationship properties. The `start_id_col` and `end_id_col` parameters
/// specify the column names used to match existing nodes. Nodes are matched by
/// label and property value.
pub async fn load_parquet_relationships_parallel<P: AsRef<Path>>(
    graph: Graph,
    path: P,
    rel_type: &str,
    start_label: &str,
    start_id_col: &str,
    end_label: &str,
    end_id_col: &str,
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

    // ------------------------------------------------------------------
    // Build color groups so that relationships within the same color do not
    // touch the same nodes. Each color can be processed concurrently without
    // lock conflicts. Colors themselves are processed sequentially.
    #[derive(Default)]
    struct ColorBatch {
        rows: Vec<Row>,
        used: std::collections::HashSet<String>,
    }

    let mut batches: Vec<ColorBatch> = Vec::new();

    for row in rows {
        let mut start_val = None;
        let mut end_val = None;
        for (key, field) in row.get_column_iter() {
            if key == start_id_col {
                start_val = Some(field.to_json_value().to_string());
            } else if key == end_id_col {
                end_val = Some(field.to_json_value().to_string());
            }
        }
        let start_val = start_val.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("missing start id column: {}", start_id_col),
            )
        })?;
        let end_val = end_val.ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("missing end id column: {}", end_id_col),
            )
        })?;

        let mut placed = false;
        for batch in &mut batches {
            if !batch.used.contains(&start_val) && !batch.used.contains(&end_val) {
                batch.used.insert(start_val.clone());
                batch.used.insert(end_val.clone());
                batch.rows.push(row.clone());
                placed = true;
                break;
            }
        }
        if !placed {
            let mut used = std::collections::HashSet::new();
            used.insert(start_val.clone());
            used.insert(end_val.clone());
            batches.push(ColorBatch { rows: vec![row.clone()], used });
        }
    }

    // Process each color sequentially but allow parallelism within the color.
    for batch in batches {
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut tasks = FuturesUnordered::new();

        for row in batch.rows {
            let graph = graph.clone();
            let rel_type = rel_type.to_owned();
            let start_label = start_label.to_owned();
            let end_label = end_label.to_owned();
            let start_id_col = start_id_col.to_owned();
            let end_id_col = end_id_col.to_owned();
            let permit = semaphore.clone().acquire_owned().await?;
            tasks.push(tokio::spawn(async move {
                let _permit = permit;
                let mut props: HashMap<String, BoltType> = HashMap::new();
                let mut start_id: Option<BoltType> = None;
                let mut end_id: Option<BoltType> = None;
                for (key, field) in row.get_column_iter() {
                    let json = field.to_json_value();
                    let bolt: BoltType = json.try_into()?;
                    if key == &start_id_col {
                        start_id = Some(bolt);
                    } else if key == &end_id_col {
                        end_id = Some(bolt);
                    } else {
                        props.insert(key.clone(), bolt);
                    }
                }
                let start_val = start_id.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("missing start id column: {}", start_id_col),
                    )
                })?;
                let end_val = end_id.ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("missing end id column: {}", end_id_col),
                    )
                })?;
                let q = query(format!(
                    "MATCH (a:{start_label} {{{start_id_col}: $start}}) \
                     MATCH (b:{end_label} {{{end_id_col}: $end}}) \
                     CREATE (a)-[r:{rel_type} $props]->(b)"
                ).as_str())
                    .param("start", start_val)
                    .param("end", end_val)
                    .param("props", props);
                graph.run(q).await
            }));
        }

        while let Some(res) = tasks.next().await {
            res??;
        }
    }
    Ok(())
}

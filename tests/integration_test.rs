use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dotenvy::dotenv;
use neo4j_parallel_rust_loader::{
    Neo4jConfig, connect, load_parquet_nodes_parallel, load_parquet_relationships_parallel,
};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

fn create_parquet(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_rel_parquet(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("start_id", DataType::Int64, false),
        Field::new("end_id", DataType::Int64, false),
        Field::new("since", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![2, 3])),
            Arc::new(Int64Array::from(vec![2020, 2021])),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

#[tokio::test]
async fn test_connection() {
    dotenv().ok();
    let cfg = match Neo4jConfig::from_env() {
        Ok(cfg) => cfg,
        Err(_) => {
            eprintln!("Skipping test_connection: missing env vars");
            return;
        }
    };
    let graph = match connect(&cfg).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Could not connect to database: {e}");
            return;
        }
    };
    let mut result = graph.execute(neo4rs::query("RETURN 1 as n")).await.unwrap();
    while let Ok(Some(row)) = result.next().await {
        let n: i64 = row.get("n").unwrap();
        assert_eq!(n, 1);
    }
}

#[tokio::test]
async fn test_loader() {
    dotenv().ok();
    let cfg = match Neo4jConfig::from_env() {
        Ok(cfg) => cfg,
        Err(_) => {
            eprintln!("Skipping test_loader: missing env vars");
            return;
        }
    };
    let graph = match connect(&cfg).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Could not connect to database: {e}");
            return;
        }
    };
    let parquet = "tests/data/sample.parquet";
    create_parquet(parquet).unwrap();
    load_parquet_nodes_parallel(graph.clone(), parquet, "Person", 4)
        .await
        .unwrap();
    let mut result = graph
        .execute(neo4rs::query("MATCH (n:Person) RETURN count(n) as c"))
        .await
        .unwrap();
    let mut count = 0;
    while let Ok(Some(row)) = result.next().await {
        count = row.get::<i64>("c").unwrap();
    }
    assert!(count >= 3);
}

#[tokio::test]
async fn test_relationship_loader() {
    dotenv().ok();
    let cfg = match Neo4jConfig::from_env() {
        Ok(cfg) => cfg,
        Err(_) => {
            eprintln!("Skipping test_relationship_loader: missing env vars");
            return;
        }
    };
    let graph = match connect(&cfg).await {
        Ok(g) => g,
        Err(e) => {
            eprintln!("Could not connect to database: {e}");
            return;
        }
    };
    let parquet = "tests/data/sample.parquet";
    create_parquet(parquet).unwrap();
    load_parquet_nodes_parallel(graph.clone(), parquet, "Person", 4)
        .await
        .unwrap();
    let rel_parquet = "tests/data/rels.parquet";
    create_rel_parquet(rel_parquet).unwrap();
    load_parquet_relationships_parallel(
        graph.clone(),
        rel_parquet,
        "KNOWS",
        "Person",
        "start_id",
        "id",
        "Person",
        "end_id",
        "id",
        4,
    )
    .await
    .unwrap();
    let mut result = graph
        .execute(neo4rs::query(
            "MATCH (:Person)-[r:KNOWS]->(:Person) RETURN count(r) as c",
        ))
        .await
        .unwrap();
    let mut count = 0;
    while let Ok(Some(row)) = result.next().await {
        count = row.get::<i64>("c").unwrap();
    }
    assert!(count >= 2);
}

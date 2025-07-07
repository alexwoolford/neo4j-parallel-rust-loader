use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use neo4j_parallel_rust_loader::{
    Neo4jConfig, connect, load_parquet_nodes_parallel, load_parquet_relationships_parallel,
};
use neo4rs::query;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

fn create_nodes_parquet<P: AsRef<Path>>(
    path: P,
    rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let ids: Vec<i64> = (0..rows as i64).collect();
    let values: Vec<i64> = (0..rows as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn create_rels_parquet<P: AsRef<Path>>(
    path: P,
    rows: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("start_id", DataType::Int64, false),
        Field::new("end_id", DataType::Int64, false),
        Field::new("since", DataType::Int64, false),
    ]));
    let start: Vec<i64> = (0..rows as i64).collect();
    let end: Vec<i64> = (0..rows as i64).map(|i| (i + 1) % rows as i64).collect();
    let since: Vec<i64> = (0..rows as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(start)),
            Arc::new(Int64Array::from(end)),
            Arc::new(Int64Array::from(since)),
        ],
    )?;
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

fn bench_nodes(c: &mut Criterion) {
    dotenvy::dotenv().ok();
    let cfg = match Neo4jConfig::from_env() {
        Ok(c) => c,
        Err(_) => return,
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    let graph = rt.block_on(connect(&cfg)).expect("connect");

    rt.block_on(async {
        graph
            .run(query(
                "CREATE INDEX bench_id IF NOT EXISTS FOR (n:Bench) ON (n.id)",
            ))
            .await
            .ok();
        graph
            .run(query("MATCH (n:Bench) DETACH DELETE n"))
            .await
            .ok();
    });

    c.bench_function("load 1000 nodes with relationships", |b| {
        b.to_async(&rt).iter(|| async {
            let nodes_parquet = "bench_nodes.parquet";
            let rels_parquet = "bench_rels.parquet";
            create_nodes_parquet(nodes_parquet, 1000).unwrap();
            create_rels_parquet(rels_parquet, 1000).unwrap();
            load_parquet_nodes_parallel(graph.clone(), nodes_parquet, "Bench", 8)
                .await
                .unwrap();
            load_parquet_relationships_parallel(
                graph.clone(),
                rels_parquet,
                "CONNECTED",
                "Bench",
                "start_id",
                "id",
                "Bench",
                "end_id",
                "id",
                8,
            )
            .await
            .unwrap();
            graph
                .run(query("MATCH (n:Bench) DETACH DELETE n"))
                .await
                .ok();
            std::fs::remove_file(nodes_parquet).ok();
            std::fs::remove_file(rels_parquet).ok();
        });
    });
}

use std::time::Duration;

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(5));
    targets = bench_nodes
}
criterion_main!(benches);

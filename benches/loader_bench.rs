use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, criterion_group, criterion_main};
use neo4j_parallel_rust_loader::{Neo4jConfig, connect, load_parquet_nodes_parallel};
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
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let names: Vec<String> = (0..rows).map(|i| format!("N{}", i)).collect();
    let values: Vec<i64> = (0..rows as i64).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
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
                "CREATE INDEX bench_name IF NOT EXISTS FOR (n:Bench) ON (n.name)",
            ))
            .await
            .ok();
        graph
            .run(query("MATCH (n:Bench) DETACH DELETE n"))
            .await
            .ok();
    });

    c.bench_function("load 1000 nodes", |b| {
        b.to_async(&rt).iter(|| async {
            let parquet = "bench_nodes.parquet";
            create_nodes_parquet(parquet, 1000).unwrap();
            load_parquet_nodes_parallel(graph.clone(), parquet, "Bench", 8)
                .await
                .unwrap();
            graph
                .run(query("MATCH (n:Bench) DETACH DELETE n"))
                .await
                .ok();
            std::fs::remove_file(parquet).ok();
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

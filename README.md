# Neo4j Parallel Rust Loader

This crate provides utilities to load Parquet files into Neo4j.

## Running the example loader

The repository now includes a small command line application. To run it, set the following environment variables so the program can connect to your Neo4j instance:

```
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=secret
NEO4J_DATABASE=neo4j   # optional
```

Then execute:

```
cargo run -- <parquet-file> <node-label> [concurrency]
```

The application reads the Parquet file and creates nodes with the given label in the database. The optional concurrency argument controls how many rows are inserted in parallel (default is 4).

## Loading relationships

The crate also includes a helper to create relationships from Parquet files. Each
row should specify the identifiers of the start and end nodes and any additional
relationship properties.

```rust
use neo4j_parallel_rust_loader::{connect, load_parquet_relationships_parallel, Neo4jConfig};

let graph = connect(&cfg).await?;
load_parquet_relationships_parallel(
    graph,
    "rels.parquet",
    "KNOWS",      // relationship type
    "Person",     // start node label
    "start_name", // column to match start node
    "Person",     // end node label
    "end_name",   // column to match end node
    4,
).await?;
```

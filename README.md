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

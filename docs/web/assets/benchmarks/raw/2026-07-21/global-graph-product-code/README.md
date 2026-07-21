# Global graph product-code validation

This directory is reserved for the raw AWS validation of BORSUK's experimental
resident global graph. The harness is the ignored Rust test
`global_graph::tests::global_graph_product_code_curve`.

The validation graph uses the existing deterministic HNSW builder and retains
its compact adjacency, including sparse upper layers. It intentionally stores
no full vector per node. This isolates the quality and memory cost of the new
rotated product codes; it is not yet the proposed alpha-pruned Vamana engine.

Run a small deterministic synthetic smoke with:

```bash
GLOBAL_GRAPH_SYNTHETIC_N=512 \
GLOBAL_GRAPH_SYNTHETIC_DIMENSIONS=64 \
GLOBAL_GRAPH_QUERY_LIMIT=20 \
GLOBAL_GRAPH_PQ_M=16 \
GLOBAL_GRAPH_R=16 \
GLOBAL_GRAPH_EF=64,128 \
GLOBAL_GRAPH_RERANK=20,40 \
GLOBAL_GRAPH_SAMPLE_LIMIT=256 \
GLOBAL_GRAPH_CENTROIDS=64 \
GLOBAL_GRAPH_PQ_ITERATIONS=3 \
BORSUK_SOURCE_SHA=<source-sha> \
CARGO_INCREMENTAL=0 \
cargo test -p borsuk --release --lib \
  global_graph::tests::global_graph_product_code_curve -- \
  --ignored --nocapture
```

For GIST, set `GIST_DIR`, `GIST_LIMIT`, and the same explicit sweep variables.
The emitted CSV reports memory-preloaded in-process traversal and exact rerank
CPU time. `modeled_rerank_sectors` is a locality-layout model, not measured S3
GET latency. AWS CPU/RSS/disk timelines and real S3 rerank measurements must be
kept beside the final CSV before any production or publication claim is made.

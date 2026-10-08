# Closed S3 Vectors reference

Same retained 100k Cohere D1024 corpus, 1000 queries, k=10; serial clients.

| Measurement | BORSUK baseline | S3 Vectors first pass |
|---|---:|---:|
| Recall@10 | 97.23% | 96.56% |
| p50 | 181.36 ms | 60.35 ms |
| p90 | 207.87 ms | 118.48 ms |
| p95 | 219.46 ms | 149.06 ms |
| p99 | 245.02 ms | 215.43 ms |

S3 first query: 310.05 ms after 63.39 minutes inactivity. Backend cache state is unknown; this is not guaranteed cold. BORSUK payload caches were disabled. Reciprocal serial throughput is not concurrent service capacity. Costs are unmeasured. BORSUK has higher recall but slower pooled latency; no cold win is established.

Turbopuffer published results use 10M documents with undisclosed exact query split, so our 100k results do not establish parity. Cold optimization proceeds with frozen 16/32 scheduling comparisons; warm caches are deferred.

The native Rust reducer produced the statistics. Root verification authenticated sealed response bytes, original exit statuses, cleanup and instance termination without locally recomputing scientific results. Original artifacts are retained losslessly; compressed logs use deterministic gzip.

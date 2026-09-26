# V270 full CoHere 10M cached dual graph gate

Preregistered 2026-09-26 UTC, before any V270 reservation or measurement.

## Decision

Test whether the V268 cached dual graph architecture builds, serves, and meets
quality and resource limits at 10,000,000 rows without a vector-count-specific
memory knee. Keep the V268 search parameters fixed: diverse source graph
`M=32`, `M0=64`, `ef_construction=128`, PQ64, FP16 fast navigation, one
`4096-4096` search arm, eight loaded workers. No retuning on the test panel.
V269 is the strongest HTTP product baseline, but it is 1M, so its latency is
historical context rather than a paired 10M comparison.

## Inputs and execution

- Corpus: authenticated `cohere-large-10m-768`, all 458 canonical train shards,
  10,000,000 rows, D768 cosine, k=100. Stage receipt SHA-256
  `0965aa0241199822dfac3410bba4edad5536ac0eb0aaa8ab83c216e8c5749a87`.
- Queries: authenticated `test.parquet`, SHA-256
  `5e0123f163df0e53a7e329fd92fbfd49f079756acfb47387ee6664c267b6f94e`,
  first 1,000 prior-used test queries. Development ordinals 0–255 and validation
  ordinals 256–999 are both disclosed as prior-used.
- Exact ground truth: all 10M F32 source rows, F64 normalized cosine, stable
  score then ID ordering. Compute only after sealing and reading back ANN raw
  output. Report combined and each split recall@100, p05 per-query hits, and
  GT100 hits. Preserve raw samples and source/graph/plane hashes.
- One `r7i.8xlarge` Spot worker in eu-central-1c, 256 GiB RAM, 250 GiB
  encrypted gp3 root, 12-hour hard stop. Use `causality`. Authenticate every
  source shard and generated artifact. Discard an interrupted measurement cell
  and use a new attempt. Sync terminal artifacts to S3, replay SHA-256s from
  closed terminal, then terminate immediately. No incomplete CSV inspection.

## Predeclared pass criteria

1. Full 10M build and query run complete on one immutable source archive.
   Generation, source, plane, graph, map, PQ books/codes, request and raw
   identities agree. Zero query-time object GETs and zero swap.
2. Development and validation each have mean recall@100 at least 0.995 and
   fifth-percentile hits at least 98/100. Combined GT100 hits at least 99,500
   of 100,000. No query is dropped; return exactly 100 unique valid IDs.
3. Eight-worker loaded in-process throughput at least 55 QPS, p95 at most
   150 ms, p99 at most 180 ms from the same raw query panel. Report
   p50/p90/p95/p99, per-query samples, visit counts, construction time,
   hydration time, peak build/prep/truth/serve RSS, storage and network bytes,
   GET counts, instance identity and elapsed compute cost. Loaded in-process
   latency is not HTTP product latency.
4. Serving process peak RSS at most 32 GiB. The authenticated resident plane
   cap is `2000 * rows` bytes (20 GB at 10M); the graph, PQ and workspace add
   to process RSS. The plane cap scales with data rather than switching at a
   particular row count. Report observed RSS rather than substituting the
   model. Total preparation/truth/build memory must fit 256 GiB.

If construction or exact truth exceeds the 12-hour cap, close the failed cell
and decide on a streaming build/truth redesign before another 10M run. If
recall or loaded latency misses, inspect the closed raw samples and choose one
material algorithm change with a cheap 100k falsifier. If all criteria pass,
promote this exact format to a frozen 10M HTTP serving and paired competitor
gate. No result from this preregistration alone establishes a S3 Vectors or
Turbopuffer win.

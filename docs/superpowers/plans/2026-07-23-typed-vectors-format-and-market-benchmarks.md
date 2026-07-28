# Typed Vectors, Standard Formats, And Market Benchmark Plan

## Phase 1: freeze the distributed mutation baseline

1. Keep the immutable global ANN artifact as the indexed base.
2. Merge exact WAL rows and materialized delta segments without disabling the
   base fast path.
3. Preserve the base through bounded compaction; replace it only during an
   explicit full rebuild.
4. Pass local multi-reader, snapshot, upsert, delete, flush, compaction, GC,
   formatting, strict lint, and benchmark-validator gates.

## Phase 2: remove the private exact-vector container

1. Add failing Arrow IPC sidecar tests for footer-only open, bounded record
   batches, duplicate-row range deduplication, corruption rejection, typed
   physical schemas, and exact round trips.
2. Implement standard Arrow IPC encode/footer parse/range decode.
3. Change ranged rerank to fetch each selected record batch once and share it
   across candidate rows.
4. Switch segment writes and reads to Arrow IPC.
5. Remove `BSKVEC01`, its compatibility parser, and misleading documentation.
6. Rebuild all fixtures and rerun the complete local gate.

## Phase 3: typed vector fields

1. Add `VectorElementType` and typed dense constructors/binding inputs.
2. Round and validate records to the declared type before routing/build.
3. Persist f32/f16/bfloat16/i8/binary exact rows in their standard Arrow
   physical layouts.
4. Add true packed-binary Hamming/Jaccard kernels and SIMD/scalar equivalence
   tests.
5. Add sparse-f16 postings and SIMD-block score accumulation.
6. Extend manifests, stats, CLI, Python, and bulk Arrow import/export.
7. Verify WAL, refresh, flush, compaction, GC, and multi-reader behavior for
   each type.

## Phase 4: late interaction

1. Add `List<FixedSizeList<T, N>>` entity storage.
2. Implement scalar reference MaxSim and optimized blocked/SIMD MaxSim.
3. Flatten token vectors into an immutable child ANN index with generation-safe
   entity mapping.
4. Aggregate entity candidates and exact-rerank token matrices.
5. Add dense+text, sparse+text, dense+sparse, and dense+sparse+text fusion
   experiments including rerank cost and effectiveness.

## Phase 5: physical-format A/B

Build identical exact-vector rows as:

1. Arrow IPC bounded record batches;
2. Parquet bounded row groups with page/offset indexes;
3. Vortex bounded layouts.

Measure build throughput, bytes/vector, footer/open requests, selected-row
requests/bytes, decode CPU, p50/p95/p99/stddev, RSS, disk I/O, and mixed-cache
concurrency on local NVMe and same-region S3. Keep format selection
configurable until repeated evidence selects a default.

## Phase 6: publication benchmark matrix

Run all cases from fresh prefixes and source checksums:

| Goal | Dataset/workload | Required output |
|---|---|---|
| Dense ANN baseline | DBpedia OpenAI 1M / 1536D | recall@10, p50/p95/p99/stddev, QPS, bytes/vector |
| Dense QPS/cost curve | Cohere 1M and 10M | latency-recall frontier, fixed-cost QPS, CPU/RAM/NVMe |
| Write-to-serve | LAION 100M / 768D | ingest, searchable/indexed windows, write amplification |
| Very large dense | MS MARCO V2 138M | uncached/disk-cached/mixed latency, storage cost, scan ratio, skew |
| Filter-heavy | synthetic 15M / 256D | narrow/wide filters, fallback-exact ratio, planner choice |
| Multi-tenant | namespaces from 10k to 1M | noisy-neighbor, cache locality, auth overhead, cost/tenant |
| Sparse/hybrid | BEIR-style dense+sparse+BM25 | NDCG@10, recall@50, MRR, latency, rerank cost |
| Cold economics | cleared local data cache over object storage | first query, requests, bytes, cost/1k |
| Typed storage | same corpus in f32/f16/bf16/i8/binary | quality, bytes/vector, build/search CPU and latency |
| Late interaction | ColBERT-compatible passage corpus | NDCG/MRR, MaxSim cost, candidate amplification |

For every case also run 0/25/50/75/100% cache-hit mixtures, bounded production
concurrency, uncapped research ceiling, mutation-under-load, and post-compaction
recovery. Publication charts show distributions and sample standard deviation,
not single latency values.

## Phase 7: external controls and claims

1. Use VectorDBBench-compatible Cohere/LAION inputs where possible.
2. Run direct identical-client controls for Amazon S3 Vectors and available
   embedded/self-hosted systems.
3. Keep vendor-reported values in a separately labeled context table.
4. Compare Arrow IPC/Parquet/Vortex from direct BORSUK harness measurements.
5. Regenerate docs/web tables and charts only from validator-approved artifacts.
6. State systems-composition novelty narrowly; do not claim novelty for
   TurboQuant, IVF/PQ, MaxSim, Arrow, Parquet, Vortex, WAL, or LSM compaction.

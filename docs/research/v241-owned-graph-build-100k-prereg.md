# V241 owned F32 graph build, ReLAION-100k

Decision: the current graph builder holds source F32 rows and a second
normalized F32 copy. V241 consumes and normalizes the source rows in place.
It changes build memory only. The HNSW insertion algorithm, graph format,
PQ search, FP16 rerank and serving inputs remain unchanged. This gate checks
that claim before considering a parallel builder or a 1M build.

Use one `causality` c7i.4xlarge Spot cell, with the V218 authenticated
ReLAION-100k D768 physical-order source, plane, PQ books/codes and request
roster. Its prior-used development queries are ordinals 0–255 and
method-held-out queries 256–999; k=100 with exact GT100. Freeze M=32,
M0=64, ef_construction=128, PQ ef=2048, FP16 shortlist=2048, eight
serving workers. Source archive, input sizes and SHA-256 values are pinned
by the launcher. Seal returned IDs before opening truth and prior results.
An interrupted cell is discarded and repeated at a new attempt, never
resumed as one measurement.

The paired baseline is V218's same-source graph build: artifact SHA-256
`d8b70919243a7cd6ecb9448ce23f776374738476c1882cbc6a651fb34753af2f`,
177.702 s Rust build time, 842,149,888 B peak process RSS. Its returned
quality was 99,771/100,000 exact GT100 hits, development 25,537/25,600,
held-out 74,234/74,400, p05=99. These are historical verified values;
the V241 worker will measure its own build and serving results.

Pass requires the exact historical graph SHA-256, 100,000 reachable rows,
minimum in-degree four, identical ordered returned IDs on all 1,000
requests, build RSS at most 600,000,000 B, and Rust build time at most
210 s. The memory ceiling leaves allocator and host variation over the
rough 535 MB expected after removing a 307.2 MB F32 row copy; that
expectation is a model, not a measurement. Exact graph identity is the
quality gate: if it passes, the V218 recall counts are inherited only
after raw ID replay independently confirms equality. Record p50/p90/p95/p99,
QPS, serving RSS and GETs descriptively; there is no new serving latency
win claim from this build experiment.

If graph identity or returned IDs differ, reject the implementation and
inspect normalization order and source binding. If memory or time fails,
keep the existing builder and profile its actual allocations or hot path
before a redesign. A pass promotes only the owned builder to a frozen 1M
build resource gate; it does not establish 10M/100M build time. The next
algorithm decision then tests serial insertion against one deterministic
parallel design at 100k. No corpus-size threshold or dataset-specific
parameter is introduced.

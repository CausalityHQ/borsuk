# Scan codecs and storage-tier execution design

**Date:** 2026-07-22

## Decision

BORSUK separates the vector-code algorithm from the storage execution policy.
The two choices are independently configurable and independently reported.
No cache-aware graph policy becomes a production default until recall-matched
AWS experiments pass the promotion gates in this document.

## Public scan-codec names

The public names describe the encoded representation and scoring algorithm:

- `pq-scan`: classical learned product quantization without a rotation;
- `srht-pq-scan`: seeded SRHT/FWHT rotation followed by learned product
  quantization; this is the current resident global-PQ implementation;
- `fast-turboquant-mse-scan`: the MSE-only ablation: unit normalization, a fast
  structured orthogonal rotation, fixed dimension-derived Lloyd-Max levels,
  bit-packed coordinate codes, and one stored vector norm;
- `fast-turboquant-scan`: the full two-stage structured TurboQuant profile. Its MSE
  stage uses one fewer scalar bit and its full-width residual stage stores one
  sign bit per padded coordinate plus the residual norm.

The implementation does not allocate or persist a dense `dimensions x
dimensions` rotation matrix. Both rotated codecs reconstruct their seeded sign
vector and use the in-place FWHT implementation. `fast-turboquant-scan` is not an
alias for `pq-scan`, and the old alias is removed. Because BORSUK has not been
released, incompatible indexes are rejected and rebuilt instead of silently
upgraded.

Until matched evidence qualifies another codec, `srht-pq-scan` remains the
production default.

## Storage execution policy

`CacheExecutionPolicy` is orthogonal to the scan codec:

- `scan` always executes the selected packed scan codec, whether its immutable
  pages arrive from S3 or the local cache. This remains the default.
- `graph` is an experimental preference. It selects graph only with a complete,
  checksum-validated local snapshot and otherwise selects scan before query
  execution. It never issues graph pointer reads against S3.
- `auto` is a real tier selector: it selects graph only when the exact active
  manifest version has a complete local coverage certificate and otherwise
  selects the configured storage scan. The recall-matched promotion matrix
  decides whether `auto` becomes the default, not whether explicit `auto`
  changes behavior.

Selection is made per routed global cell before candidate generation. Covered
cells may use their local graph while uncovered cells use the configured scan
codec; both streams merge before one exact rerank. A cell never begins graph
traversal and falls back partway through. Search reports include requested
policy, work by engine, observed cache coverage, and fallback reasons.

The read-through cache backend remains configurable separately: cache path,
byte budget, memory-decoded budget, and whether warmed graph pages are pinned.
The production defaults are bounded. A cache coverage certificate is keyed by
manifest checksum and graph-bundle checksums and is invalidated by eviction or
manifest replacement.

## Cached graph layout

The primary design under experiment stores a graph per global semantic IVF
cell. This preserves the production global router, bounds traversal memory,
allows immutable cells to be shared among concurrent callers, and avoids a
single 100M-node resident graph. Current segment-local graphs remain an
explicit experimental control because physical ingestion segments do not align
with global IVF routing. A whole-index graph is also a research control, not a
production candidate, because its build cost, footprint, and random access are
unbounded at the target scale.

Same-checksum reads and decodes are single-flight across users. Global request,
decode, and decoded-byte admission caps remain authoritative for both scan and
graph execution.

## Experiment matrix

### Scan codecs

For `pq-scan`, `srht-pq-scan`, `fast-turboquant-mse-scan`, and
`fast-turboquant-scan`, collect:

1. equal layout, routing probes, and candidate budgets;
2. equal persisted-code footprint points;
3. recall-matched frontiers at every corpus target; and
4. exact formal-recall controls, labeled separately.

Both TurboQuant profiles sweep total 2-, 3-, and 4-bit rates. The full profile
always uses a full-width residual stage; the old partial-QJL configuration is
not a publication codec. Report actual persisted bytes rather than nominal
bits. Each selected point gets three fresh-process repetitions. Multi-shard
MSE-only TurboQuant is labeled as a BORSUK ablation; the faithful full control
uses one whole-vector transform.

### Storage execution

For every qualified codec and dataset, measure:

- remote/uncached `scan`;
- disk-cached `scan`;
- direct remote and disk-cached segment-graph controls without decoded
  retention, so NVMe bytes are never mistaken for a RAM graph;
- memory-preloaded graph as separately labeled research evidence; and
- experimental `auto`, proving both its cache-hit and cache-miss decisions.

The direct graph control does not imply that `auto` should select a graph from
NVMe. It first measures whether decode, checksum validation, and adjacency
preparation make that tier faster than the packed scan. If it qualifies, a
manifest/checksum-specific disk coverage certificate and bounded decoded LRU
become a separate implementation gate. If it does not, `auto` uses packed scan
for NVMe/S3 and graph only for already-decoded hot cells.

Bounded decoded-graph profiles sweep 64, 128, 256, and 512 MiB. Each emits a
per-query mixed-cache artifact for requested hot-query mixes of 0/25/50/75/100%.
The row records actual decoded-memory, disk-cache, and backing-storage access
fractions because query identity is not cache coverage: nominally cold queries
can share cells with the seeded hot set, and hot cells can be evicted. Promotion
uses the observed fractions and tails, never the requested mix alone.

The graph sweep varies graph degree, build search width, traversal width,
routed cells, and rerank candidates. A graph point is recall-matched against
the scan control built from the same immutable vectors.

### Corpora and scale

Development qualification uses deterministic unit data, then Fashion-MNIST,
GloVe, GIST, and Deep-Image. Qualified candidates run on all six public
corpora: Fashion-MNIST, GloVe, SIFT, NYTimes, GIST, and Deep-Image. The final
winner is validated on clustered, uniform, and adversarial synthetic data,
including 100M vectors.

Every run records p50/p95/p99/max latency, throughput, recall, exact rows
reranked, S3 requests and bytes, local-cache hits/bytes/evictions, process CPU,
RSS, process disk I/O, cache size, build time, scratch peak, index footprint,
object count, hardware, source checksum, and complete configuration.

## Promotion gates

A non-default configuration can be published as experimental with complete
evidence. A default changes only when all applicable public corpora satisfy:

- recall no worse than 0.001 below its recall-matched control;
- p95 and p99 no slower across all selected repetitions;
- no throughput regression at bounded production concurrency;
- peak RSS within the configured budget and no unexplained >20% paired
  regression;
- no remote GET or byte read for a row labeled fully cached;
- no multi-second overload tail caused by admission, duplicate decode, or
  graph validation; and
- complete build, footprint, CPU, memory, disk, and cost evidence.

`auto` has the additional requirement that forced cache miss, partial cache,
eviction, manifest replacement, and corrupt-cache tests all select scan before
query execution. Until these gates pass, the defaults are
`srht-pq-scan + scan`.

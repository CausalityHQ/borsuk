# Adaptive graph-default promotion experiment design

**Date:** 2026-07-21

## Decision under test

BORSUK will not promote the segment-local graph from an experimental leaf mode
to the production default on the strength of the Fashion-MNIST result alone.
The promotion decision will use paired AWS measurements on every supported
public benchmark corpus and the controlled synthetic families. The existing
TurboQuant `pq-scan` path is the control. A graph default is permitted only if
it meets the recall, latency, concurrency, and resource gates below.

The preferred outcome is an adaptive default: select graph only for an index
profile whose measured or declared resource envelope supports it, and retain
`pq-scan` as the graph-free fallback. A universal graph default is allowed only
if graph passes every gate on every public corpus.

## Scope

### Public corpora

- Fashion-MNIST: 60,000 × 784, Euclidean.
- GloVe: 1,183,514 × 100, cosine.
- SIFT: 1,000,000 × 128, Euclidean.
- NYTimes: 290,000 × 256, cosine.
- GIST: 1,000,000 × 960, Euclidean.
- Deep-Image: 9,990,000 × 96, cosine.

Each run uses the corpus's shipped full-corpus ground truth and all benchmark
queries used by the existing production evidence. A profiling subset may be
used only to locate a bottleneck and must never be published as a final recall
or latency result.

### Controlled datasets

- sklearn digits.
- Uniform synthetic vectors.
- Clustered synthetic vectors.
- Adversarial synthetic vectors.

Synthetic cases use deterministic seeds and cover dimensions representative of
the public suite, including a high-dimensional case. Record count and dimension
must be encoded in every artifact row.

## Compared systems and index layouts

Every dataset receives paired measurements for:

1. the current graph-free production index using TurboQuant `pq-scan`;
2. the same graph-free index queried with `flat-scan`, identifying cases where
   exact full-cell scoring—not graph traversal—is the faster explanation;
3. a graph-enabled index queried with `pq-scan`, isolating graph-index overhead;
4. the same graph-enabled index queried with `graph`, isolating leaf-method
   behavior from index/layout differences.

Graph promotion selection excludes `max_candidates >= segment_max_vectors`.
Those rows remain visible as full-cell-scan ablations, but cannot be presented
as graph traversal or used to promote the graph default.

Vamana-PQ and hybrid may be retained as research context, but they do not decide
whether pure graph replaces `pq-scan`. Index build time, object count, logical
and physical footprint, graph bytes, and local-cache footprint are recorded.

The dimension-aware cell-size recommendation is the starting layout. Layout
changes are permitted only as explicit ablations; a winning method may not hide
a different layout without labeling it.

## Recall and latency procedure

For both `pq-scan` and graph, sweep routed-cell count and candidates per cell.
Keep the complete recall/latency frontier, then select:

- the first configuration meeting the corpus's publication recall target;
- a recall-matched point against the current production `pq-scan` row; and
- the best latency point that does not lose more than 0.001 absolute recall
  against that control.

Final selected points use at least three fresh-process repetitions and the full
query set. Report p50, p95, p99, mean, throughput, recall@10, exact rows scored,
routed cells, candidate width, GETs, bytes read, and estimated request cost.

## Cache-state contract

- `uncached`: initialization and serving metadata are complete and resident,
  while query cell/graph payloads are absent from the local disk cache. Network
  reads are included in query latency.
- `disk_cached`: the identical working set is present in the bounded local disk
  cache, with zero backing GETs and zero backing bytes required for a valid row.
  Decoded-segment retention is disabled for this profile: otherwise pq-scan
  stops using projected reads and the result becomes a mixed process-memory
  cache measurement. Same-cell single-flight and the global decode cap remain
  enabled.
- `memory_preloaded`: decoded segments and, for graph-enabled indexes, decoded
  validated graphs are intentionally pinned before measurement. This is a
  separately labeled research state and is never substituted for disk-cached
  production results.

The first query after library/index initialization is recorded separately so
metadata or graph initialization cannot leak into an ambiguously named cold
row.

## Concurrency and resource procedure

Every selected real-dataset point receives:

- a production profile with bounded global request admission and bounded cell
  decode/prefetch parallelism; and
- a research-ceiling profile with uncapped cell-read parallelism and increasing
  concurrent users, explicitly labeled as non-production.

Collect a time series for process CPU, peak and sampled RSS, process disk I/O,
local cache size, cache hits/misses, S3 GET/HEAD/LIST counts, bytes fetched,
latency percentiles, throughput, and errors. The production profile must prove
that concurrent queries cannot multiply per-query graph width into unbounded
memory. Same-checksum overlapping reads must remain single-flight.

## Promotion gates

Graph may become the universal default only when all six public corpora satisfy
all of these gates in the disk-cached production profile:

1. recall is no worse than 0.001 below the recall-matched `pq-scan` control;
2. the selected graph point meets the corpus target and truncates the cell;
3. p95 and p99 are each no slower than the control in all three repetitions;
4. aggregate throughput is no lower at production concurrency;
5. peak RSS remains within the configured RAM budget and no more than 20%
   above the paired graph-free production process;
6. no query exceeds the global admission/decode caps;
7. backing GETs and bytes are zero for a valid disk-cached row;
8. uncached behavior has no multi-second latency outlier attributable to graph
   decode, validation, or duplicate fetches; and
9. build time, S3 footprint, and local-cache footprint are reported without an
   unexplained regression.

If graph wins only on a subset, BORSUK keeps `pq-scan` as the universal fallback
and introduces an adaptive selection policy only for the passing profiles. If
graph fails materially on any corpus, it remains an explicit experimental
option and the failure is published rather than hidden.

## Reproducibility and evidence

AWS runs use the existing Frankfurt benchmark bucket and Graviton client class,
recording region, instance type, CPU architecture, vCPU count, RAM, Rust commit
or source checksum, build mode, dataset checksum, index prefix, cache state,
configuration, seed, and exact command. Raw logs, CSVs, resource time series,
and generated SVGs are checked into the dated research artifact tree and
archived to S3 before the benchmark instance is stopped.

The research validator must reject missing public-corpus/method/cache-state
cells, missing resource telemetry, incomplete repetitions, mixed query counts,
or a claimed default that does not pass the promotion gates.

## Documentation and default change

The research pages will show the complete graph and `pq-scan` frontiers,
selected repetitions, resource plots, footprint/build comparisons, failures,
and promotion decision. The normal API and architecture pages change the
default only after the evidence validator records a passing decision. Any
adaptive policy must expose the chosen leaf mode and reason in the search plan
and report; users can always pin `pq-scan` or graph explicitly.

Before publishing the new matrix, audit every result rendered by the README,
documentation, and website. Each visible number must resolve to a dated raw
artifact and identify dataset, method, index capability/layout, cache state,
query count, and whether the number is startup or query latency. Superseded
rows—including the reported GloVe view showing roughly 28 MB per query and
roughly two seconds at 256 candidates—must be removed from current-result views
or moved into an explicitly historical section. Startup time must never be
plotted as query latency, and transferred bytes must not be described as
resident memory.

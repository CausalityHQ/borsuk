# BORSUK Design Extension: Multi-Mode Leaf Engines

**Project name:** BORSUK
**Expansion:** **Blob-Oriented Retrieval with Segmental Unified KNN**
**Tagline:** *Digs through blobs, not RAM.*

This document extends the core BORSUK design with a more explicit model for **segments/leaves** and **multiple search modes**. The key extension is that each segment can contain a local leaf index, and that local leaf index can be implemented in different ways depending on the performance, exactness, storage, and vector metric requirements.

The strongest proposed default for optimized read-heavy vector search is:

```text
BORSUK global segment router
+ Vamana-like page-packed leaf graph
+ compressed PQ/SQ codes for traversal
+ exact vector block rerank
+ LSM-style L0/L1/L2 segment lifecycle
```

This can be called the **VamanaPQ leaf engine**.

---

## 1. Design Goal

BORSUK should not be “one giant HNSW on disk”. The design should be:

```text
many external segments/leaves
+ small local indexes inside selected leaves
+ global low-RAM router deciding which leaves to open
```

The goal is to keep memory usage proportional to:

```text
O(number_of_segments + number_of_pivots + active_queries + cache_budget)
```

not:

```text
O(number_of_vectors)
```

For 100M vectors, this means the system should keep only the manifest, pivots, segment summaries, hot page cache, and active query state in RAM. Raw vectors, graph pages, compressed codes, and most leaf indexes live in local files or blob/object storage.

---

## 2. Terminology

### Segment / Leaf

A **segment** or **leaf** is an immutable storage unit containing a subset of vectors and the local structures needed to search them.

The words can be used as follows:

```text
segment = physical storage unit
leaf    = logical searchable unit inside the global index
```

In most implementations, one segment equals one leaf.

### Global Router

The **global router** is the top-level BORSUK layer. It keeps small metadata in RAM and decides which leaves are worth opening for a query.

### Leaf Engine

The **leaf engine** is the local search method used inside a selected leaf.

Examples:

```text
FlatScan
PQScan
HNSW
VamanaPQ
```

Public BORSUK APIs are vector-only. String distances and object-based metric
leaves are out of scope for the supported API surface.

---

## 3. Segment Structure

A BORSUK segment should be self-contained and range-readable.

Recommended physical layout:

```text
leaf_000123.parquet
├── header
├── segment_summary
├── routing_mini_header
├── local_index_block
├── compressed_codes_block
├── graph_pages_block
├── exact_vector_blocks
├── id_map
└── footer/checksums
```

Alternative directory-style layout:

```text
leaf_000123/
├── header.bin
├── summary.bin
├── graph.bin
├── codes.bin
├── vectors.blocks.bin
├── ids.bin
└── footer.bin
```

For blob storage, a single large object with internal offsets is often preferable because it allows predictable range reads and avoids too many tiny objects.

---

## 4. What Was in Segments Before Local Graphs

Before introducing local HNSW/Vamana-style leaf indexes, a segment was primarily:

```text
routing summary + compressed candidate layer + exact vector blocks
```

The original search path was:

```text
1. Global router ranks segments.
2. Query reads promising segment sketches/codes.
3. Segment is scanned or filtered using compressed representations.
4. Top candidates are selected.
5. Exact vector blocks are fetched.
6. Candidates are reranked with the true metric.
```

Original segment contents:

```text
header
segment summary
vector-level sketches
compressed vector codes
exact vector blocks
record ids
```

This was simple, blob-friendly, and easy to make exact within a selected segment by scanning all vectors in that segment. The weakness was that selected leaves could still be expensive to scan if they contained many vectors.

---

## 5. Why Add Local Leaf Indexes

A local leaf index can reduce work inside selected segments.

Instead of:

```text
open leaf -> scan 100k compressed codes -> rerank top M
```

we can do:

```text
open leaf -> graph/search local index over maybe 500–3000 nodes -> rerank top M
```

Expected benefits:

```text
less CPU per selected leaf
less candidate scoring
fewer exact vector blocks read
better query latency in approximate mode
better candidate quality for rerank
```

But local graph indexes must be designed carefully. A graph that performs thousands of tiny random reads can be worse than a simple compressed scan, especially on S3/GCS/Azure Blob/MinIO.

---

## 6. Leaf Engine Options

BORSUK should support multiple leaf engines rather than hardcoding one algorithm.

### 6.1 FlatScan Leaf

The simplest leaf engine.

```text
selected leaf -> scan exact or compressed vectors -> return candidates
```

Best for:

```text
MVP
exact vector mode
all supported vector metrics
small leaves
debugging and correctness testing
```

Advantages:

```text
simple
predictable
can be exact inside the leaf
works with all supported vector metric functions
very good baseline
```

Weaknesses:

```text
can read too much data
CPU-heavy for large leaves
not ideal for very low latency
```

---

### 6.2 PQScan / SQScan Leaf

A compressed scan leaf.

```text
selected leaf -> scan PQ/SQ/binary/projection codes -> choose candidates -> exact rerank
```

Best for:

```text
blob storage
local NVMe
vector metrics
LLM/RAG approximate retrieval
predictable I/O
```

Advantages:

```text
sequential reads
SIMD-friendly
blob-friendly
compact
simple compared with graphs
```

Weaknesses:

```text
approximate candidate generation
requires quantization training or calibration
less generic than FlatScan
```

---

### 6.3 HNSW Leaf

A small HNSW index inside each leaf.

```text
selected leaf -> local HNSW traversal -> candidate ids -> exact vector rerank
```

Best for:

```text
local NVMe
warm cache
fast approximate vector search
smaller leaves
```

Advantages:

```text
excellent candidate generation
well-known algorithm
fast in memory or warm cache
```

Weaknesses:

```text
not naturally blob-friendly
random graph access
higher memory/page-cache pressure
not exact unless followed by full scan or exact local bounds
harder to support uncommon vector metrics efficiently
```

---

### 6.4 VamanaPQ Leaf

A Vamana/DiskANN-like local graph packed into storage pages, combined with compressed vector codes and exact rerank.

```text
selected leaf
-> choose local entrypoint
-> page-packed graph traversal using compressed codes
-> collect candidates
-> fetch exact vector blocks
-> exact rerank
```

Best for:

```text
read-optimized leaves
local NVMe
blob storage with page-group reads
large vector datasets
low-RAM approximate search
```

Advantages:

```text
disk-oriented graph structure
better suited to external storage than naive HNSW
can reduce candidate work dramatically
works well with exact rerank
compatible with LSM-style compaction
```

Weaknesses:

```text
more complex to implement
requires careful page layout
not exact by itself
bad layout can cause too many random reads
expensive to rebuild on every insert if not handled by compaction
```

Recommended optimized default:

```text
leaf_engine="vamana_pq"
```

---

### 6.5 Custom Vector Leaf

A future local index for user-defined vector metrics, backed by typed vector
inputs and metric-specific lower-bound summaries.

Examples:

```text
specialized distribution-vector bounds
specialized binary-vector bounds
domain-specific dense-vector transforms
```

Best for:

```text
custom vector metrics
exact or bounded vector search
domain-specific pruning
```

Advantages:

```text
can support exact vector pruning
keeps plugin inputs typed as vectors
can optimize for domain-specific vector layouts
```

Weaknesses:

```text
can degrade in high dimensions
may be slower than vector-specific ANN
requires explicit lower-bound contracts
```

---

## 7. Recommended Default Strategy

BORSUK should use different leaf engines at different lifecycle levels.

```text
L0: fresh mutable/append-friendly leaves
    engine: FlatScan or PQScan

L1: recently compacted leaves
    engine: PQScan or VamanaPQ

L2: stable read-optimized leaves
    engine: VamanaPQ
```

This avoids expensive online graph mutation.

Insert path:

```text
insert batch
-> write small L0 leaf with FlatScan/PQScan
-> publish new manifest
-> searchable immediately
-> background compaction merges L0 leaves
-> builds optimized VamanaPQ L1/L2 leaves
-> publishes new manifest
```

Query path:

```text
query
-> search L0 leaves with scan
-> search L1/L2 leaves with VamanaPQ
-> merge candidates
-> exact rerank
```

This is probably the most practical design.

---

## 8. VamanaPQ Leaf Design

The VamanaPQ leaf is the most important optimized leaf engine.

### 8.1 Goals

It should:

```text
avoid full leaf scans in approximate mode
avoid tiny random reads
use compressed codes during graph traversal
fetch exact vectors only for final rerank
work from local files or blob storage
support page/cache-friendly execution
```

### 8.2 Physical Layout

Recommended VamanaPQ leaf layout:

```text
leaf_000123.parquet
├── header
│   ├── metric
│   ├── dimension
│   ├── count
│   ├── leaf level: L1/L2
│   ├── medoid/radius
│   ├── pivot bounds
│   ├── graph entrypoints
│   ├── block offsets
│   └── checksums
│
├── routing_mini_header
│   ├── representative vectors/codes
│   ├── representative -> graph page mapping
│   └── optional local centroid table
│
├── graph_page_groups
│   ├── page_group_0
│   ├── page_group_1
│   └── ...
│
├── compressed_codes
│   ├── PQ/SQ/residual/binary codes
│
├── exact_vector_blocks
│   ├── vector_block_0
│   ├── vector_block_1
│   └── ...
│
└── ids_and_vectors
```

### 8.3 Graph Page Contents

Each graph page or page group should contain:

```text
node ids
neighbor adjacency lists
compressed vector codes for nodes
optional norms or precomputed terms
local offsets
```

The key design rule:

```text
one graph page read should enable multiple graph expansion steps
```

Bad:

```text
read one node adjacency per range GET
```

Good:

```text
read a page/page-group containing many nearby graph nodes and their neighbor lists
```

### 8.4 Page Size

For local NVMe:

```text
graph page:   4–64 KB
vector block: 1–8 MB
```

For blob storage:

```text
graph page group: 256 KB – 2 MB
vector block:     8–32 MB
```

Blob storage should not fetch 4 KB graph pages. The latency would dominate.

---

## 9. VamanaPQ Leaf Search Algorithm

Pseudo-code:

```python
def search_leaf_vamana_pq(query, leaf, top_m, beam_width):
    # 1. Choose local graph entrypoint.
    entrypoints = leaf.routing_mini_header.entrypoints
    entry = choose_best_entrypoint(query, entrypoints)

    # 2. Initialize local beam.
    beam = PriorityQueue()
    visited = BitSet()
    beam.push(entry)

    # 3. Traverse page-packed graph.
    while not beam.finished():
        batch = beam.pop_unexpanded(beam_width)

        # Group nodes by graph page/page group.
        page_ids = group_by_graph_page(batch)

        # Read graph pages from file/blob/cache.
        pages = read_graph_pages(leaf, page_ids)

        for node in pages.nodes:
            if visited.contains(node.id):
                continue
            visited.add(node.id)

            # Use compressed code, not full vector, during traversal.
            d_approx = approx_distance(query, node.code)
            beam.update(node.id, d_approx)

            for nb in node.neighbors:
                if not visited.contains(nb):
                    beam.add_candidate(nb)

    # 4. Take candidate IDs.
    candidates = beam.best(top_m)

    # 5. Group candidate IDs by exact vector block.
    vector_blocks = group_by_vector_block(candidates)

    # 6. Fetch exact vectors and rerank.
    vectors = read_vector_blocks(leaf, vector_blocks)
    return exact_rerank(query, candidates, vectors)
```

Critical point:

```text
compressed codes are used for traversal
exact vectors are used only for final rerank
```

---

## 10. Global Search with Multiple Leaf Modes

Global BORSUK search internals:

```python
def global_search(query, k, mode, params):
    # 1. Compute query-to-pivot or query-to-representative distances.
    q_summary = compute_query_summary(query)

    # 2. Rank leaves using global summaries.
    leaf_queue = rank_leaves(q_summary)

    # 3. Search selected leaves.
    candidates = TopK(k)

    while should_continue(leaf_queue, candidates, mode, params):
        leaf = leaf_queue.pop_best()

        if leaf.engine == "flat":
            local = search_leaf_flat(query, leaf, params)
        elif leaf.engine == "pq_scan":
            local = search_leaf_pq(query, leaf, params)
        elif leaf.engine == "hnsw":
            local = search_leaf_hnsw(query, leaf, params)
        elif leaf.engine == "vamana_pq":
            local = search_leaf_vamana_pq(query, leaf, params)
        elif leaf.engine == "metric_tree":
            local = search_leaf_metric_tree(query, leaf, params)
        else:
            raise UnsupportedLeafEngine()

        candidates.merge(local)

    return candidates.top_k()
```

---

## 11. Search Modes

BORSUK should expose multiple modes because no single mode is best for all workloads.

### 11.1 Approx Mode

Fastest mode.

```text
router selects limited number of leaves
leaf engine returns approximate candidates
exact rerank on returned candidates
```

Typical parameters:

```text
max_leaves
beam_width
leaf_ef / search_list_size
rerank_count
time_budget_ms
```

Example API:

```python
idx.search_ids(
    query,
    k=20,
    mode="approx",
    max_leaves=32,
    beam_width=64,
    rerank=200,
)
```

Best for:

```text
LLM/RAG
agent memory
semantic document search
large-scale retrieval where 0.95–0.99 recall is enough
```

---

### 11.2 Bounded Approx Mode

A conservative approximate mode.

The search uses more leaves and/or a safer stopping condition than pure approximate mode.

Example API:

```python
idx.search_ids(
    query,
    k=20,
    mode="bounded",
    eps=0.05,
    max_leaves=128,
    rerank=1000,
)
```

Meaning:

```text
try to return results close to exact
visit more leaves than approximate mode
use lower-bound pruning where available
stop earlier than full exact mode
```

This is a good production default for quality-sensitive RAG.

---

### 11.3 Exact Mode

Exact mode should return the true top-k according to the index metric.

Important:

```text
Vamana/HNSW leaf traversal is not exact by itself.
```

For true exact mode, BORSUK must either:

```text
scan all vectors inside every leaf that cannot be safely skipped
```

or use:

```text
an exact local leaf engine with valid metric lower bounds
```

Exact search can still use VamanaPQ as a warm-up:

```text
1. VamanaPQ quickly finds good candidate distances.
2. Better candidate distance improves global pruning.
3. Exact phase scans only leaves that cannot be skipped.
```

Exact mode path:

```text
global router
-> use lower bounds to skip impossible leaves
-> for remaining leaves, scan exact or exact-local-index path
-> exact rerank
```

Example API:

```python
idx.search_ids(
    query,
    k=20,
    mode="exact",
)
```

Warning:

```text
exact mode can degrade to broad scanning in high-dimensional or adversarial data
```

---

### 11.4 Time-Budget Mode

Useful for LLM pipelines.

```python
idx.search_ids(
    query,
    k=20,
    mode="time_budget",
    time_budget_ms=750,
    min_rerank=100,
)
```

Behavior:

```text
search as much as possible within the budget
return best candidates found so far
include telemetry about visited leaves and approximate confidence
```

Best for:

```text
RAG with total response time around 10–30 seconds
interactive search
agent systems
```

---

### 11.5 Hybrid Mode

Hybrid mode combines multiple leaf engines.

Example:

```text
L0 leaves: Flat/PQ scan
L1/L2 leaves: VamanaPQ
small leaves: FlatScan
large leaves: VamanaPQ
custom vector metric leaves: CustomVectorLeaf
```

This should probably be the internal default.

---

## 12. How VamanaPQ Speeds Things Up

Assume a selected leaf contains 100k vectors of 768 dimensions.

Raw leaf size:

```text
100k * 768 * 4 bytes = ~307 MB
```

A full exact scan of that leaf is expensive. A compressed scan may read far less, but still scores many objects.

With VamanaPQ:

```text
visit maybe 500–3000 graph nodes
read a few graph page groups
use compressed codes for approximate distance
fetch exact vectors only for final candidates
```

Expected per-selected-leaf behavior, very roughly:

```text
Flat exact scan:        high bytes, exact
PQ/SQ compressed scan:  medium bytes, predictable
VamanaPQ:              lower candidate work, lower CPU, fewer exact vector reads
```

Local NVMe approximate expectations per selected leaf:

```text
compressed scan leaf: 2–15 ms
VamanaPQ leaf:        0.5–5 ms
exact scan leaf:      10–100+ ms
```

Blob storage expectations depend mostly on range read count:

```text
bad graph layout:  dozens/hundreds of range GETs -> terrible
page-packed graph: 1–5 range GETs per leaf -> acceptable
```

---

## 13. Storage Overhead of VamanaPQ

If each node stores 32 neighbors and neighbor IDs are 4 bytes:

```text
32 * 4 = 128 bytes / vector
```

For 100M vectors:

```text
100M * 128 bytes = 12.8 GB adjacency only
```

With metadata, page alignment, levels, and codes:

```text
20–40 GB graph overhead is plausible
```

For comparison, raw 100M × 768D float32 vectors are:

```text
100M * 768 * 4 = 307.2 GB
```

So the graph overhead is acceptable for a disk/blob-first design.

---

## 14. RAM Model With VamanaPQ

VamanaPQ should not require the full graph in RAM.

Permanent RAM:

```text
manifest
pivots/global representatives
leaf summaries
routing metadata
hot page cache
active query buffers
```

For 100M vectors:

```text
base RAM:        300 MB – 1 GB
recommended:    1–4 GB
fast cache mode: 4–16 GB
```

The key rule remains:

```text
do not keep per-vector graph or per-vector sketches globally in RAM
```

Only active graph pages and hot cache entries should be resident.

---

## 15. Blob Storage Considerations

BORSUK is not S3-only. It should target blob/object storage generally:

```text
S3
GCS
Azure Blob
MinIO
local filesystem
NVMe
network filesystems where reasonable
```

For blob storage, design around:

```text
large immutable objects
range reads
few requests per query
local NVMe read-through cache
page groups, not tiny pages
```

Bad design:

```text
one range GET per graph node
```

Good design:

```text
one range GET per graph page group
one range GET per exact vector block group
```

Recommended blob sizes:

```text
graph page group: 256 KB – 2 MB
vector block:     8–32 MB
leaf object:      tens to hundreds of MB
```

---

## 16. Local Filesystem / NVMe Considerations

For local files, BORSUK can be much faster.

Local mode can use:

```text
pread
mmap optional
io_uring optional
OS page cache
explicit block cache
```

But the same logical layout should work for both local and blob storage.

Local recommended sizes:

```text
graph page:   4–64 KB
vector block: 1–8 MB
leaf size:    50k–200k vectors
```

---

## 17. Insert and Compaction Strategy

Do not dynamically update Vamana graphs on every insert.

Use LSM-like levels:

```text
L0: fresh append leaves
L1: compacted medium leaves
L2: stable optimized leaves
```

### Insert Flow

```text
1. receive batch
2. compute metric-specific summaries/sketches
3. write L0 leaf with FlatScan/PQScan
4. update manifest atomically
5. query can see new data immediately
```

### Compaction Flow

```text
1. select several L0/L1 leaves
2. merge vectors
3. repartition if needed
4. build optimized summaries
5. build VamanaPQ graph
6. write new L1/L2 leaves
7. publish new manifest
8. delete old leaves later through garbage collection
```

This keeps inserts fast and moves graph-building cost to background compaction.

---

## 18. API Sketch

### Create Index

```python
import borsuk

idx = borsuk.create(
    uri="file:///data/docs-index",
    metric="cosine",
    dim=768,
    leaf_size=100_000,
    default_leaf_engine="vamana_pq",
    ram_budget="1GB",
)
```

### Open From Blob Storage

```python
idx = borsuk.open(
    uri="s3://my-bucket/indexes/docs-index",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
    max_concurrent_reads=32,
)
```

### Insert

```python
idx.add(vectors, ids=ids)
```

Internally this writes L0 leaves first.

### Approx Search

```python
ids = idx.search_ids(
    query,
    k=20,
    mode="approx",
    max_leaves=32,
    beam_width=64,
    rerank=200,
)
```

### Bounded Search

```python
ids = idx.search_ids(
    query,
    k=20,
    mode="bounded",
    eps=0.05,
    max_leaves=128,
    rerank=1000,
)
```

### Exact Search

```python
ids = idx.search_ids(
    query,
    k=20,
    mode="exact",
)
```

### Time-Budget Search

```python
ids = idx.search_ids(
    query,
    k=20,
    mode="time_budget",
    time_budget_ms=750,
)
```

### Per-query Engine Override

```python
ids = idx.search_ids(
    query,
    k=20,
    mode="approx",
    leaf_engine="pq_scan",
)
```

---

## 19. Rust Trait Design

Core abstraction:

```rust
trait LeafIndex {
    fn search_candidates(
        &self,
        query: &[f32],
        params: &LeafSearchParams,
        io: &dyn BlockReader,
    ) -> Result<Vec<Candidate>>;
}
```

Possible implementations:

```rust
struct FlatScanLeaf;
struct PQScanLeaf;
struct HnswLeaf;
struct VamanaPqLeaf;
struct CustomVectorLeaf;
```

Storage abstraction:

```rust
trait BlockReader {
    fn read_range(&self, object: &ObjectId, offset: u64, len: u64) -> Result<Bytes>;
}
```

Backends:

```text
LocalFileReader
S3Reader
GcsReader
AzureBlobReader
MinioReader
CachedReader
```

---

## 20. Recommended MVP Roadmap

### MVP 1: Segment Router + FlatScan

Build the skeleton first.

```text
manifest
segments
global summaries
FlatScan leaves
exact rerank
local file backend
```

Goal:

```text
prove low-RAM querying of millions of vectors
prove API and storage format
establish correctness baseline
```

### MVP 2: PQ/SQ Compressed Scan

Add compressed candidate generation.

```text
PQ/SQ codes
compressed scan
exact vector rerank
blob-friendly range reads
```

Goal:

```text
reduce bytes/query
make blob mode practical
```

### MVP 3: LSM Inserts

Add L0 leaves and compaction.

```text
append-only L0
manifest versioning
background compaction
```

Goal:

```text
fast inserts
stable read performance
```

### MVP 4: VamanaPQ Leaf

Add optimized read-heavy local graph leaves.

```text
page-packed graph
compressed traversal codes
exact rerank
local + blob page cache
```

Goal:

```text
speed up approximate search
reduce selected-leaf cost
compete with disk ANN designs while keeping blob compatibility
```

### MVP 5: Custom Vector Leaf

```text
metric-specific vector lower-bound pruning
typed vector plugin ABI
exact local vector search where possible
```

Goal:

```text
support custom vector metrics beyond the built-in dense/binary/distribution set
```

---

## 21. Benchmark Plan

Measure each mode separately.

### Required Metrics

```text
latency p50/p95/p99
recall@k
exact top-k correctness
GETs/query or read calls/query
bytes/query
segments visited/query
leaf pages read/query
exact vector blocks read/query
RAM usage
cache hit rate
insert throughput
compaction amplification
```

### Test Modes

```text
FlatScan exact
PQScan approximate
VamanaPQ approximate
VamanaPQ bounded
Exact mode with warm-up
Time-budget mode
```

### Dataset Sizes

```text
1M vectors
10M vectors
100M vectors
```

### Dimensions

```text
384D
768D
1536D
```

### Backends

```text
local NVMe
local HDD if desired
S3 Standard cold
S3 Standard with local NVMe cache
S3 Express optional
MinIO local/network
```

---

## 22. Expected Behavior

### Local NVMe

Approximate VamanaPQ mode should be significantly faster than scanning large leaves.

Rough target for 100M vectors, 768D:

```text
RAM:                    1–4 GB
storage:                ~350–450 GB
approx p50 latency:     15–80 ms
approx p95 latency:     50–200 ms
bounded p95 latency:    100–500 ms
exact latency:          highly data-dependent, often hundreds ms to seconds
```

### Blob/S3 Cold

Blob cold search is latency dominated.

Rough target:

```text
approx p50 latency:     300 ms – 1.5 s
bounded p95 latency:    1–4 s
exact latency:          often seconds or worse
```

### Blob/S3 With Local Cache

Warm cache can approach local behavior for hot leaves.

```text
hot leaves: near local NVMe
cold leaves: blob latency
mixed workloads: depends on cache hit rate
```

---

## 23. Important Warnings

### VamanaPQ Does Not Make Exact Search Free

Vamana-like graph traversal is approximate candidate generation. Exactness still requires either:

```text
full scan of necessary leaves
```

or:

```text
exact local metric index with valid lower bounds
```

### Bad Page Layout Can Kill Performance

A graph algorithm designed for RAM can perform badly on blob storage if every edge traversal becomes a remote read.

BORSUK must optimize:

```text
page grouping
node ordering
cache locality
range-read batching
candidate block grouping
```

### Inserts Should Not Mutate Graphs In Place

Fast inserts require L0 append leaves. Optimized graphs should be built during compaction.

### Curse of Dimensionality Still Exists

Exact high-dimensional search can degrade to scanning many leaves. BORSUK does not eliminate this. It gives a low-RAM, storage-friendly way to degrade more gracefully.

---

## 24. Final Recommended Architecture

The best current BORSUK design is:

```text
BORSUK
├── global low-RAM router
│   ├── manifest
│   ├── pivots / representatives
│   ├── segment summaries
│   └── optional segment overlay graph
│
├── L0 leaves
│   ├── FlatScan or PQScan
│   └── fast append/searchable immediately
│
├── L1/L2 leaves
│   ├── VamanaPQ page-packed local graph
│   ├── compressed traversal codes
│   ├── exact vector blocks
│   └── record ids
│
├── query modes
│   ├── approx
│   ├── bounded
│   ├── exact
│   ├── time_budget
│   └── hybrid
│
└── storage backends
    ├── local files / NVMe
    ├── S3
    ├── GCS
    ├── Azure Blob
    └── MinIO
```

The most important product message:

```text
BORSUK is not trying to be the fastest RAM ANN index.
It is a low-RAM retrieval engine for huge indexes living in files or blobs.
```

The most important technical message:

```text
BORSUK should use global segment routing plus pluggable local leaf engines.
The optimized default should be VamanaPQ leaves built during compaction.
```

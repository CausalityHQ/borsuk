# BORSUK: Low-RAM Similarity Search on Local Files and Blob Storage

**Working name:** BORSUK
**Expansion:** **Blob-Oriented Retrieval with Segmental Unified KNN**
**Tagline:** *Digs through blobs, not RAM.*

BORSUK is a proposed Rust library with native Python and TypeScript bindings for low-RAM similarity search over large vector or metric datasets stored primarily on local files, NVMe, S3, GCS, Azure Blob, MinIO, SeaweedFS, or other S3-compatible/blob/object storage systems.

The core idea is not to build another RAM-heavy vector database. The goal is a library that can query millions to billions of vectors while keeping only a small routing layer in memory and storing almost all data and index structures externally.

---

## 1. Motivation

Most high-performance ANN systems assume that a large part of the index lives in RAM, or that the storage layer behaves like a local SSD. This is not ideal for workloads where:

- vectors are large,
- the index is much larger than RAM,
- data should live durably on cheap storage,
- query latency around hundreds of milliseconds or even about one second is acceptable,
- the system is used for LLM/RAG retrieval where total response time may be 10–30 seconds,
- inserts should be fast and not require expensive in-place graph mutation,
- local file mode and blob-storage mode should use the same conceptual index layout.

For LLM/RAG, a retrieval latency of 500 ms to 1.5 s is often acceptable because the full pipeline may look like:

```text
retrieval:       0.5–1.5 s
reranking:       0.2–1.0 s
LLM generation: 10–30 s
```

So the design should optimize for:

```text
low RAM
cheap storage
append-friendly writes
local-file and blob-storage compatibility
reasonable real-time query latency
exact mode where possible
approximate mode where desired
```

---

## 2. Core Assumptions

### 2.1 One physical index per metric

The metric is fixed when the index is created or when data is inserted. At runtime, a query uses that already-defined metric.

This means BORSUK should not try to support arbitrary runtime metric switching inside one physical index.

Correct model:

```text
raw objects/vectors can be shared
but index structures are metric-specific
```

Example layout:

```text
objects/
  shard-000001.parquet
  shard-000002.parquet

indexes/
  cosine_v1/
    manifest...
    segments...

  l2_v1/
    manifest...
    segments...

  edit_distance_v1/
    manifest...
    segments...
```

So “multi-metric” should mean:

```text
the library supports many metrics
not one index supports many metrics at runtime
```

### 2.2 Blob storage is not a disk

S3/GCS/Azure Blob/MinIO should not be treated like a normal mmap-able disk.

Bad pattern for blob storage:

```text
query -> read tiny graph node -> read tiny neighbor list -> read tiny vector -> repeat
```

Good pattern:

```text
query -> score segment summaries -> fetch promising segment blocks -> search locally inside fetched blocks
```

The main I/O unit should be the **segment**, not the individual vector or graph node.

---

## 3. High-Level Architecture

BORSUK uses immutable external segments plus a small in-memory routing layer.

```text
RAM:
  manifest
  metric metadata
  global pivots / routers
  segment summaries
  query frontier
  small optional cache

Disk / blob storage:
  vector-level sketches
  local graphs
  raw vectors or payload references
  immutable segments
  delta segments
  compaction outputs
```

The design target is:

```text
RAM = O(number_of_segments + number_of_pivots + active_queries + cache_budget)
```

Not:

```text
RAM = O(number_of_vectors)
```

---

## 4. Storage Layout

BORSUK durable storage should be binary and efficient. All persistent index
tables except `CURRENT` must be Arrow-compatible Parquet: manifests, segment
summaries, vector payloads, vector sketches, routing tables, pivot/router
tables, and local graph blocks. Arrow is the in-memory and FFI data model;
Parquet is the durable file/object format. The only tiny special object is
`CURRENT`, which is a fixed binary pointer to the current manifest version and
checksum.

JSON, ad-hoc bincode blobs, and custom `.kseg` files are not durable index
formats. They may be used only for tests or debugging fixtures, not for the
published storage layout.

### 4.1 Format decision

Decision for BORSUK's use case:

```text
in-memory / FFI boundary: Apache Arrow arrays, schemas, and record batches
durable local/blob output: Apache Parquet files using those Arrow schemas
tiny pointer file: fixed binary CURRENT record
RPC/control plane, if needed later: optional Protobuf messages
streaming ingest logs, if needed later: optional Avro object container files
```

The critical distinction is that BORSUK is a table/segment scanner, not an
RPC/message store. Query performance depends on reading the right columns and
row groups from large immutable segment objects, not on decoding many small
records one at a time.

Why Arrow + Parquet is the best fit:

- Arrow gives BORSUK a language-independent columnar memory model for Rust,
  Python, and TypeScript/Node native bindings. It is the right shape for
  vectors, routing summaries, graph edges, and batched FFI without inventing a
  BORSUK-specific binary envelope.
- Parquet is column-oriented and optimized for efficient storage and retrieval,
  compression, column projection, row-group statistics, and blob/object-store
  range reads.
- Avro is compact and schema-evolution friendly, but it is row-oriented and
  better suited to streaming/event data than vector/index segment scans. It
  must not be the canonical vector, graph, routing, manifest, or payload
  format.
- Protobuf is compact and good for RPC/control messages, but it is message
  oriented and does not provide the column projection, row-group metadata, or
  analytics interoperability needed for large vector/index tables. It also
  assumes whole messages can be loaded at once, which is a poor match for large
  multidimensional numeric data. It may be useful later for a remote control
  plane, not for persisted index data or FFI data transfer.

So the practical rule is:

```text
Arrow schema first.
Persist tables as Parquet.
Use Arrow arrays at the FFI boundary.
Do not use Arrow IPC/Feather as the canonical durable index format.
Do not persist vector/index data as Avro or Protobuf.
Do not use JSON or a Rust CLI subprocess as the Python/TypeScript data plane.
```

Arrow IPC/Feather is useful for local interchange, tests, and diagnostics, but
it is not the persistent index format. BORSUK needs Parquet's compressed column
chunks, row groups, footers, statistics, projection, and object-store-friendly
range-read behavior for large segment and routing tables.

FFI and persistence should therefore use the same Arrow schemas but different
physical representations:

```text
Python/TypeScript/Rust batch data in process -> Arrow-compatible arrays/buffers
durable local/S3-compatible objects          -> Parquet files
tiny active-version pointer                  -> fixed binary CURRENT
```

This avoids a Rust CLI bridge, avoids JSON-over-stdio, and keeps Python and
TypeScript as native API surfaces over the Rust core.

Python should load BORSUK through the PyO3/maturin native extension. TypeScript
should load BORSUK through the N-API native addon. Both bindings should call
coarse Rust operations such as create/open/add/search/compact/GC, pass vector
data as contiguous numeric buffers or memory views where practical, and grow
toward Arrow-compatible record batches for bulk APIs. They should not call a
Rust CLI process, exchange JSON with a subprocess, or add Avro/Protobuf as an
FFI payload format.

Published index output is Parquet. Query output can be native language objects
for scalar calls today and Arrow-compatible record batches for bulk calls later.
The CLI may print JSON for administrator convenience, but that JSON is not a
persisted storage format and not the Python/TypeScript runtime transport.

The Parquet layout is intentionally table-oriented:

| Table | File pattern | Purpose |
|---|---|---|
| Current pointer | `CURRENT` | Fixed-size binary pointer to active manifest version/checksum |
| Manifest | `manifests/manifest-*.parquet` | Version metadata, index config, active segment list |
| Segment summaries | `routing/segments-*.parquet` | Segment-level routing rows kept in RAM after open |
| Pivots/router | `routing/pivots-*.parquet` | Global pivots and routing metadata |
| Segment vectors | `segments/L*/xx/seg-*.parquet` | Immutable vector records, payload refs, sketches |
| Local graph blocks | `graphs/L*/xx/graph-*.parquet` | Segment-local ANN graph/posting data |
| Object payload shards | `objects/shard-*.parquet` | Optional object payloads shared across metric-specific indexes |

Vector columns should use Arrow `FixedSizeList<Float32>` when dimensions are
fixed, or typed binary/list columns for non-vector metric spaces. This keeps the
format usable from Rust, Python, and TypeScript tooling without decoding a
BORSUK-specific binary envelope.

Example local layout:

```text
/data/borsuk-index/
  CURRENT
  manifests/
    manifest-000001.parquet
    manifest-000002.parquet
  routing/
    segments-000001.parquet
    pivots-000001.parquet
  segments/
    L0/
      ab/seg-000001.parquet
    L1/
      91/seg-000120.parquet
    L2/
      f0/seg-000900.parquet
  graphs/
    L1/
      91/graph-000120.parquet
  objects/
    shard-000001.parquet
    shard-000002.parquet
```

Example blob layout:

```text
s3://bucket/borsuk/
  objects/
    shard-000001.parquet
    shard-000002.parquet
  indexes/
    docs_cosine_v1/
      CURRENT
      manifests/
        manifest-000001.parquet
        manifest-000002.parquet
      routing/
        segments-000001.parquet
        pivots-000001.parquet
      segments/
        L0/ab/seg-000001.parquet
        L1/91/seg-000120.parquet
        L2/f0/seg-000900.parquet
      graphs/
        L1/91/graph-000120.parquet
```

Use hashed prefixes for object storage to avoid concentrating requests into a single prefix.

---

## 5. Segment Format

Each segment is an immutable Parquet object or file.

A segment should be a small set of Parquet row groups so local reads and blob
range reads are predictable. Row-group statistics and optional bloom filters
should be used for cheap coarse filtering where they are valid for the metric.

```text
Segment Parquet file
├── Arrow schema metadata
├── row group 0
│   ├── object_id
│   ├── vector or metric object
│   ├── payload_ref
│   ├── sketch columns
│   └── local routing code
├── row group N
└── Parquet footer / checksums / column statistics
```

### 5.1 Segment Metadata

Segment metadata lives in Parquet file metadata and in the segment-summary
Parquet table. It contains:

```text
segment_id
metric_id
level: L0/L1/L2
object_count
dim
created_at/version
medoid id or centroid
covering radius
pivot min/max intervals
offsets to internal blocks
checksums
compression info
```

### 5.2 Segment summary table

A compact Parquet table used for initial pruning/ranking. It is loaded at index
open and is the main RAM-resident routing layer.

Possible contents:

```text
medoid/centroid summary
radius
pivot distance intervals
quantized routing code
density estimate
local quality stats
```

### 5.3 Vector sketch columns

Stored externally, not permanently in RAM.

Possible contents per vector row:

```text
object_id
compressed distances to selected pivots
quantized projection codes
optional local cell/routing code
```

Important: do not keep all vector-level sketches in RAM by default.

### 5.4 Local graph table

A graph only inside the segment.

This avoids S3-unfriendly global node-by-node traversal.

The graph should be stored as Parquet rows, for example:

```text
segment_id
object_id
neighbor_ids
neighbor_distances
entry_point_rank
shortcut_level
```

### 5.5 Vector/payload columns

Contains raw vectors or pointers to raw object shards.

For exact search, original vectors or exact reconstructable representations must be available.

For approximate search, quantized vectors may be enough.

---

## 6. Query Algorithm

The query algorithm has two phases:

1. global segment selection,
2. local segment search/rerank.

### 6.1 Exact or safe-pruned search

The previously used word “certificate” should be avoided in API/docs because it sounds confusing. The better terms are:

```text
lower-bound pruning
safe pruning
exact stop condition
bounded approximation
```

The exact mode depends on a lower-bound condition.

For each segment, compute a lower bound on the possible distance from the query to any vector in that segment.

Simple example using medoid and radius:

```text
LB(segment) = max(0, distance(query, medoid) - radius)
```

If current best kth result has distance `best_k`, and:

```text
LB(segment) >= best_k
```

then this segment cannot contain a better result and can be skipped safely.

### 6.2 Approximate mode

Approximate mode can stop earlier using one of several parameters:

```text
eps
lambda
max_segments
max_bytes
max_latency_ms
target_recall
```

Possible stop condition:

```text
LB(segment) >= best_k / lambda
```

or more conventional:

```text
LB(segment) >= best_k / (1 + eps)
```

Practical API should probably expose simple knobs:

```python
idx.search(q, k=10, mode="exact")
idx.search(q, k=10, mode="approx", eps=0.05)
idx.search(q, k=10, mode="approx", max_segments=32)
idx.search(q, k=10, mode="approx", max_latency_ms=800)
```

### 6.3 Query pseudocode

```text
SEARCH(query q, k, mode):
    load manifest and routing metadata from RAM
    compute distances from q to global pivots

    frontier = priority queue of segments ordered by lower bound / score
    best = empty top-k heap

    while frontier is not empty:
        seg = pop best segment candidate

        if mode == exact and LB(seg) >= best kth distance:
            continue or stop if all remaining segments have worse LB

        if mode == approx and stop condition is satisfied:
            break

        fetch segment summary / sketch block if not cached
        choose promising vector candidates inside segment

        optionally run local graph search inside segment
        fetch raw vectors or exact payloads for candidates
        compute exact distances
        update best

    return best
```

---

## 7. Insert Algorithm

Blob/object storage is bad for in-place mutation. Inserts should be append-first and compaction-based.

Use an LSM-like structure:

```text
L0: fresh small immutable delta segments
L1: compacted medium segments
L2: large optimized stable segments
```

### 7.1 Insert flow

```text
INSERT batch:
    compute metric sketches
    create new L0 segment
    write segment as one object/file
    write new manifest version
    atomically update CURRENT pointer
```

### 7.2 Compaction

Compaction rewrites segments out-of-place.

```text
COMPACT:
    pick several L0/L1 segments
    load/build optimized grouping
    rebuild local segment graphs
    write new L1/L2 segments
    publish new manifest
    garbage collect old segments later
```

No segment should be mutated in place.

---

## 8. RAM Estimates

For 100M vectors, RAM should not scale with vector count except through number of segments.

Assume segments of 100k vectors:

```text
100M / 100k = 1000 segments
```

Permanent RAM:

```text
manifest:             10–50 MB
pivots/router:         10–100 MB
segment headers:       50–200 MB
insert metadata:       10–100 MB
query scratch:         10–200 MB depending on concurrency
```

Recommended targets:

```text
minimum usable RAM: 256–512 MB
recommended RAM:   1–2 GB
fast local mode:    4–8 GB
high-QPS server:    8–32 GB cache budget
```

### 8.1 What not to keep in RAM

Do not keep these permanently in RAM:

```text
raw vectors
per-vector graph neighbors
all per-vector pivot sketches
all local graph blocks
all segment vector blocks
```

If you keep per-vector sketches in RAM, memory grows quickly.

Example:

```text
100M vectors * 32 pivot distances * uint16 = 6.4 GB
100M vectors * 64 pivot distances * uint16 = 12.8 GB
```

Still smaller than raw vectors, but no longer “near-zero RAM”.

---

## 9. Storage Estimates

For 100M vectors:

| Vector dimension | Raw vectors | Estimated total with index overhead |
|---:|---:|---:|
| 384D float32 | 153.6 GB | ~180–240 GB |
| 768D float32 | 307.2 GB | ~350–450 GB |
| 1536D float32 | 614.4 GB | ~700–850 GB |

For 1B vectors, multiply approximately by 10.

Exact mode usually requires raw vectors or exact reconstructable data.

Approximate mode can use quantized vectors to reduce storage.

---

## 10. Expected Performance

These are not benchmark results. They are implementation targets / estimates.

### 10.1 Local NVMe, 100M vectors, 768D

Expected with good implementation:

```text
RAM:                    512 MB – 2 GB
storage:                ~350–450 GB
p50 approximate query:  15–50 ms
p95 approximate query:  50–150 ms
exact mode:             100 ms to seconds depending on data distribution
```

Exact mode can degrade badly when lower-bound pruning is weak.

### 10.2 Blob storage cold path

For pure cold S3/blob reads:

```text
RAM:                    1–4 GB
p50 approximate query:  300 ms – 1.5 s
exact mode:             often seconds
```

This is still acceptable for some LLM/RAG workflows.

### 10.3 Blob storage with local NVMe cache

Recommended production mode:

```text
blob storage = durable source of truth
local NVMe = read-through cache
RAM = manifest + routing + small cache
```

Hot queries can approach local NVMe behavior.

Cold queries pay blob-storage latency.

---

## 11. S3 / Blob Storage Cost Model

For 100M 768D vectors, estimated index storage:

```text
~350–450 GB
```

At roughly $0.023/GB/month for S3 Standard:

```text
storage cost ≈ $8–11/month
```

Query request cost is usually more important than storage cost, but still often manageable.

Example:

```text
20 GETs/query
1M queries/month
= 20M GETs/month
≈ $8/month at $0.0004 / 1k GETs
```

For 100 GETs/query:

```text
1M queries/month
= 100M GETs/month
≈ $40/month
```

Approximate total for 100M 768D vectors:

```text
S3 storage only:        ~$10/month
S3 + 1M queries/month:  ~$20–50/month
S3 + 10M queries/month: ~$100–300/month
```

The main blocker is not S3 price. It is latency and bandwidth.

---

## 12. Prior Art and Positioning

BORSUK is not novel because of one isolated ingredient. Most ingredients exist somewhere.

### 12.1 Relevant prior art

| System / family | What it covers | Gap relative to BORSUK |
|---|---|---|
| FAISS | excellent vector ANN/exact toolkit | not blob-storage-first, not generic metric external-tier library |
| HNSW / hnswlib | strong RAM graph ANN | RAM-heavy, node-level graph traversal |
| DiskANN | disk-resident graph ANN | SSD-centric, not object-store segment-first, approximate-focused |
| SPANN | disk ANN with partition/posting style | vector ANN, not generic metric exact/controlled search |
| Annoy | mmap read-only ANN | local-file only, static, approximate |
| NMSLIB | many metrics and spaces | not blob-storage-first, not modern Rust/Python/TypeScript low-RAM external-tier design |
| M-tree / PM-tree | exact metric indexes with pruning | tree/page model, not segment/blob/LSM architecture |
| Milvus | vector database using object storage for persistence | heavy DB, object storage not usually active query path |
| LanceDB / Lance | object-store-friendly vector/data format | not generic metric exact/controlled external-tier index |
| OpenSearch k-NN | production vector search with disk/remote options | heavy system, not lightweight library, limited metric flexibility |
| S3 Vectors | managed S3-native vector search | not OSS, limited metrics, managed service |

### 12.2 Unique OSS niche

BORSUK can occupy a unique niche if positioned as:

```text
Rust/Python/TypeScript low-RAM similarity search library
for local files and blob storage
with one physical index per metric
immutable segment layout
append-friendly inserts
local graph inside segments
safe lower-bound pruning for exact mode where possible
bounded approximate mode for speed
```

It should not be positioned as:

```text
faster than HNSW in RAM
always exact and always fast
magic cure for curse of dimensionality
one index for arbitrary runtime metrics
```

---

## 13. Metric Support

### 13.1 Metric categories

BORSUK should distinguish:

```text
true metrics
vector similarities
custom user-defined distances
non-metric similarities
```

Exact safe pruning requires metric properties, especially triangle inequality.

If the distance is not a true metric, BORSUK can still provide approximate search, but exact pruning may not be valid.

### 13.2 Initial metrics

Good initial metrics:

```text
L2 / Euclidean
cosine distance, with normalized vectors
Manhattan / L1
Hamming
Jaccard distance
Levenshtein/edit distance for strings
```

Potential later:

```text
Wasserstein variants
DTW-like distances, if lower bounds are available
custom metric plugins
```

### 13.3 Lower-bound oracle API

For generic metrics, the best abstraction may be a lower-bound oracle.

```rust
trait Metric<T> {
    fn distance(&self, a: &T, b: &T) -> f32;
}

trait SegmentLowerBound<Q> {
    fn lower_bound(&self, query: &Q, segment_summary: &SegmentSummary) -> f32;
}
```

This lets different metrics define different pruning logic.

---

## 14. API Sketch

The public language APIs must be native bindings over the Rust core, not shell
wrappers around a command-line program. The CLI can exist for administration,
debugging, and examples, but Python and TypeScript packages must call Rust
through FFI/native extension boundaries. The CLI is never the runtime transport
for package APIs.

### 14.1 Binding architecture

```text
crates/borsuk/            Rust core library
crates/borsuk-python/     PyO3 extension module, built with maturin
crates/borsuk-node/       N-API extension module for Node/TypeScript
crates/borsuk-cli/        optional admin/debug CLI, not used by bindings
python/                  Python packaging/docs/tests around the PyO3 module
packages/borsuk/          TypeScript package and generated types around N-API
```

Binding rules:

```text
no subprocess-based query path
no JSON-over-stdin/stdout API between bindings and Rust
no Python/TypeScript reimplementation of search logic
all search/add/open/create behavior calls Rust core functions directly
errors cross the FFI boundary as typed Python exceptions / JS Error subclasses
vectors cross the FFI boundary as contiguous numeric arrays where practical
future batch APIs should expose Arrow-compatible arrays/record batches where useful
```

The FFI packages should expose the same high-level API shape in each language
while keeping Rust as the implementation source of truth.

FFI calls should be coarse-grained:

```text
good: open index, add a batch, search a batch, compact, gc
bad: one FFI call per vector, graph node, edge, or storage read
```

Python should use PyO3/maturin to build a native extension module. TypeScript
should use a Node native extension through N-API. Both bindings should pass
typed arrays, memory views, or Arrow-compatible batches into Rust and receive
typed result objects back. They should not reimplement indexing, search,
compaction, caching, object-store access, or Parquet decoding.

### 14.2 Python API

Python should be a native extension built with PyO3 and maturin. The Python
package imports a compiled Rust module, for example `_borsuk`, and wraps it only
where Python ergonomics require thin adaptation.

```python
import borsuk

idx = borsuk.create(
    uri="file:///mnt/nvme/docs.borsuk",
    metric="cosine",
    dim=768,
    ram_budget="1GB",
    segment_size="64MB",
)

idx.add(ids, vectors)

hits = idx.search(
    query,
    k=20,
    mode="approx",
    eps=0.05,
)
```

Blob storage mode:

```python
idx = borsuk.open(
    uri="s3://my-bucket/indexes/docs.borsuk",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
    max_concurrent_gets=32,
)

hits = idx.search(query, k=20, mode="approx", max_latency_ms=1000)
```

Exact mode:

```python
hits = idx.search(query, k=10, mode="exact")
```

Budgeted approximate mode:

```python
hits = idx.search(
    query,
    k=10,
    mode="approx",
    max_segments=64,
    max_bytes="128MB",
)
```

### 14.3 TypeScript / Node API

The TypeScript package should use N-API, with generated or hand-maintained type
declarations. It must load a native module rather than spawning the CLI.

```ts
import { create } from "borsuk";

const index = await create({
  uri: "s3://my-bucket/indexes/docs.borsuk",
  metric: "cosine",
  dimensions: 768,
  cacheDir: "/mnt/nvme/borsuk-cache",
  ramBudget: "2GB",
  maxConcurrentReads: 32,
});

const hits = await index.search(query, {
  k: 20,
  mode: "approx",
  eps: 0.05,
});
```

### 14.4 Rust API concept

```rust
let index = BorsukIndex::open(
    "s3://bucket/indexes/docs.borsuk",
    OpenOptions {
        ram_budget: ByteSize::gb(2),
        cache_dir: Some("/mnt/nvme/borsuk-cache".into()),
        max_concurrent_reads: 32,
        ..Default::default()
    },
).await?;

let hits = index.search(&query, SearchOptions {
    k: 20,
    mode: SearchMode::Approx { eps: 0.05 },
    ..Default::default()
}).await?;
```

---

## 15. Implementation Plan

### Phase 0: Brute-force external baseline

Build a local-file and blob-storage reader that can scan Parquet vector
segments with limited RAM.

Goal:

```text
prove Parquet/Arrow storage format
prove Python/Rust integration through PyO3 FFI
prove TypeScript/Rust integration through N-API FFI
measure real disk/S3 throughput
```

### Phase 1: Segment summaries and lower-bound pruning

Implement:

```text
Parquet segment vector tables
Parquet manifest tables
Parquet segment-summary routing tables
Parquet pivot tables
medoid/radius summaries
exact search with safe pruning
```

Benchmark against brute force.

### Phase 2: Vector-level sketches

Add per-vector sketches stored inside segment blocks.

Use them to reduce exact vector reads.

### Phase 3: Local segment graph

Add local ANN graph inside each segment.

The graph is used only after a segment is selected/fetched.

### Phase 4: LSM-style inserts

Implement:

```text
L0 append segments
manifest versioning
background compaction
query over multiple levels
```

### Phase 5: Blob backend and cache

Implement:

```text
object_store or direct cloud SDK backend
range reads
local NVMe read-through cache
concurrency control
retry/backoff
```

### Phase 6: Advanced metrics

Add:

```text
custom metric plugins
string metrics
metric-specific lower-bound summaries
```

---

## 16. Benchmark Plan

Datasets:

```text
SIFT-128
GloVe angular
BEIR embeddings
MSMARCO embeddings
synthetic Gaussian vectors
clustered vectors
adversarial/high-intrinsic-dimensional vectors
string/edit-distance datasets
```

Baselines:

```text
FAISS Flat
FAISS IVF/PQ
hnswlib
Annoy
DiskANN if practical
LanceDB
NMSLIB for non-vector metrics
brute-force external scan
```

Metrics:

```text
recall@k
exact top-k agreement
p50/p95/p99 latency
GETs/query
bytes/query
segments touched/query
RAM usage
cache hit ratio
index build time
insert throughput
compaction amplification
storage overhead
```

Target early success criteria:

```text
100M 768D local NVMe:
  RAM <= 2GB
  p50 approx <= 50ms
  p95 approx <= 150ms

100M 768D blob + cache:
  RAM <= 4GB
  p50 warm approx <= 200ms
  p50 cold approx <= 1.5s

Exact mode:
  100% agreement with brute force on benchmark subsets
```

---

## 17. Key Risks

### 17.1 Weak pruning

If lower-bound pruning is weak, exact mode may scan too many segments.

This is the biggest algorithmic risk.

### 17.2 Curse of dimensionality

No index can magically avoid worst-case high-dimensional exact nearest-neighbor behavior.

The honest promise should be:

```text
graceful degradation
not impossible guarantees
```

### 17.3 Blob latency

Cold blob-storage query latency may be hundreds of milliseconds to seconds.

This is acceptable for some LLM/RAG workflows but not for ultra-low-latency ANN.

### 17.4 Cache complexity

The practical performance will depend heavily on:

```text
local NVMe cache
range-read coalescing
prefetching
concurrency limits
retry/backoff
prefix sharding
```

### 17.5 Metric plugin complexity

Arbitrary metrics are hard.

BORSUK should provide:

```text
many built-in metrics
clear distinction between metric and non-metric distances
custom lower-bound hooks
approx-only fallback for non-metric similarities
```

---

## 18. Naming

Recommended name:

```text
BORSUK = Blob-Oriented Retrieval with Segmental Unified KNN
```

Why it works:

```text
Polish and funny
short
memorable
means mole, so it digs through storage
not S3-specific
works for local files, S3, GCS, Azure Blob, MinIO
acronym is meaningful
```

Possible README line:

```text
BORSUK is a Rust/Python/TypeScript low-RAM similarity-search library for large vector indexes stored on local files, NVMe, or blob/object storage.
```

Taglines:

```text
BORSUK — digs through blobs, not RAM.
BORSUK — big vector search, tiny RAM.
BORSUK — similarity search on external tiers.
```

---

## 19. Final Product Positioning

BORSUK should be positioned as:

```text
A Rust/Python/TypeScript low-RAM similarity-search library for large indexes stored on local files or blob/object storage.
```

Not as:

```text
A replacement for RAM HNSW when you need microsecond latency.
```

Best use cases:

```text
LLM/RAG retrieval
agent memory
large document search
cheap semantic archive search
company knowledge bases
cold/warm vector search
multi-tenant retrieval where RAM cost dominates
local NVMe indexes larger than memory
blob-backed persistent vector stores
```

The key technical thesis:

```text
Use RAM to know where to look.
Use external storage to hold almost everything.
Search segments, not individual remote nodes.
Use exact lower-bound pruning when possible.
Use approximate budgeted search when speed matters.
```

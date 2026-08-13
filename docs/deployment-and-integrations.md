# Deployment and Integrations

BORSUK is an embedded vector-search engine. The application process is the
search process; durable index state is a local directory or an object-storage
prefix. A separate database cluster is not required. Compute is not eliminated:
the client process performs routing, TurboQuant shortlist scoring, sidecar range
reads, and exact reranking.

## Production topology

```text
application process (Rust, Python, or TypeScript)
  ├─ resident serving metadata prepared during open
  ├─ bounded searches: 4 admitted queries, 24 active cell decodes
  ├─ bounded workers: 4 CPU workers + 24 small-stack I/O waiters
  ├─ graph-free SRHT-rotated product-PQ scan + exact rerank
  ├─ optional read-through cache on local NVMe
  └─ durable index at file://... or s3://bucket/prefix
```

New indexes are `pq-scan-only` by default and do not build graph objects.
Approximate searches default to the TurboQuant `pq-scan` leaf when no leaf mode
is supplied. Exact mode remains exact and ignores the approximate leaf choice.
The experimental graph modes require an explicitly `graph-enabled` index.

## Local disk

Python:

```python
import borsuk

index = borsuk.create(
    uri="file:///var/lib/my-app/borsuk/products",
    metric="cosine",
    dimensions=768,
)
index.add(vectors, ids=ids, metadata=metadata)
report = index.search_with_report(
    query,
    k=10,
    mode="approx",
    max_segments=32,
    max_candidates_per_segment=32,
)
```

TypeScript:

```ts
import { create } from "borsuk";

const index = await create({
  uri: "file:///var/lib/my-app/borsuk/products",
  metric: "cosine",
  dimensions: 768,
});
await index.add(vectors, { ids, metadata });
const report = await index.searchWithReport(query, {
  k: 10,
  mode: "approx",
  maxSegments: 32,
  maxCandidatesPerSegment: 32,
});
```

Rust:

```rust
use borsuk::{BorsukIndex, IndexConfig, LeafMode, SearchOptions, VectorMetric,
             recommended_segment_max_vectors};

let mut index = BorsukIndex::create(IndexConfig {
    uri: "file:///var/lib/my-app/borsuk/products".into(),
    metric: VectorMetric::Cosine,
    dimensions: 768,
    segment_max_vectors: recommended_segment_max_vectors(768),
    ram_budget_bytes: Some(borsuk::DEFAULT_RAM_BUDGET_BYTES),
    text: false,
    named_vectors: Default::default(),
})?;
let report = index.search_with_report(
    &query,
    SearchOptions::approx(10, LeafMode::PqScan)
        .with_max_segments(32)
        .with_max_candidates_per_segment(32),
)?;
```

The runnable API tours are
[`python/examples/local_index.py`](../python/examples/local_index.py),
[`packages/borsuk/examples/local-index.ts`](../packages/borsuk/examples/local-index.ts),
and [`crates/borsuk/examples/local_index.rs`](../crates/borsuk/examples/local_index.rs).
Those tours explicitly enable graphs to demonstrate every leaf API; production
creation does not.

## AWS S3 and local cache

Credentials, region, retry settings, and role-based authentication come from
the standard AWS environment/provider chain. Put the bucket and the application
in the same Region unless cross-region latency and transfer charges are
intentional.

```python
index = borsuk.open(
    "s3://my-bucket/indexes/products",
    cache_dir="/mnt/nvme/borsuk-cache",
    cache_max_bytes=20 * 1024**3,
)

# Serving metadata is already prepared. This uncached request may fetch cells
# from S3; repeating the same working set should use disk and report zero
# backing-store GETs.
report = index.search_with_report(query, k=10, mode="approx")
```

The cache is disposable and checksum-validated. `CURRENT` is still read from
the backing store so a process observes the active snapshot. Do not describe
the first query as “cold” if open/startup work has already completed: use
`uncached` for absent query-cell data and `disk_cached` for a repeated working
set with zero measured backing GETs.

Run the maintained S3 examples with a unique test prefix:

```bash
export BORSUK_S3_TEST_URI=s3://my-bucket/borsuk-smoke
cargo run --locked -p borsuk --example s3_index
python python/examples/s3_index.py
(cd packages/borsuk && npm run example:s3)
```

See the checked-in source for
[Rust](../crates/borsuk/examples/s3_index.rs),
[Python](../python/examples/s3_index.py), and
[TypeScript](../packages/borsuk/examples/s3-index.ts).

## S3-compatible endpoints

MinIO, SeaweedFS, and compatible stores use the same `s3://` URI and storage
layout. Configure the endpoint and path-style behavior through the object-store
AWS variables:

```bash
export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=borsuk
export AWS_SECRET_ACCESS_KEY=borsuk-secret
export AWS_ALLOW_HTTP=true
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes
```

The repository includes complete [MinIO](../examples/minio/README.md) and
[SeaweedFS](../examples/seaweedfs/README.md) smoke stacks. They exercise the
same Rust, Python, and TypeScript paths, not a mock transport.

## LangChain and LCEL

The Python adapter implements a LangChain `VectorStore`, so the resulting
retriever can be placed in LCEL chains or LangGraph nodes:

```python
from borsuk.compat.langchain import BorsukVectorStore
from langchain_openai import OpenAIEmbeddings

store = BorsukVectorStore.from_texts(
    chunks,
    OpenAIEmbeddings(),
    uri="s3://my-bucket/indexes/rag",
)
retriever = store.as_retriever(search_kwargs={"k": 4})
```

The complete LCEL chain is
[`examples/rag/langchain_rag.py`](../examples/rag/langchain_rag.py), with setup
and an offline alternative in [`examples/rag/README.md`](../examples/rag/README.md).

## Compatibility adapters

Adapters help migrate call shapes; they are not unconditional binary or
behavioral drop-ins. Python ships Pinecone, Amazon S3 Vectors, turbopuffer,
Chroma, Qdrant, and LangChain adapters. TypeScript ships Pinecone, Amazon S3
Vectors, and turbopuffer adapters. Unsupported control-plane operations,
consistency differences, and filter translations are listed in
[`drop-in.md`](drop-in.md). Validate your exact methods and error semantics
before switching production traffic.

## Choosing cells and concurrency

The v8 default physical segment size is dimension-aware: it targets roughly
16 MiB of lossless float32 vectors and clamps the result to 64–131,072 rows. This
keeps a 100M-vector index to thousands rather than tens of thousands of routing
and sidecar objects. Search does not retain those full scan cells: routed
global-PQ code chunks are consumed in fixed 32-chunk waves, and only bounded
top candidates survive into exact rerank. Tune layout only with full-corpus
recall and the same cache/concurrency state used in production.

`prefetch_depth` defaults to a per-query requested width of 16. The handle-wide decode gate
still caps all queries at 24 active cell decodes, and the search-admission gate
admits four queries by default. Keep both caps. The uncapped multi-user profile
exists only to measure the research ceiling and must not be copied into a
production deployment.

Keep CPU and I/O concurrency separate. `BORSUK_CPU_THREADS=4` is the default
compute ceiling. `BORSUK_IO_THREADS=32` provides one shared set of 1 MiB-stack
waiters for S3 reads; it does not permit extra scoring work or bypass the
24-read/decode gate. Raise either only from a matched-recall concurrency run
that includes peak CPU, RSS/VMS, and tail latency.

Measured defaults, recall/latency curves, CPU/RAM/disk graphs, cost formulas,
and uncapped scaling live in [`research/`](research/README.md). The concise
operating contract is [`benchmarks.md`](benchmarks.md).

# BORSUK

**Vector search that lives in your bucket.**

*BORSUK — **B**lob-**O**riented **R**etrieval with **S**egmental **U**nified **K**NN.*

[![CI](https://github.com/CausalityHQ/borsuk/actions/workflows/ci.yml/badge.svg)](https://github.com/CausalityHQ/borsuk/actions/workflows/ci.yml)
[![Pages](https://github.com/CausalityHQ/borsuk/actions/workflows/pages.yml/badge.svg)](https://github.com/CausalityHQ/borsuk/actions/workflows/pages.yml)
![License](https://img.shields.io/badge/license-BUSL--1.1-blue)
![Python](https://img.shields.io/badge/python-3.12%2B-3776AB)
![Node](https://img.shields.io/badge/node-22%20%7C%2024%20%7C%2026-339933)

BORSUK keeps your entire vector index as immutable Parquet objects in the S3,
MinIO, SeaweedFS, GCS, or Azure storage you already pay for — and answers a query
with a few hundred bytes of resident memory. There is no always-on RAM cluster to
provision, scale, or feed between queries. It's a Rust library with first-class
Python and TypeScript bindings, and a **drop-in replacement** for Pinecone,
turbopuffer, Amazon S3 Vectors, Chroma, and Qdrant.

- 🪣 **The index is the bucket.** Vectors, sketches, graphs, and routing pages are
  Parquet objects fetched on demand and dropped after use — not a resident arena.
- 🧠 **Near-zero RAM.** Paged routing resolves the few segments a query needs from
  binary routing pages; a million-vector index and a hundred-vector index have
  nearly the same footprint (~hundreds of bytes).
- 🔎 **Metadata + filtered search.** Attach schemaless metadata to any vector and
  filter with a Pinecone-style operator dictionary; selective filters skip whole
  segments they can't match.
- 💸 **You mostly just pay for storage.** No per-vector service fee — your object
  store plus whatever compute you point at it.
- 📊 **Everything is measured.** Every query returns a report of bytes read, cache
  behavior, resident memory, and why it stopped.

```mermaid
flowchart LR
  app["Rust · Python · TypeScript"] --> api["typed vector API"]
  api --> route["binary routing layers"]
  route --> seg["Parquet vector leaves"]
  route --> edges["Parquet leaf graph blocks"]
  seg --> rerank["exact metric rerank"]
  edges --> rerank
  rerank --> out["ids · vectors · SearchReport"]
```

> **Where it fits:** BORSUK is in the object-storage-native family alongside
> turbopuffer, Pinecone Serverless, and S3 Vectors — its centroid-and-radius
> "bubble" tree with LSM compaction shares the SPFresh/SPANN research lineage.
> Its niche is the **lowest resident memory and storage cost**. Because the whole
> index lives in the bucket and resident memory stays near zero at any size, there
> is no RAM ceiling on how many vectors you hold — it scales to whatever your object
> store can store. It's slower cold than an in-RAM engine, but competitive with the
> other object-storage systems, and a local NVMe cache makes warm reads single-digit
> milliseconds — and in a real pipeline the read usually overlaps an
> embedding/LLM/guardrail step anyway.
> Full comparison and references in the
> [web docs](http://causality.pl/borsuk/docs.html#landscape).

## Quick start

Three lines: create an index, add vectors, search. That's it.

**Python**

```python
import borsuk

index = borsuk.create(uri="file:///tmp/my-index", metric="cosine", dimensions=768)
index.add(embeddings, ids=["doc-1", "doc-2", "doc-3"])

index.search_ids(query, k=5)   # → ['doc-2', 'doc-1', ...]
```

**TypeScript**

```ts
import { create } from "borsuk";

const index = await create({ uri: "file:///tmp/my-index", metric: "cosine", dimensions: 768 });
await index.add(embeddings, ["doc-1", "doc-2", "doc-3"]);

await index.searchIds(query, { k: 5 });   // → ['doc-2', 'doc-1', ...]
```

**Rust**

```rust
use borsuk::{BorsukIndex, IndexConfig, SearchOptions, VectorMetric};

let mut index = BorsukIndex::create(IndexConfig {
    uri: "file:///tmp/my-index".into(),
    metric: VectorMetric::Cosine,
    dimensions: 768,
    segment_max_vectors: 1024,
    ram_budget_bytes: None,
    sparse: false,
    text: false,
})?;
index.add_vectors_with_ids(embeddings, vec!["doc-1".into(), "doc-2".into()])?;
let ids = index.search_ids(&query, SearchOptions::exact(5))?;
```

Point `uri` at `s3://bucket/prefix` (AWS S3, MinIO, SeaweedFS, …) and the same
code runs against object storage. Ids are optional — omit them on `add` and you
get generated ids back. Want the vectors too? Use `search_vectors` /
`searchVectors`. Want the full I/O and timing report? `search_with_report`.

## Filtered search

Attach metadata when you add, then filter any search with a Pinecone-style query.
Matches are found *before* ranking, so a selective filter is fast and exact.

```python
index.add(
    embeddings,
    ids=["song-1", "song-2"],
    metadata=[{"genre": "rock", "year": 1975}, {"genre": "jazz", "year": 1999}],
)

index.search_ids(query, k=10, filter={"genre": "rock", "year": {"$gte": 1970}})
# → ['song-1']
```

Full operator reference: [`docs/api.md`](docs/api.md#metadata-and-filtered-search).

## Distance metrics

Pick the metric at create time: `metric="cosine"` (or `"euclidean"`, `"dot"` via
`"inner-product"`, `"minkowski:3"`, …). Over 30 are built in — the Lp family
(`euclidean`, `manhattan`, `chebyshev`, `minkowski:p`, `gower`), angle/dot
(`cosine`, `angular`, `inner-product`, `correlation`), abundance
(`bray-curtis`, `canberra`, …), set/binary (`jaccard`, `dice`, `hamming`, …), and
distribution (`jensen-shannon`, `hellinger`, `kullback-leibler`, …) metrics. Every
one returns a distance, so search always keeps the *k* smallest.

**One tradeoff worth knowing:** the Lp-family metrics satisfy the triangle
inequality, so BORSUK can *prove* a segment holds nothing closer and skip it
unread — that is what makes exact search over a huge index touch only a few
segments. Other metrics still work and approximate latency is similar, but exact
and recall-guaranteed searches scan every candidate. Prefer an Lp metric for
exact search at scale. Full equations and tradeoffs:
[`docs/api.md`](docs/api.md#distance-metrics).

## Drop-in replacements

Change the import, point at a bucket, keep your upsert / query / filter calls.
Each namespace (or collection, or S3 index) becomes its own BORSUK index.

```python
# before: from pinecone import Pinecone; pc = Pinecone(api_key="…")
from borsuk.compat.pinecone import Pinecone
pc = Pinecone(base_uri="file:///data/vectors", dimension=768, metric="cosine")

index = pc.Index("products")
index.upsert([("a", embedding, {"genre": "rock"})], namespace="store-1")
index.query(vector=embedding, top_k=10,
            filter={"genre": {"$eq": "rock"}}, include_metadata=True, namespace="store-1")
```

Five adapters ship: **Pinecone**, **Amazon S3 Vectors**, and **turbopuffer**
(Python + TypeScript under `borsuk/compat/{pinecone,s3vectors,turbopuffer}`), plus
**Chroma** and **Qdrant** (Python under `borsuk.compat.{chroma,qdrant}`). Native
`$`-operator filters pass straight through; turbopuffer and Qdrant filter dialects
are translated. Full reference and honest limits: [`docs/drop-in.md`](docs/drop-in.md).

## ELI5 Intuition

Think of the index as many sealed boxes of vectors, stored on disk or in S3. RAM
keeps a small *map*, not every vector. A query reads the top of the map, walks
down to the few boxes that look relevant, opens only those, and exact-reranks the
candidates it found.

"Map plus boxes" is the beginner picture. At scale that map is not one flat list;
it is a **computed multi-level routing tree** — a root routing index, parent
routing pages when needed, then L0 routing pages, then bounded vector and graph
blobs. You tune the tree width with `routing_page_fanout` and query safety with
`routing_page_overfetch`; BORSUK computes the depth during publish and compaction
from the actual leaf count.
Single-level routing is only the small-index degenerate case of that same algorithm.

Writes stay fast because new vectors go into fresh L0 boxes. Compaction is the
cleanup after a delivery rush: it groups nearby vectors into read-optimized leaves
and rebuilds small graph blocks. BORSUK does not promise magic recall from a tiny
budget — exact search can cover the whole index, and approximate search is a
controlled tradeoff whose `SearchReport.recall_guarantee` tells you whether the
result was `exact`, `budget-complete`, or `degraded`.

How it actually picks which boxes to read (the lower bound, the provably-safe
stop) is walked through — with a live 3D demo — in the
[web docs](http://causality.pl/borsuk/docs.html#math).

## Updates and deletes

Deletes are soft. `delete(ids)` records the ids in a cumulative **tombstone** that
is filtered out of every search and `get_vector` immediately; the search report
never claims deleted rows. Tombstoned rows are reclaimed lazily by the next
compaction, or on demand with `purge`, which rewrites the affected segments and
clears the tombstone. Until an id is purged, re-adding it is rejected as a
duplicate. There is no in-place value update — to change a vector, delete it,
`purge`, and add the new value.

For a wholesale dataset replacement, use `rebuild` to rebuild the live records
into a fresh index, then let garbage collection remove the superseded objects.
`borsuk gc --delete` (the garbage-collection cleanup step) only removes objects that no retained
manifest version references. Background incremental maintenance keeps the layout
healthy as you mutate — oversized segments split, sparse ones merge — and that
work is sharded across nodes. Full contract:
[`docs/api.md`](docs/api.md#deletion).

## Performance evidence

Every performance claim is backed by a checked-in artifact under
`docs/web/assets/benchmarks/`, rendered as interactive charts on the
[web docs](http://causality.pl/borsuk/docs.html#performance). Highlights from the
one-million-vector gate: `1.000000` recall@10, ~hundreds of bytes resident, a
few object reads per query, single-digit-millisecond warm p95. The same gate runs
against a real S3-compatible store to measure network overhead. Reproduce or
extend:

```bash
cargo run --locked --release -p borsuk --example benchmark_report -- \
  --queries 100 --parallelism 1,2,4,8 --artifacts-dir /tmp/borsuk-bench
```

See [`docs/benchmarks.md`](docs/benchmarks.md) for the full matrix, the
million-vector large-scale gate, and the filtering / sparsity sweeps.

## Documentation

- **Web docs (interactive):** <http://causality.pl/borsuk/>
- **API reference:** [`docs/api.md`](docs/api.md)
- **Drop-in replacements:** [`docs/drop-in.md`](docs/drop-in.md)
- **Architecture:** [`docs/architecture.md`](docs/architecture.md)
- **Storage format:** [`docs/storage-format.md`](docs/storage-format.md)
- **Benchmarks:** [`docs/benchmarks.md`](docs/benchmarks.md)

Runnable examples live in
[`crates/borsuk/examples`](crates/borsuk/examples),
[`python/examples`](python/examples),
[`packages/borsuk/examples`](packages/borsuk/examples), and a local S3 stack in
[`examples/seaweedfs`](examples/seaweedfs/README.md).

**Building a RAG app?** [`examples/rag`](examples/rag/README.md) is a complete,
one-file retrieval-augmented chatbot over a BORSUK index — ingest your `.md`/`.txt`
files (local or S3), retrieve, and answer with an LLM. It runs offline as a demo
and upgrades to real embeddings with an `OPENAI_API_KEY`.

## Object storage

Use `s3://bucket/prefix` for AWS S3, MinIO, SeaweedFS, and other S3-compatible
stores; endpoint and credentials come from standard object-store/AWS environment
variables. Pass a local `cache_dir` (NVMe) to keep fetched immutable objects hot:

```python
idx = borsuk.open(
    "s3://my-bucket/indexes/docs-index",
    cache_dir="/mnt/nvme/borsuk-cache",
    ram_budget="2GB",
)
```

`CURRENT` (the one small binary pointer to the active version) is always read
from the backing store; content-addressed objects are cached read-through and
checksum-validated. Full concurrency, retry, multipart, and GC-retention caveats
are in [`docs/storage-format.md`](docs/storage-format.md#s3-assumptions-and-caveats).

## Packages

CI builds and tests the Python package on Python 3.12, 3.13, and 3.14, and the
TypeScript/Node package on Node 22, 24, and 26. Every build runs across
`Linux x64, Linux arm64, Windows x64, macOS arm64, and macOS Intel`. BORSUK is
young software under active development; pin a specific build and confirm it
against its benchmark artifacts before you depend on it.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace --all-targets
cargo package --locked -p borsuk --allow-dirty
cargo bench --locked --workspace --no-run
(cd python && uvx maturin build --locked --out dist)
wheel="$(ls -t python/dist/borsuk-*.whl | head -1)"
BORSUK_WHEEL_PATH="$wheel" uv run --with "./$wheel" python -m unittest discover python/tests
(cd packages/borsuk && npm ci && npm run build:native && npm test)
```

Format and lint every language before committing:

```bash
cargo fmt --all
uvx ruff check --fix python examples scripts && uvx ruff format python examples scripts
npx prettier@3 --write "packages/borsuk/**/*.ts" "docs/web/*.js" "scripts/*.mjs"
```

Install the git hooks so these run automatically — `pre-commit` formats and lints,
`pre-push` runs Clippy, type checks, and compiles every test target:

```bash
scripts/install-hooks.sh
```

## License

BORSUK is licensed under the Business Source License 1.1 with a revenue-limited
Additional Use Grant: free production use unless your company, organization, and
affiliates make over US $100,000/year. See [LICENSE](LICENSE).

# V156 owned graph generation: local format v1

This contract describes the first authenticated load of an immutable graph
generation. It is a pre-release format and may be replaced before production
qualification. The caller supplies the SHA-256 of `manifest.json` from an
authorized, conditionally updated generation pointer. A digest supplied by an
untrusted query or file listing is not an authority.

## Manifest and objects

`manifest.json` uses `borsuk-graph-generation-v1` and rejects unknown fields.
It declares `generation`, `metric: "l2"`, `normalization: "none"`, `rows`,
`dimensions`, `page_rows: 256`, `unit_rows: 32`, `blocks_per_page`, source
verification block bytes, `max_nominees`, source and physical-layout SHA-256s,
and the SQ8 object's key, ETag, SHA-256 and exact byte length. The SQ8 key is
`objects/<sq8_sha256>`. These fields are generation-wide identities; they are
not selected by a dataset name or row-count threshold.

`artifacts` declares an exact byte length and SHA-256 for each fixed local
path:

| Role | Path | Further binding |
| --- | --- | --- |
| Source-only PQ64 router | `router/manifest.json` | Nested manifest binds generation, source/layout/SQ8 hashes and section lengths/digests for `summaries.bin`, `books.bin`, `codes.bin`, `low.bin`, `step.bin`. |
| f16 centroid blob | `centroids.bin` | `BORSUCP1` header binds geometry before f32 decode. |
| Graph adjacency | `graph.bin` | `BORSUKG1` header binds centroid digest and fixed graph parameters; decoded structure must match declared resident bytes. |
| Exact f32 source tier | `source.bin` | `BORSST02` binds generation, source hash, row count and dimensions. |
| Source-ID map | `id_map.bin` | `BORSMAP1` binds the exact source-tier artifact hash and generation. |
| SQ8 page authority | `page_manifest.json`, `page_digests.bin` | Manifest and sidecar bind generation, SQ8 hash and every physical page digest. |

The loader authenticates the small top-level manifest, computes the checked
known-payload envelope and rejects an insufficient operator cap before large
decodes. It then checks the nested router geometry and section lengths before
router arrays are read, and the centroid header before f32 arrays are made.
Artifact reads are bounded by their declared lengths and SHA-256. The graph
preflight scans the authenticated adjacency without allocating its nested
vectors; its exact structural byte count must equal the manifest claim.
The returned `Arc<GraphServingGeneration>` owns all decoded planes and exposes
only shared references. Its source-ranking method computes exact squared L2
within a caller-supplied candidate union, with ascending scores and stable ID
tie breaks.

The resource cap models known array payload and loading overlap. A caller must
also reserve enough per-query transient memory for the verified source block,
row/query buffers, PQ table, page roster and candidate copies. The loader
rejects a declared transient allowance below that geometry-derived floor.
Allocator overhead, process RSS and charged cgroup memory still require
measurement and operational headroom. An atomic reservation ledger is needed
before concurrent generation swaps or query permits can be enforced.

## Qualification boundary

This load path does not publish the trusted pointer, fetch SQ8 pages from S3,
verify a live `If-Match` response, merge mutation deltas, enforce transport
caps or guarantee recall. The paired V153/V154/V155 used cohorts remain
historical evidence for their recorded source revisions. Fresh paired
returned-quality, live-S3 latency, charged RAM and 10M/100M gates must run
from a frozen revision after the query path and mutation semantics are wired.

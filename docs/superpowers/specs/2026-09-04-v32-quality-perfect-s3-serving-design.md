# V32 Quality-Perfect S3 Serving Design

## Decision

V32 restores the authenticated row-PQ candidate router that reached 320/320
Recall@10 with 16 pages, and removes the rejected page-centroid router from the
pre-release production path. It then optimizes serving around the proven
quality boundary. S3 Standard remains the durable authority; an optional
same-AZ S3 Express directory-bucket replica is the low-latency serving tier.
No serving process downloads or persists the corpus.

## Frozen quality boundary

The governing 100,000-row Deep Image development terminal is source
`2bce312c1bc7759efc1e540e2787750775ff85e8`, SHA-256
`f7ca28d37e1fe1d2cc08790d7155980bdeede8b6ce8fd78faf8635373ca2641f`.
Its route uses root beam 8, leaf beam 64, candidate depth 12,288, a 24-byte
PQ8 base code with query-independent 5-percent 48-byte refinement, exactly 16
unique pages, and exact reranking after page decode. It reached 320/320 hits,
32/32 perfect queries, at most 33,001 scanned codes and 2,928,808 page bytes.

The single-page-centroid route reached only 273/320 and is deleted rather than
retained behind a mode or compatibility alias. The claim-ineligible V31
residual experiments remain historical evidence only.

## Pre-release format

V32 replaces the experimental V30 manifest with one schema version and no
legacy reader. The canonical JSON manifest binds the source commit, dataset,
and exact identities of every resident and page artifact. Resident roots,
leaves, and PQ codebooks use Arrow IPC; leaf/page ranges and refinement ranks
use Parquet. Page bodies remain non-null Arrow IPC rows containing an eight-byte
source ordinal and fixed-size-list `float32[96]` vector.

The serving-location table is a strict Parquet artifact with one row per page:
`page_ordinal:uint32`, `sha256:string`, `encoded_bytes:uint64`,
`standard_uri:string`, and nullable `express_uri:string`. A copied Express
object must be byte-identical to the Standard object and retain the same
SHA-256 and length. The reader accepts exactly one selected tier per request;
it never silently falls back across tiers because that would make latency and
availability evidence ambiguous.

## Query path

1. Normalize one finite 96-dimensional query.
2. Score eight roots, then the leaves beneath them, retaining exactly 64.
3. Scan no more than 1,000,000 authenticated PQ codes and retain the best
   12,288 rows with deterministic `(score, source ordinal)` ties.
4. Reduce the ranked rows to exactly 16 unique physical pages.
5. Issue all page reads concurrently through one persistent async client and
   connection pool. Responses remain reference-counted byte buffers; no
   `Bytes -> Vec<u8>` copy is allowed before Arrow validation.
6. Validate length, SHA-256, Arrow schema, row counts, and source ordinals;
   exact-rerank the decoded rows and return ten deterministic matches.

The process may cache only bounded page bodies under an explicit byte limit.
The cache is optional acceleration and never authority. Resident metadata plus
cache must stay below 3 GiB. Full-corpus staging, local corpus paths, discovery
of latest objects, D3 access, and query-derived construction are absent.

## Latency and throughput

Standard S3 cold latency and compute latency are separate products. The
authenticated Standard result measured 144,065,141 ns cold p99, 74,808,007 ns
process CPU p99, 8,185,812 ns maximum routing elapsed, and 11,159,727 ns
maximum exact-rerank elapsed. V32 does not relabel that result as 15 ms.

Before every scientific run a metadata-only simulator consumes an injected
request-p99 and aggregate-throughput profile and computes
`routing + request_p99 + ceil(bytes / throughput) + decode_rerank`. It rejects
any arm whose lower-bound projection misses its tier gate. The simulator is a
fail-fast estimate; only a same-AZ measured run can pass a release gate.

The targets are:

- 1,000,000-ppm aggregate and minimum Recall@10 and 32/32 perfect development
  queries;
- exactly 16 page selections and no more than 3,145,728 fetched bytes;
- hot/local-page p99 at most 15 ms;
- same-AZ S3 Express end-to-end p99 at most 15 ms;
- Standard S3 cold p99 at most 150 ms, reported separately;
- process CPU p99 at most 64 ms, corresponding to at least 1,000 single-query
  QPS of CPU capacity on 64 vCPUs before network saturation;
- projected 100-million-row resident bytes at most 3 GiB and observed RSS at
  most 3 GiB.

## Qualification order

All code changes use narrow synthetic RED/GREEN tests first. A 16-object
same-AZ S3 Express microbenchmark follows using only already-selected 100K page
objects; it is not a corpus copy. Then one 100K end-to-end Spot run verifies
quality and timing. Only a passing implementation proceeds to the disjoint
9.99-million-row cohort and then a 100-million-row construction/serving run.
Each campaign uses `causality` Spot, writes canonical JSON plus typed Parquet
evidence, terminates immediately, and keeps D3 and competitor claims fenced.


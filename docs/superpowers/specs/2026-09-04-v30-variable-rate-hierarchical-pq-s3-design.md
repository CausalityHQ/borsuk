# V30 Variable-Rate Hierarchical PQ S3 Index Design

## Decision

V30 replaces the experimental V28 routing format with one query-independent,
variable-rate hierarchical IVF-PQ index. Exact vectors and IDs remain only in
immutable Arrow pages in S3. Serving retains a two-level centroid hierarchy,
packed residual codes, strict leaf/page offsets, and bounded scratch in memory;
it scans at most one percent of the compact code plane, fetches exactly ten
pages in one concurrent wave, and exact-reranks only those decoded rows.

This is a pre-release replacement. There is no V28/V29 reader, alias, migration
path, dual writer, or format negotiation.

## Evidence and causal boundary

On the burned 100,000-row Deep Image fixture, fixed page prototypes, a
65,536-leaf incidence router, page graphs, OPQ, additive PQ, multiview PQ,
radius routing, wider page counts, sparse secondary placement, and small f16
sidecars all failed. They are immutable negative evidence, not fallback paths.

V28's query-independent 24-byte residual code reached 318/320 hits. Upgrading
five percent of rows to a 48-byte residual code reached 319/320, 996,875-ppm
aggregate recall, 900,000-ppm minimum recall, and 31/32 perfect queries with a
2,625,266,208-byte 100-million-row projection. An exact-distance control over
the same bounded hierarchical candidates reached 320/320. The remaining loss
is therefore compressed ordering inside a successful frontier, not a need to
scan the corpus or fetch hundreds of pages.

The single burned miss is not a license for query-specific tuning. V30 freezes
the smallest passing variable-rate arm and evaluates it on a larger untouched
cohort. Perfect Recall@10 remains visible as a stretch result. It is not a
formal guarantee: guaranteed exactness would require a separately exposed
adaptive/exhaustive mode whose worst case cannot share the fast-path SLO.

## High-dimensional strategy

The hierarchy and residual quantizer have separate jobs:

1. 1,024 normalized f16 roots and 65,536 normalized f16 leaves remove broad
   angular variation and constrain the query to nearby regions.
2. Every normalized row is assigned to exactly one leaf without query, truth,
   evaluation, or prior-result access.
3. A global 48-by-2D, 16-centroid PQ4 codebook computes a 24-byte base
   residual code and its reconstruction error for every row during
   construction. The persisted base-code plane contains only the 95 percent
   of rows assigned base fidelity.
4. A global 96-by-1D, 16-centroid PQ4 codebook persists a 48-byte
   high-fidelity residual code instead of the base code for the five percent
   of rows with greatest base-code
   squared reconstruction error. Selection orders rows by
   `(error.total_cmp reversed, source_ordinal)` and takes exactly
   `floor(source_rows * 50_000 / 1_000_000)` rows. It is corpus-only and fixed
   before any query object is available.
5. Query ADC subtracts the selected leaf centroid and dispatches by the stored
   row fidelity bit. Each width's SIMD table builder returns an explicit
   `(minimum_sum, scale)` calibration. Candidate comparison uses
   `minimum_sum + scale * u16_score` in the common f32 squared-distance domain;
   comparing raw scores from the two widths is forbidden. Scalar f32 ADC is
   the differential oracle for the calibrated optimized path.
6. Page ownership remains implicit in leaf-local logical code order. A
   fidelity bitmap with rank checkpoints maps each logical position to exactly
   one compact base or high plane. A bounded heap retains at most 12,288
   candidates and selects the first ten unique pages by
   `(approximate_distance, logical_code_position, page_ordinal)`. Source IDs
   are deliberately unavailable until authenticated Arrow pages are decoded.
7. Exact normalized f32 vectors from the ten S3 pages determine the final
   `(distance, source_ordinal)` top ten.

This is not exhaustive search disguised as hierarchy. Production rejects more
than 1,000,000 scanned rows, a corpus-sized score allocation, more than 12,288
retained candidates, more than ten pages, or more than 4,587,520 page bytes.

## Query-independent construction

The construction process receives only the registered corpus manifest, ordered
Parquet training shards, scratch, and output capability. It cannot access test
queries, neighbor truth, prior results, or page-read credentials.

A deterministic hash sample trains roots, leaves, and both PQ codebooks. One
ordered corpus stream normalizes each row, assigns its leaf, computes the base
and high-fidelity residual encodings plus base reconstruction error, and spills
fixed records under an explicit memory limit. A bounded external selection pass
finds the exact five-percent error cutoff with source ordinal as the tie break.
The merge emits logical rows in
`(leaf_ordinal, base_code, fidelity_desc, high_code, source_ordinal)` order.
It writes each row to exactly one compact code plane: 24 base bytes for a base
row or 48 high bytes for a high row. Base bytes are absent for high rows and
high bytes are absent for base rows; neither plane contains zero-filled
placeholders. The base code remains a transient construction sort key for a
high row and is not persisted.

Each leaf is split into pages of at most 1,024 rows. Every source ordinal has
one owner and the primary union equals the corpus authority. The same merge
order emits code blocks, fidelity bits/rank offsets, leaf ranges, page offsets,
and Arrow page bodies, so no resident row-to-page array is needed.

## Persistent cross-language format

Scientific tables use Parquet and typed serving artifacts use Arrow IPC:

- `roots.arrow`: non-null `centroid: fixed-list<element:f16>[96]`;
- `leaves.arrow`: the same centroid plus non-null `root_ordinal:u16`;
- `pq24-codebook.arrow`: width 24 and non-null f32 centroid payload;
- `pq48-codebook.arrow`: width 48 and non-null f32 centroid payload;
- `pq-base-codes.arrow`: 32-row transposed fixed-binary base blocks for the
  exact 95-percent base population;
- `pq-fidelity.arrow`: non-null fidelity bitmap plus monotone u64 rank offsets;
- `pq-high-codes.arrow`: 32-row transposed fixed-binary 48-byte blocks for the
  exact five-percent high population;
- `leaf-ranges.arrow`: monotone base/high block and page ranges;
- `page-offsets.parquet`: leaf-local row ranges and immutable page identities;
- S3 page bodies: the existing strict Arrow `id` plus f32[96] vector schema;
- manifests, receipts, and results: sorted compact JSON with one trailing LF.

Every artifact binds role, schema version, source commit/archive, dataset/index
identity, row count, byte length, SHA-256, and dependency digests. Readers
authenticate exact bytes before semantic decoding and reject extra fields,
nullability drift, wrong child names, wrong physical types, non-finite values,
offset drift, duplicate ownership, or incomplete source union.

## Serving data flow and S3 boundary

One query normalizes once, scores all roots, scores leaves only beneath the
selected roots, scans only selected leaf code ranges, retains a bounded
candidate heap, reduces candidates to ten unique page identities, and calls
`V30PageStore::read_wave` once. The S3 implementation issues those ten exact
key GETs concurrently with bounded connect/read timeouts and no discovery,
listing, prefix inference, ETag digest, retry after terminal, or page-body
cache hidden from measurement.

Standard S3 cold latency is a separate end-to-end metric. Existing evidence is
approximately 39/65/93 ms p50/p95/p99 for ten concurrent GETs totaling about
2.06 MB. The 15-ms target applies to resident routing plus exact rerank (and to
a separately measured hot-cache path); cold Standard-S3 release qualification
uses 150-ms p99. A future S3 Express tier may target lower cold latency but is
not required or silently substituted.

## Memory and work bounds

The registered 100-million-row resident projection is 2,625,266,208 bytes for
the frozen five-percent arm: the code planes average exactly 25.2 bytes per
row (`95% * 24 + 5% * 48`), and the registered non-code components total
105,266,208 bytes. Persisting both widths for a high row would violate this
authority. The total is below 3,221,225,472 bytes. Runtime additionally
checks component allocations and process peak RSS. It rejects any fidelity
fraction other than 50,000 ppm, any width other than 24/48 bytes, any code scan
above 1,000,000 rows, or any candidate/page/byte bound above the fixed limits.

Construction streams Parquet shards and spills bounded runs; it never retains
the full corpus. Serving keeps no exact corpus locally. Only the selected ten
Arrow page bodies are transiently decoded and released after exact rerank.

## Quality, latency, and release gates

The seconds-long 100K gate is burned development evidence and runs after
affected changes. It requires at least 996,875-ppm aggregate recall, 900,000-ppm
minimum recall, 31/32 perfect queries, exactly ten pages, and the registered
work/memory limits. It is a regression gate, not a claim.

Only after code and authority gates pass may one untouched 9.99-million-row
cohort run on `causality` Spot. It requires:

- at least 995,000-ppm aggregate Recall@10;
- at least 997,500-ppm queries meeting an 800,000-ppm per-query floor;
- at least 800,000-ppm absolute minimum Recall@10;
- exact ten-page and 4,587,520-byte upper bounds;
- at most 1,000,000 scanned codes and 12,288 retained candidates;
- under 3 GiB peak RSS;
- at most 15,000,000-ns CPU/hot-cache p99;
- at most 150,000,000-ns cold Standard-S3 p99;
- deterministic scalar/optimized ordering and exact final rerank equality.

All 100-percent observations are reported explicitly. A missed stretch target
does not override the preregistered release gates. A failed release gate stops
the revision; parameters are not changed after sealed metrics are visible.

## Fast verification and execution fence

Development runs the narrow PQ/layout/search unit selectors first, then the
100K quality gate. Formatting and scoped static checks follow only at coherent
checkpoints. Strict workspace Clippy and the full locked workspace/all-targets
suite run once for a release candidate, never after each small repair.

After a clean fast-forward push, construction and evaluation use disposable
Spot instances with exact S3 prefixes. Progress, RSS, PSI, swap, interruption,
and wall limits are monitored. An interrupted cell is discarded and may be
restarted only under its preregistered rule. Every terminal is uploaded before
immediate instance termination. No 100-million-row build, D3 campaign, or
competitor claim is authorized until the 9.99-million-row gate passes.

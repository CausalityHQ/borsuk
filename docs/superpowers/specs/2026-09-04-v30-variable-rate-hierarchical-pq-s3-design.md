# V30 Variable-Rate Hierarchical PQ S3 Index Design

## Decision

V30 first reproduces the only promising V28 variable-rate result with a
committed, authority-complete evaluator. Only a matching interpretation may
become a query-independent hierarchical IVF-PQ index. Exact vectors and IDs
remain only in immutable Arrow pages in S3. Serving retains a scale-derived
two-level centroid hierarchy, packed residual codes, strict leaf/page offsets,
and bounded scratch in memory; it scans only selected leaf ranges, fetches
exactly ten pages in one concurrent wave, and exact-reranks only those decoded
rows.

This is a pre-release replacement. There is no V28/V29 reader, alias, migration
path, dual writer, or format negotiation.

## Evidence and causal boundary

On the burned 100,000-row Deep Image fixture, fixed page prototypes, a
65,536-leaf incidence router, page graphs, OPQ, additive PQ, multiview PQ,
radius routing, wider page counts, sparse secondary placement, and small f16
sidecars all failed. They are immutable negative evidence, not fallback paths.

V28's archived variable-rate result reports a 24-byte base, a five-percent
refinement fraction, 25.2 average bytes per row, 319/320 hits, 996,875-ppm
aggregate recall, 900,000-ppm minimum recall, and 31/32 perfect queries. An
exact-distance control over the same bounded hierarchical candidates reached
320/320. The result and terminal did not preserve the evaluator or an input
manifest, and the archived `PQ8` label conflicts with the committed V28 PQ4
codec. The 2,625,266,208-byte projection also accounts for exactly 24 extra
bytes per refined row but not a sparse-plane bitmap, ranks, framing, or range
metadata. These numbers are historical evidence, not production authority.

The surrounding archived ladders narrow the intended mechanism: the standalone
24-byte PQ8 residual arm and the variable-rate zero-percent arm both reached
993,750 ppm, while a separate additive-PQ arm regressed to 984,375 ppm and
1.296-second CPU p99. The variable arm adds exactly one 100M-row bitmap at zero
percent and exactly 24 bytes per refined row thereafter. The first gate
therefore reproduces one 24-byte/48-byte PQ8 replacement interpretation on the
same burned fixture. It must match the archived counts without query-dependent
training before production code proceeds.

## High-dimensional strategy

The hierarchy and residual quantizer have separate jobs:

1. The hierarchy size is derived only from corpus rows: leaves are
   `min(65,536, next_power_of_two(ceil(rows / 512)))` and roots are
   `min(1,024, max(1, leaves / 16))`. Thus the 100K/9.99M/100M shapes use
   16/256, 1,024/32,768, and 1,024/65,536 roots/leaves respectively. Training
   rejects fewer than two rows per leaf.
2. Every normalized row is assigned to exactly one leaf without query, truth,
   evaluation, or prior-result access.
3. A global 24-by-4D, 256-centroid PQ8 codebook computes a 24-byte base residual
   code and its reconstruction error for every row during construction. Base
   codes persist only for base-fidelity rows.
4. A global 48-by-2D, 256-centroid PQ8 codebook computes a replacement 48-byte
   code for the five percent of rows with greatest base-code squared
   reconstruction error. Selection orders rows by
   `(error.total_cmp reversed, source_ordinal)` and takes exactly
   `floor(source_rows * 50_000 / 1_000_000)` rows. It is corpus-only and fixed
   before any query object is available.
5. Query ADC subtracts the selected leaf centroid and dispatches through the
   fidelity bitmap. Both PQ8 widths use 256-entry lookup tables and return
   approximate squared distance in one f32 domain. The optimized and scalar
   paths use the same table values and reduction order; raw width-specific
   integer scores are never cross-compared.
6. Page ownership remains implicit in leaf-local logical code order. A
   fidelity bitmap with rank checkpoints maps each logical position to exactly
   one compact base or high-fidelity row. A bounded heap retains at most 12,288
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

A deterministic hash sample trains roots, leaves, and both PQ8 codebooks. One
ordered corpus stream normalizes each row, assigns its leaf, computes the base
and high-fidelity residual encodings plus base reconstruction error, and spills
fixed records under an explicit memory limit. A bounded external selection pass
finds the exact five-percent error cutoff with source ordinal as the tie break.
The merge emits logical rows in
`(leaf_ordinal, base_code, high_fidelity_desc, high_code, source_ordinal)`
order. It persists a 24-byte base code or a replacement 48-byte high code,
never both and never a zero-filled placeholder. The transient base code remains
the layout key for high-fidelity rows so changing fidelity does not change page
membership.

Each leaf is split into pages of at most 512 rows. Every source ordinal has
one owner and the primary union equals the corpus authority. The same merge
order emits code blocks, fidelity bits/rank offsets, leaf ranges, page offsets,
and Arrow page bodies, so no resident row-to-page array is needed.

## Persistent cross-language format

Scientific tables use Parquet and typed serving artifacts use Arrow IPC:

- `roots.arrow`: non-null `centroid: fixed-list<element:f16>[96]`;
- `leaves.arrow`: the same centroid plus non-null `root_ordinal:u16`;
- `pq24-codebook.arrow`: width 24 and non-null f32 centroid payload;
- `pq48-codebook.arrow`: width 48 and non-null f32 centroid payload;
- `pq-base-codes.arrow`: 32-row transposed fixed-binary 24-byte blocks for the
  exact 95-percent base population;
- `pq-fidelity.arrow`: non-null fidelity bitmap plus monotone u64 rank
  checkpoints;
- `pq-high-codes.arrow`: compact 32-row transposed fixed-binary 48-byte blocks
  for the exact five-percent high population;
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

Standard S3 cold latency is a separate end-to-end metric. Before a paid gate,
the reduced harness injects request-latency distributions and reports the
decomposition without sleeping; this is a planning estimate, not evidence.
The 15-ms target applies to resident routing plus exact rerank. The untouched
Spot gate must measure ten concurrent Standard-S3 GETs and requires 100-ms p99
with a hard 150-ms ceiling. No unregistered cache or S3 Express substitution is
allowed.

## Memory and work bounds

The archived projection is not the V30 authority. It already includes the
12,500,000-byte fidelity bitmap. V30 provisionally projects 2,630,588,896 bytes
at 100 million rows: the archived 2,625,266,208 bytes plus 3,125,000 rank bytes,
1,048,576 leaf plane-range bytes, a second 98,304-byte PQ8 codebook, at most
2,232 bytes of global block padding, and a 1,048,576-byte Arrow framing reserve.
Both code planes are globally packed and leaf ranges may start inside a block;
per-leaf block padding is forbidden. The total is below 3,221,225,472 bytes. Runtime additionally
checks component allocations and process peak RSS. It rejects any fidelity
fraction other than 50,000 ppm, any width other than 24/48 bytes, any code scan
above 1,000,000 rows, or any candidate/page/byte bound above the fixed limits.

Construction streams Parquet shards and spills bounded runs; it never retains
the full corpus. Serving keeps no exact corpus locally. Only the selected ten
Arrow page bodies are transiently decoded and released after exact rerank.

## Quality, latency, and release gates

The first seconds-long 100K gate is a reproduction falsifier. It fixes the
24-byte/48-byte PQ8 replacement interpretation with identical construction,
hierarchy, pages, queries, and truth across 0/5/10/20-percent diagnostic arms.
The five-percent candidate must exactly reproduce at least 319/320 hits, 900,000-ppm
minimum recall, 31/32 perfect queries, and ten pages. The winning interpretation
and every input/output identity are then frozen; later per-edit gates rerun only
that arm. This burned evidence is not a claim.

Only after reproduction, code, and authority gates pass may one untouched
9.99-million-row cohort run on `causality` Spot. Its query ordinals must be
registered and disjoint from every burned cohort before construction. It requires:

- at least 995,000-ppm aggregate Recall@10;
- at least 997,500-ppm queries meeting an 800,000-ppm per-query floor;
- at least 800,000-ppm absolute minimum Recall@10;
- exact ten-page and 4,587,520-byte upper bounds;
- at most 1,000,000 scanned codes and 12,288 retained candidates;
- under 3 GiB peak RSS;
- at most 15,000,000-ns CPU/hot-cache p99;
- at most 100,000,000-ns cold Standard-S3 p99 and no sample above
  150,000,000 ns;
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

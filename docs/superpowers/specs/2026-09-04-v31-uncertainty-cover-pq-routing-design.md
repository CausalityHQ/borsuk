# V31 Uncertainty-Cover PQ Routing Design

## Decision

V31 replaces the rejected single-page-centroid production router with a
bounded row-PQ router that selects ten primary pages by approximate distance
and six disjoint uncertainty pages by a conservative reconstruction-error
lower bound. Exact vectors and identifiers remain only in immutable Arrow page
objects in S3. Construction streams Parquet corpus shards on disposable
`causality` Spot workers and never materializes the corpus on the devbox.

This is a pre-release replacement. V31 has one format and one reader; it does
not retain the V30 page-centroid schema, compatibility aliases, or fallback
dispatch.

## Evidence and causal boundary

The reproduced variable-rate 24/48-byte PQ8 route reached 319/320 hits,
996,875-ppm aggregate recall, 900,000-ppm minimum recall, and 31/32 perfect
queries at ten pages. Exact distances over the same bounded hierarchical
candidates reached 320/320, so the hierarchy and page budget contained the
answer and compressed ordering caused the remaining miss.

The later flat leaf-to-page-centroid route selected 16 pages but regressed to
273/320 hits, 853,125-ppm aggregate recall, 300,000-ppm minimum recall, and
15/32 perfect queries. Its routing elapsed time was only 255,522 ns and its
maximum page payload was 794,728 bytes. Page means are therefore rejected as a
quality signal, while flat leaf scoring and the 16-page byte envelope remain
useful.

## Query-independent uncertainty authority

The constructor already computes exact squared reconstruction error while
encoding each residual. V31 persists a conservative one-byte radius per
logical row:

1. For each leaf, compute the maximum finite `sqrt(error)` across its rows.
2. Persist `step = next_up(max_radius / 255)` as finite non-negative f32. A
   zero-error leaf has `step=0` and all codes zero.
3. Persist `ceil(radius / step)` as u8, rejecting overflow or a decoded value
   below the original radius. Decoding uses `next_up(code * step)` for nonzero
   codes, so the stored radius is an upper bound rather than an estimate.
4. The plane is emitted in the same logical order as the base/high PQ planes.
   Leaf ranges bind the exact error-plane slice and its scale.

The data is derived only from corpus rows, centroids, and PQ codebooks. No
query, truth, prior result, page-read, or D3 capability reaches construction.

## Serving algorithm

1. Normalize the query and score every leaf centroid. Retain the best 192
   leaves at 100K and at most 512 leaves at larger shapes by
   `(distance, leaf_ordinal)`.
2. Scan at most 1,000,000 variable-rate PQ codes in those leaves. For each row,
   compute ADC squared distance `a`, decode conservative radius `e`, and compute
   `lower = max(0, sqrt(a) - e)^2`.
3. Maintain one bounded record per encountered logical page containing the
   minimum `(ADC, logical)` and minimum `(lower, logical)`. A corpus-sized row
   score allocation or sort is forbidden.
4. Select ten primary pages by `(min_ADC, logical, page_ordinal)`.
5. Select six additional pages, excluding primary pages, by
   `(min_lower, logical, page_ordinal)`. Fewer than sixteen total pages is an
   error. The fixed 10+6 split is not tuned per query.
6. Fetch the sixteen immutable Arrow pages concurrently in one wave,
   authenticate exact bytes, deduplicate source ordinals, and exact-rerank f32
   vectors.

The lower bound is used as uncertainty coverage, not as a proof that the final
top ten is exact. The release claim remains empirical Recall@10. Work receipts
separately report leaf/code/page counts, primary and uncertainty pages, GETs,
bytes, and routing/read/rerank CPU and elapsed time.

## Persistent cross-language format

Typed serving artifacts remain Arrow IPC, scientific tables remain Parquet,
and manifests/results remain sorted compact JSON with one trailing LF.

- `pq-error-radius.arrow`: non-null `radius_code:u8`, one row per logical PQ
  row in identical order;
- `leaf-ranges.arrow`: adds non-null `error_step:f32` and exact error-plane
  start/count fields;
- roots, leaves, variable-rate PQ codebooks/codes/fidelity, page offsets, and
  Arrow page bodies retain their concrete roles but receive the V31 schema and
  dependency identities;
- the page-centroid column is removed because it is not a production input.

Readers authenticate complete bytes before semantic decoding and reject
missing/extra fields, type/nullability drift, non-finite or negative scales,
non-conservative decoded bounds, range/order drift, unequal logical counts,
duplicate page ownership, incomplete source union, or dependency mismatch.

## Memory and work bounds

Starting from the authenticated V30 variable-rate projection of
2,630,588,896 bytes at 100 million rows, the error plane adds exactly
100,000,000 bytes. Existing logical leaf ranges also address the error plane,
so only one f32 step per leaf is added: at most 262,144 bytes for 65,536
leaves. The maximum serialized resident projection is 2,730,851,040 bytes,
leaving 490,374,432 bytes below the binary 3-GiB limit of
3,221,225,472 bytes. Runtime peak RSS, not this arithmetic alone, remains the
release authority.

Per query, V31 scores at most 65,536 leaf centroids, scans at most 1,000,000
row codes, retains at most 32,768 page records, selects exactly 16 pages, and
reads at most 1,048,576 encoded page bytes in the 100K falsifier. Resident
loads must use authenticated zero-copy/mapped buffers at scale so encoded and
decoded code planes are not simultaneously duplicated.

## Fast falsifier and gates

Unit tests first prove upward radius quantization, zero-error handling,
deterministic ties, bounded storage, exact 10+6 disjoint selection, scalar/SIMD
equality, schema rejection, and a fixture where ADC misses a truth page but the
uncertainty reserve recovers it.

The first scientific run rebuilds only the frozen 100K Deep Image development
fixture and evaluates the same 32 burned queries. It must achieve all of:

- exactly 320/320 Recall@10 hits, 1,000,000-ppm aggregate recall, and 32/32
  perfect queries;
- at least 800,000-ppm minimum-query recall;
- exactly ten primary plus six disjoint uncertainty pages;
- at most 1,048,576 encoded page bytes and one concurrent read wave;
- at most 15,000,000-ns routing CPU p99 and under 3 GiB peak RSS;
- deterministic output and no authority, PSI, swap, or resource stop.

Failure stops V31 before any larger build. A pass freezes the algorithm and
permits one disjoint 9.99-million-row confirmation. Physical coalescing is a
separate subsequent change that groups four logical pages per object or
authenticated range, targeting four GETs without altering logical selection.
Cold Standard-S3 latency is reported separately; the 15-ms objective applies
to resident routing plus reranking, while cold storage must meet its own
100-ms p99 and 150-ms ceiling or qualify a lower-latency S3 tier.

## Fences

No 100-million-row build, D3, sealed-query tuning, competitor claim, page
cache, S3 Express substitution, or full-corpus local download is authorized by
the 100K falsifier. Only a passing frozen 9.99M confirmation may advance the
architecture.

# Native ANN G1 Row-Width Screen Design

## Purpose

G1 chooses the smallest resident routing representation that clears the frozen
ReLAION-1M development-quality gates. It changes no production code. The
winner, its artifacts, and the source commit become the only representation
eligible for G2.

## Frozen evidence boundary

- Dataset: ReLAION-1M, 1,000,000 rows of 768-dimensional `float32` vectors.
- Split: all 1,000 registered development queries and their exact top-100.
- Physical layout: the authenticated 900,000-row base plus 100,000-row delta
  generation already used by V85. Every arm shares the same tier-aware
  row-to-page map. Both tiers are routed and charged; delta hits are never
  treated as resident or free, and ranges never cross an object boundary.
- Query budget: at most 32 one-page GET ranges and 16,777,216 encoded bytes.
- Summary fence: rank all pages by the minimum ADC score of their two PQ16x8
  block summaries and retain the best 1,024 pages across both tiers. V104
  killed 128, 256, and 512 pages; V105 retained 1,024 as the qualified exact
  capacity boundary after the 768-page arm lost all three paired confidence
  intervals against it.
- Row shortlist: score only rows in those 1,024 pages and retain the best 512 by
  the global `(distance, feature_row_id)` order, including ties at the boundary.
- Planner: walk ranked evidence, keep the first occurrence of each page whose
  exact encoded bytes still fit, and stop at 32 pages or 16,777,216 bytes.
  Every page is one object-local range GET. This bounded shape matches the
  native two-stage router and excludes V85's global PQ scan and dense traceback.
- Gates: average Recall@10 at least 960,000 ppm, average Recall@100 at least
  975,000 ppm, and nearest-rank p05 Recall@100 at least 900,000 ppm.
- No validation or holdout query, truth, or result may influence training,
  layout, codebooks, arm selection, or stopping.

## Arms

All product quantizers train on the same query-blind, seed-selected source-row
sample. Distances are asymmetric distance computation: an ascending-subspace
sum of lookup-table squared-L2 terms, followed by the registered ID tie-break.

| arm | subspaces | centroids/subspace | persistent row bytes |
|---|---:|---:|---:|
| `pq16x8` | 16 | 256 | 16 |
| `pq24x8` | 24 | 256 | 24 |
| `pq32x8` | 32 | 256 | 32 |
| `pq32x4` | 32 | 16 | 16 |
| `summary-only-pq16x8` | 16 | 256 | 0 |

For `pq32x4`, subspace `2*i` occupies the low nibble and subspace `2*i+1`
the high nibble of byte `i`. Odd subspace counts are forbidden. The scorer
must produce the same total order from packed bytes as a scalar unpacked
reference, including exact ties.

The summary-only arm forms exactly two contiguous block means per physical
page, trains a distinct PQ16x8 quantizer on those means, ranks all summary
pages by the minimum of their two ADC scores, and passes that ranked page list
to the shared bounded planner without row scoring. It is a
zero-row-code architectural control, not an exact-f32 summary ceiling and not
a representation-only comparison with the four row-code arms.

Before the five-arm result is eligible, a fresh exact-f32 diagnostic must score
the rows behind the same 1,024-page summary fence, route and charge both tiers
under the same planner, and pass all three quality gates.
The historical V85 exact-f32 result remains labelled base-only containment
because its evaluator counted delta truth hits without fetching delta pages.

## Evidence and decision

Each arm emits all 1,000 per-query samples: query ordinal, truth IDs, hit IDs,
Recall@10/100 counts, selected pages, ranges, GET count, and bytes. The result
binds the source/query/truth/generation/base/delta URIs, SHA-256 digests and
lengths; source commit; seeds; quantizer/code artifacts; page-map identity;
planner parameters; and exact arm configuration.

An independent validator recomputes every sample and aggregate, the p05,
budget maxima, gates, memory worksheet, and deterministic paired 10,000-draw
bootstrap intervals for Recall@10 and Recall@100 differences. Bootstrap draws
resample query ordinals with replacement from a fixed seed and pair arms on
the same draws. It also rejects duplicated/missing query ordinals, inconsistent
truth, noncanonical artifacts, identity drift, or any query/truth-dependent
training declaration. It independently derives each hit set from authenticated
page membership and selected page identities; producer-reported hits are not
retrieval authority.

Eligibility requires all three point gates and a strict resident projection
below 3 GiB. Among eligible arms, choose the smallest persistent row width.
For equal width, an arm is inferior when its paired Recall@100 difference CI is
strictly below zero; otherwise prefer higher p05 Recall@100, then average
Recall@10, then lexicographic arm name. An arm that misses a gate once is not
promoted; only a preregistered exact rerun may rescue it, and a second miss
kills it. Exact per-query identity kills the redundant arm without a rerun.

## 100M resident worksheet

The validator recomputes, rather than trusts, these terms for 100,000,000
rows and `ceil(rows/256)` pages:

- packed row codes: `rows * row_bytes`;
- two 16-byte summary codes per page;
- row codebook: `centroids * dimensions * 4`, absent for summary-only;
- summary codebook: `256 * dimensions * 4`;
- mutation directory: 1,000,000 bounded entries at 96 bytes each;
- resident delta: 100,000 bounded rows at `dimensions + 72` bytes each;
- page-directory reserve: 134,217,728 bytes, which must later be replaced by
  measured allocator evidence or a compact typed directory before release;
- 16 response buffers of 16 MiB each: 268,435,456 bytes;
- bounded heap planner workspace: 128,000,000 bytes;
- runtime reserve: 536,870,912 bytes.

The worksheet is deliberately resident-only and assumes admission prevents two
complete generations from being resident during reload or compaction. S3
corpus/page bytes and build scratch are separately reported and never used to
make an over-budget arm appear eligible. PQ24x8 and PQ32x8 remain diagnostic
quality arms even when their projection makes them promotion-ineligible.

## Execution boundary

Local execution is limited to narrow unit/static tests while RSS is below
3 GiB and cgroup memory PSI full avg10 is at most 0.5. The complete 1M cell is
one Causality AWS Spot attempt. It publishes terminal evidence and shuts down;
an interruption discards that attempt. G1 receives exactly one Fable 5.1 plus
GPT-6 Astra dual critique, whose result hash is bound into the final ledger.

The single G1 critique is group `6b009a42966d415d`. Its canonical combined
result SHA-256 is
`edcde2149f98bc38c5121b387b165fe9baf0ca6df79658ac624abf86866bcb87`
(Fable output `380d4ca4a7f625bf1f6975c99378452f9469231d188a11822d620b2de5990901`,
Astra output `2f258253a1f4f855ca59ec983030a13d0a912abf8c700a29ebca35483e72bb13`).
Both critics independently identified the free-delta, production-router, tie,
and worksheet gaps corrected above. Their proposal to add further 16-byte
challengers is deferred: the user-registered G1 arm set is exact, and a
no-winner result must be reported rather than silently changing that family.

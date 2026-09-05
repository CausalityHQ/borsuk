# V32 Global Geometric Ownership Design

## Decision and alternatives

Replace the rejected microleaf-exclusive diagnostic with a single query-blind
global balanced partition. Keep existing candidate generation and first-distinct
selection fixed. The alternative of further within-leaf splitting is impossible
for q66/q69 (ten/nine truth leaves). Boundary replication is deferred: it changes
capacity, ownership and reducer semantics together. Global unique ownership is
the smaller next falsifier, not an asserted production solution.

The governing authority is the 262,537-byte V32 terminal with SHA-256
`88226dcc0bc3a6b7034349d95698c0946d500a40b7ba1133bdd418fc5eefb74e`
registered in the preceding virtual-repacking spec and attempt ledger. Preserve
its exact per-query controls, input bindings and source identity. The burned
Deep1M queries 64..95 are diagnostic only. No historical selector score is
claimed independently regenerated unless the corresponding inputs are present.

## Compact geometry

Create `crates/borsuk/src/v32_global_pages.rs`. The private input consists of
borrowed `&[[f32;96]]` vectors and `&[u64]` unique source ordinals, indexed by
logical ID. It exposes no query, truth, leaf, code-parent or page-client input.
Return logical-indexed `Vec<u32>` owners and `Vec<u16>` row counts. No vector
record cloning, per-row heap allocation or source-ID tree map is allowed.

Production diagnostic shape is exactly 1,000,000 reconstructed rows and page
capacity480; the private core accepts 1..1,000,000 rows and capacities1..480 for
synthetic tests. Reject unequal lengths, duplicates, nonfinite coordinates,
nonfinite/zero squared norms and oversized shape before geometry allocation.
Code reconstruction uses the frozen 24/48-byte codebook and code-parent centroid,
then the existing normalization. Reconstruct once, independently of truth.

Freeze this scalar f32 algorithm:

1. Allocate one u32 logical permutation, one f32 inverse-norm vector and one
   f32 margin array. Validate source uniqueness by sorting the permutation.
2. A node with n rows assigned p pages ends if p=1. Otherwise its left child
   receives `l=p/2` pages and `l*(n/p)+min(n%p,l)` rows.
3. Sort node indices by source ordinal. Normalize its first vector for the left
   seed. Choose minimum cosine to that seed, with source-ordinal ties, then
   normalize that selected vector using identical dimension-order arithmetic for
   the right seed. Use scalar dimension-order dot sums multiplied by the precomputed
   inverse norm, matching the existing geometric splitter's operation order.
4. Four times, compute right cosine minus left cosine, sort logical indices by
   `(margin.total_cmp, source_ordinal)`, accumulate each child's centroid in that
   sorted order, and normalize. Reject zero/nonfinite centroids; no fallback.
5. Perform the fifth margin sort, then recurse left before right. Page ordinals
   follow this traversal. Every source has exactly one owner; every page has
   1..capacity rows; page count is exactly ceil(N/capacity).

The initial page count at1M is2084; depth is at most12. Each level evaluates at
most five two-centroid margins per row plus seed/centroid work; no all-pairs
clustering. Allocation bound includes384N vector bytes,8N source bytes,4N each
for permutation/inverse norm/margins/owners, and2ceil(N/capacity) count bytes:
408,004,168 bytes at1M/cap480, below512MiB. Inputs' excess capacity and allocator
overhead still count against measured2GiB total process RSS. Construction is
serial initially. Scientific execution uses Causality Spot with a7200s cap,
preserved terminal/pressure stop and no automatic retry after scientific failure.

## Replay integration

In `v30_s3_search.rs`, separate global-prefix candidate replay creation from
virtual reduction. Preserve ranked/admitted leaves, logical IDs, score bits,
stop reason and work counters in one immutable per-query replay. Authenticate
resident inputs once. Generate all32 controls and compare their complete
canonical bytes with the governing terminal **before** global reconstruction.
Python `run_v32_no_page_containment.py` retains the exact authority comparison;
aggregate-only equality cannot unlock construction or treatment output.

The qualifier uses a two-phase local boundary: control replay produces the
registered control records; only the verified controller invokes treatment with
the same authenticated inputs and rechecks control equality on its output.
Control mismatch must prevent invoking the costly treatment process. Recomputed
replay hashes must match across phases. The treatment constructs one global map
and applies first-distinct8 (decision) and16 (explanatory) to the same replay.
The former microleaf occupancy rejection is not a cross-leaf failure gate.

Arrow/Parquet remains the vector/request/evidence format; canonical JSON holds
small receipts. Bind algorithm ID `v32-global-balanced-cosine-v1`, exact inputs,
source, map hash, replay hashes, all per-query memberships and resource facts.
Historical microleaf layout hashes must not masquerade as this algorithm.

## Fast gates and scientific interpretation

Unit tests first: source-reversal determinism, exact capacity/count/coverage,
duplicate/invalid shape/norm rejection, scalar differential equality with the
existing splitter on small compatible populations, and a ten-old-leaf synthetic
population that fits eight new pages without the obsolete leaf gate. Truth is
absent from construction and selection APIs. Altering truth may change hits only.

Then one frozen1M no-page replay. Require320/320, worst10/10,32 perfect, exactly8
selected pages, zero page reads, unchanged candidate/work evidence and encoded
upper bound `8*196608=1572864` bytes. Derive the envelope from selected page row
counts and the registered codec before claiming it as measured bytes. A truth
set occupying>8 global pages rejects this partition even with an ideal reducer;
otherwise the actual first-distinct8 selection must pass.319/320 rejects; no
tuning on the result. Failure rejects this algorithm/partition, not all480-row
layouts. A pass permits one preregistered disjoint cohort, not a release claim.

## Independent release gates

Current row-code residency projects to25.2GB at1B and cannot meet3GiB **total**.
No per-shard redefinition is permitted. A later page-addressed tree with row
codes only in objects is a candidate, not yet a validated router. At about2.084M
pages, two f16[96] child centroids per internal node cost about800MB;64-byte node
and64-byte page-directory records add about267MB. Actual recall, bounded work,
allocation and process RSS must be tested with that router, not resident-PQ
replay. Prior rejected multi-anchor scorers are not silently reintroduced.

The operator explicitly withdrew the hard15ms cold-S3 gate on2026-09-05.
Optimize measured latency, high read/write throughput, recall and scalability;
do not treat the historical144ms Standard-S3 result as an immutable lower bound
or pretend that a resident-only diagnostic measures object-store latency.
A separate cold8-GET transport measurement on intended storage class, AZ and
load is required, counting physical retries and slowest completion. Report
cold/warm p50/p95/p99, read QPS under concurrency, bytes/GETs/query, write
vectors/s and bytes/s, flush/visibility latency, write amplification, sustained
RSS and mixed read/write behavior. No sum-of-p99 or page-count linear scaling inference.
At the byte ceiling,1000QPS requires about12.6Gbit/s payload alone. Finite-cohort
perfect recall is not a universal exact-neighbor guarantee. Exactly8 remains
this falsifier's frozen experimental budget, not proof that a16-page system
with a better measured recall/throughput tradeoff is unusable. Do not change
the experimental budget after seeing results. D3,100M construction
and competitive claims remain fenced until these distinct gates pass.

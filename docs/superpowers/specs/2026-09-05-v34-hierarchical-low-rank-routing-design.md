# V34 hierarchical low-rank storage-group routing design

## Status and decision

V33's preregistered rank-two covariance arm is rejected. On the exposed
Deep Image 1M mechanism cohort it retained 1,279/1,280 truth owners and
127/128 complete queries. The rank-four diagnostic retained 1,280/1,280 owners
and 128/128 complete queries, with truth-informed required frontiers of
5/21/43 groups and 27,792/121,896/242,638 rows at p50/p95/max. Those numbers
generate a new hypothesis; they do not promote rank four, predict fresh-query
tails, or describe the number of groups a live query-only router will select.

V34 is a new prospective feasibility study. It relinquishes V33's failed
131,072-row fourfold-reduction objective and freezes one decoded-f32 rank-four
candidate at 64 groups, 262,144 rows, 12,288 retained candidates, and the first
64 distinct pages. The actual encoded code-object payload must independently
remain at or below 8 MiB per query. The candidate is compared with the frozen
fine-leaf centroid and a 2,320-byte equal-resolution control. No result from the
exposed V33 cohort is eligible for a V34 quality claim.

The production hypothesis is an exact best-first hierarchy over compact
rank-four leaf summaries. It must return the same ordered group prefix as an
exhaustive rank-four scan while evaluating at most 25% of leaves at p95 and
using at most 5 ms router CPU at p95 on one pinned target core. Failure of
either performance gate rejects this acceleration; it does not authorize an
outcome-tuned beam.

This is a pre-release format replacement. V34 introduces no legacy reader,
alias, migration layer, or dual write path.

## Novelty position

Ellipsoidal cells, Gaussian mixtures with per-cluster covariance, Mahalanobis
routing, local PCA trees, learned cluster ranking, and hierarchical selective
storage are prior art. V34 must not claim novelty for using covariance or an
ellipsoid. Relevant prior work includes DBIN (KDD 1999 and US6263334B1), the
SS-tree and SR-tree, covariance quadtrees, learned cluster routing, ScaNN's
anisotropic quantization, SPANN, and SPFresh.

The research contribution under test is narrower: a deterministic compact
low-rank Gaussian lower-tail score for Euclidean candidate routing; a
conservative best-first bound over that signed score; query-independent
streaming construction; immutable cross-language Arrow/Parquet authority; and
selective S3 code/page execution with capability-separated evidence. This
description is a technical prior-art position, not a legal patentability
opinion. Any external novelty claim requires a professional claim-by-claim
search.

## Fixed leaf representation and score

For each reconstructed routing leaf persist only:

- population `n`, mean `mu[96]`, group and logical interval;
- residual diagonal `d[96]`;
- four eigenvalues `lambda[4]` and four directions `v[4][96]`;
- precomputed population factor `a=sqrt(2*ln(n))`, trace `t`, trace-square `h`,
  and a conservative spectral-norm bound.

The decoded covariance approximation is

`Sigma = diag(d) + sum(k=0..3, lambda_k * v_k * v_k^T)`.

For `u=q-mu` and `D=||u||^2`, the exact stored-representation score is

`s(q)=D+t-a*sqrt(2*h+4*u^T*Sigma*u)`.

Directions are not assumed orthogonal after f32 persistence. Trace-square and
spectral terms are computed from decoded factors, with deterministic dimension
order, f64 accumulation, material-negative rejection, signed-zero
canonicalization, and `(score, ordinal)` ties. This is a routing heuristic over
Euclidean neighbours, not Mahalanobis distance, ellipsoid-surface distance, or
a lower bound on exact vector distance.

The equal-resolution control stores six deterministic f32 centers per leaf plus
16 padding bytes, exactly matching the 2,320 numeric bytes of the rank-four
summary. Recursive maximum-variance splits, logical-ordinal ties, and singleton
duplication are frozen before queries. Rank four may support a shape-efficiency
claim only if it dominates both this control and the fine-leaf centroid.

## Exact hierarchical traversal

Build a deterministic 16-way bulk tree over decoded leaf means. Storage groups
and their fetch boundaries do not change. An internal node stores an enclosing
ball `(c,R)` for descendant means and outward-rounded bounds
`t_min`, `h_max`, `a_max`, and `L_max`, where every descendant covariance obeys
`||Sigma||_2 <= L_max`. A valid leaf bound is

`max(d_j) + sum(lambda_k*||v_k||^2)`.

For a query, define

- `D_min=max(0,||q-c||-R)^2`;
- `D_max=(||q-c||+R)^2`;
- `f(D)=D+t_min-a_max*sqrt(2*h_max+4*L_max*D)`.

The node lower bound is the minimum of `f(D)` on `[D_min,D_max]`. For
`L_max>0`, evaluate it at

`clamp(a_max^2*L_max-h_max/(2*L_max), D_min, D_max)`;

handle zero covariance separately. All persisted bounds use directed outward
rounding or an explicit error envelope covering decoded scoring. Ambiguous
comparisons expand rather than prune.

Best-first traversal expands unresolved nodes until it can certify the next
leaf in global `(score,leaf_ordinal)` order. The first certified leaf belonging
to a group establishes that group's minimum score. Unique groups are emitted
by `(score,group_ordinal)`. Admission keeps the longest complete-group prefix
within all fixed group, row, and byte limits; the first overflowing group stops
the prefix without skipping and must itself be score-certified. The optimized
and exhaustive routes must be byte-for-byte equal in selected group order,
overflow identity, selected rows, and derived object identities.

The bound is exact for the stored rank-four heuristic, not for true neighbour
distance. Correctness does not imply useful pruning in 96 dimensions. The
reduced performance gate records nodes expanded, bounds evaluated, exact leaf
scores, summary bytes touched, CPU, and wall time. At p95 it must evaluate no
more than 25% of leaves, take no more than 5 ms on one pinned target core, and
use no more than half the complete resident query CPU of the paired exhaustive
rank-four control. Otherwise V34 stops before fresh data or paid execution.

## Serving memory and object model

At an observed-density projection of 414,100 leaves for 100M rows:

- rank-four numeric payload: `414,100*2,320 = 960,712,000 B`;
- 24-byte leaf identifiers/intervals: `9,938,400 B`;
- four cached f64 scalars per leaf: `13,251,200 B`;
- approximately 27,607 tree nodes at a provisional 512-byte cap:
  `14,134,784 B`.

The count transfer and Arrow envelope are hypotheses checked from actual build
counts. The complete process, including active and retiring generations,
directories, mmapped resident pages, caches, SDK/runtime, allocator, bounded
deltas, and active-query workspaces, must stay strictly below 3 GiB. Freeze the
following admission budget before qualification: 1,040 MiB active generation,
1,040 MiB retiring generation, 128 MiB shared caches, 160 MiB runtime/fixed
state, 512 MiB for sixteen 32-MiB query workspaces, and 96 MiB unallocated
headroom. The admitted sum is 2,976 MiB, leaving 96 MiB between the admission
budget and the 3,072-MiB hard limit. A third pinned generation is forbidden;
refresh and query admission backpressure before the boundary.

Persist one versioned rank-four-only Arrow IPC generation with exact non-null
physical schema and logical ordering. Authenticate once before publication and
once when mapping a generation, never for every query. Do not retain the
rank-one/two diagnostic ladder, reconstructed rows, duplicate encoded/decoded
copies, or a 100M-entry page-owner map in serving memory. Parquet remains the
cross-language format for corpus/query/truth bulk data; canonical JSON is
limited to small manifests and receipts.

Codes and exact vectors remain remote. Fetch only complete selected code-group
objects and final page objects. Page ownership travels in the bounded code
objects. Every result separately reports selected groups/rows, actual code and
page GETs, retries, bytes, candidate retention, selected pages, final recall,
latency, and QPS. Cold S3 latency includes both dependent fetch stages and has
no 15-ms requirement; the goal is strict improvement over the paired V32
control under identical connection reuse and concurrency.

## Streaming construction and writes

V34 keeps the same PQ-reconstructed population as V33 so the experiment changes
only routing representation and execution. A builder streams complete logical
leaves, buffers one bounded leaf per worker, accumulates count/mean/dense
co-moment in deterministic order, performs one eigendecomposition when sealing
the leaf, emits its compact summary, and releases reconstructed rows. It never
materializes the corpus or complete reconstructed population.

Online writes use append-only immutable delta segments. Inserts and tombstones
become visible through conditional manifest publication; readers pin a
generation plus its ordered deltas. Search applies one global group/row/byte/page
budget across base and deltas, resolves latest versions before final top-k, and
never lets stale versions consume final candidates. Initially admit at most four
uncompacted runs and one million delta rows. Backpressure thereafter.

Do not eigendecompose a global leaf on each write. Segment sealing summarizes
new leaves in batches. Compaction deterministically reconstructs affected rows
from PQ codes and rebuilds their summaries; rank-four factors are not treated
as sufficient statistics for arbitrary reassignment. Qualification measures
sustained ingestion, visibility delay, compaction debt, write amplification,
reader pin duration, and concurrent-read latency. No write-throughput claim is
made from offline construction arithmetic alone.

## Prospective experimental protocol

The registration commit must precede creation or hashing of fresh query
artifacts. Rank four is the sole eligible new candidate. Fine-leaf centroid and
equal-byte six-center routes are controls, never fallback winners substituted
after a rank-four failure.

Use 600 fresh development queries and 600 separately sealed holdout queries.
Exclude exact and preregistered near-duplicates of every exposed V32/V33 query,
source-family duplicates, and duplicates across splits. A custodian creates
exact GT@10 with a code-independent source-vector scan. Builder credentials see
construction inputs only; router credentials see summaries and the permitted
query split only; evaluator credentials see committed route outputs and truth.
Holdout vectors and truth are separate unreadable objects until the development
choice, format, budgets, and receipt are committed.

Development is one burn. It first runs a truth-only layout oracle; if any query
cannot fit all ten owner groups inside 64 groups and 262,144 rows, stop because
the layout—not the scorer—is the blocker. It then compares the frozen three
routes and exact-vs-hierarchical rank-four execution. Rank four must retain every
owner on every query, pass all group/row/byte caps, dominate both controls under
the committed ordering, and pass all hierarchy CPU/pruning/memory gates. No
seed, bound, tree fanout, budget, score, or control changes are permitted after
opening development.

Only a committed development pass opens holdout. Holdout runs once. Any missing
owner, imperfect query, identity mismatch, resource breach, or prefix mismatch
rejects V34 permanently. A passing route then runs unchanged PQ candidate
selection, first-distinct page selection, selective S3 reads, and exact rerank.
It must achieve 1,000,000-ppm aggregate and minimum recall at 64 pages and show
non-worse p95 GETs, bytes, latency, and QPS plus a strict GET or byte improvement
over the same-host V32 control.

All paid work uses AWS profile `causality`, EC2 Spot by default, immutable S3
terminal receipts, registered pressure/time stops, and immediate instance
termination. An interruption reruns the identical registered cell under a new
attempt identity; a completed scientific failure is never retried.

## Fail-fast implementation order

1. Lock authority, score algebra, conservative bounds, tie behavior, and checked
   100M memory/work projections in sub-second unit tests.
2. Build a rank-four-only Arrow round trip and prove exact equivalence to the
   existing V33 decoded rank-four reference on synthetic and authenticated
   metadata-only fixtures.
3. Implement exact exhaustive group emission, then hierarchical emission, and
   differential-test complete prefix/overflow equality under adversarial
   negative scores, singular leaves, ties, repeated means, rounded directions,
   and worst-case no-pruning trees.
4. Run the authenticated exposed-1M V33 rank-four summary and its 128 burned
   queries as a claim-ineligible performance falsifier. Time at least 10,000
   deterministic query invocations after 1,024 warmups. Stop on any exact-route
   mismatch, 25%-leaf, 5-ms, half-CPU, or 3-GiB projection failure. These
   queries may reject the implementation but cannot establish V34 quality.
5. Only after those cheap gates, create the fresh capability-separated cohort
   and run one bounded Spot campaign sequentially: development, committed
   freeze, holdout, then selective S3 replay.
6. Only a holdout and serving pass authorizes replacing the experimental V32
   reader and proceeding to 100M qualification.

## Explicit non-goals

- No Mahalanobis final-neighbour metric or query-specific covariance learning.
- No claim that ellipsoids solve concentration of measure.
- No approximate beam hidden behind an exact-hierarchy result.
- No whole corpus, PQ plane, or page corpus downloaded to a serving node.
- No promotion from the exposed 128-query V33 cohort.
- No compatibility with V32/V33 experimental persistent formats.
- No production, 100M, competitor, or novelty claim from the 1M feasibility
  campaign alone.

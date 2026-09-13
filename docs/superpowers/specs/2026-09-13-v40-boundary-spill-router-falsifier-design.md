# V40 Boundary-Spill Router Falsifier Design

## Status and purpose

V40 is a claim-ineligible, one-million-row routing falsifier. It asks whether a
query-only algorithm can attain the already authenticated V38 boundary-spill
layout closely enough to justify building scalable coarse objects.

The frozen population is ReLAION2B cohort A: 1,000,000 non-null `f32[768]`
rows, 1,000 burned development queries, and exact GT@100. V40 consumes the
immutable V38 ownership tree, boundary-spill relation, posting summary, and
their exact authorities. It does not rebuild the layout, inspect page bodies,
open validation or holdout, or claim production S3 latency.

The V39 ceiling ladder established the relevant integer boundary. K20 is
certifiably insufficient at 997,970 ppm aggregate and 880,000 ppm minimum.
K21 has a feasible witness at 998,510 ppm aggregate and 900,000 ppm minimum,
only 51 hits above the aggregate gate. K21 is therefore the sole V40 posting
budget. V40 must attain at least 99,800 of 100,000 GT occurrences and at least
80 of 100 for every query using its own selections. Oracle posting identities
are not a routing target, and duplicate hits never count twice.

## Causal ladder

V40 freezes two ordered router arms before development selection opens:

1. **Direct tree baseline.** Reuse the V37 best-bin-first ownership-tree
   traversal and emit exactly 21 unique V38 posting ordinals.
2. **Accepted-spill challenger.** Run only if the direct arm fails. Use the
   same tree frontier plus a compact query-independent summary of rows that
   were actually accepted into primary and alternate V38 postings. Select 21
   postings by deterministic marginal modeled distinct coverage.

The challenger may not reuse a differently tuned tree margin and call it new
geometry. Its only additional signal is the accepted primary/alternate
assignment relation, including capacity outcomes. A learned classifier,
posting sketch, additional tree, changed frontier, changed K, or post-outcome
weight is a different experiment and is forbidden in V40.

Construction, selection, and truth evaluation are separate capabilities:

- summary construction receives the sealed V38 tree/relation/summary and their
  authorities, but no query or GT artifact;
- selection receives the sealed V38 tree, optional sealed spill summary, and
  the registered development query vectors, but no GT or feature-neighbour
  mapping; and
- evaluation receives a sealed selection artifact, the sealed V38 relation,
  and registered GT, but no query vectors or selector implementation.

Selection must reach its create-only terminal and authenticate before the
truth evaluator may open GT. This prevents router tuning from individual
misses. The development split is already burned; a pass only freezes a
candidate for later validation and holdout.

## Direct tree baseline

For each query, score every visited internal node once with the exact V37/V38
registered projection and fused backend. Traverse the containing child at the
parent path penalty and queue the sibling with

`max(parent_penalty, squared_normalized_wrong_side_margin)`.

The priority key is `(path_penalty_bits_total_order, node_ordinal)`. Leaf ties
then use posting ordinal. Emit the first 21 unique posting ordinals. The heap
may pop at most 1,024 nodes; exhaustion, malformed topology, a non-finite
score, a duplicate leaf identity, or fewer than 21 postings is a selection
stop. The selected sequence is order-bearing even though truth evaluation
uses it as a set.

This arm tests whether the existing ownership geometry alone can attain the
V38 layout. It is the mandatory baseline because V38 alternate selection and
V37 traversal share the same path-violation geometry.

## Accepted-spill summary

For each primary posting `i`, summary construction scans every V38 row once
in increasing source ordinal. It counts accepted rows by alternate posting
`j`, with a separate category for rows that have no alternate assignment.
It checks that each row has exactly one primary, at most one distinct
alternate, and that all per-posting populations and capacities equal the
authenticated V38 posting summary.

Order alternate categories by `(count descending, posting_ordinal ascending)`
and retain at most 32. Combine omitted alternate categories and the no-alternate
category into residual primary-only mass. This intentionally discards the
omitted alternate coverage edge instead of inventing unbounded fan-out.

Apportion each primary posting's complete population into exactly `2^24`
integer mass units. Compute floor quotas with checked integer arithmetic, then
assign remaining units by `(remainder descending, category kind, posting
ordinal)` where retained alternate categories precede the residual category.
The retained masses plus residual must equal `2^24` exactly. Zero-count
categories are absent; zero residual is permitted.

The runtime structure-of-arrays representation contains:

- `u64[P+1]` offsets;
- at most 32 `(alternate_posting:u32, mass_q24:u32)` records per posting; and
- `u32[P]` residual masses.

At the 100M projection of 12,208 postings this is at most 3,271,752 bytes,
excluding small Arrow framing. The projection demonstrates that the summary
can be resident; it does not qualify V38's current spill builder.

## Overlap-aware challenger

The query frontier is the first `L=min(64,P)` leaves from the exact direct-tree
ordering, under the same 1,024-pop cap. A zero-based frontier rank `r` receives
integer weight `w=L-r`.

The candidate pool is the frontier postings plus every retained alternate of
those postings. It contains at most `64 + 64*32 = 2,112` postings. Relation
expansion is one hop only. Duplicate candidates are removed by posting ordinal.

For frontier posting `i`, retained category `(i,j)` is modeled covered when
either `i` or `j` is selected. Residual mass is modeled covered only when `i`
is selected. For selected set `S`, the checked integer objective is

`F(S) = sum_i w_i * (residual_i * I[i in S] + sum_j mass_ij * I[i in S or j in S])`.

Choose exactly 21 distinct postings greedily. At each step recompute the
integer marginal gain against the current covered-category state. Order by
`(marginal_gain descending, best_frontier_rank ascending, posting_ordinal
ascending)`, using `u32::MAX` for a candidate not present in the frontier.
There is no reserved primary slot. After all positive gains are exhausted,
the same tie rule deterministically fills remaining slots with zero-gain
candidates. Fewer than 21 candidates is `candidate-shortage`.

This objective accounts exactly for overlap inside the truncated two-owner
model. It is not calibrated probability and is not a GT containment estimate.
The falsified hypothesis is that query frontier rank combined with
query-independent accepted-spill proportions predicts useful distinct
neighbour coverage.

## Artifacts and cross-language contract

Cross-language tables use Parquet, dense typed arrays use Arrow IPC, and
authorities, manifests, progress, results, and terminal receipts use recursively
sorted compact JSON followed by one LF. Rust memory layout is never serialized.

- `v40-spill-counts.parquet` contains non-null primary posting, alternate
  posting or explicit residual role, exact count, primary population, and
  deterministic ordering before Q24 encoding.
- `v40-spill-summary.arrow` contains the packed offsets, alternate ordinals,
  Q24 masses, and residual masses with exact physical schemas.
- `v40-selections.parquet` contains query ordinal, selection rank, posting
  ordinal, node-pop count, candidate count, objective value, and router arm.
- canonical JSON results bind every input and output URI, SHA-256, BLAKE3,
  encoded length, schema, source/archive/index identity, projection/backend,
  K, limits, gates, and predecessor terminal.

Readers reject aliases, unknown or missing fields, nulls, wrong physical
types, non-finite values, duplicate or unordered ordinals, invalid topology,
length or digest drift, Q24 mass drift, cross-object binding drift, and
trailing bytes. Every input is opened once, length-bounded with one-byte
overflow detection, authenticated through the retained descriptor, and never
reopened by pathname for scientific use.

## Truth-only evaluation

The evaluator validates that every query has exactly 21 distinct selections
with ranks 0 through 20. It maps each GT feature ID through the authenticated
V38 relation and counts a neighbour once when either accepted owner appears in
the selected set. It recomputes all 1,000 samples, aggregate hits, minimum
hits, percentiles, and pass status in query-ordinal order.

An arm passes only with aggregate containment at least 998,000 ppm and every
query at least 800,000 ppm. Results additionally report exact-query count,
the per-query gap to the V39 feasible witness and certified upper bound, node
pops, tree scores, candidate touches, marginal recomputations, and duplicates
suppressed. A direct pass terminates V40 without constructing the challenger.
A direct failure permits exactly one challenger run. Challenger failure
terminates `router-rejected`; no constant may change afterward.

## Transport simulator

Only a logical router pass permits a metadata-only transport simulation. The
simulator consumes authenticated posting populations and immutable logical
object descriptors; it has no S3 client and cannot read a page body.

For each query it records 21 logical coarse GETs, requested posting records
before deduplication, unique candidate rows afterward, and modeled bytes. The
frozen worst-case coarse bound is

`21 * (10,240 records * 48 bytes + 32,768 framing bytes) = 11,010,048 bytes`

which is exactly 10.5 MiB. A K21 pass therefore replaces the former K14/
7 MiB contract; it must not be reported under the old budget. Candidate
deduplication cannot reduce already requested coarse bytes.

The simulator separately models cold and warm metadata, versioned object keys,
range coalescing, concurrency, retries, and delta generations. It reports
logical requests and modeled bytes, not measured S3 latency. Fetching full
`f32[768]` vectors for the worst-case 215,040 records would be 660,602,880
bytes (630 MiB) before framing, so fine-stage serving remains explicitly
unqualified.

## Resource and scalability boundaries

Warm native query-only routing must complete 1,024 warmups and at least 10,000
timed queries with p99 at most 5,000,000 ns. Timing includes projection, tree
scoring, candidate construction, greedy selection, and output materialization,
but excludes artifact loading and truth evaluation. It records fused MACs,
node pops, heap operations, candidate touches, and marginal recomputations.

Summary construction streams the sealed V38 relation and uses bounded posting
counters; it does not reopen the source corpus or retain per-row maps. Query
selection admits under a 256 MiB cgroup limit. Summary construction and truth
evaluation each admit under 512 MiB. All phases stop on swap growth or memory
PSI full avg10 above 0.75.

The current V38 layout builder is not scalable to 100M. It scores every row at
every internal node, approximately `O(N*P*d)` at fixed posting occupancy, and
measured 3,084,148,736 bytes at 1M. A V40 router pass only authorizes a separate
construction redesign: exact best-first alternate search with tie-correct
termination and a fail-closed work cap, plus shard streaming and bounded
external ordering. It does not authorize 10M/100M or a write-throughput claim.

Writes, deltas, deletes, compaction, fine-page packing, cache behavior, and
physical S3 latency remain separate release gates. Pre-release freedom permits
replacing V38 formats rather than retaining compatibility readers once a
production architecture qualifies.

## Test and execution order

1. Unit-test tree ordering against exhaustive references, including equal
   margins, signed zero, malformed topology, duplicate leaves, and cap stops.
2. Unit-test spill counting, top-32 truncation, largest-remainder Q24
   apportionment, residual mass, overflow, and exact Arrow/Parquet schemas.
3. Differentially test greedy marginal selection against an independent tiny
   recomputation. Include duplicate-heavy fixtures where additive voting
   wastes slots and exhaustive state checks after every choice.
4. Test canonical results, cross-language round trips, bounded reads,
   capability separation, truth recomputation, and transport arithmetic.
5. Run one source-free native preflight covering worst-case heap ties, 2,112
   candidates, Q24 reductions, and query timing under registered limits.
6. Run one direct-tree selection on the immutable 1M development artifacts,
   then its truth-only evaluation.
7. Only direct failure permits one spill-summary build, one challenger
   selection, and one truth-only evaluation.
8. Only a logical pass permits the metadata-only request/byte simulator.
9. Freeze a passing arm and all limits before opening independent validation
   and sealed holdout. A failed 1M arm never advances to larger populations.

Full repository assurance runs once after the implementation diff stabilizes.
Iteration uses narrow unit gates and source-free preflight; compilation and
scientific execution run on `causality` AWS Spot rather than the pressured
local devbox.

## Terminal dispositions

Each phase emits one authenticated create-only terminal:

- `preflight-rejected` for deterministic, numeric, resource, or throughput
  failure;
- `direct-passed` or `direct-failed` from the direct truth evaluation;
- `challenger-passed` or `router-rejected` from the spill challenger;
- `simulation-rejected` when a logically passing router violates the frozen
  request/byte contract;
- `simulation-feasible` for a logical and modeled-transport pass; or
- `infrastructure-failed` for a bounded platform/controller failure carrying
  no scientific conclusion.

No terminal automatically launches physical S3, fine pages, validation,
holdout, 10M, 100M, or paid follow-on work.

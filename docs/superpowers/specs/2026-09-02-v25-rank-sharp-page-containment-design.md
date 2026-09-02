# V25 Rank-Sharp Page Containment and Fail-Fast Qualification

**Date:** 2026-09-02

**Status:** Approved prerelease replacement design

## Decision

V25 replaces query-independent witness mass with query-conditioned row rank.
It does not begin by rebuilding the page layout. The first deliverable is a
claim-ineligible page-containment decomposition on the authenticated existing
layout. Only a passing exact-rank control may authorize a bounded resident row
router. Page-layout co-design is a later conditional repair, not an assumption.

V25 is a new format. It has no V24 reader, migration, alias, duplicate write
path, or version dispatch. Bulk cross-language artifacts use Parquet or Arrow
IPC. Canonical JSON is restricted to small manifests, progress, receipts,
policies, and aggregate results.

Qualification also changes. An individual repair runs one named RED/GREEN
test. A coherent slice runs its affected module gate. An architecture runs an
open 262,144-row real-data causal screen and then a sealed 1,048,576-row
sentry. Full 10-million-row and 100-million-row work is forbidden until those
cheap gates pass with margin. Strict Clippy and the full workspace suite run
once per verified milestone, not after each repair.

Pure V25 authority, reducer, metric, and codec contracts live in the small
internal `borsuk-v25` workspace crate. Their named tests must not relink the
monolithic `borsuk` crate. Main-crate integration is a milestone gate. This
keeps a pure contract RED/GREEN loop in seconds rather than approximately one
minute of unrelated recompilation. The first measured slice compiled cold in
4.70 seconds and reran warm in 0.63 seconds; later gates must report their own
wall time so regressions are visible.

## Evidence and corrected failure mechanism

The V24 full-scale screen used 9,990,000 unique rows, 18,620,111 physical page
assignments, 28,282 pages, 1,048,576 witnesses, and 1,024 corpus-uniform
pseudoqueries. The best exact-eight-page cell reached only 652,636 ppm
aggregate recall and 659,463 ppm oracle attainment. The screen selected the
pseudoquery's own indexed page 938 times; removing that optimistic signal
reduced aggregate recall to 452,929 ppm.

The existing layout's exact eight-page oracle recovered 10,134 of 10,240
hits: 936 queries fit all ten neighbors, 70 fit nine, and 18 fit eight. Literal
perfect recall is therefore impossible on this layout under an exact-eight-
page contract, while 975,000 ppm remains possible. A separate authenticated
V23 development control ranked every row exactly and recovered 318 of 320
layout-achievable hits: 993,750 ppm aggregate and 1,000,000 ppm oracle
attainment.

The repeated failure is rank destruction. Leaf incidence and V24 witness
postings aggregate mass; the prior reciprocal-rank reducer also allowed ranks
11--4,096 to displace top-ten page evidence. Recall@10 instead needs the first
high-ranked row that supports each page. The untested minimal reducer is:

`page_score(p) = min(distance(q, r) for candidate row r assigned to p)`.

Pages sort by `(page_score, page_ordinal)` and the first distinct pages win.
With exact global scores this is order-equivalent to taking the first distinct
pages in exact row rank. It retains sharp rank information and does not reward
dense but irrelevant pages.

Diagnostic controls may emit fewer than eight pages when their registered
candidate prefix contains fewer than eight distinct pages; this is scientific
evidence, not an authority failure. The bounded serving control must emit
exactly eight pages.

## Phase A: page-containment decomposition

The first V25 artifact reuses authenticated construction rows, page-row
assignments, and corpus-uniform pseudoqueries. It reads no page bodies and no
benchmark development or holdout roles. For each pseudoquery it reports these
controls in fixed order:

1. `layout`: exact optimal page cover of the ten ground-truth rows;
2. `exact-global`: exact f32 row order reduced by best row per page;
3. `exact-contained`: exact f32 order over only rows returned by a registered
   containment candidate set;
4. `coded-contained`: residual-code order over the same candidate set;
5. `bounded`: the production-shape hierarchy, code scorer, and page reducer.

The result independently recomputes every per-query hit count, aggregate,
minimum, oracle attainment, candidate count, page list, and causal class. The
classes have strict precedence:

- `authority-stop`: identity, schema, completeness, finiteness, capability,
  determinism, or resource authority failed;
- `layout-rejected`: the exact page oracle failed the registered target;
- `rank-reducer-rejected`: exact global best-row-per-page failed;
- `containment-rejected`: exact global passed but exact contained failed;
- `code-rejected`: exact contained passed but coded contained failed;
- `bounded-router-candidate`: every control passed.

This phase decides what to build. It prevents a new codebook, hierarchy, or
page corpus from masking a reducer defect.

For the exact-global control, retain the best `K` rows for the fixed ladder
`10, 32, 128, 512, 2,048, 4,096`. Report both best-row-per-page and the V24
mass reducer on the identical ranked rows. The open result reports every arm
and may nominate the lexicographically first passing arm for the sealed sentry;
it is never itself a release claim.

## Conditional resident router

The resident router is authorized only if `exact-global` passes. It uses a
capacity-bounded coarse hierarchy trained only on a query-independent corpus
split:

- 16,384 coarse lists at 100 million rows;
- at most 6,104 rows per list;
- fixed query probe count 128, scanning at most 781,312 rows or 0.782% of the
  corpus;
- deterministic ties `(distance, source_ordinal, list_ordinal)`;
- a 12-byte rotated residual sign code plus one f16 residual norm and one f16
  alignment denominator per row.

Assignment and encoding run over disjoint source Parquet shards on independent
Spot workers. Each worker emits sorted Parquet runs. A deterministic external
merge verifies the complete source-ordinal inventory and emits coarse-list
runs; worker count, shard order, interruption, and retry cannot change the
bytes.

Codes remain in coarse-list order, so row ordinal and list identity are
implicit. Primary and optional replica pages are explicit `u32` values. The
serving path:

1. normalizes one 96-dimensional query;
2. searches the 16,384 coarse centroids and retains the best 128 lists;
3. scores at most 781,312 residual row codes from those lists;
4. retains the best 4,096 `(distance, code_ordinal)` rows in bounded storage;
5. updates each row's primary and optional replica page with the minimum score;
6. emits the first distinct pages ordered by `(minimum_score, page_ordinal)`.

There is no exhaustive serving fallback, mass vote, outcome-dependent probe
widening, page-body access, storage client, or neighbor input. Scalar reference
and fused SIMD implementations must select identical pages. Scalar is test
evidence and never a scientific fallback.

## Exact 100-million-row serving bound

The registered upper bound for the existing page layout is 2,811,172,872 bytes
(2.618 GiB):

| Resident component | Bytes |
|---|---:|
| 100,000,000 12-byte row codes | 1,200,000,000 |
| f16 residual norm and denominator | 400,000,000 |
| primary page `u32` per row | 400,000,000 |
| optional replica page `u32` per row | 400,000,000 |
| 16,384 f32 coarse centroids | 6,291,456 |
| coarse-list `u64` offsets | 131,080 |
| centroid graph, degree 32 `u32` | 2,097,152 |
| bounded query workspace | 67,108,864 |
| executable and static tables | 67,108,864 |
| allocator and residency reserve | 268,435,456 |

The sum is below 3 GiB by 410,052,600 bytes. The preflight must separately
measure decoded peak RSS below 3 GiB. Arithmetic is not a substitute for the
load measurement.

At 100 million rows the selector traverses the 16,384-centroid graph, scores
at most 781,312 row codes, and scans no more than 0.782% of resident rows. A
production-shape preflight must demonstrate warm p99 below 12 ms, leaving
margin to the 15 ms release gate. Any cell scanning more than 2% of rows is
`disguised-exhaustive-scan` and cannot pass regardless of recall.

## Fail-fast qualification funnel

### Gate 0: named correctness test

Every behavior change runs only its named test. Fixtures cover exact schemas,
normalization, deterministic ties, bounded heaps, complete source inventory,
scalar/SIMD equality, canonical evidence, and cleanup. No full workspace suite
or AWS phase runs after an individual fix.

### Gate 1: authentic boundary smoke

One small fixture carries the production Parquet/Arrow physical schemas,
registered first and last row groups, a reduced hierarchy/codebook, and exact
manifest bindings. The direct offline executable stages, authenticates,
scores, serializes, and cleans up one query in under 90 seconds with zero page
GETs.

### Gate 2: open 262,144-row real-data screen

The tunable cohort contains exactly 262,144 hash-ranked rows and 512
leave-self-out pseudoqueries. Exact truth scores 12,884,901,888 dimensions.
The exact-global control requires no hierarchy or codebook. If it passes, the
bounded reduced router uses 4,096 lists of exactly 64 rows and probes 32 lists,
preserving the production 0.782% scan fraction. Controls stop at the first
failure:

1. layout oracle at least 975,000 ppm, with 995,000 ppm as the target;
2. exact-global aggregate at least 975,000 ppm and oracle attainment at least
   995,000 ppm;
3. exact-contained aggregate at least 975,000 ppm and oracle attainment at
   least 995,000 ppm;
4. coded-contained aggregate at least 975,000 ppm and oracle attainment at
   least 995,000 ppm;
5. bounded selector at least 975,000 ppm aggregate and 995,000 ppm oracle
   attainment;
6. report the max-per-page versus mass-fusion contrast on identical exact
   candidates as causal evidence; it is not an additional pass gate;
7. warm production-shape p99 below 12 ms and projected memory at or below the
   registered bound.

The complete screen must take under five scientific minutes on one
`causality` Spot worker. Failure forbids the sealed sentry and all full builds.

### Gate 3: sealed 1,048,576-row sentry

The sentry uses 1,048,576 different hash-ranked rows, 4,096 capacity-bounded
lists, fixed probe count 32, and 1,024 leave-self-out pseudoqueries. Its cohort
and truth remain unreadable until source, manifest, and parameters are
committed. One terminal attempt is allowed per architecture version.

It must reach 975,000 ppm aggregate, 995,000 ppm oracle attainment, the
oracle-relative minimum gate below, warm p99 below 12 ms, and measured peak RSS
below 3 GiB. A pass authorizes a 10-million-row screen, not a release claim.

### Gate 4: full scale and page-budget decision

A passing 10-million-row pseudoquery screen precedes the 100-million-row
campaign. Report the fixed page ladder 8, 12, and 16 without selecting on the
reporting cohort. Freeze the smallest page budget that achieves the recall and
latency gates. Eight pages remains the efficiency target; prerelease policy
does not pretend that an information-theoretically impossible exact-eight-page
perfect-recall claim is attainable. A larger fixed budget is acceptable only
if measured end-to-end p99 remains at most 15 ms and the comparison reports its
additional I/O honestly.

The release gates are at least 975,000 ppm aggregate, 995,000 ppm oracle
attainment, oracle-relative minimum recall at least 800,000 ppm, warm selector
p99 at most 15 ms, and RSS below 3 GiB. The oracle-relative per-query value is
`selected_hits / layout_oracle_hits`; absolute minimum recall remains reported
but cannot reject a query whose layout oracle itself is below ten. An
oracle-relative minimum of 995,000 ppm is the quality target, not the release
floor.

Strict Clippy and the full locked workspace assurance run once immediately
before the immutable release build. They do not run during each scientific
repair.

## Leakage and authority

Training, open development, sealed sentry, full pseudoquery, burned benchmark
development, and final holdout are disjoint SplitMix64 rank intervals under one
committed seed. Training sees corpus vectors only. It cannot read query,
neighbor, page-quality, sentry, development, holdout, or prior-result roles.

Every corpus-derived pseudoquery excludes its own row and its own primary and
replica pages from both the selector and the recomputed layout oracle. The
own-page-included result remains a labeled sensitivity only and cannot pass a
gate. This removes the exact leakage that inflated the V24 screen.

The open cohort is tunable and never reported as unbiased. Each architecture
version consumes one unopened sentry cell. A sentry rejection cannot tune and
rerun that version. Benchmark development opens only after the full
pseudoquery pass, and sealed holdout opens only after one cell is frozen.

Every bulk table has exact field names, order, physical types, nullability,
row count, row-group policy, finite values, complete source ordinals, URI,
encoded length, and SHA-256. Training, hierarchy, code, page-map, query, truth,
and result identities are cross-bound. Results recompute metrics from canonical
Parquet evidence; JSON cannot introduce independent scientific values.

## Spot execution and cleanup

All scientific work uses AWS profile `causality` and EC2 Spot with registered
multi-AZ fallback. No DGX is used. Parallel assignment and encoding begin only
after Gate 3 authorizes full construction. Each phase publishes
content-addressed Parquet/Arrow output and one canonical terminal, then
terminates immediately. An interrupted nonterminal cell may restart from
immutable inputs; a terminal scientific cell never restarts.

Workers stop on RSS cap, swap growth, memory PSI full avg10 above 0.50, 20
minutes without registered progress, or the phase wall cap. Scratch uses one
explicit directory, owns only named files, verifies process clearance, unlinks
those files, and removes the empty directory. No corpus or page body persists
on the devbox. D3 and release claims remain fenced until full scale, sealed
holdout, and a separate end-to-end fetch/decode/rerank latency gate pass.

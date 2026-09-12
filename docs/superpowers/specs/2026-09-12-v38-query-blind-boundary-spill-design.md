# V38 Query-Blind Boundary Spill Design

## Status and purpose

V38 is a claim-ineligible, one-million-row falsifier. It answers one narrow
question left by the rejected V37 layout: can a bounded amount of
query-independent coarse-row replication repair balanced-hyperplane ownership
cuts while preserving the S3 object and write-amplification envelope?

The frozen population is ReLAION2B cohort A: 1,000,000 non-null `f32[768]`
source rows, the registered 1,000 development queries, and exact GT@100. V38
reuses the authenticated V37 projection, ownership tree, and primary ownership.
It changes only the row-to-coarse-posting relation. It does not train or test a
router, encode or fetch fine pages, open validation or holdout, or scale to 10M
or 100M.

The registered logical gates remain 998,000 ppm aggregate containment and
800,000 ppm minimum-query containment at exactly fourteen coarse postings.
They are deliberately not weakened to make this experiment pass.

## Evidence and causal hypothesis

V37 produced 123 single-owner postings and an exact K=14 ownership ceiling of
980,290 ppm aggregate and 700,000 ppm minimum on 100,000 development-neighbour
occurrences. It missed 1,971 occurrences; the aggregate gate permits 200. A
successful layout must therefore recover at least 1,771 of the current misses,
or 89.85%, before allowing any routing loss. The worst query needs ten more
contained neighbours to reach the minimum gate.

The V37 ceiling is exact because every row has one owner. No centroid,
ellipsoid, relation prefix, tree traversal, or other routing score can retrieve
a row that is absent from the selected postings. V38 tests whether the missing
rows are concentrated near balanced-hyperplane cuts and can be repaired by one
geometrically justified alternate coarse owner.

This is not a claim that boundary spill is generally sufficient. A failure
rejects the registered spill rule and budget. It does not reject replication,
graph-derived packing, or all possible high-dimensional layouts.

## Frozen inputs and phase isolation

Construction authenticates and retains file capabilities for these exact V37
objects:

- construction authority and successful construction result;
- ownership tree Arrow IPC object;
- primary ownership Parquet object;
- the original V36 source Parquet object; and
- the projection/numeric authority bound by the V37 construction result.

The construction service cannot read query vectors, GT, development results,
validation, holdout, prior per-query failures, or S3 page bodies. It has no
network capability after controller staging. Paths, modification times, local
directory contents, environment variables, and task scheduling are not
scientific inputs. Each input is opened once, authenticated through that file
descriptor, and never reopened by pathname.

The ceiling phase runs in a fresh capability sandbox. It receives only the
sealed V38 relation artifacts, the exact V36 development-GT object, and a
minimal ceiling authority binding their immutable identities. It cannot read
the source, projection, queries, construction scratch, or prior V37 per-query
samples.

## Registered spill arm

V38 has one arm. No spill-fraction, capacity, seed, or score sweep is allowed
after development evidence opens.

- primary ownership: unchanged V37 posting;
- alternate owners per row: at most one;
- accepted alternate assignments: at most 250,000;
- total assignments: at most 1,250,000;
- stored rows per coarse posting: at most 10,240;
- coarse record payload: exactly 48 bytes before object framing;
- selected coarse postings per query: exactly fourteen;
- duplicate candidate row IDs: deduplicated before containment accounting.

At the posting cap, row payload is `10,240 * 48 = 491,520` bytes, leaving
32,768 bytes inside a 512 KiB coarse-object cap for a complete header, offsets,
checksums, and framing. Any actual object encoding that exceeds 512 KiB rejects
the physical interpretation; fragmentation or hidden overflow objects are not
allowed. The experiment does not replicate fine vectors.

## Alternate-owner geometry

For each projected row `x`, evaluate every internal V37 ownership-tree node
exactly once using the registered fused numeric backend and coordinate order.
At node `v`, let `s_v(x)` be the deployed dot product and `b_v` the persisted
boundary score. The wrong-side margin for a leaf that takes the left branch is
`max(0, s_v(x) - b_v)`; for a leaf that takes the right branch it is
`max(0, b_v - s_v(x))`. Equality has zero margin on either branch. Persisted
normals are unit length under the V37 contract, so no new normalization or
division occurs. For a leaf `p`, define its path-violation score as

```
violation(x, p) = max over nodes v on path(root,p)
                  squared_wrong_side_margin(x, v, p)
```

where `squared_wrong_side_margin` is zero when `x` lies on the side required by
`p`, and otherwise is the square of that finite signed margin. The score is the
exact f32 returned by the V37 fused dot kernel; the boundary is reconstructed
from its persisted f32 bits; subtraction and multiplication each round with
Rust f32 semantics; and path maxima use `f32::total_cmp`. The persisted normal
is used exactly as stored. It is not renormalized and the margin is not divided
by its recomputed norm, because V37 validates approximate unit length rather
than bit-exact unit length. This is a deterministic spill heuristic, not a
certified distance to the leaf region.

The proposed alternate is the nonprimary leaf minimizing
`(violation(x,p), posting_ordinal)`. The implementation may propagate bounded
path maxima down the 123-leaf tree, but it must produce the same result as the
straight exhaustive definition. It must not allocate a row-by-leaf matrix.

For each primary posting, order its rows by
`(alternate_violation, source_ordinal)`. A proposal's local fairness rank is
its zero-based position plus one, paired with the primary population. Globally
order proposals by:

1. exact fraction `local_rank / primary_population`, compared by checked
   integer cross-products;
2. alternate violation under the registered f32 total order;
3. source ordinal; and
4. alternate posting ordinal.

Visit proposals in that order. Accept a proposal when its alternate posting
has fewer than 10,240 total primary-plus-alternate rows. Skip a full target and
do not search for a third leaf. Stop after 250,000 accepted proposals or after
all proposals are exhausted. Primary ownership never changes.

This rule distributes spill opportunities across primary postings rather than
allowing one dense region to consume the budget. Construction records proposed,
accepted, capacity-rejected, and exhausted counts and the complete histogram of
owners per row and rows per posting.

## Numeric determinism

Projection and node scores reuse the V37 numeric contract. Scientific runs
require the registered fused SIMD backend; scalar evaluation is evidence only.
Coordinates, dot products, margins, squared margins, and path maxima must be
finite. Negative zero is canonicalized to positive zero at persistence
boundaries. Ties use persisted tree ordinal, posting ordinal, and source ordinal
exactly as specified above.

The fused and scalar controls must agree on alternate owner and proposal order
for the registered preflight corpus. A disagreement, unsupported backend,
overflowed cross-product, non-finite value, or nondeterministic digest is a
terminal determinism stop.

## Relation artifacts

Cross-language tables use Parquet; dense typed arrays use Arrow IPC; authority,
manifests, progress, results, and terminals use recursively sorted compact JSON
plus one LF. Rust memory layout is never an interchange format.

`spill-relation.parquet` contains exactly one primary record per source row and
zero or one alternate record:

- `source_ordinal:u64`, non-null;
- `feature_row_id:u64`, non-null;
- `posting_ordinal:u32`, non-null;
- `owner_role:u8`, non-null (`0=primary`, `1=alternate`);
- `posting_local_ordinal:u32`, non-null; and
- `alternate_violation:f32`, nullable only for primary records.

Rows sort by `(source_ordinal, owner_role)`. Primary records must equal the V37
ownership object as concrete typed values for every shared column; Parquet file
bytes are not treated as a canonical row comparison. Alternate violations are
finite and nonnegative. Feature IDs are globally unique at the source-row
level, not per assignment. Primary records retain their exact V37
`posting_local_ordinal`. After all accepted owners are frozen, alternates sort
within each target by `source_ordinal` and receive consecutive local ordinals
beginning at that posting's primary population. Every posting's local ordinals
must cover `[0,total_population)` exactly once. Admission order is not a
persisted local ordinal.

`spill-postings.parquet` contains one row per posting with non-null
`posting_ordinal:u32`, `primary_population:u64`,
`alternate_population:u64`, `total_population:u64`,
`projected_payload_bytes:u64`, and `projected_framing_allowance_bytes:u32`.
Rows sort by posting ordinal. Totals must reproduce the relation table;
`projected_payload_bytes` must equal `total_population * 48`; and total
population must not exceed 10,240. The framing allowance is exactly 32,768
bytes. This is a payload capacity screen, not proof of a complete encoded
object size. Later physical qualification must serialize the complete coarse
object and reject it above 512 KiB; it may not use this allowance to omit
metadata or create a side object.

The construction manifest binds every input and output role, URI, SHA-256,
BLAKE3, encoded length, schema, source identity, V37 predecessor, numeric
backend, fixed limits, and construction counts. Input and output role/URI sets
are disjoint. Readers reject aliases, optional fallbacks, unknown fields,
nullability drift, invalid enums, type drift, order drift, digest drift, and
cross-object inconsistencies.

## Multi-owner K=14 evaluator

With replication, summing the fourteen largest posting hit counts is invalid
because a truth row can occur in two postings. Greedy maximum coverage is a
feasible lower bound but is not an exact ceiling.

For each query, construct a weighted graph over the 123 postings. Each of its
100 GT rows contributes one unit-weight loop for a primary-only row or one
unit-weight edge between its primary and alternate postings. Selecting a
posting covers an incident GT row once. The objective is maximum distinct
coverage using at most fourteen postings.

Before any search, the evaluator computes and retains for all 1,000 queries a
greedy feasible selection and the cheap admissible upper bound from the
fourteen largest individual posting hit counts, capped at 100. It reduces all
1,000 lower and upper bounds in query-ordinal order. If the lower selections
already meet both global gates, the layout is feasible with zero DFS visits. If
summed upper bounds or any minimum-query upper bound make a gate impossible, it
is rejected with zero DFS visits. Only when those complete-population bounds
straddle a gate do gate-ambiguous queries enter deterministic depth-first
branch-and-bound in increasing query ordinal over posting ordinals with:

- a greedy incumbent as a feasible lower bound;
- checked distinct-row bitsets for exact objective accounting;
- an admissible upper bound equal to the current branch coverage plus the smaller
  of the still-uncovered row count and the sum of the largest remaining
  per-posting uncovered-row counts for the unused slots; double-counting in
  that sum can only loosen the bound;
- include/exclude branching on the lowest-ordinal posting among those tied for
  the largest uncovered-row count; and
- no dominance transformation or retained priority frontier; and
- a cap of 250,000 branch nodes per query and 25,000,000 nodes over the whole
  evaluation, not a wall-clock-dependent scientific result.

The solver evaluates one query at a time. Its depth-first stack holds at most
124 fixed-size frames and its complete working set, including posting masks,
mutable coverage, undo records, and unexplored-branch upper bounds, is at most
1 MiB. At a node or whole-run cap, the reported upper bound is the maximum of
the incumbent coverage and every admissible bound stored for the current and
unexplored branches on the stack. An empty frontier yields the incumbent and
therefore proves exact completion. A missing bound for a nonempty frontier is
an evaluator error, not `indeterminate`.

Each per-query persisted certificate is at most 256 bytes: the fourteen-posting
feasible selection, its distinct hit count, the certified upper count, exact
flag, and visit count. The complete 1,000-query certificate section is at most
256,000 bytes before canonical JSON framing. Validation receives the
authenticated ceiling authority, sealed relation, and GT inputs; reruns the
same deterministic evaluator under the same visit limits; and requires byte
equality with the claimed canonical result. It thereby recomputes selections,
frontier bounds, aggregate reductions, visit accounting, and disposition rather
than trusting receipt claims. The compact result is not treated as a
stand-alone proof without those authenticated inputs.

Tiny fixtures independently enumerate every subset and include a case where
greedy is suboptimal. For a scientific query, the evaluator returns an exact
optimum when lower and upper bounds meet. Otherwise it preserves both bounds
and reports `indeterminate`.

The layout is feasible only when actual selected posting sets establish at
least 99,800 contained occurrences across 1,000 queries and at least 80 for
every query. It is rejected when certified per-query upper bounds make either
gate impossible. If bounds straddle a gate when the node cap is exhausted, the
whole experiment stops as `indeterminate`; an incumbent is never called an
exact ceiling.

The result records aggregate and minimum lower and upper bounds, exact-solved
query count, empirical lower-bound p1/p10/median/max, selected-posting counts,
duplicate suppression, solver visits, and every per-query certificate. The
canonical result is claim-ineligible and binds the exact ceiling authority and
all prerequisites.

## Resource and work bounds

The resident V37 build measured 2,716,549,120 bytes of peak cgroup memory,
578,934,000 bytes above its 2,137,615,120-byte projection and only 504,676,352
bytes below 3 GiB. V38 therefore does not admit new resident row-by-leaf,
per-row map, graph, or unbounded sort structures.

The spill implementation may add at most 64 MiB of packed owner, proposal,
capacity, and sorting state. Proposal ordering uses bounded external runs when
the checked resident projection would exceed that amount. Monitoring records
`memory.current`, `memory.peak`, anonymous/file/kernel components, swap, and
PSI separately; it does not label cgroup memory as RSS.

At 1M, evaluating 122 internal nodes over 192 projected dimensions costs at
most 23,424,000,000 coordinate multiply-accumulates plus bounded propagation
and sorting. The source-free preflight executes the actual scoring and ordering
path and must project no more than 300 seconds for scoring and 450 seconds total
construction. The one-million construction retains the 600-second scientific,
720-second wrapper, 120-second progress, 3 GiB cgroup-memory, PSI-full-avg10
0.75, and zero-swap-growth stops.

The ceiling evaluator has a 120-second scientific cap, 180-second wrapper cap,
30-second progress timeout, 256 MiB cgroup-memory cap, PSI-full-avg10 stop above
0.75, and zero-swap-growth stop. A controller/resource stop is infrastructure
failure and does not convert an unknown scientific bound into a rejection.

At 100M, assignment payload is at most 6 GB before framing, but projected
coordinates would be 76.8 GB. Any later scale builder must stream Arrow shards,
emit external proposal runs, and merge bounded partitions. The resident 1M
implementation is not accepted as the 100M builder.

## Fast-fail execution ladder

1. **Static authority and arithmetic tests.** Validate exact schemas, canonical
   bytes, identities, role disjointness, checked limits, posting capacity, and
   result recomputation.
2. **Tiny exhaustive correctness tests.** Differentially compare alternate
   selection, fairness ordering, capacity admission, and branch-and-bound
   coverage against independently enumerated scalar references. Include ties,
   duplicates, saturated postings, degenerate geometry, and greedy-suboptimal
   coverage.
3. **Source-free native preflight.** Exercise the actual fused scorer,
   propagation, external ordering, Parquet writers, and evaluator on synthetic
   registered shapes. Freeze the binary, authority, resource projection, and
   node-visit cap.
4. **One corpus-only 1M build.** Stream the frozen ReLAION source in the same
   region. Seal and authenticate the V38 relation and posting summary. Any
   resource, determinism, capacity, or integrity failure stops the experiment.
5. **One GT-only 1M ceiling.** Evaluate the already sealed layout on the same
   1,000 development queries. A certified fail or indeterminate result stops
   V38. No alternative spill budget is tried.
6. **Only a logical pass opens later qualification.** Routing attainment,
   encoded coarse objects, fine-page selection, S3 request counts/bytes,
   throughput, write amplification, updates, deletes, compaction, validation,
   holdout, 10M, and 100M each remain separately fenced.

The expected feedback loop is seconds for unit gates, under a minute for
source-free preflight, roughly two minutes of wrapper time for the authenticated
1M build if V37 staging dominates, and seconds to minutes for the ceiling
depending on solver certificates. Full repository gates run once only after a
stable implementation diff.

## Serving and write implications

A passing coarse layout would allow the small V37 tree to remain resident.
Tree routing could select coarse postings containing primary and spill code
records without downloading the corpus. Coarse candidates carry stable row IDs
and fine-page references; the later fine stage must fit its own fourteen-GET,
7 MiB wave and deduplicate candidates. V38 does not assume that logical
containment guarantees fine-page concentration.

For writes, immutable layout epochs publish primary and alternate visibility
atomically. New rows enter bounded deltas; queries search base plus registered
deltas within the same object budget. Deletes and reinserts use stable ID plus
generation and cannot expose only one owner. Compaction recomputes spill from
corpus geometry without queries or GT. These properties require later tests for
skewed inserts, duplicate-heavy data, partial publication, replica failure,
delete/reinsert, and recall decay before release.

## Leakage and generalization

Construction is query-blind and GT-blind. Nevertheless, the 1,000 development
queries have already influenced the sequence of architectures and are not fresh
generalization evidence. V38 is therefore a falsifier only. It must not inspect
individual missed IDs, tune the 25% budget, select a second arm, or claim a
release result from this split.

If the registered rule passes, it is frozen before any validation or holdout
opens. A later release claim requires independent datasets, dimensions, metrics,
filters, update workloads, and held-out queries. If it fails, the next family is
query-independent approximate-neighbour graph or hypergraph page packing, not a
post-hoc threshold adjustment.

## Terminal dispositions

Every run emits exactly one authenticated terminal disposition:

- `preflight-rejected` for numeric, resource, determinism, or throughput
  failure;
- `construction-rejected` for integrity, capacity, publication, or resource
  failure;
- `layout-rejected` only from certified upper bounds below a quality gate;
- `indeterminate` when solver bounds straddle a gate at the registered cap;
- `layout-feasible` only from explicit feasible selections meeting both gates;
  or
- `infrastructure-failed` for a bounded controller or platform failure that
  carries no scientific conclusion.

No disposition launches another arm, router, larger population, or paid run.

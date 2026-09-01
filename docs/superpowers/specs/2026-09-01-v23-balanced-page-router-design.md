# V23 Balanced Page Router and Layout Falsifier Design

**Date:** 2026-09-01

**Status:** Approved design for a claim-ineligible development falsifier

## Purpose

Build the smallest credible falsifier for a production read layout that can
meet BORSUK's frozen Deep Image constraints at 100 million rows:

- the smallest preregistered page budget in `8, 12, 16` that passes every
  quality and resource gate;
- at least 99.375% aggregate recall@10 (`318/320`) on the burned development
  cohort;
- at least 90% recall@10 for every development query;
- at least 99.5% attainment of the layout oracle;
- less than 15 ms resident CPU p99;
- less than 3 GiB projected serving RAM; and
- at most 16 page GETs and 1,966,080 projected encoded page bytes per query;
- no benchmark-query-dependent construction or parameter selection.

This is an architectural experiment, not a compatibility extension. BORSUK is
pre-release. The falsifier receives a new schema family and does not read or
write legacy layouts through aliases, migrations, or fallback paths.

## Evidence and causal diagnosis

The authenticated fixed-reducer RaBitQ result establishes three facts:

1. An exhaustive exact-f16 scan recovers all 318 oracle-reachable neighbors.
   The top-ten reducer is correct.
2. The best exact tree cell recovers only `259/320`, despite remaining below
   the 15 ms diagnostic timing boundary. The current hierarchy does not retain
   enough top-ten evidence in its serving candidate set.
3. The paired RaBitQ cell recovers `218/320`, so the tested estimator adds a
   second loss but is not the first blocker.

The older page-summary experiments also rejected one centroid, farthest-point
representatives, K32 prototypes, and per-page minimum ADC over the existing
pages. Those pages were created independently of the router and are too
geometrically diffuse for compact summaries to rank reliably.

The next architecture must therefore construct routing regions and physical
pages as one object. Merely increasing a beam, retaining more rows, scanning
all leaves, or compacting the current 65,536 leaves is rejected: those choices
either violate the CPU gate or preserve the known containment defect.

## Considered approaches

### Selected: balanced two-level spherical pages

Construct balanced supercells and balanced pages from normalized corpus
vectors. At query time, score every supercell centroid, score page centroids
under the best supercells, and select the frozen budget of 8, 12, or 16 pages.
A single query-independent margin replica can bridge a page boundary.

This makes page membership directly routable, keeps serving state proportional
to page count rather than row count, and has enough measured SIMD headroom for
the 15 ms gate.

### Deferred: witness routing

A sampled row graph whose witnesses post neighbor-page labels is genuinely
different and fits the memory gate, but it requires a large query-independent
k-nearest-neighbor construction and two approximation layers. It is the next
architecture only if balanced pages fail their layout gate.

### Deferred: learned page rescorer

An extreme multi-label or bilinear page rescorer can operate over the selected
approach's page shortlist. It is not the first falsifier because it adds a
training pipeline and leakage surface before geometric containment has been
proved. It may be considered only if the layout oracle passes and the fixed
geometric page selector narrowly fails.

## Scope

The falsifier constructs and evaluates compact routing metadata. It does not
materialize page bodies, publish a production index, open the sealed holdout,
run D3, or make release claims.

It reuses:

- the authenticated Deep Image source-shard manifest and strict Parquet
  reader;
- the authenticated f16 corpus control and deterministic reservoir seed;
- the frozen query and neighbor Parquet artifacts;
- fused 8x12 SIMD scoring and scalar differential controls;
- bounded Arrow external-sort runs;
- exact bounded-page coverage for each preregistered budget;
- canonical typed JSON manifests and receipts; and
- existing resource-stop and Spot orchestration patterns.

The new work is limited to balanced supercell/page construction, margin
replicas, strict bulk artifacts, causal evaluation, and thin local/AWS
orchestration.

## Construction geometry

### Normalization and identity

Every corpus vector is a finite, nonzero 96-dimensional float32 vector. It is
L2-normalized once with the existing deterministic kernel. `source_ordinal` is
the immutable Deep Image row identity and must be strictly increasing across
the registered source shards.

The construction manifest binds the exact source commit, source archive,
dataset identity, ordered source-shard identities, f16 control identity, query
and neighbor identities, deterministic seed, shape, output roles, scratch
limit, the exact page-budget ladder `[8, 12, 16]`, and resource-stop envelope.
It does not carry a legacy scalar `selected_pages` field. Query and neighbor
bytes are not available to construction or pseudoquery selection processes.

### Deterministic training split

The existing 2,097,152-row hash reservoir is divided by the registered source
hash:

- 2,096,128 rows train the geometry; and
- 1,024 rows form a corpus-derived pseudoquery cohort.

The pseudoquery rows are excluded from centroid fitting. They are still normal
corpus rows and are assigned to final pages. Their exact leave-self-out top ten
is computed only against the training reservoir. This cohort selects an
amplification arm without exposing any official query or neighbor artifact.

### Supercells

For corpus cardinality `N`, the production supercell count is

```text
S = min(8192, next_power_of_two(ceil(N / 12,288)))
```

This yields 1,024 supercells for 9,990,000 rows and 8,192 for 100 million rows.
Reduced test shapes use explicit test-only counts and cannot serialize as a
production receipt.

A balanced binary spherical tree is fitted to the training reservoir with the
existing deterministic seed, four Lloyd refinements per split, fused SIMD
distance calculations, and source-ordinal tie breaking. The full corpus is
routed through the tree once. A production construction is rejected unless
every supercell contains from 6,144 through 24,576 primary rows after the
complete stream; changing this bound requires a new schema and experiment.

The persistent representation stores the final f16 centroid and the maximum
cosine radius of every supercell, both recomputed from its complete routed
population. The serving loader normalizes and expands every authenticated f16
centroid to f32 once before timing; query execution never decodes f16 values.
The binary tree remains construction/write-routing evidence but is not used by
the read selector. Reads exhaustively score all `S` supercell centroids and
rank them by `(max(0, centroid_distance - cosine_radius), supercell_ordinal)`,
which removes both hierarchical pruning and centroid-only containment as causal
variables.

### Pages

Rows are externally sorted by `(supercell_ordinal, source_ordinal)`. Each
supercell with `n` primary rows creates exactly `ceil(n / 384)` pages. Existing
deterministic spherical bisection partitions the rows so page primary counts
differ by at most one and never exceed 384. Empty pages are forbidden.

The total page count is bounded by

```text
ceil(N / 384) + S - 1
```

which is at most 268,608 pages at 100 million rows. Page ordinals are assigned
by `(supercell_ordinal, local_page_ordinal)`. Each page records its primary
centroid, primary population, and its contiguous supercell range.

### Margin replicas

Each row is routed through the construction tree with a fixed beam of two final
supercells. The first result is its authoritative primary supercell and the
second result is its boundary-consistent runner-up supercell. The row then
scores every page centroid in those two supercells. Its assigned balanced
primary page remains authoritative even when duplicate vectors create tied
centroids. The closest distinct page, ordered by `(distance, page_ordinal)`, is
its replica candidate. The margin key is

```text
(second_distance / max(primary_distance, f32::MIN_POSITIVE), source_ordinal)
```

Nonfinite distances and negative distances below `-16 * f32::EPSILON` are
rejected. Smaller negative roundoff is clamped to zero. A row can receive at
most one replica and never replicates into its primary page.

Replica candidates are sorted in bounded Arrow IPC runs and merged with fan-in
at most 64. The primary page plus three replica decisions use one fixed
`[uint32; 4]` table indexed by authenticated source ordinal: exactly 16 bytes
per corpus row, or 1.6 GB at 100 million rows. This construction-only
allocation is preregistered separately from serving RAM and cannot grow with
candidate count. Every replay of routed rows is digest-bound to the first pass.
At the registered production shape of 384 primary rows/page and at most 268,608
pages, the concurrent selection table, three f64 centroid sum tables, arm page
tables, and primary centroid tables project below 2.6 GB (2.43 GiB) before
allocator overhead. The bounded candidate run buffer is dropped before those
tables are allocated. Every construction-sized allocation is fallible; the
external pressure monitor remains authoritative for stopping construction
before host exhaustion. This construction peak is not serving state and is
separate from the 3 GiB serving gate. Reduced deterministic test shapes may use
smaller pages but do not define the production memory projection.

Three fixed arms are constructed from the same sorted margin candidates:

| Arm | Maximum occurrences | Maximum replicas/page |
|---|---:|---:|
| `amp-1125` | 1.125x primary rows | 48 |
| `amp-1250` | 1.250x primary rows | 96 |
| `amp-1500` | 1.500x primary rows | 192 |

Candidates are accepted in margin order while both the global occurrence cap
and target-page cap permit them. A cap can reduce but never increase an arm's
actual amplification. Arm and page-budget selection use this fixed order:

```text
(8, amp-1125), (8, amp-1250), (8, amp-1500),
(12, amp-1125), (12, amp-1250), (12, amp-1500),
(16, amp-1125), (16, amp-1250), (16, amp-1500)
```

The first pair that passes the pseudoquery gates is frozen. This minimizes
GET count and projected page bytes first and amplification second without
consulting the burned development cohort. If no pair passes, construction is classified
`pseudoquery-layout-rejected`; the burned cohort is never opened.

For every arm the selector produces one deterministic ranked page list and
evaluates its 8-, 12-, and 16-page prefixes. All nine pair metrics are
materialized before applying the fixed selection order. The selected page
budget and arm are bound into the construction terminal receipt before the
official query and neighbor capabilities become available.

After replicas are assigned, the final page centroid and maximum cosine radius
are recomputed from all primary and replica occurrences. Each page contains at
most 576 occurrences. The falsifier records projected page bytes but does not
write page bodies.

## Persistent artifacts

Persistent bulk data uses Parquet. Arrow IPC is allowed only for bounded
scratch sort runs that are deleted after terminal receipt and PID clearance.
Small authority, progress, stop, and result documents use typed canonical JSON
with one trailing newline.

### `supercells.parquet`

Exactly one row per supercell, ordered by ordinal:

- `supercell_ordinal: uint32`, non-nullable;
- `centroid: fixed_size_list<float16>[96]`, non-nullable with non-nullable
  `element` child;
- `cosine_radius: float32`, finite, nonnegative, non-nullable;
- `primary_rows: uint64`, non-nullable;
- `first_page: uint32`, non-nullable; and
- `page_count: uint32`, non-nullable.

### `pages-<arm>.parquet`

Exactly one row per page, ordered by ordinal:

- `page_ordinal: uint32`, non-nullable;
- `supercell_ordinal: uint32`, non-nullable;
- `primary_rows: uint16`, non-nullable;
- `replica_rows: uint16`, non-nullable;
- `centroid: fixed_size_list<float16>[96]`, non-nullable with non-nullable
  `element` child; and
- `cosine_radius: float32`, finite, nonnegative, non-nullable.

### `row-pages-<arm>.parquet`

Exactly one row per unique corpus row, ordered by source ordinal:

- `source_ordinal: uint64`, non-nullable;
- `primary_page: uint32`, non-nullable; and
- `replica_page: uint32`, non-nullable, with `uint32::MAX` as the sole no-replica
  sentinel.

The sentinel cannot equal a real page ordinal. Primary and replica pages must
be distinct and in range. Each page's recomputed primary/replica counts must
equal `pages-<arm>.parquet`.

### Receipts

Every artifact has an ordered role, absolute S3 URI, digest algorithm, lowercase
digest, and encoded length. Parquet and JSON use SHA-256. Scratch Arrow runs
use BLAKE3 internally but are never registered outputs. Receipts independently
recompute schemas, row counts, page counts, amplification, memory projections,
and every parent/output binding before canonical serialization.

Each future production page body is bounded to 122,880 encoded bytes. The
selected budget is therefore both the exact GET count and a conservative byte
projection of `selected_page_budget * 122,880`, capped at 1,966,080 bytes. This
projection includes up to 576 occurrences of one `uint64` source ordinal plus
one 96-dimensional f16 vector and bounded page metadata. The falsifier records
the projection; a later page-body integration must authenticate the actual
bytes before any production or D3 qualification.

The typed result records `selected_page_budget: uint8` and rejects values
outside `8, 12, 16`. Every sample must contain exactly that many distinct,
in-range page ordinals. Serialization independently recomputes the selected
pair, sample hits, aggregates, oracle attainment, timing evidence, memory
and page-byte projections, and causal class; changing the budget or any
dependent field without recomputing all authority is rejected.

## Pseudoquery arm selection

For each of the 1,024 pseudoqueries, the corpus-routing pass also computes ten
exact full-corpus neighbors with bounded `(distance, source_ordinal)` heaps.
The pseudoquery's own source ordinal is excluded from its neighbor heap. Their
new page assignments define the layout oracle. The serving selector described
below runs without access to those neighbor IDs.

This exact pseudoquery control performs exactly `1,024 * N * 96`
scored dimensions and uses a bounded top-ten heap; it cannot allocate or sort
all row-distance pairs. Its work count, backend, scalar differential, and
leave-self-out evidence are registered before arm selection.

An arm/budget pair passes pseudoquery selection only if it meets all of:

- aggregate recall@10 at least 993,750 ppm;
- minimum per-query recall@10 at least 900,000 ppm;
- oracle attainment at least 995,000 ppm;
- exactly its preregistered 8-, 12-, or 16-page budget for every query;
- projected encoded page bytes at most 1,966,080;
- at most 4,000,000 scored dimensions per query; and
- its registered amplification and per-page occurrence caps.

The first passing pair is frozen before the official query and neighbor objects
become readable. No parameter can be changed after opening the burned cohort.

## Serving selector

The read selector performs these deterministic steps using the page budget
frozen by pseudoquery selection:

1. normalize the query;
2. fused-SIMD score all supercell centroids by cosine distance;
3. retain the 96 smallest
   `(max(0, distance - cosine_radius), supercell_ordinal)` pairs;
4. enumerate every page belonging to those supercells;
5. fused-SIMD score page centroids;
6. rank pages by
   `(max(0, centroid_distance - cosine_radius), page_ordinal)`; and
7. return the first `page_budget` distinct page ordinals.

During pseudoquery selection, fewer candidates than a candidate budget fails
only that pair. After selection, fewer candidates than the frozen budget, a duplicate, a nonfinite score,
backend drift, or scalar/SIMD page disagreement is a terminal authority
failure. The scientific
timing includes normalization, both centroid stages, bounded selection, and
page ordering. It excludes page GETs and exact reranking, which are separately
projected for the frozen page budget and require a new production page-body
integration after this claim-ineligible falsifier passes.

The same geometry supports high-throughput writes without an 8,192-centroid
scan: writes traverse the balanced construction tree and score only the pages
inside one supercell. This is a projection until the falsifier passes; no write
performance claim is made by this experiment.

## Causal development evaluation

Only after the selected pair and its terminal construction receipt are frozen
may the development process authenticate and open query ordinals 0--31 and
their neighbor Parquet rows.

For each query it records three controls:

1. **Layout oracle:** exact optimal cover of the ten ground-truth rows' primary
   and replica pages across all pages.
2. **Supercell containment:** exact optimal cover restricted to pages under the
   selected 96 supercells.
3. **Serving selector:** the fixed centroid-minus-radius result at the frozen
   page budget.

Classification precedence is fixed:

- invalid authority, schema, resources, or scalar/SIMD evidence:
  `authority-stop`;
- layout oracle below `318/320` aggregate or below `9/10` minimum:
  `balanced-layout-rejected`;
- layout passes but supercell containment misses any oracle-reachable hit:
  `supercell-containment-rejected`;
- containment passes but the serving selector misses the quality or timing
  gate: `page-selector-rejected`;
- every gate passes: `balanced-page-candidate`.

The serving candidate requires:

- at least `318/320` hits;
- at least `9/10` hits for every query;
- at least 995,000 ppm attainment of the new layout oracle;
- exactly the frozen 8-, 12-, or 16-page budget per query;
- less than 15 ms resident CPU p99 over at least 10,000 raw iterations after
  1,024 warmups;
- identical scalar and fused-SIMD pages;
- at most 4,000,000 scored dimensions per query;
- less than 3 GiB projected 100M serving RAM; and
- at most the frozen budget's 8, 12, or 16 page GETs and at most 1,966,080
  projected encoded page bytes; and
- the selected pair's amplification cap.

A failed burned confirmation ends this architecture. It does not authorize
trying a larger page budget, a new amplification arm, supercell count,
top-supercell count, radius rule, or threshold on the same 32 queries.

## 100M projections

At 100 million rows:

- supercells: 8,192;
- primary pages: at most 268,608;
- supercell centroid dimensions/query: `8,192 * 96 = 786,432`;
- maximum candidate pages: `96 * 64 = 6,144`, enforced by a maximum 24,576
  primary rows per supercell;
- page centroid dimensions/query: at most `6,144 * 96 = 589,824`;
- total scored dimensions/query: at most 1,376,256;
- serving f32 supercell centroids plus radii/ranges: at most 3,276,800 bytes;
- serving f32 page centroids: at most 103,145,472 bytes;
- page radii/counts/ranges and object references: at most 17,190,912 bytes; and
- conservative tree, selector workspaces, page cache, and runtime reserve:
  less than 850 MiB total.

The measured ARM fused throughput of roughly 336 million dimensions/second
projects the two centroid stages near 4.1 ms before bounded selection. The
15 ms gate is measured rather than inferred.

## Resource and execution policy

Before any corpus construction, a reduced deterministic shape must prove:

- bounded sort-run bytes and deletion;
- exact single consumption of each source row;
- balanced page cardinality;
- all three replica caps;
- deterministic artifacts across worker counts;
- scalar/fused equality;
- exact work and memory projections; and
- canonical stop receipts for timeout, RSS, PSI, swap, and progress stalls.

The development construction uses one Spot instance under AWS profile
`causality`, in the source bucket's region. It authenticates all inputs before
scientific work, writes progress and terminal evidence to S3, and terminates
immediately after completion or stop. An interruption invalidates the cell; it
may be restarted only as a new attempt under the unchanged manifest.

No incomplete query metrics may be inspected. Construction health is observed
only through registered progress, resource pressure, EC2 health, and terminal
markers.

## Testing strategy

Implementation follows strict TDD with independently reviewable slices:

1. authority, schemas, projections, and canonical receipts;
2. deterministic supercell training and exhaustive read scoring;
3. balanced page construction and bounded external sort;
4. replica arms and final centroid/radius recomputation;
5. strict Parquet encoders/readers and mutation matrices;
6. pseudoquery selection and outcome-blind arm freezing;
7. causal development evaluator and canonical result;
8. local direct executable with no storage/page-body surface;
9. Python source-stream and Spot orchestration; and
10. repository-wide fmt, Ruff, syntax, Clippy, and locked test gates.

Reduced fixtures must exercise ties, empty/oversized regions, nonfinite values,
cardinality drift, source-order drift, schema/nullability/type mutations,
digest and URI drift, replica saturation, scalar/SIMD drift, and all causal
classification branches.

## Pass and kill decisions

If `balanced-page-candidate` passes, the exact source revision and format are
frozen for a separate production page-body integration and sealed holdout plan.
Budgets 12 and 16 deliberately supersede the historical eight-page-wave
contract; the old D2/D3 harness cannot qualify them. A new versioned
page-body/read-wave contract must prove the 122,880-byte page cap, frozen GET
count, 1,966,080-byte wave cap, transient capacity, and failure behavior before
any D3 or competitor claim is authorized.

If the layout oracle fails, balanced geometric pages are rejected and witness
routing is the next architecture. If containment passes but the page selector
fails narrowly, a separately designed page rescorer may be considered. No
failed result is repaired by changing the frozen page budget, 15 ms, 3 GiB, or
the frozen quality gates.

# V26 Dual-Tree Neighborhood Page Layout

**Date:** 2026-09-02

**Status:** Approved prerelease falsifier design

## Decision

V26 replaces the inherited physical page layout before it builds another row
router. V25 proved that the inherited 10-million-row page identities, when
restricted to a 262,144-row Deep Image cohort, give an exact eight-page oracle
of 953,125 ppm and exact-global rank reduction of 644,921 ppm. That cohort is a
sparse projection of the old pages rather than a native 262,144-row rebuild, so
it is a valid failure of the tested V25 artifact but not evidence that the
incumbent packing recipe loses at native density. V26 therefore treats
953,125 ppm only as the rejected V25 artifact result and uses the absolute
995,000 ppm target, rather than a cross-density comparison, as its promotion
gate.

The next falsifier builds two independent, deterministic balanced projection
trees from construction vectors only. One tree assigns each row's primary page;
the other assigns its replica page. Pages from the two trees occupy disjoint
ordinal ranges and contain at most the registered page capacity. The open
development screen tests the ascending capacity ladder
`704, 768, 896, 1,024, 1,408, 2,048, 2,816, 4,096, 8,192` and stops at its
smallest passing member.
Every row therefore has exactly two
page choices without exceeding the existing approximately 1.86-copy storage
shape by more than the explicitly reported delta.

V26 is a clean format. It has no V24/V25 reader, migration, alias, version
dispatch, or duplicate writer. Bulk cross-language artifacts are Parquet or
Arrow IPC. JSON is limited to small manifests, progress, receipts, and results.

## Why this architecture

Three replacements were considered:

1. **Dual balanced projection trees (selected).** They directly preserve local
   neighborhoods, give each row two independent page choices, guarantee page
   capacity, and naturally provide a bounded query router.
2. **Centroid microcluster packing.** It is cheaper to train, but centroid-cell
   boundaries can reproduce the measured neighbor fragmentation and require a
   second within-cell layout mechanism.
3. **Replication-first repair of V24 pages.** It can improve a fixed oracle, but
   it spends storage around a layout already proven structurally inadequate and
   has no clean query-routing story.

The selected design is the smallest change that attacks the causal failure and
can later serve within eight page reads.

## Leakage and phase capabilities

The layout builder receives only the authenticated construction Parquet,
source-map Parquet, layout manifest, and an empty output directory. The manifest
binds the exact URI, SHA-256, byte length, role, and generation of the executable
and both Parquets;
coherent substitution of another valid input is rejected before parsing. It cannot
open pseudoqueries, truth, evidence, prior results, benchmark queries, neighbors,
or page-quality metrics. File roles, exact path inventory, and byte identities
are authenticated before vectors are parsed. The query and truth Parquets are
made available only to a separate evaluator after the layout terminal exists.
For the structural smoke only, `expected_rows` selects the exact leading source
ordinal range from those same authenticated construction and source-map files;
the two full-file row counts must match and both must cover that prefix.

The original 512 corpus-row pseudoqueries are retired from V26 evaluation.
They make the registered 975,000 ppm gate impossible after their own rows and
pages are excluded: at the selected 2,816-row capacity, 443 of 512 queries have
at least one exact neighbor on a forbidden page and the resulting exact
eight-page ceiling is only 951,562 ppm. Lowering the gate or retaining two
incompatible exclusion rules would conceal the protocol defect.

The replacement open cohort is query ordinals `0..512` from the immutable
Deep Image `test.parquet` (SHA-256
`296d45828020c1c0b88c6a1d5c822f6283280513b8c58d01cfa961f3a139a5d4`,
3,843,448 bytes). It is external to the 262,144-row construction and therefore
has no construction `source_ordinal` and no own-page exclusion. After a layout
terminal closes, a separate truth phase exact-scans the frozen construction
for each external query and writes deterministic top-ten
`(distance_bits, source_ordinal)` pairs to Parquet. Distance ties break by
source ordinal. The query cohort, construction identity, metric, f32 kernel,
and truth serializer are fixed before any page-quality result is opened.

The builder has no query or truth capability. The truth phase has construction
and query capability but no page assignment, layout result, or router
capability. The evaluator receives the closed truth and layout only. Page
capacity is selected solely by the preregistered ascending ladder on this
burned open cohort; all later parameters are frozen before a disjoint sentry.
The sentry uses a preregistered SplitMix selection and permits no retry.

## Deterministic layout algorithm

Let `C` be one member of the exact registered ladder
`[704, 768, 896, 1024, 1408, 2048, 2816, 4096, 8192]` rows per physical page and
`L = ceil(row_count / C)` leaves per tree. Tree A uses seed
`0x5632362d54524545`; tree B uses `0x5632362d5245504c`. Each node owns a stable
ascending list of source ordinals and a registered number of descendant leaves.

For a node requiring more than one leaf:

1. Generate 16 dense sign directions from `(tree_seed, node_ordinal,
   direction_ordinal, dimension)` with SplitMix64. Each coordinate is exactly
   `-1` or `+1`; no fitted query or outcome data enters a direction.
2. Score every row with a fixed 96-step f32 accumulation. For each direction,
   identify the exact registered split rank and compute the adjacent score gap.
3. Select the direction with the largest finite gap, breaking ties by direction
   ordinal. Sort rows by `(score, source_ordinal)`.
4. Split at
   `left_rows = min(rows - right_leaves, left_leaves * C)`, where
   `left_leaves = leaves / 2` and `right_leaves = leaves - left_leaves`.
   This guarantees at least one row per leaf and no leaf above `C`.
5. Number internal nodes and leaves by preorder. Leaf page ordinals follow leaf
   order. Tree B page ordinals begin at `L`, so primary and replica pages cannot
   coincide.

The stored threshold is the selected ordering's left maximum score. A query
with score less than or equal to the threshold descends left; a greater score
descends right. Best-first traversal records the absolute f32 distance to that
threshold as the sibling margin and breaks equal margins by
`(tree_ordinal, node_ordinal)`. These rules are part of the format rather than
implementation choices.

The selected adjacent split gap is stored on every internal node. A zero gap is
valid and later causes best-first routing to enqueue the sibling with zero
margin; it is never silently represented as a separating hyperplane.

All integer derivations are checked. Scores must be finite. Empty nodes,
duplicate/missing ordinals, capacity overflow, nondeterministic bytes, and
primary/replica overlap are authority failures. A one-worker and four-worker
reduced harness must produce byte-identical trees and assignments.

At the original `C=704`, the open cohort has 373 leaves per tree and 746
physical pages. Across the registered ladder, 100 million rows project from
284,092 pages at `C=704` down to 24,416 pages at `C=8,192`; the largest page is
3,145,728 raw vector bytes and eight such pages are 24 MiB. Two copies store
exactly 200 million row occurrences; the manifest reports this 7.303%
increase over V24's 186,387,497.497-row 100-million-scale projection rather
than hiding it.

## Fail-fast evaluation

The same frozen 262,144-row construction and source map are reused by exact
identity. Evaluation uses the first 512 immutable external test queries and a
new exact-truth Parquet bound to those queries and that construction. Bulk data
remains Parquet; JSON is used only for small authority and terminal records.

1. **Named contract gate:** synthetic reducer/tree/codec tests run in the small
   V26 crate and must rerun warm in under one second.
2. **Authentic 4,096-row smoke:** read the first 4,096 source ordinals from the
   authenticated construction and source-map Parquets, build both trees, and
   verify every assignment and output identity. This is a structural boundary,
   not a recall claim: it opens no pseudoquery or truth role. Wall time is below
   30 seconds, RSS below 512 MiB, and page reads are zero.
3. **External truth screen:** after construction closes, exact-score all
   262,144 construction rows for each of the 512 external queries with the same
   registered f32 distance kernel. Persist the exact top ten distance bits and
   source ordinals. This phase opens no layout or page artifact.
4. **Layout-only 262,144-row screen:** join only the closed external truth to
   each closed page assignment. Starting at 704, advance through the exact
   capacity ladder and stop at its smallest passing member. The oracle may
   select at most eight unique pages; among equal-hit covers it prefers fewer
   pages and then the lexicographically smaller vector. Report the 975,000 ppm
   lower floor, but stop unless aggregate oracle recall reaches the 995,000 ppm
   promotion target and minimum-query recall reaches 800,000 ppm. A result
   between the floor and target is useful evidence, not authority to build the
   router.
5. **Exact-global screen:** only after layout passes, exact f32 scoring evaluates
   rank limits `10, 32, 128, 512, 2,048, 4,096`. Stop unless aggregate recall is
   at least 975,000 ppm and oracle attainment at least 995,000 ppm.
6. **Tree-router screen:** only after exact global passes, route each query with
   a fixed best-first margin heap and select exactly eight leaves across both
   trees. No outcome-dependent widening or exhaustive fallback is allowed.

The first failing class has strict precedence: `authority-stop`,
`layout-rejected`, `rank-reducer-rejected`, `tree-router-rejected`, or
`bounded-layout-candidate`. Every result is claim-ineligible.

The exact-global evidence persists the first ten ranked source ordinals,
distance bits, and page assignments. A truth-rank injection control must
independently prove reducer bindings. External queries have no construction
row or page identity, so adding an own-row or own-page exclusion is an authority
failure rather than a tunable option.

## Serving projection

The two tree shapes contain at most 284,090 internal nodes at 100 million rows.
Each node stores two `u32` children, one f32 threshold, and one `u8` direction
ordinal plus padding: at most 16 bytes, or 4,545,440 bytes total. Direction
vectors are regenerated from the registered seed and node ordinal, so no
96-dimensional planes are resident. Leaf ranges require at most 2,272,736
bytes. Two row page ordinals require 800,000,000 bytes.

A query descends both trees and best-first expands sibling margins until eight
unique leaves are selected. At depth at most 19, this performs fewer than 64
fixed 96-step projections and maintains an eight-entry output plus bounded
64-entry heap. It scans no row codes before page selection. The tree and page
references add under 808 MiB; the complete later production projection must
remain below 3 GiB and warm selector p99 below 12 ms before any 15 ms release
claim.

## Open-screen resources and execution

The in-memory 262,144-row builder holds approximately 101 MiB of f32 vectors,
two 2 MiB ordinal buffers, projection scores, Parquet decode buffers, and bounded
sort scratch. Its hard RSS cap is 1 GiB. The registered upper work is
`2 * 16 * 19 * 262144 * 96 = 15,300,820,992` multiply-add steps before shrinking
nodes are accounted for; actual work must be reported. Wall and no-progress
caps are five minutes, memory PSI full avg10 is at most 0.5 percent (encoded in
receipts as integer milli-percent, maximum 500), and swap growth is zero.

Scientific execution uses one `causality` EC2 Spot worker with multi-AZ fallback,
immutable inputs, one original process, terminal upload, and immediate instance
termination. Interrupted nonterminal construction may restart from the same
manifest; a terminal scientific result never restarts. No DGX, On-Demand
default, page-body read, or devbox corpus persistence is allowed.

## Evidence contract

The construction terminal binds source commit/archive, binary, manifest, input
Parquets, seeds, page capacity, tree/assignment Parquets, row/page counts,
actual projection work, elapsed/CPU/RSS/PSI/swap, and zero query-role opens. The
truth terminal binds the construction terminal, external-query identity, all
512 ordered query ordinals, every top-ten source ordinal and distance bit, the
exact scoring kernel, runtime evidence, and truth Parquet identity. The
evaluation terminal binds the construction and truth terminals, every
per-query oracle and reducer row, aggregates, minima, gates, and causal
disposition. Serializers independently recompute all derivable values and emit
sorted compact JSON with one trailing newline.

Promotion authorizes only the next V26 router task. It does not authorize a
sealed sentry, D3, full-scale construction, or a competitor claim. Strict Clippy
and the full workspace suite run once after a coherent implementation milestone,
not after each repair.

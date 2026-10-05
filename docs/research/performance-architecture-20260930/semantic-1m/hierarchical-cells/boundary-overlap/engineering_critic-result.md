The bounded overlap falsifier is defensible, but **three contracts must be resolved before accepting the child and freezing the experiment**. This reviews `d16eb7c9ce16b84a8fb0782ccbdeda42db4720e6`; the child implementation remains **UNVERIFIED**.

**Blocking findings**

1. **P1 — “Original cut predicates” do not uniquely define sibling descent.**

   The builder assigns ordinary splits with `a <= b`, repairs capacity by sorting rounded `f32` `(a-b, source_id)`, and handles coordinate fallback with `(coordinate.total_cmp, source_id)`. It does not currently store an executable split predicate. Several thresholds can reproduce every original membership yet route a row arriving from the opposite subtree differently. Therefore, successful primary replay alone does not validate alternate destinations. [Partition implementation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:249), [coordinate fallback](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:475).

   **Smallest repair:** freeze the predicate extension explicitly—for example, the last-left lexicographic key for repaired cuts—separately from the margin used to prioritize replicas. Preserve capacity cuts’ signed-zero normalization and coordinate cuts’ actual `total_cmp` behavior.

   For an ideal squared-distance separator,
   \[
   m(x)=\frac{|\Delta(x)-\tau|}{2\|c_R-c_L\|_2},
   \qquad
   \Delta(x)=\|x-c_L\|^2-\|x-c_R\|^2.
   \]
   Coordinate cuts use \(|x_j-\tau|\). Using the builder’s rounded `f32` delta makes this a geometric **proxy**, not an exact distance to its discrete assignment boundary. Specify threshold placement, evaluation precision, equal-margin path ties, and near-zero denominators. Existing tests explicitly demonstrate that exact-real differences and rounded capacity ordering can disagree. [Rounding fixture](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:3988).

   Also specify that crossing forces one branch and descends using the **unchanged canonical vector**. A degenerate node may be ineligible as a crossing candidate while still requiring its exact ID-based predicate during descent. Test both original members and synthetic outsiders; membership-only fixtures miss this failure.

2. **P1 — Existing SQ8 range APIs cannot directly represent replicated, framed extents.**

   `ReturnedRange.start` denotes an offset in a tightly packed physical SQ8 object; bounds derive from `geometry.rows * (D+12)`. The current ranker rejects duplicate IDs. Replication introduces three distinct quantities: logical source count, physical record count, and framed file offsets. Reusing primary `first_row`, logical `N`, or framed offsets interchangeably can reject valid extents or report incorrect physical identities. [Range contract and validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:34), [physical geometry](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/exact_sq8_nominee.rs:9).

   **Smallest repair:** make those quantities explicit in the additive API. Authenticate and charge complete extents including framing, then score validated record slices. Compare duplicate **record bytes**, not scores; distinct bodies can produce equal scores for a query. Deduplicate before truncation and validate conflicting copies even when their ID is suppressed.

   The selection seam must also come from the actual primary/wider union. The existing nomination API requires **exactly 24** cells; it is not a general implementation of the proposed union bounded by 32. [Union24 restriction](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:1592).

   Require identical selected-cell IDs in both arms and independently verify that candidate unique rows equal control rows plus precisely the admitted replicas. Preserve query normalization, coefficients, and SQ8 arithmetic.

3. **P1 — Mutation reuse needs an explicit visibility and scoring contract.**

   The existing mutation snapshot suppresses IDs and ranks pending replacements as normalized **FP32**, using different arithmetic from base SQ8. Its merge function expects stale base IDs already removed. Blindly reusing it therefore does not establish “unchanged SQ8 scoring” for replacements, and applying suppression after base top-k can permanently lose valid replacement candidates or underfill results. [Mutation merge](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_mutations.rs:95), [existing pre-truncation exclusions](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:61).

   **Smallest repair:** pin an immutable, authenticated layout/base/delta combination; validate base copies, suppress every superseded copy before base truncation, then merge pending replacements. Distinguish legitimate base-versus-delta replacement from conflicting copies within the immutable base.

   Freeze the scientific pair with an **empty delta**. If reusing FP32 pending-row scoring, declare that API behavior explicitly. Required fixtures should cover a replacement whose owner cell is absent but replica is fetched, simultaneous old/new readers, all-base-rows-suppressed underfill, and capacity refusal leaving the published revision unchanged.

**Evidence that holds**

- The oracle argument is sound: primary membership is a bijection, so selecting cells by descending truth count maximizes coverage at fixed cardinality. Those selections fit the byte cap. CoHere’s **97.171875% / p05 88** therefore excludes the stated target for any selector fetching at most 32 unchanged cells on this consumed panel. [Oracle computation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/layout-oracle-check.py:37).
- The witness/oracle hashes identify the **capacity-constrained v4** layouts, not the older directories named `original`. Freeze those exact identities; the name “original builder” is otherwise ambiguous.
- SQ8 records are `8-byte ID + 4-byte norm + 768 codes = 780 bytes`. Thus `32 × 640 × 780 = 15,974,400`, leaving **802,816 bytes** under 16 MiB. This is a payload bound, not a memory or lifecycle bound.
- Exact canonical replay and captured training centers are necessary. Child means or reconstructed SQ8 vectors cannot supply those boundaries. Preserve input order and target arithmetic; the distance kernel documents target-dependent reduction ordering. [Distance kernel](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/metric.rs:496).

**Optional improvements and release limits**

Describe a successful result as evidence for **this bounded replication policy**, not proof that geometric proximity uniquely caused recovery. The paired control isolates adding those replicas, but does not compare geometric selection against another replication policy.

Report a local serial implementation as one **dependency wave**, not measured parallel fetching: existing accounting explicitly records `max_parallel_gets: 1`. [Fetch accounting](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:834).

The documented deferral of incremental publication, crash recovery, compaction and GC is appropriate for this falsifier; those remain release blockers. Preserve witness scientific **FAIL** and outer **INVALID** separately.

No files were edited, tests executed, or native/data/cloud jobs, children, or consultations launched.

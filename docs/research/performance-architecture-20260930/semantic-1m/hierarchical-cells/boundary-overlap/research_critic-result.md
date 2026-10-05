**Bounded overlap is a defensible falsifier, but the protocol is not ready for scientific freeze.** The blocking issues are precise definitions and source/API contracts; they do not require a different architecture or another implementation child.

I reviewed `d16eb7c9ce16b84a8fb0782ccbdeda42db4720e6`. These are protocol findings and compatibility risks, not verified defects in the unverified child.

The oracle argument is sound. With disjoint primary memberships, summing the largest per-cell truth counts gives the cardinality optimum. All recorded optima fit the byte cap. I independently checked the receipt arithmetic: CoHere’s perfect32 ceiling is **97.171875%, p05 88**. Removing SQ2 payload cannot overcome that cardinality ceiling. Historical scientific **FAIL** and outer infrastructure **INVALID** remain separate. [Oracle implementation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/layout-oracle-check.py)

**Blocking before freeze**

1. **“Normalized margin” still needs an executable definition.**

   For centers \(c_L,c_R\), the real-arithmetic expression is

   \[
   \Delta(x)=\|x-c_L\|^2-\|x-c_R\|^2,\qquad
   m(x)=\frac{|\Delta(x)-\tau|}{2\|c_R-c_L\|}.
   \]

   The denominator needs the **square root** of squared center separation. Coordinate-cut distance is \(|x_j-\tau|\); these then have comparable distance units.

   However, the actual builder uses separately rounded f32 distances followed by f32 subtraction. Its SIMD reduction differs from scalar accumulation. Replacing that predicate with the algebraically equivalent dot-product plane can change memberships near cuts. [Distance kernel](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/metric.rs:485)

   **Smallest repair:** separate the exact membership predicate from the proposal-priority calculation. Freeze the priority’s arithmetic, threshold choice and comparison order. A normalized rounded-margin proxy is implementable; describe it as a proxy rather than an exact distance to the implemented floating-point decision surface.

2. **Capacity and coordinate cuts require ordered predicates, not just thresholds.**

   Ordinary splits assign left on `d_left <= d_right`. Capacity repairs select a prefix ordered by `(rounded margin, source ID)`, canonicalizing signed-zero margins. Coordinate fallbacks order by `(coordinate.total_cmp, source ID)` and take `floor(n/2)`. A scalar `<= threshold` cannot reproduce a cut through tied values. [Partitioner](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:254)

   **Smallest repair:** capture the boundary’s exact predicate, including the cut tuple and equality behavior. Separately specify the numeric threshold used for priority—last-left value or midpoint between adjacent cut values can rank proposals differently.

   Also resolve equal margins between boundaries on the same row’s path. Global `(margin, source ID, destination ID)` ordering does not itself resolve that earlier choice. Rows with no eligible geometric boundary should produce no proposal. A wholly identical coordinate split must not become “geometric” merely because its axis normal has length one.

3. **Exact replay must use the retained v4 builder.**

   I authenticated the retained manifest pins: ReLAION has **301 cells, 53 semantic repairs**; CoHere has **308 cells, 87 repairs**. Both are resident-v4/build-v2, compatible with the current primary builder.

   There is a concrete trap: `split_balance_diagnostic::replay` calls its old median-fallback membership `original`. That is the earlier partition rule, not the retained v4 primary rule. Reusing it would invalidate this arm. [Diagnostic replay](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:6020)

   **Smallest repair:** capture boundaries through the shared current `Builder::node` path. Require agreement with retained cell IDs, ordered memberships, SQ8 bodies, coefficients, topology and every stored prototype’s bits. Preserve projected child ordering before prototype accumulation.

   The witness archive proves the retained layout; it does not establish availability or authentication of the full original canonical inputs. Missing originals must remain INPUT_UNAVAILABLE/INVALID. No reconstruction from SQ8 or stored means is admissible.

4. **Define sibling descent explicitly and prove paired selection identity.**

   Capacity predicates were established over each node’s original source set. Their extension to a row entering from another subtree must be deterministic.

   **Smallest repair:** force the opposite child at the chosen boundary, then descend using the unchanged canonical vector and captured predicates. Do not reflect/project the vector, retrain centers, insert it into sibling capacity sorting, or recompute primary ownership. Freeze the behavior when the destination is full: reject that sole proposal and continue admission, without trying another destination.

   Both arms should consume the same retained router selection. Assert identical selected cell IDs, primary flags and distance bits before payload reads. Route8 and route24 are independent beams; their union need not equal24 or32.

   Crucially, existing `WholeCell` still uses SQ2 block nomination before SQ8 ranking. Merely selecting that fetch policy does **not** implement the proposed full-SQ8 control. [Existing search](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:1804)

5. **Duplicate-aware ranking needs physical geometry and byte identity.**

   The current ranker rejects repeated IDs, while `Sq8Geometry.rows` means physical records. An overlap object has \(N+R\) physical records and \(N\) logical sources. Framed file offsets also cannot automatically serve as row-aligned offsets into a headerless SQ8 plane. [Ranker contract](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:66)

   **Smallest repair:** distinguish logical count, physical count and framed extent offsets. Validate complete authenticated extent bodies; compare repeated records’ full bytes, including stored norm and codes; reject conflicts; deduplicate before top-k using unchanged scoring arithmetic.

   Preserve the existing query normalization, including its near-unit tolerance. Define a deterministic representative physical ordinal for duplicate IDs and explicit underfill behavior. Ordinary identical replicas are valid; stale replacements must be suppressed by revision visibility.

6. **A pinned revision must bind both base and delta atomically.**

   Pinning a base generation while consulting a mutable latest delta can mix revisions. Deletes and replacements must suppress every old primary/replica copy before base truncation. A replacement must also contribute its new candidate; suppression alone implements a delete.

   **Smallest repair:** pin an immutable tuple containing the overlap root, delta revision/digest and scoring interpretation. Capacity refusal must leave the published tuple unchanged. Old readers retain old visibility; new readers see the complete replacement.

   Existing mutation foundations help, but their scoring differs: `TwoBitMutationSnapshot` scores pending normalized FP32 puts, whereas the resident-graph snapshot stores FP16 coordinates. Neither is automatically an unchanged SQ8 replacement path. [Mutation scoring](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_mutations.rs:95)

   Declare the delta scoring contract and keep the static paired measurement delta-free. The prospective document already defers affected-cell publication, recovery and compaction to product promotion; that scope is reasonable if unsupported operations fail explicitly.

7. **Freeze actual framing, admission and evaluation rules.**

   The payload arithmetic is correct:

   \[
   32\times640\times780=15{,}974{,}400,
   \]

   leaving **802,816 bytes**, or **25,088 bytes per extent** at32 extents. This remains a raw-record bound until headers, mappings, checksums and padding are specified.

   **Smallest repair:** charge complete serialized extents before submitting any payload read. Account separately for router/head/delta hydration and retries. One dependency wave of serial local reads demonstrates known locations, not parallel remote performance.

   Freeze p05 as the existing sorted index `floor(.05*(64−1)) = 3`, retain fixed100-neighbor denominators despite underfill, and seal both arms’ selections/results before truth opens. Add an independent invariant: selected unique coverage cannot decrease when primary memberships and selection are unchanged. Returned recall can decrease because additional SQ8 candidates can displace truth neighbors.

**Research interpretation and optional improvements**

The experiment can causally measure the effect of **this bounded replication policy** under fixed routing and scoring. It cannot establish that normalized margin is the best replication rule, or that geometric boundaries explain all losses. The query router ranks stored child means rather than following the captured split planes, so “near a partition boundary” is not necessarily “near a routing mistake.”

The required improvement is substantial. Historical same-layout CoHere coverage was77.484375%, implying about **20.52 percentage points** of recovery to reach98% if those selections remain unchanged. The perfect32 oracle’s shortfall is only0.828125 points; that smaller gap is not the operational recovery requirement.

Prior art supports trying overlap, but provides no guarantee for this policy. Spill trees replicate an overlap region recursively; the proposed one-alternate-leaf rule covers less. [Spill-tree paper](https://papers.nips.cc/paper_files/paper/2004/file/1102a326d5f7c9e04fc3c89d0ede88c9-Paper.pdf) SPANN uses closure assignment and representative replication to reduce redundancy between nearby posting lists. That highlights a plausible weakness here: replicas may land in cells frequently fetched together and add little unique coverage. This is an inference about BORSUK, not a measured result. [SPANN paper](https://www.microsoft.com/en-us/research/uploads/prod/2021/11/SPANN_finalversion1.pdf)

After sealing, useful optional receipts are unique rows gained per replica byte, repeated-copy fraction, quota rejections and gains by split depth. A random-replication comparator becomes necessary only for a stronger claim about the geometric priority itself. It need not expand this authorized falsifier.

A row graph over unchanged cells remains bounded by the oracle. Finer fetch units, regrouping or a different resource envelope are distinct future arms. No replacement implementation is required now.

No files were edited, and no native/data/paid jobs, children or consultations were run. The child remains **UNVERIFIED** until exact-source native checks, Clippy and workspace test compilation pass; scientific freeze follows these repairs.

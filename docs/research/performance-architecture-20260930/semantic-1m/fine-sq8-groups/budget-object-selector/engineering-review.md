# Engineering review

Original consultation `788d4f0d14c64667`, group `bf121fb269bb4ea6`, effective GPT-6 Astra, completed exit0. Read-only review; no native qualification.

**ACCEPT WITH CONCRETE REPAIRS for one bounded offline Rust falsifier. The proposal is not yet implementation-ready.** Freeze the training specification and independent oracle’s expected cases first. This decision does not qualify serving, lifecycle behavior, recall, or a paid launch.

Reviewed the complete proposal, the requested closed decisions, supporting reports, and relevant Rust paths. The proposal remained SHA-256 `d4cf35a345e302284aad7549556ef564e018aae003922403bfa0d35c53d24a03`; HEAD advanced concurrently from `00b32192` to `b68e5353`.

1. **P1 — The proposed training input is not yet defined.**  
   The proposal alternates between “source-neighbor labels” and reusing existing source selections. Those selections contain **old PQ nominees**, anchor identities and query hashes—not exact SQ8 neighbors or feature vectors. Their serializer confirms this. Counting these nominees as true neighbors would preserve a teacher limitation already exposed by CoHere’s 95.609375% nomination containment. [Proposal: training](./docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-result.md:30), [selection serialization](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/co_selection_layout.rs:1526).

   **Repair:** Freeze the authenticated feature source, teacher algorithm, neighbor count, normalization, self-exclusion, ID mapping and train/held roster. Existing nomination metadata may support a placement diagnostic; label it accordingly. Freeze activation, loss, optimizer, learning rate, initialization, seed, batching, rounds, numeric ordering, move/swap enumeration and resource ceilings. None may be selected using held anchors or consumed queries.

   For move acceptance, use fixed training neighbor IDs and the **actual bounded serving selector**. Require strictly increased total training coverage with no regression in the frozen training lower-tail statistic. Recompute affected coverage exactly; reject zero-gain moves and roll back any refit/layout pair that fails acceptance. Held results must never select checkpoints.

2. **P1 — Cartesian truncation can exclude the globally best object. Empty-label behavior is also unspecified.**  
   Four equal-weight mixtures suffice for a counterexample. Give each component its own disjoint eight-label block. In both heads, assign probability `0.112` to each block label and `0.104` to one shared ninth label. The shared pair scores `0.010816`; every enumerated block pair scores only `0.003136`. Yet the shared pair never enters the candidate set. A small positive perturbation of the zero probabilities preserves this result. This is a valid selector loss, not an implementation defect. [Candidate rule](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-result.md:22).

   **Repair:** Freeze stable score/label ties, normalization, candidate deduplication, occupancy representation, and filtering order. Exclude empty labels before choosing the final 15; prohibit unbounded refill or a serving fallback scan. Record candidate count, occupied candidate count and selected count. A short candidate set must be visible in diagnostics.

   At 100k, `s=11`; at 100M, `s=335`. A 100k pass cannot establish that this truncation generalizes to the larger label space.

3. **P1 — The cold arithmetic is correct, but its supporting format and allocation contract is incomplete.**  
   I independently obtain `J=112,225`, root `4,496,144 B`, maximum object `798,784 B`, and query allowance `64,360,704 B`. At the declared root ceiling, cold transfer leaves only **11,328 B** unused. Using the projected root’s exact size leaves **233,776 B**. [Allocation formulas](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-result.md:40).

   A seemingly small four-byte length field per label would add `448,900 B`, exceeding the chosen root ceiling. Digest-only addressing can work, but then the planner must reserve a complete maximum-sized body unless another frozen size representation exists.

   **Repair:** Specify every head, root and object field, empty-label encoding, version, length bound and authentication dependency. Bound allocation before reading or trusting body lengths; reject overflow, malformed counts, nonfinite parameters, coefficient mismatches and oversized responses. Authenticate complete bodies before scoring.

   Reserve metadata, delta and every attempt before dispatch. The `17 GET` statement is an empty-delta, one-attempt bound; cold execution still has dependent head → root → body stages. Admission must sum unique pinned generations, raw/decoded opening coexistence, concurrent query allocations, delta/WAL buffers, maintenance scratch and runtime/transport overhead. Serialized root size is not process RSS.

4. **P1 — Spare capacity does not establish new-ID insertion or compaction correctness.**  
   The proposed lifecycle has no deterministic destination rule for a new ID, no rule for a saturated destination, and no defined transition when the label grid grows. Recomputing the formula changes `s=335` to `336` at **100,553,601 rows**, despite the existing grid having capacity for **114,918,400 rows**. Label coordinates and model outputs therefore cannot silently track current row count. [Lifecycle proposal](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-result.md:128).

   Existing mutation support explicitly rejects IDs outside the base roster; existing compaction rejects `Fresh1m`. Neither supplies this lifecycle. [Mutation validation](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:1025), [compaction restriction](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_compaction.rs:389).

   **Repair:** Freeze grid dimensions per generation. Define durable acknowledgement, latest-state upsert/delete ordering, new-ID encoding with frozen coefficients, destination/capacity rules and explicit backpressure. Compaction must capture a delta watermark, preserve later writes, conditionally publish the complete model/layout/coefficient identity, and retire data only after pins release. A record searchable through the delta must not silently become unreachable when compacted into an object the model cannot retrieve. Keep these as release blockers; implementing the complete lifecycle is outside the first falsifier.

5. **P1 — “Best possible coverage” needs separate, precisely named measurements.**  
   The 13-object lower bound permits hypothetical query-specific packing. It does not establish that **one shared layout** works across anchors, or that the trained selector finds its useful objects. The prior co-selection objective improved while held fits remained only `381/512` and `216/512`. [Closed surrogate audit](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/native-diagnostic/a0002/surrogate-objective-audit.json:3).

   **Repair:** Report separately:

   - Best legal object-subset coverage for the **fixed membership**.
   - Selection using exhaustive model scores over occupied labels, diagnostic only.
   - Selection using the actual Cartesian candidates and budget.
   - Returned top100 from every visible row in the actual fetched SQ8 population.
   - Subsequent external-query recall.

   Source-held agreement with exhaustive SQ8 measures routing fidelity to SQ8. It cannot establish original-vector recall. The historical CoHere result specifically depended on surrounding rows, so retaining old nominees or winners cannot certify a changed population. [Closed SQ8 decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/decision.md:16).

6. **P2 — Result and failure semantics need freezing before the falsifier.**  
   “At most 15 objects” allows fewer than `k` visible records after empty-label filtering, deletions or delta reservation. Define an explicit insufficient-results/budget outcome and preserve charged work; do not silently report ordinary success or retry beyond the budget. Bind plans to the query and snapshot identities. Specify held-anchor count: “fourth-smallest” is not p05 for the existing 512-anchor roster. [Acceptance sequence](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/research-result.md:134).

The **single accepted first slice** is a pure Rust selector/membership evaluator: frozen model, membership, source-label weights and remaining budget in; candidates, selected objects, coverage and charged costs out. Reuse the SQ8 scorer later. Do not implement the six proposed lifecycle APIs in this slice.

Before implementation, freeze one small independent oracle specification:

- Enumerate six groups assigned to four labels, capacity two groups per object, and every legal subset of at most two objects. Include unequal body sizes and a short tail. Independently recompute coverage and move deltas.
- Include the 33-label mixture counterexample above, duplicate components, empty labels, ties and fewer-than-`k` visible records.
- Include exact byte-boundary, malformed-header, hash, coefficient and snapshot-mismatch fixtures. The oracle must not call production candidate or move helpers.

After that slice, root may authorize the frozen source-only test. Retain the proposed **99.5% mean agreement / fourth-smallest ≥99** mechanism gate with an explicit roster. Any later consumed-panel falsifier must seal both actual-population result sets before GT and retain **6,272/6,400 hits / fourth-smallest ≥95 per panel**, alongside the total read/byte gates. Fresh queries remain necessary for promotion.

Preserve every centroid, hierarchy, overlap, packing, PQ and low-bit arm’s original disposition. This hypothesis reverses none of them and earns no parameter ladder or budget relaxation.

No files edited, native tests run, corpus/request/GT bodies opened, AWS actions, children or consultations performed.

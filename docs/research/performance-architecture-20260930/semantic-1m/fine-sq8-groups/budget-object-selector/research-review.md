# Research review

Original consultation `b7762a8e0af247b0`, group `bf121fb269bb4ea6`, completed exit0. Requested Opus; effective GPT-6.1 Sol after authentication failure. Read-only; no native qualification.

**ACCEPT WITH CONCRETE REPAIRS for one bounded Rust selector-and-membership falsifier.** The architecture is worth testing. Implementation must wait for a frozen training specification and independent oracle contract; this decision does not qualify the full index API or a scale campaign.

The strongest evidence supports changing the routing objective: co-selection improved its surrogate while physical feasibility remained **48/64 and 33/64**. It does not establish that a compact classifier can recover the required neighborhoods. Likewise, CoHere’s passing SQ8 result depends on surrounding fetched rows, so retaining old nominees cannot certify this design. [Co-selection decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/native-diagnostic/a0002/decision.md), [paired SQ8 decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/decision.md).

1. **Treat candidate enumeration as an approximate algorithm.**

   Top-8×8 per component does not contain the mixture’s global top labels. With four equally weighted components, eight private leaders per component and one common ninth-ranked label, the common/common pair can outrank every emitted candidate while remaining absent from all 256 candidates. The newly committed counterexample establishes this exactly; the failure also survives small positive probabilities, so softmax does not remove it. [Counterexample](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/candidate-enumeration-counterexample.md).

   Freeze row-major label IDs, head normalization, finite-value checks, tie ordering and deduplication. For the smallest policy, represent empty labels with a reserved directory sentinel, discard them after enumeration, and perform no refill search. Return explicit underfill when fewer objects or visible rows remain. Empty labels must consume neither GETs nor object slots.

   Enforce both **≤64 groups and ≤1,024 actual rows** per object, with a complete group bijection and correct short-tail accounting. Target fill 56 supplies aggregate spare capacity; it does not guarantee space at a preferred destination.

2. **Separate attainable coverage from actual selection.**

   The oracle must report these quantities independently:

   | Diagnostic | What it establishes |
   |---|---|
   | Best legal object cover on the fixed membership | Neighborhood concentration |
   | Best legal cover restricted to the emitted candidates | Candidate-set limitation |
   | Full-grid ranking by trained mixture scores | Model ranking without truncation |
   | Actual truncated, budgeted selector | Serving routing behavior |
   | Returned SQ8 top100 over every fetched visible row | Actual ranking behavior |

   Full-grid model ranking is not an oracle for neighbor coverage. Tiny exhaustive membership search supplies an additional joint-placement upper bound; it does not prove attainability on the real population.

   The **13-object lower bound** from 777/799 mandatory groups proves only that individual sets are not excluded by raw capacity. It proves neither a common feasible partition nor learnability.

   Also, 100k has **s=11**, versus **s=335** at 100M. Top eight per head is a much weaker restriction at 100k. Include a synthetic, scale-shaped selector fixture before using a 100k pass to justify further qualification.

3. **Freeze training before writing the fitter.**

   The existing selections contain authenticated nominees and group membership, not retained query vectors. They describe the old nomination program, not exhaustive SQ8 neighborhoods. Their coverage of all 6,250 groups therefore establishes neither sufficient training labels nor recall. [Input-seam audit](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/budget-object-selector/root-preimplementation-audit.md).

   Root must freeze one manifest specifying:

   - Authenticated vector access, anchor counts and deterministic splits; self-ID exclusion and normalization.
   - Exact teacher population, neighbor count, SQ8 coefficients and score/ID ordering.
   - Initial membership, hidden activation, initialization, optimizer, learning schedule, batches and finite round count.
   - Neighbor-count-weighted loss, destination enumeration, group traversal and tie rules.
   - Separate operation, memory, scratch and execution ceilings.

   At 100k, use exhaustive unchanged SQ8 neighbors as the authoritative mechanism labels. Old nominations remain diagnostic metadata.

   Make acceptance operational: evaluate the actual bounded selector on training anchors; accept membership changes only when total covered neighbors strictly increases and the frozen lower-tail statistic does not decrease. Recompute after each accepted change. Apply the same acceptance check to retrained model checkpoints. Held anchors must not select moves, checkpoints or constants.

   A fixed anchor count is not a qualified scalable teacher. For example, 4,096 anchors with 100 neighbor labels can supervise at most 409,600 distinct groups—only **6.55%** of the 100M group population.

4. **Freeze the physical format and complete admission ledger.**

   The proposed arithmetic checks out:

   | Allocation or transfer | Bytes |
   |---|---:|
   | Projected root | 4,496,144 |
   | Root ceiling | 4,718,592 |
   | Head reservation | 65,536 |
   | Fifteen maximum bodies, including headers | 11,981,760 |
   | Total ceiling reservation | **16,765,888** |
   | Remaining 16MiB allowance | **11,328** |

   Using the projected root instead of its ceiling leaves 233,776 bytes; neither calculation includes a delta.

   Specify every root/header field, format marker, digest interpretation and object-key derivation. A digest-only directory can work with implicit label positions and conservative maximum-body reservation. Adding eight directory bytes per label would add **897,800 bytes**, exceeding the chosen root ceiling.

   Admit head, root, delta, bodies and additional attempts together before dispatch. Reduce object count when required. The cold dependency chain is **head → root → bodies**: 17 GETs does not mean one network round trip. Preserve one-attempt behavior. AWS also requires separate requests for disjoint ranges. [GetObject documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html).

   The **64,360,704-byte query allowance** is a projection, not established allocation ownership or RSS. Count actual capacities for unique pinned roots, decoded model state, concurrent workspaces, opening buffers, delta snapshots, compaction and runtime. Opening a new generation can coexist with old queries; those allocations must be added when simultaneous.

5. **Make the smallest oracle independent, and keep the first primitive small.**

   Before implementation, freeze an oracle specification containing:

   - A tiny exhaustive capacity-constrained membership and legal-subset enumeration.
   - Independent mixture evaluation, including the 33-label counterexample.
   - Independently recomputed move deltas and byte/read charges.
   - Empty labels, duplicate candidates, ties, short tails, malformed geometry and budget exhaustion.

   The first Rust primitive should evaluate frozen weights, membership and source-neighbor metadata, emitting the five diagnostics above and complete resource charges. Reuse existing SQ8 arithmetic later. Do not expand this increment into `build/open/apply/compact`.

   Correctness means matching the declared truncated algorithm. Missing the exhaustive mixture winner is expected algorithmic loss to measure, not a defect to conceal or an automatic rejection of the research hypothesis.

6. **Specify mutation and compaction invariants without claiming they exist.**

   New IDs need a durable, bounded latest-state delta that every query scores. Replacements and deletes must suppress stale base rows before top-k; latest upserts appear once. Acknowledged updates require crash recovery, atomic batch publication and backpressure before admission fails.

   Plans must bind query identity, root/model/coefficient identity, mutation revision and budget. Compaction must capture a sealed delta prefix, preserve later writes, publish conditionally and retain artifacts reachable from old pins.

   Capacity fallback must not move new records into labels the selector cannot recover and then discard their searchable delta copies. Frozen-model updates and model retraining require separate quality evidence.

   The inspected `FineSq8Snapshot::with_mutations` restricts IDs to the existing roster and supplies no new-ID durability guarantee. SPFresh’s local repair relies on nearest-partition assignment; its convergence argument does not transfer to these learned labels. [Existing mutation path](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:980), [SPFresh](https://arxiv.org/html/2410.14452v1).

7. **Keep qualification and dispositions explicit.**

   Freeze the source-held count before applying **≥99.5% mean agreement and fourth-smallest ≥99 per panel**. Fourth-smallest represents the recorded p05 convention for 64 observations, not for 512.

   With identical SQ8 arithmetic and tie ordering, fetched coverage of the exhaustive SQ8 top100 should equal returned agreement when at least 100 visible rows exist. This is a useful independent invariant. It establishes source agreement, not external recall.

   Subsequently, seal both panels’ complete actual-population results before GT access and retain **≥6,272/6,400 hits, fourth-smallest ≥95, ≤32 total GETs and ≤16MiB total charged bodies per panel**. Consumed panels can falsify; fresh queries are required for promotion. Complete the repository’s Rust compilation, Clippy and workspace test-build gates on the exact revision.

   Repair the blanket instruction that resource failures are INVALID: setup, authentication and implementation defects are INVALID; a correctly enforced, preregistered algorithm resource limit is a valid failed gate. Neither outcome permits retuning this arm.

The prior art supports mechanisms only. BLISS uses alternating learning and reassignment, with independent index repetitions; four mixture components over one membership do not inherit that redundancy. Its BLISS+ reordered single-index variant is closer to this layout. The cited routing paper learns rankings for fixed partitions from exact query-neighbor labels, without establishing this factorized, bounded enumeration. [BLISS](https://gaurav16gupta.github.io/papers/BLISS_KDD2022.pdf), [routing formulation](https://arxiv.org/html/2404.11731v1).

An unfactorized 64-hidden-unit output head would require about **29.18MB** of output weights alone, so factorization addresses a real cold-budget constraint. Replica multicover may improve placement but retains the large router. Neither is a qualified fallback. Turbopuffer’s disclosed hierarchy and bandwidth tradeoffs reinforce the need to measure transferred bytes and lifecycle cost; they establish no matched win here. [ANN v3](https://turbopuffer.com/blog/ann-v3).

Preserve every centroid, hierarchy, overlap, PQ and low-bit arm’s original disposition, including V150’s rejection despite its later coverage diagnostic.

Read-only review completed. HEAD advanced from `00b32192` to `fa9b1449` through evidence-document commits; inspected Rust and historical decisions were unchanged. No native runs, corpus/request/GT access, AWS actions, children or consultations.

**No-go on the plan unchanged. Go for one bounded sampled-SQ8 nomination prototype after the repairs below.** This is a plausible way to add row-derived evidence without changing layout or copying rows, but its graph, coverage metric and resource contract need sharper definitions.

I inspected Git objects at `3b70a8e3b863cca220ce1c15df5ac363f9e0aec3` and the [pending plan](/data/target/borsuk-boundary-adjacency-plan.pending/planning-answer.md). No files were edited or experiments run.

The committed reconciliation establishes that the six SOURCE omissions begin at leaf nomination. It does not establish why their prototypes rank poorly or that this adjacency will recover them. q537 remains a separate covered ranking loss; quantization causation is unproved. The inspected inventory lacks a qualified same-source FP32 graph, which is not evidence that FP32 assets are globally absent.

**Must-fixes:**

1. **Preserve the existing coverage endpoint and gate.**  
   `coverage_slot` at `compare_native_replay.rs:5308` counts membership in a nominated **physical page closure**. `loss_coverage` establishes that SOURCE scores every unit in those pages. A target unit can therefore be SOURCE-scored even when its own leaf was not selected.

   Counting only “owning nominated units” changes the metric and may change the 314/320 control. Keep SOURCE-scored closure coverage; exclude fetched-only gap bridges. Owning-leaf selection can be a separate explanatory fact.

   Remove the added zero-loss requirement. The existing gate explicitly accepts candidate315, gained2, lost1 (`compare_native_replay.rs:6759`). Replacing eight leaves intentionally permits tradeoffs, and no evidence justifies changing that criterion. Retain control313/320 returned, control314/320 coverage, candidate≥315/320 and gained>lost. Do not tune from these consumed queries.

2. **Define the edges as stored graph edges, not kNN counts.**  
   `build_hnsw_adjacency` selects with `diverse=false`; its diverse sibling exists. Plain pruning retains nearby *discovered candidates*. Diverse pruning preserves directions and bridges, sometimes retaining farther points. Reciprocal insertion and subsequent pruning mean neither builder produces a fixed, exact nearest-neighbor list.

   Historical bounded discovery lost centroid-recoverable pages, but that diagnostic did not isolate nondiverse pruning as the sole cause. V250 supports diversity’s usefulness in a different D768 row-navigation workload; V255 demonstrates that diversity still failed quality and construction gates at scale. Neither predicts this D1024 sampled nomination result.

   **Smallest repair:** retain the sequential nondiverse builder for this one arm, explicitly calling the weights **sampled graph cut-edge counts**. Switching to diversity would change the prior, not establish its fidelity. If nearest-neighbor counts are essential to the claim, explicit neighbor retrieval and validation are required; a pruning flag cannot supply them.

   Freeze directed counting: each stored base edge `i→j` contributes once to its source-leaf/target-leaf count. Base edges are `tower.last()`, not `tower[0]`. Do not mix higher-layer or artificial reachability-repair edges into boundary evidence.

3. **Make identity and nomination rules executable.**  
   Authenticate the complete SQ8 stream, exact length/EOF, physical ID order and duplicate-ID constraints. Label a sample by `membership[physical_position / 32]`, never by its logical ID. The current panel’s ordinal-ID restriction belongs in the example validator, not the generic library.

   Decode `low[d] + code[d] × step[d]`, reject nonfinite or zero reconstructed vectors, and use the existing normalization policy. The stored SQ8 scalar is a reconstructed **squared norm**.

   Bind the versioned sidecar to generation, router root, membership, SQ8, order, dimensions, normalization and construction parameters. The generation digest already binds calibration. Reject mismatched artifacts before nomination.

   Specify zero-based seed ranks, weights40…1, checked vote arithmetic, exclusion of the first40 leaves from replacement candidates, deterministic ties and L2 fallback. For ≥48 leaves, output exactly48 distinct IDs.

4. **Replace the allocation assertion with a phase model.**  
   Sample coordinates alone consume **128,000,000 bytes ≈122.1 MiB**. That excludes row headers, nested graph capacities, search heaps, validation bitsets, cut-count scratch, router metadata and serialization.

   The existing replay model is **164,435,968 bytes ≈156.8 MiB**. Keeping that modeled working set alongside samples already exceeds256 MiB before graph scratch. This does not prove an RSS failure; it proves that phase lifetimes must be explicit.

   Use bounded flat edge-pair scratch, sort and aggregate it; avoid an unbounded leaf² matrix. Borrow sampled vectors, then drop samples and graph before paired replay allocations. `ef=64` bounds retained results, **not distance evaluations or construction time**.

5. **Keep experimental IO out of the library.**  
   The semantic router currently delegates generation binding and publication to its caller. Preserve that boundary.

   The two-file scope is reasonable: put generic adjacency construction, validation and selection in `semantic_unit_router.rs`; keep generation-schema parsing, authenticated SQ8 streaming and research publication in `compare_native_replay.rs`. Expose the narrow library operation needed by the example. Do not export the private HNSW module or add another attribution framework.

The sampling and voting biases remain legitimate risks for the frozen hypothesis. A fixed midpoint is not an unbiased row sample: boundary rows can systematically occupy other positions. Raw counts also favor larger source leaves and popular destinations; short final units receive disproportionate representation. Declare these properties rather than adding normalization, randomized sampling or another vote arm. A source-only summary of cross-leaf edge count, zero-link leaves and directed reachability is sufficient context. Disconnection can weaken construction discovery, but forcing physical-order connections would fabricate boundary evidence.

Fable’s proposal is materially different: source-row kNN drives a new partition, and residual cut edges support page routing. This prototype retains the existing partition and uses a compressed prior to nominate leaves. It tests neither Fable’s layout nor full FP32 kNN. It also avoids SPANN-style boundary replication, whose historical cost failures remain preserved.

For the **future native falsifier**, use causality Spot EC2 only:

- Compile and run affected library/example tests, required workspace Clippy and `scripts/check_rust_test_build.sh` on the exact frozen source, with two compiler jobs.
- Test partial units, dimensions1 and1024/rejection1025, identity corruption, duplicate IDs, equal-distance repeatability, directed counting, no-cross-edge fallback and leaf/vote ties.
- Run authenticated source-only construction admission with workload requests and truth unopened. A small, predetermined exact-neighbor audit over reconstructed source anchors can expose graph bias; it must not select another builder or tune parameters.
- Pass the separate disposable smoke, then seal paired control/candidate nominations durably before truth opens. Reproduce313/320 returned and314/320 SOURCE coverage. Apply the existing net-gain gate.
- Survival licenses an actual native returned-recall falsifier under unchanged scoring and fetch limits. It establishes no latency, lifecycle or vendor win.

Keep the proposed **CPU1, RSS512 MiB, swap0 and900-second construction limit** provisional until real-input admission verifies the phase model. Measure both process RSS and cgroup memory peak. The2 GiB staging proposal is plausible for the nomination screen’s1.036 GB SQ8 body; it is insufficient for a later replay also staging the roughly2.06 GB SOURCE body.

The compact sidecar bound is approximately `128L + 4(L+1) + header` bytes, before decoded representation and generation pins. That is added memory, not total serving memory. Rebuilding it after order/membership changes remains an unmeasured lifecycle cost.

A validly enforced construction-limit exceedance is a **resource failure**, separate from recall failure. Identity, setup or enforcement defects are **INVALID**. Preserve interrupted attempts and restart only the interrupted measurement cell under the preregistered policy.

**Verdict: repair the contract, then implement exactly this one prototype. Production adoption remains no-go pending native evidence.**

# Architecture reconciliation

Consultation78792bc21a9044c1, Jev-selected GPT-6 Astra/XHigh, completed; read-only at36664406 with checkout advancing externally toeced3621. Root must independently verify and freeze the next falsifier. No format adoption or measurement claim follows.

**Recommend B—hierarchical semantic cells with physical locality—as the next architecture to qualify. Keep A as the measured control; leave format-rewrite approval pending.** SOURCE32 is a useful transport candidate, but its replay does not resolve routing scalability, locality, or maintenance cost.

The completed Fable consultation `72453a13276b4bba` describes the obsolete hydrated graph. Current serving already loads a small semantic root and selected leaves. Its old latency ledger, ReLAION 99.375%, and projected speedups cannot characterize the current architecture.

**Current evidence and critical path.** CoHere FIRST1M, D768 cosine, sealed64 ordinals 32–95, has 619/640 hits: **96.71875% R@10**, cold p90/p95 **541.095/557.409 ms**, and **7.597 completed QPS** over the finite 64-request cell at eight offered QPS. Sustained capacity and matching-architecture ReLAION quality remain unknown. [Closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/decision.md)

The actual dependency chain is:

`head/authenticated generation → metadata staging/open → flat leaf-prototype ranking and leaf fetch → SOURCE fetch/authentication → source scoring/page planning → SQ8 fetch/authentication/ranking`

Metadata uses root reuse and seven staged objects in one wave. The router flat-scores **723 leaf prototypes**, selects **48 whole leaves**, and fetches them with concurrency at most 16. Serving validates their FP16-bearing bodies but does **not** score those unit centroids individually. SOURCE and SQ8 remain subsequent phases. Counts divided by concurrency are not measured round trips. [Router selection](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:1126), [remote fetch](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1421)

Closed stage medians—discovery 100.15 ms, SOURCE 109.10 ms, planning 12.06 ms, SQ8 83.41 ms—are **wall intervals**, including overlapping CPU/I/O where applicable. They must not be added as a percentile decomposition.

The authenticated offline decomposition is more specific than “router loss”:

| Stage | Truth@10 positions / 640 | Truth@100 positions / 6,400 |
|---|---:|---:|
| Nominated semantic units | 619 | 5,956 |
| Physical page closure | 625 | 6,063 |
| SOURCE-scored units / ranked pages | 625 | 6,063 |
| SQ8-admitted ranges | 625 | 6,013 |
| Returned IDs | 619 | 6,003 |

Thus **15 top-10 positions are outside page closure, and six disappear during final ranking after their rows are admitted**. SOURCE scoring and SQ8 admission add no top-10 coverage loss on this panel. At R@100, admission loses 50 positions and final ranking loses another ten. The final-stage attribution does not independently separate SQ8 quantization, ranking and tie effects. Discovery versus partition-boundary causes inside the initial loss also remain unresolved. [Closed decomposition](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fixed48/scientific-execution/python-replay-a0002/a0002/screen/offline-result.json)

**CPU-only opportunity is bounded, not established.** Recounting authenticated rate5 records gives:

- Native CPU around each POST: **131.094 ms/query mean**, measured in 10-ms process ticks.
- Native lifetime user+system CPU: **181.094 ms/query mean**.
- Whole-cell cgroup CPU: **12.706764 CPU-seconds / 8.424329 seconds = 1.51 cores**, with no recorded throttling.
- Mean cold wall time: **494.318 ms**.

For SIMD-eligible CPU \(K_i\), acceleration \(s\) saves at most \(K_i(1-1/s)\) CPU time; \(K_i\) is unknown and bounded above by the measured process CPU. Optimistically treating *all* POST CPU as serial and removable gives a mean reduction ceiling of 131.094 ms. Per-record subtraction gives p90 **405.750 ms**, but this is conditional arithmetic with networking/scheduling held fixed—not a predicted latency or a demonstrated SIMD result. Hashing, allocation, system calls and non-vector work make the attainable saving smaller. [Authenticated records](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/screen/rate5-records.jsonl)

| Dimension | A: current semantic format | A + SOURCE32 | B: hierarchical cells/locality |
|---|---|---|---|
| Quality | Measured CoHere result above | Recorded candidates reused; native parity unmeasured | Unknown |
| Query GETs across64 | 8,261 | 7,142 modeled | Unknown |
| Query payload across64 | 2.393 GB | 2.582 GB modeled | Unknown |
| SOURCE tradeoff | 3,147 GETs; 1.102 GB | **35.6% fewer GETs; 17.1% more bytes** | Must reduce fragmentation without losing useful candidates |
| RAM | Whole-service peak 205.1 MB; native individual peak 43.0 MB | Actual peak unknown; larger response-buffer demand | Unknown; bounded design required |
| Cold tails / QPS | Measured above | No measured speedup | Unknown |
| Total lifecycle dollars | Unknown | Unknown | Unknown |

These are consumed payloads/submitted operations, not physical wire bytes or billing. Across64, current **whole-process** accounting is 9,285 attempts and 2.621 GB; it includes startup/control operations. Independent RSS peaks cannot be summed as simultaneous memory. [SOURCE32 evidence](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fixed48/source32-geometry/replay_fixed48_source_get_caps.md)

**Scale specification.** Let \(U=\lceil N/32\rceil\), \(C=\lceil U/64\rceil\), and \(L\) be leaf count, with \(C\le L\le2C-1\). At D768, current encoded root is \(512+3136L\); leaf payload is \(1540U\); membership is \(4U\). The actual 1M root is **2,267,840 bytes**, with **48,125,000 bytes** of leaves.

| Quantity | 1M | 10M | 100M |
|---|---:|---:|---:|
| Units \(U\) | 31,250 | 312,500 | 3,125,000 |
| Flat encoded root envelope | 1.53–3.06 MB | 15.31–30.62 MB | 153.13–306.25 MB |
| Router leaf payload | 48.125 MB | 481.25 MB | 4.813 GB |
| Membership | 0.125 MB | 1.25 MB | 12.5 MB |
| Canonical + SQ8 + two-bit payload | 4.06 GB | 40.6 GB | 406 GB |
| Existing router build allocation model | 391.6 MB | 6.679 GB | 350.310 GB |
| Existing compaction disk admission | 8.428 GB | 84.282 GB | 842.813 GB |

**These are source arithmetic, not RSS, runtime or feasibility measurements.** The current profile rejects 10M/100M; those columns hypothetically extend its formulas. Its final assignment scans every center for every unit, \(O(UCD)\): approximately **11.736 billion / 1.172 trillion / 117.190 trillion coordinate comparisons**. A hierarchical trainer does not remove this later flat assignment. [Admission/build](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:267), [assignment](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/semantic_unit_router.rs:353), [compaction bound](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_compaction.rs:254)

For A, basic resident metadata grows approximately as
\(3136L+40U+32\lceil N/256\rceil+O(D)\), before allocator/runtime overhead. The production admission is more conservative: it charges three metadata copies, another \(32\times\) root allowance, pins, and concurrent query buffers.

For either architecture, account explicitly:

\[
M_{\rm host}=\sum_{\rm owners}
\left[\sum_{\rm distinct\ pinned\ generations}R_g+
M_{\rm delta}+M_{\rm cache}+cM_{\rm query}\right]+M_{\rm runtime}.
\]

A’s query admission includes \(2B_{\rm SOURCE}+3B_{\rm SQ8}\), ranking scratch, leaf buffers and planner scratch. B must define equivalent bounds before claiming reduced RAM. Concurrency multiplies buffers; pins multiply retained metadata and delay reclamation.

For B, specify bounded authenticated directory pages, semantic cells containing colocated IDs and compressed records, then bounded SQ8/refinement reads. Initially preserve existing two-bit values and scoring; replacing them with one-bit codes is a separate quality change. With internal fanout256 and a 4-MiB root allowance, directory arithmetic can keep the top root bounded: approximately **1.53–3.06 MB / 0.063–0.123 MB / 0.599–1.199 MB** at the three scales. Remaining directory pages are fetched on demand; resident memory becomes bounded root/cache plus query buffers, rather than all leaf prototypes.

That does **not** prove three round trips. Root → cells → rerank is three dependencies only when no intermediate directory or separate head lookup is required. Paging introduces another dependency at larger scales. Fable’s “head then root” also contains two sequential reads. TPANNv3’s hierarchy, binary refinement and ≤3-round-trip context motivate the design; matched vendor p95 and dollars remain unknown. [Saved primary-source reconciliation](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/reconciliation.md:102)

B’s build must replace flat assignment with bounded hierarchical assignment and streamed/external layout construction. A candidate work model is \(O(ND+IUF D\,h)\); training skew, reservoir size and scratch constants remain unqualified. Scratch must include input/output coexistence, sort runs, IDs, journals and upload buffers—not merely router construction.

The library lifecycle specification must cover inserts in bounded deltas, replacements/deletes hiding old logical IDs before top-k, authenticated recovery, cell-local split/reassignment, atomic publication, pinned readers, and GC of unreachable objects. Existing compaction rebuilds a complete generation; cell-local maintenance is **new work**. Worst-case pinned storage remains \(pV\), with lower usage only where immutable objects are actually shared. Total cost must charge building, storage, reads, transfer, updates, compaction, recovery and GC per successful query.

**Prior results constrain the design without deciding it.** V139 rejected an unbounded threshold with severe D768 byte tails. V146 established correct flat scoring but failed its frozen CPU gate. V149’s physical-page hierarchy lost discovery coverage. V150 lost candidates during discovery and unit-to-page conversion; its later truth-coverage diagnostic did not reverse its FAIL. Semantic-router closeouts demonstrate that semantic grouping can survive returned-quality gates, but neither their 100k results nor current CoHere results qualify the new hierarchy. Existing FAIL/PASS dispositions and arm-specific caps remain immutable.

**One next falsifier: a truth-free, deterministic locality replay.** Root should authorize only this bounded check before deciding on a rewrite:

1. Authenticate the existing closed trace, generation root, page geometry and the **125,000-byte membership body** through their retained receipts. Fix a virtual permutation ordered by `(semantic_leaf_id, original_unit_id)`, preserving rows within each unit. Fit no new tree.
2. Map each query’s original SOURCE closure and all originally fetched SQ8 rows into that virtual layout. Replay bounded range covering; retain every required row. Use no query vectors, truth, scorer, ANN execution or cloud.
3. Compare this conservative locality hypothesis against recorded A and modeled A+SOURCE32. **Pass:** fewer than 7,142 aggregate query GETs, at most 2,393,364,520 aggregate query payload bytes, preserved required rows, and no frozen per-query admission violations. **Fail:** any geometry mismatch or missed criterion. Authentication/resource failure is **INVALID**, not architectural failure.
4. Bound execution to one CPU, 512 MiB, 120 seconds and 2 MiB output. No Cargo build or full payload rewrite.

This tests whether rearranging the existing semantic groups can beat the fetch tradeoff **without another quality peek**. Failure rejects that specific conservative locality hypothesis, not every hierarchy. Passing establishes conditional geometry only: it does not validate hierarchical nomination, changed page scoring, cold latency, incremental maintenance or format approval.

No files were edited, builds launched, queries executed or new consultations started. The pilot was untouched. The checkout advanced externally to `eced3621`; the inspected native source still matches `addf62bce23ceee…`.

**Select (a): 16-row authenticated SQ8 fetch groups, selected by a resident PQ64 row graph.** Implement one 100k falsifier. This has a small reusable query-path diff; a qualified 100M builder and incremental graph maintainer would require further work.

| Intervention | Causal change | Assessment |
|---|---|---|
| **(a) Finer fetch groups + compressed row routing** | Select individual source rows, then fetch small contiguous groups containing them. | Best next falsifier: separates candidate discovery from whole-page amplification. Reuses existing graph nomination, range covering and SQ8 scoring. Risks substantial resident memory and many GETs. |
| **(b) Rebuilt semantic clustering** | Source-only multiway spherical Lloyd assignment, with overflow clusters recursively split; remove forced occupancy reassignment and nearest-path replication. | Feasible using `train_logical_cell_centroids`; more natural maintenance units. However, no measured evidence yet shows that it concentrates these diffuse neighborhoods sufficiently. It changes both membership and the summaries used for routing, making the first diagnosis less direct. |

The current partitioner is already sampled two-means with capacity repair; median splitting is its degenerate fallback. Changing only the distance formula or balance threshold would not constitute the proposed intervention (b).

The [unchanged-layout oracle](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/a0001/decision.md) rules out ≤32 unchanged whole cells on CoHere. The [overlap result](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/boundary-overlap/paired100k/a0002/decision.md) leaves coverage only 0.109375/0.0625 percentage points above returned recall: missing candidates remain the dominant quality loss.

1. **Freeze this algorithm and prospective resource arm.**

   Preserve the current primary physical order, logical IDs, SQ8 codes and coefficients; omit replicas. Partition the concatenated SQ8 records into groups of **16 rows**, including a short final group.

   Build source-only PQ64 codes and a reachable resident graph in that same physical order. Freeze graph construction at `m=32`, `m0=64`, `ef_construction=128`. For native PQ fitting, use deterministic source-ordinal sampling of at most 16,384 normalized rows, 256 codewords per subspace and four training iterations. Queries and truth are unavailable to construction.

   Per query:

   - PQ cosine navigation: `ef=4096`, shortlist **1,024**, at most **65,536 scoring evaluations including upper-layer work**. On exhaustion, retain the current bounded shortlist and report exhaustion.
   - Map every shortlisted ordinal to `ordinal / 16`.
   - Use existing `cover_pages` to bridge the smallest gaps until there are at most **256 ranges**.
   - Admit only if total fetched payload, **including bridged gaps**, is ≤**16 MiB**. Preserve every shortlisted row; reject an infeasible cover.
   - Authenticate every fetched group and score all returned SQ8 rows.

   Unmerged shortlist payload is bounded arithmetically by:

   \[
   1024\times16\times780=12{,}779{,}520\ \text{bytes}.
   \]

   Only **3,997,696 bytes** remain for bridging. Whether 256 GETs can achieve this is the central falsifiable question.

   This changes the physical fetch unit and replaces cell-summary selection with row-specific navigation. [V239](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v239-graph-candidate-containment-100k-closeout.md) established candidate containment, not returned SQ8 recall; [V240](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v240-graph-local-order-100k-closeout.md) changed ordering while retaining 256-row pages. Neither establishes success here.

   The **256-GET/16-MiB** envelope is a new arm, after router admission. Historical 32-GET gates and both V239/V240 failures remain unchanged. With 32 request slots, 256 GETs can require eight scheduling batches despite having one dependency stage. Cold router hydration, mutation loading and retries require separate accounting.

2. **Keep the native implementation narrowly scoped.**

   | Rust file | Required change or reuse |
   |---|---|
   | `crates/borsuk/src/pq64_nominee.rs` | Expose a codes-only cosine scorer using the existing lookup-table arithmetic; avoid allocating unused cell summaries. Add `fit_source_codes` using source-only training. |
   | `crates/borsuk/src/resident_vector_graph.rs` | Add plane-independent authenticated opening and `nominate_pq(query, pq, limits, workspace)`. Reuse navigation; enforce the evaluation and allocation bounds. |
   | New `crates/borsuk/src/fine_sq8_groups.rs` | Own `build`, `open`, `plan` and `search`, authenticated generation identity, group hashes and resource receipts. |
   | Existing `budgeted_page_rank.rs`, `sq8_page_authority.rs`, `returned_sq8.rs` | Reuse `cover_pages`, page authentication and `rank_returned_ranges_excluding`; no new planner or scorer. |
   | `bin/hierarchical_semantic_cells.rs`, `lib.rs`, new `tests/fine_sq8_groups_native.rs` | Thin command/export integration and independent correctness checks. |

   Proposed API boundary:

   ```rust
   FineSq8Index::open(root: &Artifact, limits: &ResidentLimits) -> Result<Self>
   FineSq8Index::plan(query: &[f32], workspace: &mut GraphSearchWorkspace)
       -> Result<FineFetchPlan>
   FineSq8Index::search(plan: &FineFetchPlan, query: &[f32], k: usize)
       -> Result<FineSearchTrace>
   ```

   Bind plans to the query digest and pinned generation. The new versioned root must bind source/order identities, graph, PQ books/codes, SQ8 coefficients, record object and group-hash table. Use 780-byte records and 32-byte hashes per group. A new graph marker must replace the current FP16-plane binding with authenticated generation/layout/PQ identity. Reject incompatible artifacts.

   **Existing reuse has limits:** graph loading currently requires an FP16 plane, PQ construction requires unused summaries, and graph search has no hard evaluation cap. Those seams need explicit changes.

3. **Charge scale honestly.**

   Let \(G(N)\) be actual graph heap capacity, including nested vectors and upper layers. Prospectively admit \(G(N)\le512N\); this is a new allocation ceiling, not measured graph size.

   With a 2-MiB fixed metadata allowance:

   \[
   R(N)=G(N)+64N+4N+32\lceil N/16\rceil+2\text{ MiB}
   \le582N+2\text{ MiB}.
   \]

   The current epoch workspace costs **4N bytes per worker**. Reserve another 32 MiB per active query for payload, heaps, lookup tables, planning and ranking:

   \[
   Q(N)=4N+32\text{ MiB}.
   \]

   All sizes below are **arithmetic**, using decimal MB/GB.

   | Rows | Router admission ceiling | Workspace/query | Two pinned router ceilings | SQ8 storage | Known build charge* |
   |---:|---:|---:|---:|---:|---:|
   | 100k | 60.3 MB | 34.0 MB | 120.6 MB | 0.078 GB | 0.523 GB |
   | 1M | 584.1 MB | 37.6 MB | 1.168 GB | 0.78 GB | 5.228 GB |
   | 10M | 5.822 GB | 73.6 MB | 11.644 GB | 7.8 GB | 52.28 GB |
   | 100M | 58.202 GB | 433.6 MB | 116.404 GB | 78 GB | 522.8 GB |

   \*The existing builder retains source `Vec<Vec<f32>>` and FP16 authority. Its explicit planning charge is:

   \[
   B_{\rm build}=4716N+G(N)+W_{\rm builder}+T_{\rm fitter}+B_{\rm buffers}.
   \]

   The table substitutes the graph ceiling but **excludes the unknown terms**. It is not a peak-memory guarantee. The present builder is therefore unsuitable for an assumed small-memory 100M build.

   For output \(O=780N+R_{\rm encoded}\), reserve:

   \[
   S_{\rm build}=2O+1552N+W_{\rm disk},
   \]

   covering output/staging copies, temporary FP16 authority and ordering. Canonical originals add **3080N bytes** separately. Allocation arithmetic must be checked before allocation; actual capacities, RSS and scratch must be recorded.

   Serving and swap admission must use:

   \[
   M_{\rm peak}=\sum_{\text{unique pinned generations}}R
   +C_{\rm active}Q+M_{\Delta}+M_{\rm maintenance}+M_{\rm runtime}.
   \]

   At 100M, neither cold startup nor low lifecycle cost follows from these numbers.

4. **Make the maintenance limitation an explicit promotion gate.**

   Queries pin base plus mutation revision. Deletes and replacements suppress older logical IDs before truncation; replacements remain query-visible in a bounded delta. Publish immutable artifacts before conditional head replacement, and reclaim only after pins release.

   For a future incremental implementation, let \(u\) be changed 16-row groups, \(a\) changed 64-KiB router chunks and \(j\) changed metadata chunks. Its charged rewrite would be:

   \[
   W_{\rm incremental}
   =u(16\times780)+65536(a+j)+32u+\text{canonical/delta writes}.
   \]

   Measure read amplification, PUT/GET counts, CPU and retained generations separately. This formula becomes useful only after bounding \(a\); graph rewiring can make it large.

   **Current code does not provide this maintainer.** Its conservative fallback is whole-generation work: approximately \(780N+R_{\rm encoded}\) bytes written per rebuild, plus construction, staging and pinned-old-generation costs. A passing static falsifier cannot establish incremental maintenance or authorize 100M promotion.

5. **Run one cheap native falsifier before scale.**

   Proposed later execution bounds: datasets sequentially; build ≤2 workers, 8 GiB RAM, 8 GiB scratch, 20 minutes; query/evaluation one worker, 512 MiB, five minutes; no swap. Root owns admission and execution.

   - **Independent fixtures first:** brute-force small interval covers versus `cover_pages`; unchanged SQ8 bytes and ID bijection; short final groups; corrupt hash/length; nonunit queries; ties; evaluation exhaustion; stale-generation plans; delete/replace exclusion; old-reader pins.
   - **Consumed64 FIRST100k panels only:** seal source-built artifacts, graph nominees and fetch plans before opening truth. Compare 16-row and 256-row cover costs for the **same nominees**. Record candidate containment, fetched coverage, returned R@100, p05, actual local reads/bytes, stage CPU, RSS and scratch.
   - **Falsify** if either panel misses mean coverage or returned recall ≥98%, p05≥95, or any query cannot preserve its shortlist within 256 GETs/16 MiB. A valid infeasible cover rejects the resource hypothesis; authentication failures, crashes, timeouts and execution-resource failures are **INVALID**.
   - No parameter ladder on these panels. Survival permits fresh-panel qualification; it establishes no cold-S3, QPS or vendor result.

   Before integration, compile/run the affected native target, pass the prescribed workspace Clippy command and `scripts/check_rust_test_build.sh`, recording exact revision and exits. Paid work still requires exact-input admission and a separate disposable canary.

The latest [execution receipt](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/boundary-overlap/paired100k/a0002/screen/overlap/execution-receipt.json) bounds SIMD expectations: the paired full-SQ8 section consumed **1.967/1.871 seconds CPU across 128 arm-queries**, or **15.37/14.62 ms per arm-query averaged across control and candidate**. It includes reads, authentication, ranking and invariant checks, so eligible SIMD CPU is smaller. Even erasing that entire section removes only about **59% of evaluator CPU**; it cannot repair coverage or establish an S3 tail improvement.

Read-only inspection at `40290131`; no files changed, builds, native/data/cloud execution, children or reviews launched.

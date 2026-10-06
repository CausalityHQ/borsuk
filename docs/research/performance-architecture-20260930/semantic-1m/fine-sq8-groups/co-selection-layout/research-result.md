**Propose one falsifiable rule: pack intact 16-row groups by source-anchor nomination co-selection, then allow bounded expansion from one matching source-anchor nomination.** It preserves the current nomination program, PQ representation and SQ8 scorer. It adds no replicas.

This is justified for one virtual-layout test, not for production promotion. The missing evidence is whether source-derived co-selection predicts physical locality and useful expansion on the frozen plans.

The design is anchored to `f76314782c9758049aeee8c64bde48635b488822`. During this consultation, HEAD advanced to `3844b5d8a106bba3bd0b901079e57ecdb22b837d`; that change adds maintenance constraints to the decision document. The inspected Rust files are unchanged.

**The retained failures constrain this rule as follows.**

| Evidence | Failure mechanism and consequence |
|---|---|
| [V139 closeout](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v139-centroid-page-preflight-closeout.md) | Unbudgeted centroid threshold admission exceeded the D768 envelope on 364/1,000 queries. Expansion must be admitted by physical cost. |
| [V146 closeout](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v146-unit-centroid-score-closeout.md) | Correct flat scoring reached 11.071522 ms p95 for score plus planner at 1M and scales linearly. No serving-time global scan is proposed. |
| [V149 closeout](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v149-contiguous-hierarchy-closeout.md) | Physical-page summaries captured only 77.0956% of the reference page plan. Placement blocks here do not participate in query routing. |
| [V150 closeout](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/v150-unit-centroid-graph-closeout.md) | Bounded discovery missed candidates; nearest-unit truncation also crowded out distinct pages. Its later 99.523% fetched truth coverage did not reverse its frozen rejection. |
| Hierarchy/global-leaf/capacity partitioner | Returned quality around 65%/69%; global24 recovered little. Even perfect selection of 32 unchanged cells reaches only CoHere 97.171875% mean coverage/p05 88. Whole-cell routing cannot satisfy the gate on that layout. |
| Boundary overlap | Returned 89.34375%/80.703125%, p05 64/61. Geometric replication could not repair the selected-cell deficit. |
| Fine SQ8 groups | Nomination plus surrounding rows supports passing consumed-panel quality, but physical fragmentation violates the range cap. |
| Graph-affinity packing | Valid REJECT: 35/64 and 16/64 fits, maxima 42.49/31.15 MB. Graph adjacency is not an adequate co-selection predictor. |
| Residual source probe | Valid ranking-agreement REJECT: 98.234375%/96 and 96.296875%/93. That arm remains stopped. |

I read both requested documents in full. The earlier `fable-result.md` tree proposal changes discovery and has unresolved boundary, startup and lifecycle assumptions. `source32-geometry/consultation.md` demonstrates the opposite cost tradeoff: fewer requests increased transferred bytes. Neither supplies a replacement merge heuristic. For a fixed order and mandatory groups, the current smallest-gap merger already minimizes bytes.

The decisive current evidence is the [closed group audit](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/source-probe/native-diagnostic/a0001/closed-nominee-group-bound-audit.md): mandatory groups cost at most **8,224,320/8,748,480 bytes**, while their minimum 32-range covers reach **32,747,520/27,081,600 bytes**. The first target is therefore the physical dispersion of groups that the nomination program actually selects together.

**Freeze this complete algorithm before examining the consumed plans.**

1. **Generate source-only training selections.** Stream the authenticated source-ID roster. Order rows by SHA-256 of a fixed domain separator, source identity and logical ID; break hash ties by logical ID and original ordinal. Take the first 4,096 as training anchors and the next 512 as source-held diagnostics.

   For each anchor, read only its canonical source vector and call the unchanged native nomination path. Preserve normalization, graph entry point, PQ coefficients, navigation limits and ordering. Current `nominate_pq` uses a 65,536-evaluation ceiling and returns at most 1,024 nominees.

   Let \(H_a\) be the distinct original 16-row groups containing those nominees. Record each group’s first position in the anchor’s ordered nomination. Requests and experimental truth remain inaccessible. These are source-program selections, not graph-edge lists or a residual-training cohort.

2. **Keep every group indivisible.** Preserve all original SQ8 row bytes, within-group order, norms, logical IDs and coefficient bits. Keep the short final group last. The graph and PQ retain their original ordinal space; only payload locations change.

3. **Partition groups using complete co-selection hyperedges.** Set

   \[
   C=\min\!\left(64,\left\lfloor
   \frac{16\,\mathrm{MiB}}{32\cdot16(D+12)}
   \right\rfloor\right).
   \]

   At D768, \(C=42\), already used by the rejected packing arm. The new causal signal is the actual source nomination hypergraph.

   Initialize blocks with consecutive original group IDs, at most \(C\) groups per block. For anchor \(a\) and block \(b\), let \(n_{ab}=|H_a\cap b|\). Minimize the integer objective

   \[
   \Phi=\sum_{a,b}\left[2^{C+1}-2^{C+1-n_{ab}}\right],
   \]

   with a zero contribution when \(n_{ab}=0\). This is an exactly represented probabilistic-fanout objective; use checked `u128` totals and signed deltas.

   Perform **one sweep**, visiting full groups by original group ID. Candidate destination blocks must share a training anchor with that group. Keep the eight best destinations by relaxed move gain, ties by fixed block ID. Examine every swap partner in those blocks. Accept the greatest strictly positive reduction in \(\Phi\), ties by destination block ID and partner group ID. Zero-gain moves are rejected. The short tail never moves.

   Preserve fixed block order and sort members within each block by original group ID. No refinement ladder follows a failed replay.

   This objective is a placement heuristic. Its improvement does **not** establish a byte-budget improvement; the exact cover falsifier decides that.

4. **Create immutable, range-readable payload parts.** Concatenate groups without padding or record headers. Place 128 blocks per object, splitting only at block boundaries. At D768 a full part is 67,092,480 bytes, below 64 MiB.

   The resident directory contains every object identity, length, conditional-read identity, group location and group digest. Planning requires no remote directory lookup or pointer chasing.

5. **Expand through one matching source selection.** For a request, obtain the unchanged nominees and mandatory group set \(H_q\).

   Select one training anchor maximizing

   \[
   J(a,q)=\frac{|H_a\cap H_q|}{|H_a\cup H_q|}.
   \]

   Compare ratios by integer cross multiplication. Break ties by anchor logical ID, then original ordinal. If all intersections are zero, expand nothing.

   Start with every mandatory group. Visit groups in \(H_{a^*}\setminus H_q\) by their first anchor-nomination position, ties by original group ID. Admit a group only when the **complete new minimum cover**, including bridges and object boundaries, fits the remaining request and byte budgets. Skip an unaffordable group and continue. Expansion finishes before any payload read.

   The mechanism hypothesis is specific: a source selection overlapping the current selection may expose nearby groups that this request’s PQ shortlist missed. This hypothesis is unproved. It is more directly aligned with discovery than graph-edge affinity, and the frozen replay and subsequent SQ8 gate can reject it.

6. **Rank every actual fetched row.** Authenticate mandatory, expansion and bridge groups. Score their complete populations with unchanged SQ8 arithmetic and ascending `(score, logical ID)` ordering. No nominee-only filter is allowed.

Replication factor is **one**. Boundary replication remains deferred because existing evidence does not identify which copies would reduce this rule’s cover cost enough to justify their storage and maintenance burden.

**The cover must enforce the total budget.**

For one object, minimum cover cost equals the span of selected groups minus the largest gaps that the available requests can omit. For multiple objects, let \(J\) be the number containing selected groups and \(K\) the remaining request budget. If \(J>K\), reject. Otherwise start with one enclosing range per required object and omit the largest \(K-J\) eligible internal gaps globally. Adjacent groups introduce no gap.

Use deterministic gap ties by object ID and offset. The independent oracle should use interval DP, rather than reuse this implementation.

\[
F_{\rm total}=F_{\rm metadata}+F_{\rm base}+F_{\rm delta}
                 +F_{\rm additional\ attempts}\le32,
\]

\[
B_{\rm total}=\sum_{\rm all\ attempts}B_{\rm charged}\le16,777,216.
\]

Reserve costs before dispatch. The first falsifier uses an empty delta and no retries. Reuse `OneAttemptS3`; failures retain their charges and do not trigger an unbudgeted recovery fetch. S3 requires separate GETs for disjoint ranges. [AWS GetObject documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

All payload locations are known before fetching, giving one dependency wave. Concurrency and ordered buffering still affect scheduling and tails; request count alone does not measure transport rounds.

Resident-discovery replay does not qualify process-cold startup. If startup consumes the same total envelope, deduct it before planning. At 100M, PQ codes alone require 6.4 GB, so unchanged discovery cannot satisfy a truly cold 16-MiB boundary. This layout rule cannot conceal that blocker.

**The cheapest missing closed-data calculation is the truth contribution of the omitted winners.**

Before implementation, freeze this policy and its constants, then perform one authenticated join using the already closed result bodies, mappings and truth:

- Identify the **3 ReLAION and 52 CoHere winner appearances** outside mandatory groups.
- Classify each as a true hit or false positive using its original query ordinal and logical ID.
- For query \(i\), compute \(c_i\), the number of omitted old winners that are true hits, and the sharper retained-winner bound \(h_i-c_i\).
- Report the affected lower-tail queries and the groups containing those useful appearances. Do not use them to choose anchors, moves, capacities or expansion rules.

This needs no query vector, new truth generation or ANN run. Reuse the original truth decoder and ID-map validator; authenticate the closed authorities rather than assuming ID width or ordering.

That join still does **not** calculate exact group-only recall: replacement results below the old top100 are absent. Moreover, the proposed layout fetches new rows that may displace retained winners. Consequently, neither the existing `.9821875/94` bound nor its sharpened version certifies this candidate. Actual new-population ranking remains mandatory.

**Resource arithmetic exposes the remaining scale problem.**

Let \(G=\lceil N/16\rceil\), \(A=4096\), \(I=\sum_a|H_a|\le1024A\), \(E\) be encoded graph bytes and \(B=16\) MiB. Store two group permutation directions, forward selections and inverse group-to-anchor incidence. A conservative additional resident allowance is

\[
\Delta R=16G+8I+16A+O(\text{object directory}).
\]

With D768, retaining the current admission model:

\[
R\le590N+786,432+2\,\mathrm{MiB}+\Delta R,
\]

\[
Q=4N+3B+8\,\mathrm{MiB}
  +256\min(N,\lfloor B/780\rfloor)+4D.
\]

The layout planner must fit its actual capacities inside the existing 8-MiB allowance or increase admission explicitly.

| Projection, decimal units | 100k | 1M | 100M |
|---|---:|---:|---:|
| SQ8 payload, one copy | 78 MB | 780 MB | 78 GB |
| Added layout/anchor allowance, worst \(I\) | 33.72 MB | 34.62 MB | 133.62 MB |
| Resident admission projection | 95.60 MB | 627.50 MB | 59.137 GB |
| Current query allowance | 64.63 MB | 68.23 MB | 464.23 MB |
| Serving artifacts, excluding \(E\) and fixed metadata | 119.12 MB | 888.62 MB | 85.534 GB |

These are allocation projections, not RSS measurements. Keep the current 100k guard.

A fixed 4,096-anchor dictionary is also unqualified at scale. At 100M, its maximum 4,194,304 group incidences cannot even touch all 6,250,000 groups; repeated incidences make coverage worse. Increasing \(A\) increases training, storage and startup costs through \(I\). Measure that tradeoff rather than assuming source-anchor coverage generalizes.

Peak admission must count distinct pinned allocations, opening transients, concurrent \(Q\), delta, maintenance and runtime. Shared graph/PQ memory may be counted once only when actual ownership shares it.

Training introduces at most 268,435,456 graph evaluations for 4,096 anchors, plus fitting work. It reads anchor coordinates and metadata without hydrating SQ8 or full source vectors. Nevertheless, the graph is still resident. The previous 1.12-second packing diagnostic does not qualify this builder’s resources.

The measurable Pareto consists of exact cover bytes at 8/16/24/32 requests on the **same frozen layout**, expansion and bridge bytes, planner CPU, startup, RSS, cold p90/p95, QPS and lifecycle cost. Only the declared 32-request/16-MiB policy receives the recall gate; the curve does not authorize retuning.

**Lifecycle integration must remain native and explicit.**

Queries pin a generation, layout identity and mutation revision together. The new root binds unchanged discovery separately from the new payload mapping. Authenticate row IDs through the inverse group map; reject incompatible roots clearly.

Use a bounded durable latest-state delta for insertions, replacements and deletes. Suppress every base copy of replaced/deleted IDs; score visible SQ8 upserts once with frozen coefficients. New IDs remain searchable through the admitted resident delta scan until compaction. Full-delta backpressure must require compaction, never drop updates.

Compaction runs in-process: capture a sealed delta, build immutable artifacts, verify them, publish by conditional head update, and retain old artifacts until pins release. Content-addressed payload parts permit reuse of unchanged parts. Global reordering can still rewrite the entire \(780N\) payload; graph/PQ rebuilding for changed membership has separate costs.

Current `FineSq8Snapshot` supports in-memory replacements/deletes within the existing roster. It does not provide durable new-ID insertion or recovery. Existing `compact_two_bit_index` has an owned worker and durable journal but rejects `Fresh1m`; it is not a working fine-SQ8 integration. Those limitations remain product gates.

**The minimum initial Rust scope is small.**

- New `crates/borsuk/src/co_selection_layout.rs`: source-selection validation, deterministic fitting, authenticated group map and metadata-only planning.
- [fine_sq8_groups.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs): reuse `plan()`/`nominees()` for anchor selections; later bind the qualified layout to fetching.
- Existing native diagnostic binary plus `lib.rs`: one command and module registration.

The useful API surface is:

```rust
CoSelectionLayout::fit(source_selections, geometry, limits)
CoSelectionLayout::plan(original_nominees, remaining_read_budget)
CoSelectionLayout::replay_frozen_plans(authenticated_prefix)
```

No graph API change is needed. Reuse [rank_unique_sq8](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/returned_sq8.rs:18), group authentication and the existing one-attempt transport. Durable maintenance integration follows only after locality and quality survive.

**The first decisive test remains virtual.**

1. Authenticate source selections and seal the fitted mapping before opening the frozen nomination prefix. Replay all 128 plans with requests, truth and SQ8 payloads inaccessible.
2. Independently verify group bijection, tail placement, logical nominee retention, object boundaries and every charged row.
3. Compare covers against interval DP and tiny exhaustive covers. Include cross-object cuts, tied gaps, short tails and expansion rejection.
4. Reject the rule if any plan violates the total envelope or loses a nominee. Authentication and execution defects remain INVALID. Do not rewrite payloads after a locality REJECT.
5. Only after survival, fetch the actual populations with unchanged SQ8, compare ordered results with an independent scalar scorer, and seal **both panels’ complete results before truth access**.
6. Require each panel at least **6,272/6,400 true hits**, fourth-smallest hit count **at least 95**, and every physical-budget gate.

Root retains source qualification, admission/canary protocols, compiler checks, resource limits and paid-launch authority.

Primary research supports mechanisms, not qualification. SPANN shows why bounded postings and boundary visibility can help; its centroid selector and geometric replication are not imported here. DiskANN shows the value of obtaining refinement data through already useful I/O, but SSD graph chasing and “free” sector piggybacking do not transfer to S3. Social Hash Partitioner motivates whole-hyperedge fanout rather than pairwise graph affinity. These mechanisms transfer across dimensions; byte envelopes scale with \(D+12\), and their paper recalls or latencies do not. [SPANN](https://arxiv.org/pdf/2111.08566), [DiskANN](https://harsha-simhadri.org/pubs/DiskANN19.pdf), [Social Hash Partitioner](https://www.vldb.org/pvldb/vol10/p1418-pupyrev.pdf)

No edits, native/data execution, children, or additional consultations were performed. **This rule earns one falsifier; no new recall, latency, lifecycle or vendor claim is established.**

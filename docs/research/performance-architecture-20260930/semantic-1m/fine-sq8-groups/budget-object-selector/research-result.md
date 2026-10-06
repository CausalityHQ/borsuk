# Budgeted object selector: research hypothesis

Consultation `44c5af9c05414c41` completed exit 0. Requested Fable; Fable and Opus authentication failed; effective researcher GPT-6.1 Sol, max. No native experiment or implementation is established by this result. Root has not frozen a training method or selected production defaults.

**Choose one increment: jointly train object membership and a direct, budgeted object selector; fetch complete objects and retain unchanged SQ8 scoring.** The first deliverable should be a Rust library primitive with a metadata falsifier and source-only native qualification. This is an architecture hypothesis, not a production freeze.

The causal reason is that placement around the existing global nomination program has failed twice. Original order fits **50/64 ReLAION and 32/64 CoHere**; co-selection fits **48/64 and 33/64**, despite improving its training objective by **5.03% and 6.15%**. Held source-anchor fits are only **381/512 and 216/512**, so external-query distribution mismatch cannot explain the whole failure. Intrinsic mandatory records fit; bridging their dispersion does not. [Closed co-selection decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/native-diagnostic/a0002/decision.md)

Preserving nominees alone also cannot preserve quality: CoHere nomination containment is **95.609375% mean / 87 p05**; surrounding SQ8 rows rescue the passing **98.515625% / 95** result. Uniform SQ4, histogram SQ4, corrected four-bit and PQ-residual retain their REJECTs. [Passing SQ8 evidence](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/decision.md)

| Distinct option | Mechanism | Principal risk | Decision |
|---|---|---|---|
| **Joint budgeted object selection** | Learn neighborhood-bearing objects and their selector together; score every fetched SQ8 row | Learned routing and neighborhood concentration may fail | **Choose**: addresses dispersion, nomination loss and serving RAM together |
| **Bounded replica multicover** | Preserve nomination; duplicate at most one copy of selected groups, with total replication ≤1.125; choose copies by actual cover cost | Can improve locality, but retains the global graph, incidental-row uncertainty and expensive replica maintenance | Keep as the alternative |

Replica destinations would come from source co-selection, with gains measured against complete physical covers—not geometric boundary distance or pairwise edge count. That distinguishes the alternative from the rejected overlap policy, but supplies no evidence that it succeeds.

**Proposed algorithm and layout**

Retain original SQ8 coefficients, record bytes, norms, IDs and score ordering. Keep original 16-row groups intact initially. Build objects with a target fill of **56 groups**, a hard capacity of **64 groups / 1,024 rows**, and no padding. The spare capacity supports bounded insertion and compaction.

Give each object a learned product label `(i,j)`. Use one 64-unit hidden layer and four mixture components:

\[
p(i,j\mid q)=\sum_{r=1}^{4}\pi_r(q)\,u_{ri}(q)\,v_{rj}(q).
\]

Each component contributes the Cartesian product of its eight highest-scoring labels from each head. Deduplicate the resulting **≤256 object candidates**, evaluate their mixture scores, and select at most **15 distinct objects**. There is no serving scan over every object and no remote graph traversal.

Training uses authenticated source anchors and source-neighbor labels, excluding experimental requests and GT. Alternate selector fitting with capacity-constrained group reassignment. Weight labels by the number of source neighbors they contain, rather than “contains any neighbor.” Accept membership changes only when **actual selected-object neighbor coverage** improves; evaluate lower-tail coverage separately from mean coverage. A smooth training-loss improvement is insufficient.

The exact seed, optimizer, rounds, candidate enumeration and operation ceilings need preregistration before fitting. No parameter ladder follows rejection. At 100k, source labels can be checked against exhaustive native SQ8; a scalable approximate teacher is a separate, unqualified build component.

Each content-addressed object contains a **64-byte header plus unchanged SQ8 records**. A binary root holds model weights, coefficients and one 32-byte content digest per object. Query lookup needs no per-row location map. Authenticate complete bodies, reject incompatible formats, and score every visible fetched row.

This changes the objective from centroid proximity or probabilistic fanout to **neighbors recoverable through the legal fetch set**. It avoids V146’s global scan, V149’s physical summaries and V150’s repeated-unit crowding. It does not inherit a recall guarantee from those distinctions.

Learning partitions and routing jointly has precedent in [BLISS](https://gaurav16gupta.github.io/papers/BLISS_KDD2022.pdf); treating routing as neighbor-weighted ranking has precedent in [the SIGIR routing formulation](https://arxiv.org/html/2404.11731v1). The factorized selector and object geometry above are **my proposed synthesis**, not a published qualified configuration.

**Exact allocation and transfer projections**

Let

\[
G=\lceil N/16\rceil,\quad
s=\left\lceil\sqrt{\lceil G/56\rceil}\right\rceil,\quad J=s^2,
\]

with hidden width \(h=64\), mixture count \(r=4\), and dimension \(D\). Including biases:

\[
P=h(D+1)+2rs(h+1)+r(h+1).
\]

A root allocation with a 4,096-byte fixed allowance is

\[
R=4P+32J+8D+4096.
\]

Payload and object-header storage is

\[
S_{\rm payload}=N(D+12)+64J.
\]

At **100M, D768**:

| Quantity | Projection |
|---|---:|
| Objects \(J\) | 112,225 |
| Model parameters / bytes | 223,676 / 894,704 B |
| Complete root | **4,496,144 B** |
| SQ8 objects and headers | **78,007,182,400 B** |
| Optional disk maintenance ID-location table, 16 B/row | 1.6 GB |
| Group-owner construction array, 4 B/group | 25 MB |
| Maximum fetched object | 798,784 B |

Reserve a 64-KiB head and a **4.5-MiB root ceiling**. With an empty delta and one attempt per read:

\[
F_{\rm cold}\le1+1+15=17,
\]

\[
B_{\rm cold}\le65,536+4,718,592+15(798,784)
=\mathbf{16,765,888\ B}<16,777,216.
\]

This includes object headers and cold metadata. It uses whole-object GETs; S3 cannot combine disjoint ranges into one GET. [AWS GetObject documentation](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

Removing row-graph marks eliminates the **4N-byte workspace**. Reusing the existing conservative response/ranking allowance, plus 128 KiB for selector scratch, gives:

\[
Q=3B+8\mathrm{MiB}
 +256\lfloor B/(D+12)\rfloor+4D+128\mathrm{KiB},
\]

or **64,360,704 B/query** at D768. Admission must additionally count unique pinned roots, opening transients, delta, maintenance and runtime. These are allocation projections; no RSS or latency measurement establishes them yet.

The replica alternative instead retains approximately **59.003 GB/generation and 464.23 MB/query**, before replica directories and maintenance. Its SQ8 payload becomes at most **87.75 GB**. It does not solve bounded cold router admission.

There is a real memory–quality tradeoff: widening the proposed hidden layer to 128 raises the 100M root to about **5.38 MB**, leaving room for only 14 maximum-sized objects under the same cold byte cap. That is an unselected Pareto point, not permission to retune after failure.

The formulas apply across dimensions through \(D+12\). Paper results at other dimensions support mechanisms, not D768 recall or latency. Likewise, [turbopuffer’s ANN v3 architecture](https://turbopuffer.com/blog/ann-v3) supports coupling routing with fetch units; its published results do not establish a matched BORSUK comparison.

**What improves, and what remains exposed**

The proposed path can remove gap overfetch, PQ shortlist truncation inside selected objects, repeated candidate slots and serving row-graph residency. It preserves SQ8’s measured scoring advantage.

It cannot repair neighbors outside selected objects, biased source labels, insufficient classifier capacity or SQ8-versus-original-vector ranking error. Source self-neighborhoods may generalize poorly to external requests. Fetching larger complete populations can displace old winners, so changed-population SQ8 ranking remains mandatory.

Fewer GETs also need not improve QPS or dollars: 15 objects near target fill transfer about **10.48 MB**, above the historical 5.31/6.88-MB means. Measure bandwidth, scoring CPU, tails and maintenance cost before claiming a win.

**Native lifecycle**

The serving API should own the behavior directly:

```rust
BudgetObjectIndex::build(authenticated_source, source_neighborhoods, limits)
BudgetObjectIndex::open(authenticated_root, limits)
BudgetObjectSnapshot::plan(query, workspace, total_read_budget)
BudgetObjectSnapshot::search(plan, query, k)
BudgetObjectIndex::apply(batch)
BudgetObjectIndex::compact(limits)
```

Plans pin root, model, coefficient and mutation identities. A bounded durable latest-state delta makes new IDs searchable immediately and suppresses stale base rows before ranking. Encode query-visible upserts with matching SQ8 semantics. Charge delta loading against cold reads and bytes; reduce the base fetch allocation when necessary.

Compaction runs in-process, rewrites affected immutable objects, updates their root digests, publishes conditionally, and retains old objects until pins release. Backpressure applies when delta or object capacity is exhausted. Model retraining and widespread reassignment may require a global rebuild; local convergence is not established. [SPFresh](https://arxiv.org/html/2410.14452v1) motivates incremental rebalancing, but its centroid-assignment properties do not prove this learned-label scheme correct.

At 100M, canonical FP32 retention alone adds approximately **308 GB**. Teacher construction, sorting scratch, WAL and retained generations remain additional costs. The existing full-source builder is not qualified for this design.

**Lowest-cost falsifier and acceptance sequence**

1. **Start with sealed, truth-free metadata.** Reuse the authenticated source selections; fit only their training partition and seal it before held diagnostics or consumed-plan evaluation. I authenticated both selection binaries: each training set touches all 6,250 groups. Maximum held mandatory-group counts are **777/799**, giving a best imaginable 64-group-object lower bound of **13 objects** on both panels. Fifteen objects are therefore not ruled out by raw capacity; neighborhood concentration remains unknown.

   Report best-possible versus trained object coverage separately, actual object bytes, and retained closed nominees/winners. Use these as diagnosis. Do not reject a changed router solely for failing to reproduce the old page plan—the V150 evidence already shows that screen can be misleading.

2. **Leave one independent tiny oracle.** Exhaustively enumerate small memberships and legal object subsets; compare coverage and move deltas. Independently evaluate mixture probabilities and candidate deduplication. Include an adversarial case where the bounded Cartesian candidates miss a global winner: this is algorithmic loss, not an oracle defect. Check headers, hashes, tails, normalization, ties, stale-row suppression and old/new pins.

3. **Run a bounded native source-only gate before scale.** On disjoint source-held anchors, compare returned top100 against exhaustive native SQ8 over the same authenticated population. A proposed mechanism gate is **≥99.5% mean agreement and fourth-smallest ≥99**, with separately charged fitting and execution. This is source agreement, not external ANN recall. Preserve resource/authentication failures as INVALID.

4. **Qualify and test actual recall.** Compile affected targets, pass workspace correctness/suspicious Clippy and `scripts/check_rust_test_build.sh` on the exact revision. Then seal both panels’ complete actual-population results before GT. Retain **≥6,272/6,400 hits and fourth-smallest ≥95 per panel**, plus every total GET/byte gate. Consumed panels may falsify; fresh queries are required for promotion.

Only after those gates should root freeze any paid 1M or scale campaign and matched S3 Vectors/turbopuffer comparison.

Read-only throughout: no edits, native runs, corpus/request/GT access, AWS actions, children or consultations. HEAD advanced concurrently from `8d219d5` to `00b32192`; those changes added evidence documents, while the inspected implementation and original closed inputs remained unchanged.

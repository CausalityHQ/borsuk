# Contextless research receipt

ID: 833759170c374e45
Status: completed, exit 0. Research proposals only; no implementation or measured gain.

**Start with progressive SOURCE reads, then boundary-aware spill routing.** Keep a unit-level navigation graph as the larger architectural alternative, and local rebalancing as the lifecycle-cost path. These four options change which bytes must be fetched or maintained; none repeats the closed coalescing, framing, concurrency-reduction or cache experiments.

The BORSUK measurements below are **supplied evidence**, not independently verified here. I inspected primary papers and external OSS documentation/source; BORSUK file references are proposed seams, with no verified symbols or line numbers.

The evidence supports targeting nomination and SOURCE selection: control achieved 9902/10000 hits, deeper FP32 reranking added nothing, and SOURCE dominates approximately 48.8 MiB/query. It does **not** establish how the 98 losses divide between routing exclusion and SOURCE shortlist exclusion. The latest pipeline candidate remains research-only: its second paired p95 ratio, 1.004069, failed the unchanged both-pairs ≤0.95 screen.

The following perspectives are simulations, not testimony, consensus or evidence:

| Perspective | Argument and prediction | Objection |
|---|---|---|
| Retrieval researcher | Repair partition-boundary exclusions or improve early candidate selection. Predict additional true neighbors before changing final precision. | Redundant assignments can spend more I/O without repairing SOURCE ranking losses. |
| Object-storage systems engineer | Remove physical SOURCE bytes while retaining broad parallel reads. Predict improvement only when pruning survives range aggregation and authentication. | A second dependent fetch phase can erase bandwidth savings at p95. |
| Skeptical evaluator | Isolate routing, representation and scheduling changes against the frozen incumbent. Predict some attractive averages will fail paired tails again. | A 68-query screen cannot establish sustained QPS, distribution generality or a vendor win. |

**Ranked research order**

| Rank | Path | Expected tradeoff | First valid native result* |
|---|---|---|---|
| 1 | Progressive SOURCE precision | Potential byte reduction with preserved shortlist; extra round trip and bounds CPU threaten tails/QPS | 2–4 engineering days |
| 2 | Boundary-aware secondary assignments | Best direct quality opportunity; replication and duplicate scoring can increase I/O and maintenance | 1–3 days |
| 3 | Resident navigation over SOURCE units | Potentially selects fewer useful units; substantially more resident routing memory and rebuild work | 5–10 days |
| 4 | Local split/reassign compaction | Potential lifecycle and maintenance-tail improvement; little expected benefit on an unchanged static panel | 4–8 days |

\*Planning estimates, assuming the existing native harness is reusable; not measured completion times.

Resource illustrations assume one million rows, padded dimension 1024 and densely packed 32-row units. Actual unit counts, padding, metadata, concurrent queries and overlapping pinned generations must replace those assumptions.

**1. Progressive SOURCE precision — improve the current SOURCE stage**

Extended RaBitQ explicitly separates the most significant bit from remaining bits, filters using the first part, and refines survivors. This is described in the [16 September 2024 paper, §4.2](https://arxiv.org/html/2409.09913v1), and implemented through [RaBitQ-Library’s split batch formats](https://vectordb-ntu.github.io/RaBitQ-Library/rabitq/quantizer/#format-5-split-batch-scoring-ivf). That establishes a concrete mechanism, not an S3 performance result.

**Proposed mechanism.** First determine whether BORSUK’s current rotated two-bit score admits cheap conservative intervals after reading one bit plane. Do not assume its representation is RaBitQ.

For the existing SOURCE shortlist width \(R\), compute each row’s interval \([L_i,U_i]\). Let \(t\) be the \(R\)-th largest lower bound. Fetch refinement for every unit containing a row with \(U_i \ge t\), retaining ties. With valid outward-rounded bounds, excluded rows cannot enter the full-score top \(R\). Reconstruct surviving full codes and invoke the original scorer.

This produces two fetch phases without a sequential refine-one-unit loop. Independently authenticate both extents under the same generation pin. The proposal reaches `rotated_two_bit.rs`, `two_bit_source.rs`, `two_bit_generation.rs` and `source_capsule.rs`; leave SQ8 and canonical FP32 behavior fixed.

**Resource forecast.** Each D1024 unit contains 8 KiB of two-bit coordinate payload, split into two 4 KiB planes. If \(X\) is the original coordinate payload fetched and \(f\) is the fraction of refinement payload actually fetched **after range overfetch**, then:

\[
B_{\mathrm{new}} = B_{\mathrm{old}}-\tfrac12(1-f)X+\Delta H
\]

Here \(\Delta H\) includes added authentication, framing and factors. At \(f=0.5\), savings are 25% of coordinate payload—not 25% of total query bytes.

Retaining coarse codes could add approximately \(X/2\) scratch per concurrent query compared with a streaming reader. One additional 32-byte digest per unit adds about 0.95 MiB per generation under the illustration. Stored code volume stays similar, but request count and object bookkeeping may rise.

**Risks.** Bounds may be too loose at two bits; scattered survivors may touch almost every refinement range. Extra CPU can reduce QPS. RaBitQ’s probabilistic error intervals are not deterministic correctness certificates, as its [estimation documentation explains](https://vectordb-ntu.github.io/RaBitQ-Library/rabitq/estimator/). S3 also [does not support multiple disjoint ranges in one GET](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html). Dimension padding and all scorer correction terms must enter the bounds.

**Cheapest paired native falsifier.** On the frozen 1000-query inputs, compare full two-bit scoring with progressive scoring over identical nominated units. Require identical SOURCE shortlist IDs and final IDs/f64 bits. Count planned physical ranges and authenticated bytes, rather than rejected vectors.

Reject the drop-in proposal if bounds cannot be justified cheaply or equality fails. Retain it for cold testing only if it materially reduces physical bytes; a proposed first screen is ≥20% SOURCE-byte reduction after overhead. This threshold is a research proposal, not a replacement for the existing tail gate. If adopting a different quantizer is necessary, treat that as a separate major SOURCE replacement.

**2. Boundary-aware secondary assignments — improve routing and generation layout**

[SOAR, arXiv submitted 31 March 2024](https://arxiv.org/abs/2404.00774), chooses complementary secondary assignments by discouraging correlated partitioning residuals. ScaNN’s [partitioner source](https://github.com/google-research/google-research/blob/master/scann/scann/partitioning/kmeans_tree_partitioner.cc) contains orthogonality-amplified database spilling and primary/secondary tokenization. [SPANN, 5 November 2021](https://arxiv.org/abs/2111.08566), independently provides the balanced-posting and boundary-closure precedent.

**Proposed mechanism.** Give a bounded subset of boundary rows a secondary leaf assignment chosen using SOAR’s residual objective, rather than simply the second-nearest centroid. Materialize the necessary SOURCE code in that secondary posting; keep one logical row ID and one canonical SQ8/FP32 location. Deduplicate before downstream fetch planning.

Start with a corpus-defined 5% replication cap, chosen without evaluation queries or missed-neighbor IDs. Preserve primary assignments initially. If codes depend on the leaf centroid, secondary copies must be re-encoded correctly.

The proposed seams are `semantic_unit_router.rs` and `two_bit_generation.rs`, with duplicate handling through `two_bit_source.rs` and `returned_sq8.rs`.

**Resource forecast.** Five-percent replication adds approximately 12.2 MiB of coordinate payload at the illustrated scale, plus IDs, factors, authentication and padding. Canonical SQ8/FP32 payload need not be duplicated. Resident secondary-membership metadata grows as \(O(0.05N)\).

Query bytes are **not** guaranteed to rise by only 5%: replicated boundary rows can concentrate in popular leaves. Duplicate scoring can lower QPS. Deletes, updates and local compaction must maintain every assignment consistently.

**Risks.** This addresses routing exclusions, not every SOURCE shortlist loss. Corpus-specific boundary distributions may defeat the selected cap. Neither D1024 nor the observed query split belongs in the library policy.

**Cheapest paired native falsifier.** First partition the 98 losses into nomination exclusion versus SOURCE exclusion. Then compare incumbent and spill layouts on all 1000 queries, recording nomination coverage, SOURCE coverage, final recall and physical fetch plans. Use 64 leaves initially; try one preregistered lower-probe setting only to test whether gained coverage buys back replication cost.

Reject this particular spill policy if it recovers no relevant coverage and saves no I/O. Retain higher recall as a quality result even if efficiency misses; advancement toward the cold goal requires at least incumbent recall with a favorable physical I/O plan.

**3. Resident navigation over SOURCE units — replace the nomination stage**

[Starling, first submitted 4 January 2024](https://arxiv.org/abs/2401.02116), combines resident navigation, locality-aware layout and block search. Its [repository](https://github.com/zilliztech/starling) and [`page_search.cpp`](https://github.com/zilliztech/starling/blob/main/src/page_search.cpp) provide primary implementation references. Its NVMe measurements do not establish S3 behavior.

**Proposed mechanism.** Replace hierarchical leaf nomination with a resident graph over representatives of physical SOURCE units. Traverse that graph entirely in memory, select units under a byte budget, then issue SOURCE reads in parallel. Initially preserve unit membership, payload formats, SQ8 and FP32. This isolates nomination from layout changes.

The adaptation is BORSUK-specific research: Starling does not prove that one representative per 32-row unit suffices. Proposed seams are `semantic_unit_router.rs`, unit-directory construction in `two_bit_generation.rs`, and planning in `two_bit_source.rs`.

**Resource forecast.** With 31,250 units, one FP32 D1024 representative per unit costs about 122.1 MiB. Sixteen 32-bit neighbor IDs per unit add 1.9 MiB. Thus approximately 124 MiB per generation precedes metadata and build scratch; two overlapping generations approximately double that increment. Multiple representatives multiply the dominant term.

This adds resident routing state and generation-load traffic, while retaining SOURCE cache=0. QPS improves only if reduced remote work outweighs graph traversal and representative scoring.

**Risks.** Centroids can conceal multimodal units and individual close neighbors. Graph search may miss useful units; fragmented selections may increase GETs. Mutations must update representatives and links without mixing generations. Required RAM scales with actual padded dimension and unit count.

**Cheapest paired native falsifier.** Keep frozen unit membership and compare candidate unit sets on the 1000 queries at the incumbent physical-byte budget. First count truth neighbors contained in selected units—an upper bound on achievable downstream recall.

Reject a tested configuration whose nomination ceiling is below 9902/10000. Retain only configurations that survive the full native pipeline and offer a better byte/request tradeoff. Change physical ordering afterward as a separate variable.

**4. Local split/reassign compaction — improve lifecycle behavior**

[SPFresh, SOSP 2023; arXiv submitted 18 October 2024](https://arxiv.org/html/2410.14452v1), introduces LIRE’s local splitting, merging and boundary reassignment. Its [primary repository](https://github.com/SPFresh/SPFresh) supplies the artifact. Its SPDK/NVMe storage engine is not directly transferable to immutable authenticated blobs.

**Proposed mechanism.** Adapt local rebalancing to immutable objects: rebuild affected neighborhoods, publish a generation-pinned replacement mapping, reuse unchanged objects, and reclaim replaced objects after readers release their pins. Preserve this approach only where existing BORSUK maintenance does not already provide it.

Likely seams are routing/generation construction in `semantic_unit_router.rs` and `two_bit_generation.rs`, plus authenticated publication in `source_capsule.rs`. Actual mutation and GC coordinators require source inspection.

**Resource forecast.** For a neighborhood of \(T\) rows and padded dimension \(D_p\), simultaneous FP32, SQ8 and two-bit payload is approximately:

\[
5.25TD_p\ \text{bytes}
\]

At \(T=8192,D_p=1024\), that is 42 MiB; separate input/output buffers make 84 MiB before clustering workspace and metadata.

If touched objects contain fraction \(r\) of live bytes \(L\), ideal replacement writes approach \(rL\), rather than \(L\). Cascading reassignment and object granularity can invalidate that saving. Pinned old objects remain charged until their readers finish.

**Risks.** Distribution shifts can expand the neighborhood. Copy-on-write may amplify object counts and PUTs. Maintenance competes with authentication and query CPU. No static-panel tail improvement is predicted.

**Cheapest paired native falsifier.** Use a disposable native slice of at most 10,000 real fixture rows and 64 queries, with native exact truth. Run five fixed epochs replacing 2% of rows each, comparing current maintenance with bounded local rebalancing. Hold an old generation pin across publication and GC; inject a publication failure.

Reject on inconsistent pins, premature deletion, unbounded work or incorrect visibility. Retain if quality survives and total rewritten bytes/peak memory improve against current maintenance. A local blob-store test establishes lifecycle correctness and work counts; it supplies no S3 latency evidence.

The actionable sequence is **1 → 2 → 3**, with **4 triggered by measured maintenance cost or churn degradation**. Pair new results against frozen incumbent `73360b5566ea4c6557a6d0b837bfb977b46d236b`; keep the repaired pipeline arm separate until composition is tested. For survivors, reuse the existing cache0 A1/B1/B2/A2 cold screen and unchanged paired p95 criterion. Small-panel success still leaves sustained QPS, lifecycle cost and vendor comparisons unresolved.

No files were edited; no BORSUK code/data reads, builds, infrastructure actions, children or experiments were performed.  
Code seams and resource forecasts remain proposals; no vendor win or provider-cache eviction is established.

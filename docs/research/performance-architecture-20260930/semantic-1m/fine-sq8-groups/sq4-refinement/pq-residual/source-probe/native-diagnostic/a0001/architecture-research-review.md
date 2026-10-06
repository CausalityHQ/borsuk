**Choose A: retain unchanged SQ8 precision and investigate bounded fetch locality. B lacks evidence that radial error caused the residual failure. No specific replacement layout is justified yet; complete the coverage calculation below before implementation.**

I resolved the evidence revision to `743b3edcb7a52e852105e7038fb4265596d03d7b`. I independently authenticated all **33 terminal bodies**, recounted **128 source results**, matched the eight-file source closure to qualified native revision `7bb862b273151e91a49d1d4a8f1ba4dce8c8328b`, and checked exit/cleanup receipts for the same terminated instance. The retained qualification receipts record all 19 stages passing. No files were edited or tests/cloud/data experiments launched.

The residual arm failed its frozen **reconstruction/scoring mechanism gate**, before ANN query/GT qualification:

| Source panel | Top-100 overlap | Fourth-smallest overlap | Requirement |
|---|---:|---:|---:|
| ReLAION | 6287/6400 = 98.234375% | 96 | ≥6336/6400; ≥98 |
| CoHere | 6163/6400 = 96.296875% | 93 | ≥6336/6400; ≥98 |

This is a valid REJECT, with deficits of **49 and 173 intersections**. It measures agreement with native SQ8 over the fixed 4096-row cohort and 64 source anchors. It does **not** measure R100 against truth, identify the error’s cause, or establish that every residual representation fails. The 4.329-second process wall includes authentication, training and closure; it is not query latency. [Closed decision](./docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/source-probe/native-diagnostic/a0001/decision.md)

The implemented candidate reconstructs raw PQ codewords without normalization, adds residual centers, and scores with ordered `f32` operations:

\[
\widehat s=n_{\text{original SQ8}}-2q^\top(p+\widehat r).
\]

It copies the original SQ8 norm bytes; it neither recomputes the reconstructed norm nor divides by it. Native SQ8 includes the query constant through a different coefficient accumulation order. Omitting that constant preserves rankings in exact arithmetic, but can affect `f32` ties. Ideal candidate error is \(-2q^\top(\widehat r-r)\), which contains directional and radial components. The closed evidence contains no boundary-error decomposition or normalization-only counterfactual establishing radial dominance. Passing decoded-cosine controls on **SQ8 vectors** do not establish that cause for compressed reconstructions. Therefore B would presently be another unsupported scorer intervention. [Candidate scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/pq_residual_four_bit.rs:242)

A’s measurable obstacle is **fragmentation and gap overfetch**, with a coverage constraint:

- I reproduced all 128 minimum-gap cover calculations. At 32 ranges, unchanged SQ8 fits 16 MiB for only **50/64 ReLAION and 32/64 CoHere** plans; maxima are **32,747,520 and 27,081,600 bytes**.
- The graph-affinity permutation worsened those fit counts to **35/64 and 16/64**. Its actual closed decision is under [packing-diagnostic/a0002](./docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-diagnostic/a0002/decision.md), rather than a nested `native-diagnostic` directory.
- SQ8 rows cost 780 bytes. The budget permits **21,509 rows**, or **21,504 with full 16-row groups**. Preserving the largest frozen 41,984-row fetched population would cost 32,747,520 bytes regardless of placement.
- Conversely, the nominated 16-row groups alone cost at most **8,224,320/8,748,480 bytes**. But dropping surrounding rows requires evidence: nominee-only CoHere coverage is .95609/p05 87. [Cover evidence](./docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0002/frozen-cover-arithmetic.json)

My additional authenticated set audit found **3 ReLAION and 52 CoHere SQ8 top-100 results outside nominated groups**; CoHere’s omissions occur across 27 queries. For a subset containing all nominated groups with identical scoring, subtracting these omissions from known true-hit counts gives conservative recall lower bounds of **.99469/p05 99** and **.98219/p05 94**. The latter cannot certify p05 ≥95; it does not prove actual failure.

**The missing calculation:** join those omitted winners to the already closed truth, distinguish useful incidental rows inside nominated groups from useful rows in bridged gaps, and quantify their coverage under **one source-only placement/expansion policy**. Report exact 32-range bytes, retained nominees, incidental coverage and duplication cost. Consumed queries and truth may audit the policy, but must not train its placement. Neither graph adjacency nor geometric proximity alone supplies this evidence.

The cheapest subsequent native falsifier is a **virtual-layout replay before rewriting SQ8 payloads**:

1. Freeze one source-only placement, nomination and expansion rule. Authenticate inputs and pre-admit work; proposed ceilings: CPU1, 1 GiB, no swap, 600 seconds, 64 MiB output.
2. Replay all 128 sealed plans. Reject immediately if any loses a nominee or exceeds 32 total payload GETs/16 MiB, counting every payload wave and attempt.
3. If the cover survives, stream unchanged SQ8 for the actual fetched populations, rank every incidental row, seal all results, then independently recount pinned truth. Require each panel ≥6272/6400 true hits and fourth-smallest ≥95.
4. Use an independent interval/set oracle—largest-gap arithmetic checked by exhaustive tiny covers—and a literal scalar SQ8 scorer with stable-ID ties. Authenticate block bytes and logical/PQ/physical mappings independently of the candidate planner.

S3 cannot retrieve disjoint ranges in one GET, so batching alone cannot remove the request constraint. [AWS GetObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

Prior art supports the direction, not a qualification claim: [SPANN](https://arxiv.org/pdf/2111.08566) combines bounded postings with boundary replication; BORSUK’s earlier hierarchical failures prevent assuming that recipe works here. [DiskANN](https://harsha-simhadri.org/pubs/DiskANN19.pdf) uses SSD-resident graph/vector access, a different request boundary. [Residual source coding](https://arxiv.org/pdf/1102.3828) supplies a mechanism precedent, not evidence for this scorer’s accuracy.

Only current-source arithmetic supports the following 100M projections:

\[
S=780N+64N+8N+32\lceil N/16\rceil+E+\text{books/headers}
\approx85.4\text{ GB}+E,
\]

for one unchanged SQ8 copy; additional generations, replicas and retained sources add storage. Current conservative resident admission projects **59.003 GB per generation**, and current query admission projects **464.23 MB per active query**, including \(4N\) graph workspace. Peak admission must include distinct pinned generations, startup coexistence, concurrency, deltas, maintenance and runtime. These are formulas, not measurements; current admission rejects \(N>100k\). The old 18.4 GB projection is superseded. [Resource model](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:244)

Stop the rejected residual arm. A remains unqualified until its coverage/locality falsifier passes; cold tails, QPS, lifecycle cost and competitor parity remain unmeasured. Root retains the final decision and launch authority.

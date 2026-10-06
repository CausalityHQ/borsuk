Review0179bbb3950a4974, group ea055ab278a840cb, completed exit0 on GPT-6.1 Sol after the Opus attempt could not run. This is the research critic's separately retained answer.

**Proceed with one bounded metadata-only falsifier, but tighten the specification before scientific replay.** At `06aced808e64cb05604a16a86d6727777ac2cfc4`, the direction is defensible; the following are implementation constraints, not demonstrated locality or quality improvements.

1. **P1 — Define the exact swap delta.** The [fit rule](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/research-result.md:45) leaves “relaxed move gain” and swap evaluation implicit. Adding two relaxed gains computed against the original counts is wrong for shared anchors.

   For groups \(g\in s\), \(h\in t\), define \(d_a=1[g\in H_a]-1[h\in H_a]\). The exact reduction is

   \[
   \sum_a f(n_{as})+f(n_{at})
   -f(n_{as}-d_a)-f(n_{at}+d_a).
   \]

   Shared anchors have \(d_a=0\) and cancel. Tiny counterexample: with \(C=2\), one shared anchor and counts `(1,1)`, each independent relaxed move reports gain 2; their sum reports 4, while the swap changes nothing.

   Freeze these details: exclude the current block; rank destinations from current counts; retain the best eight even when relaxed gains are nonpositive; apply positivity only to the exact swap; update counts immediately. Previously visited groups may remain swap partners. Independently compare every accepted tiny-case delta against full objective recomputation.

2. **P1 — Freeze source encoding and all ordinal mappings.** “Fixed domain separator” does not specify its bytes, source-identity encoding, logical-ID width or endianness. Those choices change the selected anchors. Specify them and reject populations smaller than 4,608 rather than silently shrinking the split.

   Canonical-source physical order and fine-index physical order differ. Resolve anchor vectors through the original source-order map; interpret nominations in the unchanged fine/PQ ordinal space. Using the same physical ordinal for both can silently train on the wrong vector. Bind canonical source, both order authorities, graph, PQ, normalization and ordered nominations in the source-selection seal.

   Use [open_remote()](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:560) for nomination; `open()` also opens SQ8 records. Sparse anchor reads cannot establish whole-file SHA authentication by themselves. Either consume an authenticated source-stage receipt or explicitly charge a bounded full canonical authentication stream.

3. **P1 — “One sweep, eight destinations” is not a work bound.** Candidate discovery occurs before the eight-destination limit. Enumerating every incident anchor’s groups can require \(\sum_a|H_a|^2\), bounded by **4,294,967,296 visits per dataset**, before swap evaluation.

   The [resource section](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/research-result.md:117) also needs separate fitting and planning capacities: block-count tables, candidate scratch, inverse incidence, held selections, serialization copies and coexisting panel state.

   Including the 512 held anchors gives a nomination ceiling of **301,989,888 evaluations per dataset**, or **603,979,776 across both**. Consequently, the old 500-million-operation cap cannot simply be inherited. Count candidate enumeration, swap evaluation, every expansion-cover attempt and source authentication. Resource exhaustion remains INVALID; do not truncate the prescribed sweep or expansion silently.

4. **P1 — Specify how expansion replaces its reservation.** The [global-gap cover rule](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/research-result.md:83) is correct under additive range-byte costs and one GET per range.

   “Remaining budget” must mean the total envelope minus **external reservations**. Each expansion trial replaces the pending complete cover; it must not repeatedly subtract earlier trial covers. Check the mandatory cover first. Adding groups cannot make an infeasible mandatory cover feasible, so record that plan’s valid REJECT and continue validating subsequent plans.

   Freeze half-open endpoints, tail lengths, stable object ordering and equal-gap ties. Never merge across objects or treat a multipart object as one multi-range GET. [AWS explicitly disallows multiple ranges per GET.](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html)

5. **P1 — Separate scientific rejection from broken invariants.** The retained answer says to reject a rule that loses a nominee. Under this particular rule, bijection and mandatory-group retention are guaranteed invariants: losing one indicates an implementation defect and should be **INVALID**. The same applies to oracle disagreement, overflow, malformed nominations, bad maps or authentication failure.

   Use terminal precedence **INVALID > valid REJECT > locality survival**. An early budget failure must not bypass later parsing, authentication or corruption checks. The [method](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/method.md:9) correctly requires all 128 valid plans after a cover REJECT; preserve the existing late-corruption and seal-before-prefix checks.

   Seal both panels’ selections, maps, constants and source identities before acquiring the frozen-prefix descriptor. Virtual object IDs should be deterministic part ordinals bound to that seal. They cannot claim new payload SHA/ETag authentication before those payloads exist.

6. **P2 — Specify the 512-held diagnostic’s role.** Keep it outside fitting, destination generation and the serving anchor dictionary. Seal the fitted maps before evaluating held diagnostics. Declare it descriptive unless a gate is preregistered; report dictionary overlap, touched blocks/objects and cover costs without allowing retuning.

   Source self-queries are a plausible locality signal, but their distribution need not match external requests. Best Jaccard agreement also does not establish that expansion recovers the useful omitted winners. Those are two separate missing observations.

7. **P2 — The objective is justified as a surrogate, with a substantial physical mismatch.** It is exactly scaled probabilistic fanout with \(p=1/2\), as motivated by [Social Hash Partitioner](https://www.vldb.org/pvldb/vol10/p1418-pupyrev.pdf). However, equal block objective values can have very different range bytes because block adjacency and object boundaries do not enter the objective.

   Keep the declared objective frozen. Report objective change alongside mandatory-only and expanded cover costs on the same map. A failure rejects this bounded heuristic; it does not falsify all source co-selection layouts. [SPANN](https://arxiv.org/abs/2111.08566) supports bounded postings and boundary visibility, while [DiskANN](https://harsha-simhadri.org/pubs/DiskANN19.pdf) exploits SSD sector access; neither qualifies this S3 placement or expansion rule.

The independent oracle can remain small. For sorted selected intervals within an object, use

\[
D[m,r]=\min_{1\le i\le m}
\left(D[i-1,r-1]+\mathrm{end}_m-\mathrm{start}_i\right),
\]

then combine objects by request-allocation DP. This does not reuse largest-gap logic; a running minimum makes the per-object recurrence \(O(Km)\). Exhaustively enumerate tiny cuts and allocations, including tied gaps, unequal gaps across objects, `J>K`, exact-budget boundaries, short tails, free expansion inside a bridge and unaffordable expansion followed by affordable expansion.

**The product limitations are disclosed.** The documents explicitly retain roughly 59 GB per resident generation, approximately 464 MB per active query at 100M, insufficient fixed-anchor coverage, startup costs and missing durable fine-SQ8 maintenance. At D768, 100k has two payload objects; 100M would have about 1,163. Passing this replay would establish necessary locality on the consumed prefix only.

I read both requested documents and the original closed audit/reviews in full. No files edited, native/data execution, children or additional consultations.

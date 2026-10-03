# Closed-failure routing review

Consultation c5063b7c5726471c completed exit 0, GPT-6.1-Sol high. Read-only review of the actual paired100k failure; no new ANN execution or product qualification. Root accepts the single routing intervention below as a prospective diagnostic under the operator's existing authorization. Exact frozen-layout reconstruction is mandatory before any actual comparison. The original failure and its gates remain unchanged.

**Use one global top-24 leaf-ranking probe.** It isolates the cost of hierarchical selection/pruning on the frozen layout, provided prototype bits, distance arithmetic, cell membership and downstream nomination stay identical. It cannot identify the responsible depth or prove that centroids are good representatives.

1. **Recover the frozen candidate first.** Retained configs/traces contain root hashes, but omit `manifest.json`, `directories.bin` and `cells.bin`; worker cleanup deletes candidate directories. Configs alone cannot reconstruct the counterfactual. Recover authenticated frozen bytes, or a previously authenticated complete leaf/roster export. If unavailable, stop this replay: a newly trained layout is a different experiment. Exact reconstruction would need separate root authorization and identity verification.

2. **Freeze the intervention.** In [hierarchical_semantic_cells.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/hierarchical_semantic_cells.rs:1257), enumerate every authenticated leaf once. Rank using the existing `SquaredEuclidean.distance(normalized_query, stored_prototype)`, then existing `total_cmp` and ascending cell-ID ties. Select exactly 24; primary coverage may use its first eight. Keep arithmetic-mean prototypes unnormalized, existing cell layout, original-query SQ2 preparation, four nominated blocks/cell and unchanged SQ8 ranking. Compare against the frozen hierarchy’s actual 24-cell union; verify that cardinality per query before attribution. No beam sweep.

3. **Retain nominations before opening GT.** The smallest routing-only evidence is:
   - Root/config/source/request hashes; complete leaf prototypes as exact f32 bits, IDs, row counts and authenticated spans.
   - Complete cell→source-ordinal rosters, authenticated during extraction; `first_row` is a concatenation offset, not source membership.
   - For all 64 requests in both datasets, ordered selected cell IDs/distances, covered source IDs and selected bytes/read counts for both arms.
   
   Sync and hash the complete two-dataset nomination artifact before offline truth access. A routing-only probe needs no SQ8 scoring. If retaining existing downstream scoring, also freeze nominated-block IDs, `nominated_ids` and returned IDs; retain the necessary cell payloads. Current consumed truth is already known, so this sequencing prevents feedback into execution but does not create a held-out evaluation.

4. **Keep file changes bounded.** Add diagnostic routing selection and cell-selection receipts in the library above; expose explicit mode and retain the existing truth barrier in [the diagnostic binary](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/hierarchical_semantic_cells.rs:233). Update `scripts/prepare_hierarchical_cells_100k.py` for paired truth-free execution. Update the launch adapter’s artifact roster/cleanup boundary only if needed to preserve authenticated exports before deletion. No builder, metric, codec or production-default changes. Root owns implementation verification, protocol freeze and execution.

5. **Cheap synthetic falsifier.** Use a D2 binary tree with 32 equal-sized subtrees at the first wider-beam pruning level. Thirty-one have mean `(0.5,0)` and leaves `(0.5,±√0.75)`; the remaining subtree has mean `(0,0)` and leaves `(1,0)` and `(-1,0)`. For query `(1,0)`, beam24 discards the zero-mean subtree, while global leaf ranking must include its distance-zero leaf. Arrange that branch outside primary8 too. Assert hierarchy misses it, global top24 includes it, cardinality/ties are deterministic, and identical selections produce identical downstream nominations. A normalization implementation error changes the decoy ordering. Also run an unpruned small-tree control where hierarchy and global selections must agree.

**Resource arithmetic.** The retained builds have **291/281 leaves**: only **223,488/215,808 coordinate evaluations per query**, about **28.1 million across both 64-query panels**. Raw leaf prototypes total about **1.76 MB**; complete i64 rosters add **1.6 MB**. Existing encoded directories total **12.08 MB**. Twenty-four full cells cap payload at **12,140,544 B ≈11.58 MiB**, with at most 12,288 covered rows and 3,072 nominated rows. Equal cell counts do **not** guarantee equal occupancy or bytes; report both.

**Exact decision rule.** Use fetched-cell coverage as the primary endpoint; nearest-rank p05 is the fourth-lowest hit count among 64 queries.

- Identity, cardinality, admission or execution failure → **INVALID**.
- Both datasets improve mean coverage with nondecreasing p05 versus **86.140625%/55** and **74.8125%/51** → this intervention supports hierarchical pruning as a contributing loss.
- Both achieve mean coverage **≥98%** and p05 **≥95 hits** → routing coverage is adequate on this consumed panel; downstream nomination still needs qualification.
- Improvement below that gate → pruning contributes, but bypassing it is insufficient.
- Otherwise → reject global top24 as the common next remedy; this does not exonerate every hierarchical pruning policy.

Normalize-centroid and overlap proposals remain separate experiments: normalization changes the scoring objective; overlap changes membership and storage/fetch amplification. V282 flat routing is a historical quality reference with different layout/encoding, not this control. The completed research’s favorable routing hypothesis is now limited by the actual FAIL.

At 100M, full occupancy already implies **195,313 leaves**, roughly **150 million coordinate evaluations/query**, **600 MB** of raw leaf prototypes and **1.20 GB** for current binary-tree prototypes, before overhead; half occupancy doubles these. **Full-leaf ranking is diagnostic, never automatic production promotion.** No product-scale feasibility, cold-performance or vendor-win claim follows.

Read-only review only; no edits, native execution, cloud work, children or duplicated research/recount.

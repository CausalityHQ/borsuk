# Research reconciliation: proposal retained, promotion deferred

Fable consultation497cb16cb6b5447b completed exit0 in433seconds. Exact unedited answer retained in fable-result.md. No benchmark or implementation arm selected from it. No duplicate consultation needed.

## Independently verified

Closed centroid-discovery SHA256SUMS all match. Reconciliation script checks64/64 reused ReLAION first100k D768 cosine k100 development0–63 queries: selected physical pages are subsets of the159 discovered pages, and candidate GT hits equal fetched GT hits on every query (99.5625% mean). This supports a discovery bottleneck on this development panel. The subsequent strict AWS replay in ../validation-loss-diagnostic-20260928/decision.md now verifies the744-query validation layer:577missing discoveryGT hits, zero nominationGT loss, zero physical gapGT gain, exact original source/root/truth/plans. Returned KILL remains unchanged. Exhaustive ranking's99.875% came from a DIFFERENT159-page set selected from all391 pages, not a better ranking of the same159 candidates.

## Corrections required before any experiment

- Existing UnitCentroidPages decodes f16 storage into Vec<f32> plus f32 norms (unit_centroid_pages.rs:162,209). At D768/unit32 its resident arrays alone project(768*4+4)/32=96.125B/row, before graph, allocator, artifacts, adjacency, query scratch and pinned generations. Fable's48B/row describes stored centroid payload; the proposed <=56B/row resident envelope is unsupported with the current reader. Separate packed-resident representation work needs its own arithmetic/parity/performance evidence. This is source arithmetic, not RSS or a measured100M cost.
- Losing no GT under the CURRENT two-bit selection does not license removing that ranking plane. The proposed replacement ranking and its physical budget selection must be measured on the same candidates. Unit-summary failure on the other1024-row layout remains a caution, not a universal proof.
- A source kNN cut-edge layout plus page-adjacency navigation changes two causal layers. First separate layout containment from nomination, with unchanged budget accounting and actual returned SQ8 scoring. GT-aware witness is only an upper bound and cannot authorize production removal of a plane.
- Exact100k source kNN is not a qualified scalable100M builder. Fable's bounded local compaction assignment is proposed, not implemented in the current full canonical-source merge/build. Build, update, deletion, compaction, adjacency truncation and generation-swap cost/quality require an explicit lifecycle envelope.
- Page and32-row unit summaries still grow with row count; this does not have zero per-row resident cost. All footprints must include decoded arrays and actual graph structure.
- Spark execution recommendation conflicts with the operator's AWS-only/no-DGX constraint. Any future executable falsifier runs on bounded causality Spot; no Spark access or job.
- V282 attempt a0002 already measured development0–255 and validation256–999 for both100k cohorts (v282-100k-attempt-ledger.md, sealed ReLAION V114 and CoHere request hashes). Thus64–255 is not a presumptively fresh panel; require demonstrably new sealed query identity before independent confirmation. One later run.py split does not establish freshness.

Decision: keep graph-cut locality as an unqualified candidate, not a winner or a qualified experimental design. Next design work must establish an honest resident/build/lifecycle envelope and a one-layer falsifier; first use closed artifacts for discovery-vs-selection decomposition where available. All previous KILL decisions and both-vendor acceptance gates remain. No source/GT retuning, hidden copy-budget relaxation, fresh validation claim, default freeze or scale promotion follows.

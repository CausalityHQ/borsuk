# Historical query metadata inventory, incomplete exclusion audit

2026-09-29, metadata/source reads only. No query embeddings, GT, measurement
CSV or sealed corpus bodies opened. No ANN, numerical kernels or builds here.

Root discovery found341 research prefixes,279 versioned. Collection authenticated
490 versioned terminal metadata bodies by SHA256 and identified101 terminal
records with named query/preparation/population metadata locators (87 prefixes).
This locates further lineage work; it does NOT certify complete history/novelty.
Nonversioned campaigns, older producer dependencies and source-ID mappings remain.

Nine selected CLOSED campaign archives independently matched their terminal
source hashes. Saved code-file hashes and relevant source lines for V151,
V152(two revisions),V154(failed and complete),V272,V278,V279,V281. Conservatively
retain failed-terminal producer lineage too. Initial older archive lookup used
wrong `sources/<sha>.tar.gz` layout; S3 metadata located original
`source/<sha>/source.tar.gz`, then all nine archives authenticated. Locator files
remain discovery-only; separate per-archive records carry authenticated hashes.

NEW CLOSED V278 exclusion proof: CoHere train rows1001000–1001999,1000 queries,
prepare SHA952d365015ab37cd285a89132fa187fb74b1d6faacd9054e7bd4278055b6df2a,
query identity6e3505fdfc9d6a6c101cba2b7d4c16d5c07eee6ec704fa836385a810e8ca17d4.
Exclude this whole panel in addition to V2711000000–1000999 and V277104000–104999.
Archive-bound V272 uses registered CoHere test.parquet, not a new train panel;
V279 ReLAION requests derive from V116 validation. Bind their actual metadata
identities and all earlier source dependencies before selecting a fresh panel.

`terminal-collection.py` and `source-collection.py` preserve the bounded
collection commands. Source selection is fixed to nine archived experiments;
no new experiment/query selection or product quality claim. Full prior-query
audit remains FALSE, novel cohort not selected, old rejected seal stays closed.

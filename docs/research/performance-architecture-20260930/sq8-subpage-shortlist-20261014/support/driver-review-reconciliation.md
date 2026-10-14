# Full-driver review reconciliation

Reviewed source: ac05aedea3992b3418099ecc5995632a3d33dfbf.
Separate repair: fde98a39778c5fb76277327cd2b4bb224838cdc7, direct parent ac05. Original source and both complete reports are preserved.

Research bec29c5f9d3640d7 and engineering e054cc3e62264518 were read in full. Source dispositions below are UNVERIFIED until exact-source native execution.

- Research B1: replay, capture and block control now share baseline SQ8 GET budget 32. Candidate budget remains 30. Earlier prospective wording “whole-control 30 GET” was incorrect; the actual historical producer and baseline are unchanged.
- Research S1 / engineering 2: retain bounded scored-unit rosters instead of all-query JSON plan/trace trees. Require nonempty semantic leaves <=48 and scored units <=24576 before retention. Compare reconstructed canonical plan and trace hashes against the existing sealed reader fingerprints. Native RSS and allocator behavior remain unmeasured; the 32 MiB metadata allowance still needs an explicit derivation.
- Engineering 1: both expected headers must specify fetch64 and omit shared_byte_window; bound inputs must specify source_cache off. Existing pre-open refusal test now includes both-header width/window negatives.
- Engineering 3: over-cap candidates expose null quality, not measured-looking zero deltas. Aggregate hits are explicitly measured subtotals; the quality floor is unknown unless all 1000 queries were admitted. A new source-bound report fixture covers over-cap geometry and unknown quality.
- Research S2: add independent returned ID-to-physical-ordinal and range-containment checks, require K results, retain conservation only as diagnostic accounting. Label the repeated block-control rank as a determinism check, not independent validation.
- Research S3: distinguish row shortlist containment from block containment, bridge rescue from within-block expansion, and shortlisted from unshortlisted covered ranking loss. The report fixture includes eight block-expansion rescues and a bad physical mapping refusal.
- Research S4: plan geometry once with unlimited byte budget; fetch only when the resulting cover fits the declared 32 MiB cap. Workspace/shape failures remain INVALID.
- N1: bridge subtraction is checked.
- N2: acceptance requires external native exit 0 and a complete sealed report. Normal reducer errors use the shared INVALID publisher. Kill, OOM, interruption, output-cap or publication failure may leave incomplete output; preserve it as INVALID and use a new output path for any separately authorized attempt. Never classify this as a performance failure.
- N3/N4: caller payload admission precedes payload reads; generation admission still occurs later. Capture/shortlist formulas describe transient payload, not independent whole-process RSS admission. Native resource evidence remains required.
- N5: report explicitly refers to truth FILE opening after all baseline controls. Historical baseline recall rows are already present, but never select candidate sizes.
- N6: report includes baseline SQ8 GET/byte totals and discloses baseline32/candidate30 budgets.

Remaining gates: exact affected compile, eight exact fixtures, all example regressions, release fixtures/build, workspace correctness/suspicious Clippy, actual env-unset jobs1 complete test build. Then actual-input admission on authenticated requests/records/truth and separate disposable staging precede a measured full1000 shortlist curve. Successful full-driver replay and late pre-truth parity rejection are not yet demonstrated. No new recall, cold latency, QPS or vendor-win claim.

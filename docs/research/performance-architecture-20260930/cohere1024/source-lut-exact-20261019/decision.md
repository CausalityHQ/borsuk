# Exact SOURCE lookup experiment
## Decision
Keep the original 256-entry implementation as the production incumbent. Retain the 24-entry implementation and its evidence as a memory Pareto candidate. No end-to-end speed gain was established.
## Measurements
A source-bound admission and separate disposable canary passed for both arms before a cold prefix32 A1/B1/B2/A2 screen. All four measured cells returned identical plans, scores, shortlists and 320/320 hits. Each used 5,256 SDK GET submissions and 1,341,837,184 consumed payload bytes. These counters are not wire or billed S3 requests.
Weighted throughput was 5.953445 QPS for control and 5.914617 for candidate (-0.652%). Candidate cohort CPU increased 7.370%. Per-cell percentiles are in pair-result.json; percentiles are not averaged.
The D1024 primitive showed 14.05x faster preparation and 1.51x faster block32 scoring, but used 16 interleaved prepared queries. Production Q1 reuses one prepared query across thousands of units. Small dimensions showed scoring regressions. This primitive does not establish end-to-end performance.
Modeled D1024 preparation payload fell from 532,480 to 57,344 bytes; retained table payload fell from 524,288 to 49,152 bytes. These are modeled payloads, not RSS.
## Verification and limits
Affected correctness tests, locked workspace Clippy correctness/suspicious, real shim-unset workspace test build and release library/runner/reader builds passed on the frozen candidate. A broader producer-profile test remains failed at an inconsistent synthetic historical startup roster; exact-parent runtime baseline is unverified. No full-suite pass is claimed.
The old deployed control compiler flags are not fully recorded. This is a matched-input artifact comparison, with an unresolved build-provenance limit for isolating codec causality.
Library cache was disabled and each cell used a fresh process/scratch directory. Provider cache eviction is unproven. Prefix32 is neither full1000 recall nor a population tail estimate. Cohere is a benchmark fixture; library algorithms remain generic. No matched S3 Vectors or Turbopuffer win is established.
## Next falsifier
A fresh short-lived contextless specialist is implementing 68 entries per packed byte, preserving exact arithmetic. Test one prepared query reused over 256 distinct units alongside the interleaved workload, then use a freshly built same-environment 256-entry baseline. Root owns timing, integration and experiment decisions. Numerical, cache and Rust systems perspectives are simulated hypotheses to verify, not independent expert evidence.
## Evidence
All original failures, gates, source, release binaries, primitive and paired cells are retained in the checksum-verified study archive identified by archive-publication.json. candidate.patch.gz preserves the implementation without changing the active Rust default. Original full1000 control quality remains 99.02%; this screen does not replace it.

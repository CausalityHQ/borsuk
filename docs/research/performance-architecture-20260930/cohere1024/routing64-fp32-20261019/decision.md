# Routing 64 leaves with FP32 shortlist 16: retain the quality Pareto gain

Retain the explicit 64-leaf candidate as a quality Pareto result alongside the 48-leaf incumbent. This is a development-cohort quality gain with higher observed I/O and runtime; it does not change production defaults or establish production readiness, a causal speed win, a holdout estimate, provider-cache eviction, or a matched vendor win. `full_cohort_qualification=false` remains unchanged.

The generic Rust intervention expands explicit semantic coverage from 48 to 64 leaves at source `2906256b823754dd9ca1018cf6c6c3dd5ea3a6bb`. FP32 shortlist 16, coalescing off, Q1, SOURCE offload2, download parallelism 128, the 2 GiB library budget and 8 GiB host ceiling stay fixed. The two full-run configuration objects differ only in `semantic_leaf_limit`.

Closed source qualification records include passing focused coverage/FP32/leaf tests, Clippy, release build and shim-unset workspace test-build; earlier failed attempts remain preserved. Real D1024 admission selected 64 leaves and completed SOURCE closure. Separate canary, scratch cleanup, strict native replay and independent f64-scorebit/baseline-oracle checks passed. The new-build 48-leaf full1000 control exactly reproduced the prior 48-leaf returned IDs, score bits, physical ordinals, shortlist, plans, trace and I/O. These are recorded native results; this docs worker audited metadata consistency without re-running them.

The frozen 110 baseline-loss query screen improved 952 to 1003 hits out of 1100 (+51), triggering the full1000 arm. Full1000 improved 9852 to 9902 out of 10000 (+50; recall 0.9852 to 0.9902). There were 43 improved queries, 956 unchanged and one regression: ordinal 113 fell from 10 to 9. Perfect queries increased from 890 to 925.

All 98 remaining missing truth hits were outside semantic nomination/SOURCE evaluation. The newly missing ID 497968 maps to physical ordinal 840621 in the authenticated full SQ8 mapping (1,000,000 rows; 1,036,000,000 bytes; SHA256 `cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1`). Fetched SQ8 range coverage alone does not prove ranking eligibility or isolate quantization.

Observed full1000 measurements (query I/O excludes startup/binding and is verified payload, not provider billing or wire bytes):

| Metric | 48 leaves | 64 leaves |
|---|---:|---:|
| p50 query wall (ms) | 183.383985 | 200.509366 |
| p90 query wall (ms) | 289.067119 | 403.658597 |
| p95 query wall (ms) | 380.744656 | 433.67657 |
| p99 query wall (ms) | 457.481637 | 479.781347 |
| Process CPU (ns) | 122136896437 | 154216271210 |
| Process wall (ns) | 211186698281 | 253559425906 |
| Query CPU (ns) | 115883579141 | 146323915068 |
| Query wall (ns) | 204433679986 | 245061079258 |
| Observed process peak RSS (bytes) | 143106048 | 154566656 |
| Serial query QPS | 4.891561899528893 | 4.080615343031282 |
| SOURCE query GETs | 109872 | 122948 |
| SOURCE verified bytes | 28967583744 | 39255017472 |
| SQ8 query GETs | 30041 | 30076 |
| SQ8 verified bytes | 10722616576 | 11756760064 |
| CANONICAL query GETs | 16000 | 16000 |
| CANONICAL verified bytes | 65664000 | 65664000 |
| Total query GETs | 155913 | 169024 |
| Total query verified bytes | 39755864320 | 51077441536 |

SOURCE increased by 13,076 GETs and 10,287,433,728 verified bytes; SQ8 increased by 35 GETs and 1,034,143,488 bytes. Canonical FP32 stayed at 16,000 GETs / 65,664,000 bytes. Query GET failures were zero. Runtime and RSS were higher. One control preceded the candidate sequence; there were no randomized paired timing repetitions, so these observations do not isolate a causal timing effect.

The original experiment closed with exit 0 and root-reported PID 0. Its archive is `research/semantic-router/20261019/routing64-fp32-a0001/evidence.tar.gz` (40,947,113 bytes; SHA256 `5c602d3a362e7cd190cf135019a7b4226877f563db64d21800a564b892ac6a0c`). The independent closed verification is 19,690 bytes, SHA256 `7e886d2a7f21bcd91491bab94f87852089e2361b5db291ad4cb155e3abdf165a`; its publication records checksum-HEAD verification.

[verification.json](verification.json) records exact metrics, checked identities and source qualification history; [terminal.json](terminal.json) preserves closure and archive provenance. The separate planning consultation remains root-owned; no result is inferred here.

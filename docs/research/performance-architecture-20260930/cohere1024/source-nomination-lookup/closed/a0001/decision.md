# SOURCE record lookup cache: accepted incremental runtime progress

## Decision

Retain the query-local lookup cache in source revision `14aec21db4690c14e0841346f96759a51fe1518c`, parent `49abb2c7ac10dac39513c4fef8e0f0f3f0425e08`.
This is a modest measured improvement, with unchanged ANN quality and reads. It is not a final product release or a vendor win.

Only `crates/borsuk/src/two_bit_generation.rs` changes. The callback reuses the last verified range only while the offset remains between that range's start and the next range's start. This preserves the previous rightmost-start lookup, including duplicate/overlapping starts and backward completion jumps. Checked arithmetic, byte bounds, scoring, selection, authentication, budgets, drains, public APIs and formats remain unchanged. No heap allocation or production counter is added.

## Frozen full-cohort comparison

Causality Spot `i-0a8a16ceadcb884ee`, m6a.4xlarge; CPUs0–3, quota400%,16GiB,noSwap. Same persistent EBS, corpus,1000 queries,truth,1M/D1024/cosine/k10,flat layout,download width64,and logical budgets. Trace and SOURCE cache off. Two binary arms in A1/B1/B2/A2 order; A is the qualified prior binary, B the candidate. Real-input runtime admission and separate2-query canaries passed for both arms before measurement.

All four cells and strict result readers exited0. Every query's ordered IDs,score bits,physical rows,plan,trace,and logical charges match exactly. Each cell returned9783/10000 true hits: **97.83% recall@10**.

| Metric | Control: A1 / A2 | Candidate: B1 / B2 |
|---|---:|---:|
| Query CPU, seconds |114.847 /114.582|112.160 /112.934|
| Query wall, seconds |160.725 /159.702|156.110 /157.968|
| Sequential QPS |6.222 /6.262|6.406 /6.330|
| p95, ms |215.035 /215.824|212.653 /215.486|
| p99, ms |267.345 /251.918|240.871 /246.384|
| Process RSS, bytes |103460864 /87883776|92897280 /92774400|

Across the two repetitions: **1.89% lower query CPU,1.98% lower wall time,7.72% lower synchronous planning wall time,and2.02% higher sequential QPS**. Both order subsets improve CPU and wall time. Pooled p95 is213.489ms versus215.477ms (0.92% lower). Pooled QPS is6.368 versus6.242. These are2000 observations of the SAME1000 queries, not2000 independent queries. The tail gain is small; no statistical confidence or general-distribution speed guarantee is asserted. RSS variation does not establish a memory improvement.

Per1000 queries, every cell retained SOURCE109872GET/28967583744B and SQ830041GET/10722616576B. Logical charges are not billed/wire accounting. Startup,build,maintenance and concurrent-service throughput are not represented by sequential query QPS.

## Source and native verification

Contextless audit68901dbe05fa4f28 read the pinned EC2 source and located repeated per-record binary searches; query table preparation was already once per query. Contextless patch proposal e4d1edeb322a4dac supplied RED/GREEN diffs after the managed worker's recorded initialization failure; root applied and verified them on EC2.

The exact new test first compiled and failed at its intended complexity assertion (128 full searches versus4ranges), after byte/pointer/edge comparisons passed. GREEN passed the unchanged test. All47 affected SOURCE/query regressions passed. Workspace Clippy correctness/suspicious,optimized release,and actual shim-unset workspace test compilation passed (jobs8;131s test-build). The release has no cfg(test) search counter. All437 packaged source/support files match Git and the independently extracted archive.

The32-query screen suggested5.2% CPU reduction; the full-cohort result supersedes that estimate with1.89%. Original artifacts remain immutable. Two root analysis/packaging issues were repaired without rerunning measurements: a jq grouping syntax error left empty pooled.v1.json (use pooled.v2.json), and the first broad script inventory included a nonmaterialized historical script (use the437-file archive). A prior source inspection ended141 after piping to head; it establishes no successful check and is not counted as evidence.

## Comparison and next step

The retained S3 Vectors number (95.71% recall,p95211.891ms,QPS7.402) remains **unpaired**: representation and conditions differ. No matched Turbopuffer measurement exists. This increment narrows a CPU bottleneck; it does not prove a competitor latency win.

Keep the quality-preserving gain rather than discarding it for an arbitrary minimum percentage. Next, use a fresh contextless specialist to identify the next measured SOURCE planning/authentication/copy bottleneck and propose one bounded falsifier. Preserve separate download and compute limits; do not increase concurrency blindly. Continue from this new incumbent after evidence publication and compute cleanup.

Immutable closure: `s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261017/source-nomination-lookup-closure-a0001/`.
Full raw evidence and exact commands remain on retained EBS and in the closure archive. The terminal object is uploaded last. Compute termination and Git delivery are separately verified by root after publication.

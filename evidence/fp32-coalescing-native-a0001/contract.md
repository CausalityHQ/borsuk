# FP32 bounded row coalescing: early contract

Base: f2bd4e53b62a038fbc0699d6da87c0413ee5bc95. EC2 guest i-042a9830613ab480f only; existing shutdown 2026-10-19T01:14:48Z is unchanged.

First gate: authenticate the two exact brief-pinned files; consume only canonical shortlist physical ordinals and canonical geometry. For each shortlist, compare deterministic optimal GET/covered-record plans at budgets 16,32,64,128,256,512,1024 and GET cap 16. Bridges count toward all bytes. Select the cheapest adjacent gaps (tie: lower physical ordinal); this minimizes coverage for each GET count. Check independently against exhaustive tiny partitions. Report totals, median, nearest-rank p95 and max. This is a cost counterfactual, not a timing measurement or speed claim.

Stop if no GET savings. Otherwise retain the individual-row control and add an opt-in bounded coalescing candidate on the same source. Preserve the exact scored shortlist and f64 bits, upstream SOURCE/SQ8 planning and charges, pinned per-row authentication (including unscored bridge rows), range/length/ETag/ID/hash checks, pre-IO/allocation caps, shared windows, ordered errors, drain/cancel, and admission before normalization. Keep dimensions and IDs generic and avoid layout, dependency or SIMD changes.

Acceptance covers planner/caps/tails/overflow, real-HTTP parity and authentication faults, drain/cancel, resource boundaries, native producer to strict reader, actual writer success and resealed refusals. Audit fp32_rerank/generation/index/runner/replay. Root owns later performance envelope and one frozen-patch review.

Static commands: CPU12, 1 CPU, 1 GiB, no swap, pids128, 120s. A single native controller: CPU8-15, 8 CPU, 40 GiB, no swap, pids2048, 1800s, jobs8, shared cargo-target.lock and environment.sh. Preserve original job handles and require no live native controller before edits. Required checks: affected tests, workspace Clippy correctness+suspicious, actual env-unset check_rust_test_build.sh, release runner/replay. Commit tested bytes using configured Roman identity, no hooks or AI attribution; no merge/push.

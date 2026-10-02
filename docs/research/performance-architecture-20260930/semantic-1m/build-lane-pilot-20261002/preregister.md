# On-Demand Cargo lane pilot — prospective

Status: UNRUN; launcher implementation pending. This is compiler infrastructure qualification, not an ANN performance measurement. Operator requested On-Demand for faster iteration while retaining Spot for expensive experiments. No active BORSUK EC2 or tagged cache EBS was found at the read-only opening check.

## One bounded pilot

Use causality / eu-central-1, Ubuntu 24.04 x86_64 c7i.2xlarge, On-Demand (no Spot market options). Run exactly one existing narrow Rust test on unchanged qualified native source399/addf62:

`cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::fragmented_paged_source_preserves_trace_and_rank_across_get_caps -- --exact`

Freeze source/archive/controller/config before launch. Require actual named test pass and nonzero count, unchanged complete source maps, authenticated logs/receipts and all owned IDs terminated. No release, full suite, ANN queries, or automatic retry. Compile resources: jobs1, memory8GiB, swap0, CPU200%, Tasks512. Local host is orchestration/source only.

Limit original machine lifetime to3600s, estimated compute to$0.50. AWS Price List API observed2026-10-02: Linux/Shared/Frankfurt SKU C6A9VKX345MTPUYK, $0.4074/hour, effective2026-09-01. This is a rate-based estimate, not actual billing. Recheck price before launch. Allow separately disclosed root-EBS/S3/boot-tail allowance$0.15 and encrypted80GiB gp3 persistent cache storage allowance$0.40 for at most24h, verified against live EBS rates before launch. Root must delete the newly owned cache by deadline absent an explicit measured reuse decision. Existing cache volumes are never reformatted or deleted implicitly.

Cache carries exact toolchain/target metadata, exclusive ownership/lock, registry and Cargo target. First pilot starts without a cache and must be labeled cold. A later warm pilot requires a new frozen reservation and previous original terminal/cleanup; it is not an automatic retry. Cache freshness is not source qualification or binary provenance.

## Measure and decide

Record code freeze, request, launch ACK, worker boot, downloads, SDK/toolchain/cache preparation, Cargo start/end, upload and authenticated collection times. Capture peak cgroup/RSS, PSI, swap/OOM and cache bytes/identity; cost uses actual observed instance lifetime times verified rate, EBS lifetime/storage and declared request counts, marked estimated until billing is available.

Compare with the retained local original36616 (resource-stopped before test; no successful elapsed-time baseline) and source-bound historical Spot qualification (different command/cache/resources disclosed). Do not claim a speedup from unmatched conditions or a successful test against a stopped build. The decisive result is time from code freeze to authenticated narrow-test result and incremental cache/setup fraction. Continue only if bounded remote execution actually closes the gate and its measured iteration cost is justified; otherwise retain Spot as the qualification lane. ANN quality, recall, cold latency and throughput gates remain unchanged.

SOURCE32 qualification snapshot409fd7166cd33492adcfb115c79dc534f0dab5f7 on qualification/source32-20261002 is separately frozen and UNRUN. Portable preflight native399 identity47d96a9429cd4ceb1a3f812dd202d77f21e2917f15406914b2457c7727e32f36, CODE24/ARTIFACTS18 and bash-valid7049B user-data pass; no Rust compilation or speedup claim. Choose qualification lane after the pilot evidence, without duplicating jobs.

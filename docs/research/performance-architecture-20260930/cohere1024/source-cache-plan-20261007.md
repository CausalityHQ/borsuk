# Bounded compressed-source cache plan

Read-only specialist 24542ea3bcf44c54. Root decision: implement library-only source cache first. Defer runner/comparison schema changes until native correctness passes; no new controller or vendor-service run. The 32 MiB setting is an experiment point, not a library cap or production default.

**Recommend an opt-in, generation-local compressed-source cache, capped at 32 MiB.** Keep routing, source nomination, SQ8 planning and scoring unchanged. This targets the measured 66.44 ms source stage without adding another fetch framework.

The completed admission remains 97.23% recall, p90 207.87 ms and p95 219.46 ms. These are descriptive 100k results, not a vendor win.

| Option | Benefit to test | Decision |
|---|---|---|
| Bounded compressed-source cache | Repeated authenticated source reads disappear on hits; 26.4 MB of source payload at 100k fits the proposed cap | **Choose**, default disabled |
| Eager metadata/leaf residency | Root/membership are already resident; another 6.4125 MB of leaf bodies could reduce the 35.29 ms discovery stage | Smaller RAM increment, but requires eager loading and startup accounting; defer |
| Direct SQ8 scheduling from router nominations | Could remove the source dependency | Changes fetched coverage and byte admission; exact existing plans require source scores. Defer |

**Implementation boundary**

1. In [two_bit_generation.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/two_bit_generation.rs:1563), add `TwoBitGeneration::with_source_cache(self, max_bytes: usize) -> Result<Self>`. Zero disables it. Reject insufficient total memory admission before allocation; include cache storage and directory capacity in `modeled_memory_bytes` and retired-generation pins.

2. Use a private, bounded cache of **256-row compressed-source blocks**, deriving width from `plane.receipt().record_bytes`. A fixed slot array with block-ID tags is sufficient; collisions cause misses. No dataset branches, raw-vector residency, SQ8 cache, background work or single-flight machinery.

3. Preserve the existing full source cover and its GET/byte admission. Skip a planned range only when **every block in that range is cached**; otherwise fetch that original range unchanged. This avoids fragmenting misses or changing cover selection. Reassemble results in original order.

   Reuse [`fetch_verified_ranges_inner`](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/sq8_s3_range.rs:528)—its actual location is `sq8_s3_range.rs`. Populate only from successfully authenticated returned bytes. Keep its concurrency, ordered errors and drain-all-admitted-reads behavior.

4. Update [check_cohere_native_baseline.rs](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/bin/check_cohere_native_baseline.rs:916) and the existing comparison example for an explicitly versioned cache experiment. Record capacity, occupied bytes, hits, misses and evictions separately from transport charges. Preserve strict historical comparison behavior.

**Correctness and resource constraints**

- Cache ownership must bind the immutable root SHA, source location, ETag and page authority. Never share entries between generations or reopenings. A hit serves previously authenticated immutable bytes; it does not check present-day S3 availability.
- Cache hits contribute **zero submitted GETs and zero newly fetched verified bytes**. Router/SQ8 charges stay identical; source charge differences must reconcile exactly with missed original ranges.
- Copy cached ranges into already-admitted query buffers. Avoid sliced `Bytes` retaining larger, uncharged allocations. Hold no cache lock across `.await`.
- Keep one active query for qualification, with the existing 16 concurrent source/SQ8 reads and router ceiling of 16. Model additional concurrent query buffers explicitly.
- Include lookup, copying and insertion in the source timing interval. Do not move their cost outside measurement.

**Cheapest falsifier**

First extend the existing paged-source tests with one parameterized cache regression covering disabled, empty, full-hit, mixed-hit, eviction and partial-tail cases. Compare complete plans, traces, IDs and score bits. Inject wrong ETag, corrupt payload and a delayed failing miss; assert complete drain, accurate failure charges, no poisoned entry and no SQ8 reads after source failure. Exercise another dimension and concurrent queries.

Before handoff, run the affected library/runner tests, required workspace Clippy, and `bash scripts/check_rust_test_build.sh` on the exact source revision in the authorized build environment.

Then, **after the protected compaction job finishes and actual-input admission plus a separate disposable canary pass**, run one frozen, contemporaneous S3 A/B cell:

- A: cache disabled. B: 32 MiB cache.
- Same retained generation, all 1,000 requests, truth, query order, CPU allocation and limits; no rebuilding or re-encoding.
- Each arm makes two complete sweeps. Report sweep one as **initially empty, then warming**, sweep two as warm. Include fill/startup costs. Cache-disabled A does not establish physically cold S3.
- Bound the cell to 4,000 query executions, 1,800 seconds, CPU1, 512 MiB/noSwap, existing read caps and one Spot instance. Preserve terminal evidence and terminate compute immediately. No automatic rerun.

Require exact per-query IDs, score bits, plans and recall numerator **9,723/10,000**, with zero underfill. Preregister a provisional survival threshold: warm source time and source GETs/bytes each decrease at least 50%, and end-to-end p90/p95 each improve at least 5%. Any parity, accounting or memory-bound failure stops qualification. A valid timing miss stops this candidate; setup/transport failures remain INVALID. A survivor earns reverse-order confirmation.

Report both measured points—latency, serial throughput, process RSS, query-cgroup peak and cache occupancy. The historical query cgroup peak was **47,333,376 bytes**; the reducer qualification’s 8 GiB peak is not serving RAM. Keep the disabled point: faster service using more RAM is a tradeoff, not RAM dominance.

**10M projection, explicitly unmeasured:** unchanged D1024 geometry implies roughly 2.64 GB compressed source, 641.25 MB leaf bodies, 10.36 GB SQ8 and 41.04 GB canonical records. A 32 MiB cache covers only about 1.27% of source bytes; useful locality is unproven. Current semantic profiles also do not admit 10M/D1024. A 100k cache win therefore earns further investigation, not scale feasibility or production-default approval.

Read-only assessment of commit `489b3ef91e6e1bf865f0ce9c59e47da3e477fbf3`; no files changed or native/cloud work started.

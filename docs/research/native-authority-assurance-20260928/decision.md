# Decision: current bounded authority fixtures qualified

All fifteen named index::native_bounded_ library checks passed on frozen source e8b8815adb09686c8e2a85c72db137e380a7f1a0892f5a381c5f455fcdbfa39e, base be3e922d, AWS causality eu-central-1 Spot i-0f18f11d6b674fdb0. Test runtime 44.16 seconds is correctness runtime, not query latency. Every terminal artifact hash/length and the changed Rust file against its source archive were independently verified. See closeout for termination and compute estimate, excluding EBS/S3.

Ten stale native-v2 fixtures now exercise the current D64 bounded SQ8 authority. Binary IDs, collection-only visibility, reopen, committed-plus-pending writes, tombstones, latest-wins and deterministic compaction remain required. No production behavior/defaults or admission limits changed.

The hundred-flush fixture now exercises the actual bounded contract: segment 17 is rejected before PUTs, authority stays unchanged and WAL stays pending; callable compaction drains that WAL and restores headroom. All 100 flush attempts complete with explicit maintenance. Expected exact top-ten identities agree across 1/10/100 flush partitions and final compaction; distances are finite/nonnegative within per-authority SQ8 error bounds. Bit-identical distances across independently encoded SQ8 authorities are not promised.

The first fifteen-case run passed fourteen and exposed the existing admission ceiling; its failed terminal is retained in capacity-red. The earlier client-token rejection created no compute; retained in rejected-launch.

Original full library gate: 1630 pass / 57 fail / 6 ignored. Earlier slices fixed eight, this slice fixes ten: 39 original failures remain unqualified. No new full workspace result or performance/recall/vendor claim. Next: explicit diagnostic preparation in shared fixtures while preserving authentication/corruption checks; repair failing layers, then one final workspace gate. Production and paired vendor qualification goal remains active.

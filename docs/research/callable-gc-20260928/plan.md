# Bounded callable reclamation

Single-host cooperating same-directory readers only; exclusive local lifetime
lock precedes durable write fence. Keep authenticated current metadata, canonical,
SQ8 and validated delta; delete only recognized owned generation/mutation and
maintenance UUID objects. Stream listing with scan/delete/logical-byte caps,
constant keep-set and separately modeled metadata/delta payload. Unknown/external
keys are untouched. Failure retains durable fence for retry. Report attempted
versus acknowledged deletes; SDK retries/list buffers/versioned retention are
caller/storage charges. Late orphan uploads can require another pass.

No cross-host pin safety claim. Generic low-level calls are caller-coordinated.
Owner must not independently clear the active GC fence. Owned worker holds local
exclusivity even when its awaiting future drops. Compaction must rebuild a ready
attempt if GC reclaimed its uncommitted SQ8, using still-authenticated current
canonical source. Full-history rollback retention is not this keep-set.

Cheapest existing fixture checks pinned-reader refusal, partial-delete failure
and fenced retry, preserved current objects/delta and unknown-key retention.
One AWS causality Spot red/green job also invokes explicit ignored native-S3 test
on a unique owned research/native-library-check/gc prefix: actual fence epoch/body
change, stale conditional PUT rejection, populated metadata/SQ8/canonical/delta
survival, query IDs and exact test-prefix cleanup.3 local integration+4 HTTP checks
plus1 live-S3 smoke.30min/$0.30 compute cap, zero SDK retries in native smoke.
No local Cargo/Spark, quality panel, extra machine or broad benchmark.

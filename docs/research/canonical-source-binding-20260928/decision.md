# Generation v2 canonical source binding

## Decision

Build a durable source record stream with signed logical IDs and normalized FP32
rows, in the same physical order as SQ8. Reuse source-order validation and cosine
normalization, then recheck full raw/SQ8 input hashes. Bind its geometry, length,
SHA and approved content-addressed key in mandatory generation schema v2. V1
rejects; no compatibility reader or archived artifact rewrite.

Reuse the existing multipart length/hash-checked uploader before the generation
head CAS. The canonical object stays outside the unchanged eight query metadata
objects. No query open/planner/search fetches it or retains a full source plane.
Maintenance recovery streams the pinned object to owned temporary disk, validates
its whole SHA/length and performs no-clobber rename. SDK call/delivered-byte
charges survive failures. Caller owns transport retries/buffers, concurrency,
retained generations and disk admission; this is not an absolute process RSS cap.

## Verification

Missing-API red exit101; first compile exit101 (missing ObjectStoreExt import);
fixed source/publication/recovery fixtures exit0 (two tests); final local HTTP
mutation/error checks exit0 (four tests). All original logs/exits retained,
final remote/local source SHA parity and focused rustfmt/diff checks pass. The
only post-test source change is the builder module documentation comment.
The signed-ID fixture checks physical source/ID byte identity, publish/recover,
existing-output, disk/chunk cap and corruption rejection, invalid local source
blocking publication, and zero canonical/SQ8 GETs during metadata-only open.
The independent generation fixture checks exact constructed bytes/root parity,
v1 rejection and malformed source descriptor rejection. Existing HTTP mutation
and error accounting checks remain required at this revision.

[Verification receipts](verification.json) identify exact source and raw log
hashes/exits. Tests use synthetic512-row D2 data and local HTTP/InMemory stores;
they are correctness evidence, not ANN quality, AWS timing/RSS/cost or vendor
superiority. No paid run, local compiler or parameter sweep. The native dual critique request
was rejected before launch by its six-hour cooldown; no override or duplicate
review was used. [Review status](review-status.json).

## Next single gate

Implement atomic base/delta compaction handoff using this canonical source so
concurrent writes and pinned readers cannot be lost, then reachable-object GC.
Full-source maintenance reads/upload add lifecycle cost that must be measured;
no large-scale or matched vendor gate is closed by canonical durability alone.
Both-vendor quality/e2e latency/QPS per total dollar and100M remain OPEN in the
[acceptance matrix](../../release/competitive-acceptance.md).

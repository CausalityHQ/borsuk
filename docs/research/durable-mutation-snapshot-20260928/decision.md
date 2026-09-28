# Durable pending state: persistence prerequisite

## Decision

Publish a bounded latest-state delta per immutable base generation, through the
existing object-store conditional publication/read machinery. Reuse the cosine
normalizer, bounded streamed small-object helper and store error type; add no
writer service, unbounded log or full base-vector/ID map. Upsert/delete batches
are sorted unique signed-i64 IDs. CAS revision serializes writers; stale tokens
must reread/reapply. Old immutable snapshots stay available to pinned readers.

The recovered snapshot carries root/dimension binding, digest, opaque writer
token, normalized put rows and borrowed exclusion roster. Input, full merged
state and payload caps validate before staging or updating the head. Serialized
schema is new `BTMUT001`; generation root schema is unchanged. The authenticated
base head now exposes its declared dimensions. No historical measurement is
transferred to this revision.

## Verification gate

Missing-API red exit101, first application-ID/generation integration exit0
(two tests), then final expanded fault/recovery integration exit0 (one extended
fixture). Final source SHA matches Spark for all touched library/test sources
and the unchanged shared fault helper; focused rustfmt/diff checks pass.
The fixture checks publication/recovery, updates/deletes, signed-ID extremes,
stale CAS, independent namespace/dimension binding, cap/duplicate/invalid-vector
rejection, corruption, and correctly hashed malformed payload rejection.
Existing shared fault-injecting object store verifies lost CAS acknowledgement
recovery and pre-commit failure preserving the old head. No duplicate fault-store
implementation or cloud test is added.

[Verification receipts](verification.json) identify exact terminal statuses and
raw log/source SHA256. This is in-memory object-store functional evidence, not
S3 timing/reliability or a benchmark. Current helper payload model is conservative
admission, not measured process RSS. The latest-state rewrite creates lifecycle
write amplification that remains unmeasured.

## Next single implementation gate

Merge pending puts with authenticated masked base candidates through a root-bound
library search, verify mutate/reopen/search parity, then coordinate compaction
and GC without losing concurrent writes or pinned readers. Until that is done,
masking replacements alone would remove them from results. Cap exhaustion is
an error; compaction is not silently simulated by dropping pending states.

Winning ANN quality, cold HTTP, 100M scale, lifecycle accounting and matched wins
against BOTH competitors remain OPEN in the
[acceptance matrix](../../release/competitive-acceptance.md). No paid campaign,
parameter sweep, vendor claim or release completion is authorized by this slice.

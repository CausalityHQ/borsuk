# Durable generation-pinned mutation snapshots

Base91112778. Reuse object-store immutable digest objects, bounded streamed
small-object reads and conditional head publication. Each immutable generation
has its own mutation head. An opaque recovered snapshot includes the exact CAS
token, base root, dimensions, revision, sorted signed-i64 latest-state rows and
borrowable exclusion roster. No root-level global row map/full-vector hydration.

Apply a sorted unique batch of upsert/delete commands to the recovered latest
state; older states remain immutable. Normalize puts using the existing cosine
helper. Binary schema stores magic/base SHA/dimensions/revision/count, then ID,
operation and FP32 coordinates. Authenticate before decode, validate lengths,
strict IDs, finite nonzero/unit-norm puts and exact EOF. Byte and conservative
payload caps reject before allocation/publication; cap exhaustion is an error,
not silent dropped mutations. CAS sequence serializes writers; no parallel
per-row clock or divergent merge path is needed. Stale writer must reread/reapply.

Publication writes immutable snapshot first and conditional small head last;
resolve lost acknowledgements only by exact authenticated sequence+SHA readback.
Store ACL authorizes head writers. Recovery uses the opaque authorized base head
and authenticates snapshot digest and root/dimension/revision binding. Expose rows
and sorted mask to subsequent overlay search. This slice provides persistence,
not yet upsert scoring/compaction or a release/vendor certification.

Extend the existing 512-row application-ID fixture: publish, upsert/delete,
reopen same state, next update, stale CAS rejection, source namespace/dimension
mismatch, cap/duplicate/zero/nonfinite failures, corrupted snapshot rejection.
Red missing API, green original focused Spark integration. Existing generation
publication regression once. No cloud, full suite or duplicate consultation.

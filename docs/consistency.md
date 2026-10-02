# Experimental publication and maintenance

The native Rust APIs stage authenticated immutable objects, then conditionally
publish a namespace-bound head. A stale conditional writer fails rather than
replacing a newer head. Open handles pin a generation; callers must reopen to
observe a replacement.

Mutations publish durable snapshots bound to the base root and namespace.
Recovery reads the snapshot referenced by the head. Search with that snapshot
suppresses replaced and deleted base IDs before top-k and scores pending values.
Snapshot admission limits reject excess state; they do not silently drop rows.

Callable compaction recovers canonical source and deltas, prepares a replacement
generation, and uses a durable write fence around publication. Garbage
collection applies bounded scans and deletes under the fence and an exclusive
lifecycle lock. Tests cover stale commits and ambiguous delayed deletes.

Readers opened through `TwoBitIndex::open_coordinated` must share the same
maintenance directory with compaction and garbage collection. This protects
cooperating processes on one host or shared local filesystem. Other hosts and
direct remote openers are outside that lock's protection. Lockfiles must remain
in place while participants are alive.

The caller owns credentials, access control, trusted-head delivery, and
coordination. These APIs do not establish a multi-host reader-lifetime protocol.

Read the [application-ID and lifecycle fixtures](../crates/borsuk/tests/two_bit_application_ids.rs),
[delayed-delete fixtures](../crates/borsuk/tests/two_bit_gc_delayed_delete.rs),
and [native API guide](api.md).

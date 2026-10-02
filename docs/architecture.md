# Experimental search flow

The current Rust path searches immutable, authenticated generation data from
the caller's process:

1. Open a small semantic directory and membership metadata bound to the root.
2. Select centroid leaves and authenticate their fetched bodies.
3. Read two-bit SOURCE records for nominated units and form a bounded shortlist.
4. Plan and fetch generation-pinned SQ8 ranges, then score and return candidates.

This is approximate retrieval. SQ8 scoring uses quantized values. Selected-range reads avoid opening
all vector payloads at startup. Construction and full publication validation
read more data than serving.

The caller supplies resource limits and an authorized object-store head or
trusted root digest. Conditional reads and content hashes bind fetched data to
that generation. See the [native API guide](api.md) and [storage guide](storage-format.md).

The implementation is in [semantic_unit_router](../crates/borsuk/src/semantic_unit_router.rs),
[two_bit_generation](../crates/borsuk/src/two_bit_generation.rs),
[two_bit_store](../crates/borsuk/src/two_bit_store.rs), and
[two_bit_index](../crates/borsuk/src/two_bit_index.rs).
Python and TypeScript bindings use a separate experimental code surface.

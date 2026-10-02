# Experimental native generations

`TwoBitGenerationBuilder` assembles authenticated metadata from prepared SOURCE
and SQ8 inputs. Its semantic-router entrypoint binds the directory, membership,
and centroid leaves used by the current search path.

`publish_two_bit_generation` stages immutable metadata and conditionally
publishes the head. `TwoBitGeneration` opens from a trusted root or authorized
head and searches generation-pinned SQ8 ranges after two-bit nomination.
`TwoBitIndex` adds logical-ID search over populated and empty bases, including
caller-supplied recovered mutation snapshots.

Construction, publication, pinned reload, identity/corruption rejection, and
resource admission have [generation fixtures](../../crates/borsuk/tests/two_bit_generation.rs).
The [application-ID fixtures](../../crates/borsuk/tests/two_bit_application_ids.rs)
cover recovery and lifecycle operations. Those fixtures do not establish a
broad performance or capacity promise.

See [API and limits](../api.md), [publication and maintenance](../consistency.md),
and [the current measurement](../benchmarks.md). Read exact signatures in
[two_bit_build](../../crates/borsuk/src/two_bit_build.rs) and
[two_bit_generation](../../crates/borsuk/src/two_bit_generation.rs) at your pinned revision.

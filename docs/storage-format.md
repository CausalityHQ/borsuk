# Experimental storage and limits

A native generation binds routing metadata, two-bit SOURCE nomination records,
SQ8 page identities, logical IDs, and normalized canonical source through an
authenticated root. Immutable objects are published before the conditional head.

Remote serving opens routing metadata and reads selected centroid leaves,
SOURCE units, and SQ8 ranges on demand. Hash verification and conditional
object identity checks reject mismatched or corrupt data. Canonical normalized
source is retained for recovery and compaction; serving scores SQ8 values.

Callers set memory, query admission, scratch, and separate SOURCE/SQ8 I/O limits
in `TwoBitGenerationLimits`. Modeled payload limits are not an RSS guarantee.
Construction, retained snapshots, transport buffers, and runtime overhead need
their own accounting.

BORSUK is unreleased. Stored formats can change, and incompatible artifacts are
rejected. Build and open from the same pinned source revision.

See [generation fixtures](../crates/borsuk/tests/two_bit_generation.rs),
[SOURCE authentication fixtures](../crates/borsuk/tests/two_bit_source.rs),
and [publication and maintenance](consistency.md).

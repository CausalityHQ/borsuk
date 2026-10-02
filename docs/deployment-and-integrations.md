# Experimental embedding and storage

The current native Rust route runs in the caller's process. It has local
synthetic fixtures and S3 measurement evidence. The caller provides the object
store, authorized head or root digest, resource limits, and scratch directory.

Opening reads authenticated metadata; querying fetches selected centroid,
SOURCE, and SQ8 ranges. Storage must support the conditional operations used
by publication and generation-pinned reads. An object-store abstraction alone
does not qualify every backend or S3-compatible implementation.

Use a shared maintenance directory for coordinated readers, compaction, and
garbage collection on one host or shared local filesystem. Credentials,
request authentication, tenancy, and service deployment belong to the caller.

Start with the [native fixtures](../crates/borsuk/tests/two_bit_generation.rs)
and [logical-ID/lifecycle fixtures](../crates/borsuk/tests/two_bit_application_ids.rs).
Read [API](api.md), [consistency](consistency.md), and [resource limits](storage-format.md)
before adapting them to an application.

[Python](../python/README.md) and [TypeScript](../packages/borsuk/README.md) bindings
are separate experimental surfaces. Their APIs and measurements do not establish
parity with the current native generation route.

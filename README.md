# BORSUK — Experimental

**An experimental Rust library for vector search over object storage.**

BORSUK — Blob-Oriented Retrieval with Segmental Unified KNN — is unreleased.
APIs, defaults, and stored formats can change.

The current native search path opens a small authenticated semantic directory,
reads selected centroid leaves, shortlists candidates from two-bit SOURCE
records, and scores selected SQ8 vector ranges. Search runs in the caller's
process. The native path has local synthetic fixtures and S3 evidence.

Rust APIs cover generation construction, conditional publication, pinned open
and search, logical application IDs, durable mutation snapshots and recovery,
callable compaction, and garbage collection. Maintenance coordination protects
cooperating callers on one host or shared local filesystem.

Python and TypeScript packages are separate experimental bindings. Their local
create/add/search APIs do not expose the current native generation route.

## Explore the code

- [Native API guide](docs/api.md)
- [Search flow](docs/architecture.md)
- [Publication and maintenance](docs/consistency.md)
- [Storage and resource limits](docs/storage-format.md)
- [Embedding and storage setup](docs/deployment-and-integrations.md)
- [Python package](python/README.md) and [TypeScript package](packages/borsuk/README.md)
- [Native generation fixtures](crates/borsuk/tests/two_bit_generation.rs)
- [Application IDs and lifecycle fixtures](crates/borsuk/tests/two_bit_application_ids.rs)

## Development evidence

The [latest offered-load measurement](docs/research/performance-architecture-20260930/semantic-1m/fixed48/offered-http/a0002/decision.md) covers CoHere FIRST1M,
768 dimensions, cosine, top 10, and 64 sealed queries. At 8 offered queries/s,
it measured 96.71875% recall@10, 541.10 ms cold p90, and 7.597044 completed
queries/s over the finite run including drain. Each call used a fresh native
process and scratch directory. This is one development result; sustained
capacity, broader scale, lifecycle cost, and a matched vendor comparison are
unmeasured in that run.

[Research evidence](docs/web/research.html) keeps the measurement's source,
configuration, protocol, and scope reviewable. Archived research describes the
revision it measured.

## Source and license

Start from a pinned checkout and read the [source build guide](docs/release/rust-source-packages.md).
The library is source-available under [Business Source License 1.1](LICENSE),
with the Additional Use Grant defined there.

# V172 authenticated router-to-SQ8 row map

## Purpose and contract

The current `Pq64Router` code plane and page summaries retain their
original router physical order, while V163/V164 repack SQ8 rows into a
new physical order. The offline 100k/1M screens carry explicit old/new
source-row permutations. A production generation needs the same mapping
authenticated before a PQ-scored candidate unit is used. Inferring a
router ordinal from the relaid SQ8 row position is invalid.

Use one new immutable `BORSUK-ROWMAP-V1` row-map artifact. Its payload is an
array of `N` little-endian `u32` old-router ordinals in **new SQ8 physical
row order**. The 160-byte header contains a format/version marker,
generation, row count and four SHA-256 identities: source artifact,
router manifest, router's original SQ8 object and relaid SQ8 object.
The caller pins the whole artifact SHA-256 before loading. The loader
rejects a wrong marker, length, hash, generation, identity or non-bijection
before exposing any mapping. It builds and retains both `new_to_old` and
`old_to_new` arrays from that one authenticated payload. No old-format
reader or migration path is required before first release.

The intended generation binder must require the row map and compare its
generation, source identity and router manifest/old-SQ8 identity with the
loaded router, and its new-SQ8 identity with the SQ8 mirror/page authority.
The router's current old-SQ8 hash must not be silently relabeled as the
new object hash. A query field computes candidate *new* physical units,
maps their rows through `new_to_old` for resident PQ scoring, and maps
nominees through `old_to_new` for mandatory primary coverage. The binder
must reject any mismatch before the query can issue GETs.

## Resource and qualification

Two `u32` arrays cost exactly `8N` payload bytes per pinned generation:
800,000,000 bytes at 100M rows for one generation, before allocator,
router, SQ8 metadata, concurrency and multiple generations. This grows
with corpus size and generation count; there is no vector-count knee or
fixed 100M RAM ceiling. The format supports up to `u32::MAX` rows; a
larger corpus would require a new explicit format, not truncation.

The first implementation slice is the authenticated writer/loader with
bijective round-trip and tamper tests, then a narrow remote crate test.
The generation binder and serving route are separate qualification work.
This artifact alone does not establish returned recall, serving latency
or persistence recovery. A later complete-generation test must bind a
nonidentity map, reject a swapped SQ8 object or router manifest, score
candidate rows through the mapping, and verify returned IDs against an
independent exact reference.

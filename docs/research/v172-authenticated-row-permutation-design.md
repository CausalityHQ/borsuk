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

A bijection and whole-file hash alone do not prove that the payload has
the correct direction. The generation builder must stream the old and new
SQ8 bodies and verify `new_sq8_row(i) == old_sq8_row(new_to_old[i])` for
every row before publishing the map. Duplicate row bodies require the
source-row identity or another authenticated tie key as well; byte
equality alone is insufficient in that case. The full-generation test
must deliberately supply the inverse map and reject it. The existing
router loader also discards its verified manifest digest, so binder work
must retain that digest in `SourceRouterArtifact` before comparing it to
the row-map header. The current `ServingGeneration::bind` equality of
old-router and new-mirror SQ8 hashes is incompatible with a relaid SQ8
object and must be replaced by checks through this map, not bypassed.

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

## Closed narrow compile gate

The writer/loader slice was pushed at `36b9a3e31e376b0e28a4fb2d0a43007fa63be3fe`.
One Causality Spot `c7i.8xlarge` worker `i-0df21551044d4bcce` ran
`cargo test --locked -p borsuk --lib physical_row_permutation::tests`:
**2 passed**, 0 failed, 1,658 filtered. Compile and test took 104.43
wall seconds and peaked at 4,978,568 KiB process RSS on that build
worker. This is not a serving latency or resident-index measurement.
The complete terminal SHA-256 is
`beaeae8c34160a828049cdf66db42cded2ad21c134a600b24cfd89add8d94442`
at `s3://borsuk-bench-453182569524-euc1/research/v172-row-map-compile/36b9a3e31e376b0e28a4fb2d0a43007fa63be3fe/runs/a0001/`.
The controller streamed and rehashed all three terminal-listed artifacts
and confirmed the instance terminated. The crate emitted 204 warnings,
including 19 missing-documentation warnings in the new module. Those
new documentation warnings were corrected after this gate; the closed
test does not establish a warning-free crate or full-suite pass.

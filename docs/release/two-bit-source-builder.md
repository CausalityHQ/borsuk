# Rust source-only two-bit plane builder

`borsuk::two_bit_source::TwoBitSource::build` creates the nomination metadata
for the frozen object-native candidate using the public Rust codec. It accepts
no queries or GT. This is one index-preparation primitive; it does not yet
create, publish or search a complete ANN generation.

```rust
use borsuk::two_bit_source::TwoBitSource;

let receipt = TwoBitSource {
    raw: raw_path,                       // source-ordinal little-endian F32 rows
    raw_sha256: expected_raw_sha256,
    sq8: sq8_path,                       // physical-order SQ8 rows
    sq8_sha256: expected_sq8_sha256,
    rows,
    dimensions,
}.build(output_dir, max_memory_bytes)?;
```

The caller owns immutable input snapshots and the new output namespace. Raw
row ordinals must correspond to SQ8 IDs; the builder authenticates the two
files and checks the ID permutation, but their underlying vector relationship
is the caller's construction contract. SQ8 rows are i64 ordinal ID, f32
squared norm, then `dimensions` code bytes. For signed-i64 application IDs,
use the explicit-order methods described below; no separate resident ID map
is required. Nonfinite/zero source rows, unrepresentable
codec scalars, duplicate/out-of-range IDs, nonpositive SQ8 norms, bad hashes/lengths, overflow and
insufficient admission fail with an error. Existing output is never replaced.

Outputs are `mean.bin`, `records.bin`, and `manifest.json` with schema
`borsuk-two-bit-plane-v2`. Records use the Rust codec's scale and inverse
reconstructed norm; historical two-bit records used a different final scalar.
Writes are streamed and synced before `manifest.pending` is renamed last.
Failures can leave an unpublished directory for caller cleanup. A crash may
lose this local staging rename; completed artifacts must be authenticated and
published through the generation protocol. Bind the final manifest's hash in
that trusted generation root before serving; a filename alone is no authority.

Payload admission estimate, checked before allocation:

`8 * rows + ceil(rows / 8) + 128 * dimensions + 262144` bytes.

The builder holds IDs/bitset and coordinate-sized buffers, never an ND source
matrix or the output record plane. Allocator/runtime overhead and OS file cache
are outside this estimate. Physical-order source seeks can become I/O-bound
on large corpora; 100M build time/cost is unqualified.

The same API is callable from the thin CLI:

```sh
cargo run -p borsuk --bin build_two_bit_source -- \
  RAW RAW_SHA SQ8 SQ8_SHA ROWS DIMENSIONS MAX_MEMORY_BYTES OUTPUT_DIR
```

## Verified increment

One source-only CoHere first100k D768 build completed exit0 with 16 MiB payload
admission: **64.64 seconds, GNU time reported2432 KiB maximum RSS, zero swaps**.
An original `/proc`/`ps` sample showed2860 KiB RSS at55 seconds; the time
receipt is not a precise whole-process memory proof.
These are local debug source-build verification costs, not serving latency,
query RSS or a scale projection. Output is20,000,000bytes. Both mean and complete
record plane are byte-identical to sealed V294:

- Mean: `b50ffae3e16cc5e155ceee3adffcac0dc04d6812b0a1bb8fc01cbd259b58440a`.
- Records: `224d6c64758106f8d67ef1cc1a1f9a2088df9c7811d420c0d1a003f45a5db596`.
- Manifest: `758f1ffdfde1fe9454dca4a9cb5f51a74fcc869ab8fa805de3cfb5a2df8a0363`.

The focused Rust test covers D5 encoding/mean/record digests, reversed physical
IDs, wrong source hash, duplicate/out-of-range IDs, nonfinite source, memory
rejection and overwrite. It failed for the missing API before implementation,
then passed. Independent dual review was attempted but rejected by the six-hour
cooldown; no override or replacement consultation was launched.

Full paired ReLAION/CoHere validation, generation binding/open/search,
incremental mutation/GC, cold HTTP and vendor qualification remain open.

## Authenticated reload and scoring

```rust
use borsuk::two_bit_source::TwoBitPlane;

let plane = TwoBitPlane::open(
    output_dir, trusted_manifest_sha256, expected_sq8_sha256, max_metadata_bytes,
)?;
let query = plane.prepare_query(query_vector, max_query_scratch_bytes)?;
let score = query.score(plane.record(physical_row).ok_or("unknown row")?)?;
```

Both expected hashes must come from the caller's authenticated generation,
not from the artifact being opened. The opener rejects incompatible manifests,
wrong SQ8 binding, changed mean/records, invalid geometry and insufficient
admission. Exact-size reads also reject concurrent growth/truncation; content
hashes authenticate the bytes actually read. The scorer validates record scalars
before returning a value.

This intentionally loads **nomination metadata**, not source vectors or SQ8.
D768 records occupy200bytes/row:20MB at100k, projected20GB at100M before
other routing metadata and old generation pins. This primitive does not qualify
that 100M memory envelope. Open admission conservatively charges record bytes
plus128times padded dimensions plus128KiB for manifest/deserialization; runtime,
allocator and OS page cache are excluded. Each concurrent query admits scratch
separately. Caller-level total concurrency and generation-swap accounting remain
integration requirements. Complete ANN creation/open/search remains pending.

## Native physical order

The [native source-only fitter](native-source-order.md) now supplies a physical
ordinal permutation from authenticated normalized f32 input without Python.
Feed it to the public SQ8 encoder before the generation builder. Its ChaCha8
RNG/arithmetic changes the layout; new quality gates and root hashes are required.
The historical paired100k results used the original approved Python layout.

## Logical IDs and explicit source order

SQ8 may carry unique signed64-bit application IDs, including negative IDs.
`build_sq8_source_with_ids` takes IDs indexed by raw-source row plus a complete
physical-position-to-source-row permutation. `TwoBitSource::build_with_order`
and `TwoBitGenerationBuilder::build_with_order` consume that same permutation;
application IDs never address raw rows. Ordinal convenience methods still serve
corpus/control inputs and emit the same current v2 source-plane marker. There
is one codec/writer implementation per layer, no legacy reader or migration.

v2 receipts include `source_order_sha256`, hashing canonical little-endian u64
ordinals in physical order. The generation root authenticates the receipt and
SQ8 body digest; IDs stay on demand in SQ8, not a hydrated ID/vector map. v1
planes reject; historical artifacts retain their original source/schema.

Caller source/IDs/order must be immutable and describe the same rows. Duplicate
IDs, malformed permutations and over-budget input reject before a successful
manifest/receipt. Writer admission charges caller order/IDs and sorted-ID check;
source and generation builders charge the explicit caller order. Other retained
caller state, allocator/runtime/OS cache and pin overlap are separate charges.

```rust
let encoding = borsuk::sq8_source::build_sq8_source_with_ids(
    normalized_path, normalized_sha, dimensions, &order, &source_ids,
    new_sq8_path, writer_payload_cap,
)?;
// builder.source binds the same raw/order/SQ8 geometry and encoding coefficients.
let root_sha = builder.build_with_order(&order, new_generation_dir, build_cap)?;
```

This is a logical-ID construction prerequisite. String-ID catalog binding,
incremental overlays, mutation publication/recovery/compaction and fresh ANN/
HTTP/vendor/100M qualification remain open. It does not inherit a historical
benchmark win or claim wrapper parity.

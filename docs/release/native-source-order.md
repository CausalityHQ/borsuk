# Native semantic source order (candidate)

```rust
use borsuk::source_order::fit_source_order;

let order = fit_source_order(
    normalized_path, normalized_sha256, rows, dimensions, max_payload_bytes,
)?;
```

Input is a caller-owned immutable little-endian f32 source with unit cosine
rows. Normalize raw data first with `sq8_source::normalize_source`. The fitter
accepts no queries or truth, checks geometry and memory admission before
allocation, validates row norms, and authenticates the complete source on both
streamed passes. It returns physical positions mapped to unique source ordinals;
pass that order to `sq8_source::build_sq8_source`, then assemble/publish the
[authenticated generation](two-bit-generation.md). Application IDs and
incremental maintenance remain unqualified.

The recipe uses `ceil(N/256)` cells, a uniform source reservoir of at most
64 rows per cell (seed8201), sampled initial centroids (seed8202), 12 f32 Lloyd
updates, centroid chaining and radius ordering with ordinal tie breaks. Empty
centroids retain their previous value. Existing ChaCha8 and nalgebra implement
the native recipe; they differ from the frozen NumPy PCG and arithmetic.
Do not substitute a new layout/body/root into historical results or claim
Python artifact parity. No source vector matrix is retained in serving.

Fitting holds the sampled matrix, centroids, bounded256-row blocks and an
ordinal ordering buffer. The checked payload model includes these allocations
and a conservative matrix packing allowance; allocator/runtime overhead and
OS file cache are outside it. The caller must measure RSS for its resource
qualification. Flat fitting costs `O(12*S*C*D + N*C*D + C*C*D)` and are not
qualified at100M. An over-budget build rejects; this is not a100M default or a
proof of scalable construction.

The existing example can write a sealed little-endian u64 order:

```sh
cargo run -p borsuk --example build_sq8_source -- \
  fit NORMALIZED_F32 SOURCE_SHA ROWS DIMENSIONS MAX_PAYLOAD_BYTES NEW_ORDER
```

The new output is synced and installed without replacement; its JSON receipt
names the recipe and order SHA. An installation/sync error may leave an
unpublished artifact; discard it on error. The next quality gate is the
predeclared CoHere100k development0–63 page-oracle falsifier, before full paired
validation or cloud serving promotion.

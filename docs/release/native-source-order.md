# Native semantic source order (candidate)

**Not approved for cloud/scale/release promotion.** Complete native preparation
failed the frozen ReLAION100k paired quality gate: 98.851% returned R@100 versus
99.569% native SQ8 flat (0.718pp deficit; limit0.5pp). See the
[terminal decision](../research/native-pipeline-quality-20260928/decision.md).

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
unpublished artifact; discard it on error. The development page oracle passed, but did not predict the paired validation
failure. A distinct source-only layout decision and development falsifier are
required before another promotion gate.

## Hierarchical fitting prerequisite

The crate already exports `train_logical_cell_centroids`, a deterministic
hierarchical Lloyd trainer with proportional sample leaf quotas. It should be
reused for the next layout rather than reimplementing Lloyd updates. It does
not itself stream/authenticate raw source or guarantee balanced full-corpus
extents. The current `fit_source_order` remains the failed flat candidate.

The shared centroid assignment search now uses sparse per-query visited marks
instead of initializing a cell-count-sized epoch array for every query.
Construction still reuses its existing epoch array. Initial query reserve is
`min(cell_count, ef * 32)` slots; actual visits can grow to the traversed node
count, so this is not a worst-case constant-memory/search-work guarantee or a
measured latency gain. This closes one prerequisite for streaming assignment;
it does not qualify hierarchical layout quality,100M resources or vendor wins.

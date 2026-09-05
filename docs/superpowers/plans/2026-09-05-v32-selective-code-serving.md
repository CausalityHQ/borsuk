# V32 selective code serving Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the quality-preserving V32 route from authenticated bounded Arrow code objects without resident global PQ code planes or corpus downloads.

**Architecture:** Load one strict manifest, compact bounded Arrow routing-directory shards, codebooks, and a Parquet page registry into code-free metadata. Route in memory, fetch only the selected code objects through bounded async waves, score selected microleaf ranges with the shared kernel, then fetch and exact-rerank exactly sixteen pages. Emit immutable objects and publish the manifest last.

**Tech Stack:** Rust, Arrow IPC, Parquet, serde JSON, SHA-256, half f16, bytes, Tokio, async-trait, existing V30/V32 PQ and page kernels.

**Spec:** `docs/superpowers/specs/2026-09-05-v32-selective-code-serving-design.md`

## Global Constraints

- BORSUK is pre-release: introduce one strict format and no legacy reader, alias, migration, or fallback.
- Production search must not own or construct `V30CodePlanes`, the global fidelity bitmap, `V32Router`, or a corpus path.
- Dimension is exactly 96; candidate depth is 12,288; selected pages are exactly 16 and at most 3,145,728 bytes.
- Code objects remain at most 8,192 rows, 32 parent records, 128 ranges, and 524,288 encoded bytes.
- Every production routing leaf has 1..1,024 rows; this serving slice neither repairs nor changes routing geometry to meet storage limits.
- One selected query admits at most 256 code objects, 64 MiB of code bytes, 256 selected ranges, and 262,144 scored rows before any GET. Build/open validation proves these caps unreachable for a valid frozen arm.
- Code waves use one frozen width from 16/32/64/128/256; the page wave is exactly 16 and all results preserve request order.
- The supported envelope is 1B rows, 4,096 roots, 262,144 nonempty parents, at most 4,000,000 routing leaves and code objects, and at most 2.4M pages.
- Each normalized parent/leaf/object directory family uses 1..32 ordered shards of at most 64 MiB; total encoded and compact decoded directory projections are each at most 1,280 MiB.
- Resident metadata, bounded cache, and active-query reservations must have checked total below 3 GiB.
- Narrow TDD gates run before grouped checks; no dataset download or AWS experiment until local parity and static gates pass.

---

### Task 0: Object-granularity metadata simulator

**Files:**
- Create: `crates/borsuk/examples/v32_selective_layout_simulator.rs`
- Test: unit module in that example

- [ ] **Step 1: Write the simulator RED**

Given authenticated routing-leaf sizes, selected-leaf ordinals, and mixed-width row
counts, compare exactly the Cartesian ladder of 1/2/4 consecutive leaves per
object and code-wave widths 16/32/64/128/256. Recompute object requests, encoded
bytes, selected bytes, byte amplification, waves, maximum concurrent bytes,
and leaf-size histogram. Mutation tests cover an
underfull leaf, oversized object, missing/duplicate selected leaf, arithmetic
overflow, and a ladder arm omitted or selected from sealed data.

- [ ] **Step 2: Implement and run only the metadata simulation**

The simulator reads no code or page body and performs no query scoring. Run it
first on a synthetic fixture, then once on the burned 1M development metadata.
Persist the nondominated arms satisfying request/wave and admission arithmetic.
Task 6 encodes and hashes those arms to measure construction throughput and key
distribution, then freezes exactly one before any holdout; it cannot invent a
new arm or reopen a rejected one.

---

### Task 1: Authenticated compact code directory

**Files:**
- Create: `crates/borsuk/src/v32_selective_codes.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit module in `crates/borsuk/src/v32_selective_codes.rs`

**Interfaces:**
- Consumes: the existing f16 centroids, logical ranges, and `V32PageLocation`.
- Produces:

```rust
pub(crate) struct V32CodeObjectIdentity {
    pub(crate) object_ordinal: u32,
    pub(crate) root_ordinal: u32,
    pub(crate) sha256: [u8; 32],
    pub(crate) encoded_bytes: u32,
    pub(crate) row_count: u32,
    pub(crate) first_routing_leaf_ordinal: u32,
    pub(crate) routing_leaf_count: u16,
}

pub(crate) struct V32RoutingLeafMetadata {
    pub(crate) routing_leaf_ordinal: u32,
    pub(crate) code_parent_ordinal: u32,
    pub(crate) object_ordinal: u32,
    pub(crate) routing_centroid: [half::f16; 96],
    pub(crate) logical_start: u64,
    pub(crate) row_count: u32,
}

pub(crate) struct V32CodeParentMetadata {
    pub(crate) code_parent_ordinal: u32,
    pub(crate) root_ordinal: u32,
    pub(crate) centroid: [half::f16; 96],
    pub(crate) population: u64,
}

pub(crate) struct V32CodeRoutingMetadata {
    pub(crate) objects: Vec<V32CodeObjectIdentity>,
    pub(crate) parents: Vec<V32CodeParentMetadata>,
    pub(crate) routing_leaves: Vec<V32RoutingLeafMetadata>,
    pub(crate) root_parent_offsets: Vec<u32>,
    pub(crate) root_leaf_offsets: Vec<u32>,
    pub(crate) projected_decoded_bytes: u64,
}

pub(crate) struct V32DirectoryShardShape {
    pub(crate) family: V32DirectoryFamily,
    pub(crate) shard_ordinal: u16,
    pub(crate) first_row_ordinal: u32,
    pub(crate) row_count: u32,
}

pub(crate) enum V32DirectoryFamily {
    Parents,
    RoutingLeaves,
    CodeObjects,
}

pub(crate) enum V32DirectoryRows<'a> {
    Parents(&'a [V32CodeParentMetadata]),
    RoutingLeaves(&'a [V32RoutingLeafMetadata]),
    CodeObjects(&'a [V32CodeObjectIdentity]),
}

pub(crate) fn encode_v32_code_directory_shard(
    shape: V32DirectoryShardShape,
    rows: V32DirectoryRows<'_>,
) -> Result<Vec<u8>>;

pub(crate) fn decode_v32_code_directory_shard(
    bytes: &[u8],
    expected_sha256: &str,
    expected_bytes: usize,
    expected: V32DirectoryShardShape,
) -> Result<V32CodeDirectoryShard>;
```

- [ ] **Step 1: Write the directory authority tests**

Add `v32_selective_directory_roundtrips_compact_authority` with three separate
table families, two roots, four parents, six routing leaves, three objects,
parent 1 spanning objects 0 and 1, and exhaustive logical ranges
`0..4,4..7,7..12,12..14,14..19,19..24`. Assert dense object/leaf ordinals,
root offsets, leaf-to-object mapping, parent lookup, shard-boundary continuation,
and the checked compact byte projection.

Add table-driven `v32_selective_directory_rejects_schema_identity_and_global_drift` mutations for every field name/type/nullability, extra metadata/batch/dictionary/compression, corrupt SHA/length, family/role drift, shard order/count/64-MiB cap, root order/range, object order/identity/row sum, parent population, leaf duplicate/gap, range overlap/gap/overflow, nonfinite/zero centroid, cross-table parent/object binding, each local/global count cap, and encoded or compact projection above 1,280 MiB.

- [ ] **Step 2: Run the focused RED**

Run:

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_directory_ -- --nocapture
```

Expected: compilation fails only because the directory types and codec are missing.

- [ ] **Step 3: Implement the exact Arrow directory codec**

Use one uncompressed Arrow IPC batch per bounded shard and the exact normalized
parent, routing-leaf, or code-object schema from the spec.
Authenticate bytes first. Reuse the raw FlatBuffer envelope strategy from
`v32_code_objects.rs` to reject dangerous metadata and buffer extents before
Arrow materialization. Compact each authenticated shard into contiguous columns
and drop its Arrow source before opening the next. Derive root parent/leaf spans
and validate the explicit leaf-to-object column against object leaf spans.

- [ ] **Step 4: Run focused and malformed-input GREEN gates**

Run the exact selector from Step 2, then:

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_directory_malformed_ -- --nocapture
```

Require all selected tests to execute and no panic on malformed bytes.

- [ ] **Step 5: Format, diff-check, review, commit, and push**

```bash
rtk proxy cargo fmt --all
rtk proxy cargo fmt --all -- --check
rtk proxy git diff --check
git add -f crates/borsuk/src/v32_selective_codes.rs crates/borsuk/src/lib.rs
git commit -m "feat: authenticate selective code directory"
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 2: Strict manifest and code-free metadata loader

**Files:**
- Modify: `crates/borsuk/src/v32_selective_codes.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit module in `crates/borsuk/src/v32_selective_codes.rs`

**Interfaces:**
- Consumes: authenticated root Arrow bytes, base/high codebooks, ordered directory Arrow shards, and page-location Parquet bytes.
- Produces:

```rust
pub(crate) struct V32SelectiveArtifactIdentity {
    pub(crate) role: String,
    pub(crate) key: String,
    pub(crate) sha256: String,
    pub(crate) encoded_bytes: u64,
}

pub(crate) struct V32SelectiveArm {
    pub(crate) root_beam: u32,
    pub(crate) routing_leaf_beam: u32,
    pub(crate) scan_budget: u32,
    pub(crate) candidate_depth: u32,
    pub(crate) selected_pages: u16,
    pub(crate) maximum_code_objects: u16,
    pub(crate) maximum_code_bytes: u32,
    pub(crate) maximum_page_bytes: u32,
    pub(crate) code_wave_width: u16,
}

pub(crate) struct V32SelectiveLimits {
    pub(crate) maximum_directory_encoded_bytes: u64,
    pub(crate) maximum_directory_decoded_bytes: u64,
    pub(crate) metadata_cache_bytes: u64,
    pub(crate) active_query_bytes: u64,
    pub(crate) refresh_overlap_bytes: u64,
    pub(crate) runtime_headroom_bytes: u64,
}

pub(crate) struct V32SelectiveManifest {
    pub(crate) schema: String,
    pub(crate) source_commit: String,
    pub(crate) source_archive_sha256: String,
    pub(crate) dataset: String,
    pub(crate) dimensions: u16,
    pub(crate) metric: String,
    pub(crate) normalization: String,
    pub(crate) source_rows: u64,
    pub(crate) root_count: u32,
    pub(crate) code_parent_count: u32,
    pub(crate) routing_leaf_count: u32,
    pub(crate) code_object_count: u32,
    pub(crate) page_count: u32,
    pub(crate) generation: String,
    pub(crate) standard_prefix: String,
    pub(crate) express_prefix: Option<String>,
    pub(crate) arm: V32SelectiveArm,
    pub(crate) limits: V32SelectiveLimits,
    pub(crate) roots: V32SelectiveArtifactIdentity,
    pub(crate) base_codebook: V32SelectiveArtifactIdentity,
    pub(crate) high_codebook: V32SelectiveArtifactIdentity,
    pub(crate) parent_directory_shards: Vec<V32SelectiveArtifactIdentity>,
    pub(crate) leaf_directory_shards: Vec<V32SelectiveArtifactIdentity>,
    pub(crate) object_directory_shards: Vec<V32SelectiveArtifactIdentity>,
    pub(crate) page_registry: V32SelectiveArtifactIdentity,
}

pub(crate) trait V32SelectiveMetadataSource {
    fn read_authenticated(
        &mut self,
        identity: &V32SelectiveArtifactIdentity,
    ) -> Result<bytes::Bytes>;
}

pub(crate) fn canonical_v32_selective_manifest_bytes(
    manifest: &V32SelectiveManifest,
) -> Result<Vec<u8>>;

pub(crate) fn load_v32_selective_metadata<S: V32SelectiveMetadataSource>(
    manifest_bytes: &[u8],
    expected_manifest_sha256: &str,
    expected_manifest_bytes: usize,
    source: &mut S,
) -> Result<V32SelectiveMetadata>;

pub(crate) struct V32SelectiveMetadata {
    pub(crate) manifest: V32SelectiveManifest,
    pub(crate) roots: Vec<[half::f16; 96]>,
    pub(crate) base_codebook: V30PqCodebook,
    pub(crate) high_codebook: V30PqCodebook,
    pub(crate) routing: V32CodeRoutingMetadata,
    pub(crate) pages: Vec<V32PageLocation>,
    pub(crate) page_logical_starts: Vec<u64>,
    pub(crate) projected_resident_bytes: u64,
}
```

- [ ] **Step 1: Write the manifest and code-free construction tests**

Build one literal coherent manifest and seven small artifact families. Assert canonical
newline JSON roundtrip, exact roles and fixed filenames, Standard/Express prefix
rules, immutable generation and digest-sharded key derivation, arm/budget
constants, full artifact authentication, page prefix sum, and
successful `V32SelectiveMetadata` construction. A compile-time construction
test must not import or create `V30CodePlanes`, fidelity bits, or `V32Router`.

Add mutations for missing/extra/wrong JSON fields and concrete types; source,
dataset, metric, normalization, dimensions, counts, arm, budget, SHA, length,
role, filename, prefix, root hierarchy, codebook, directory, and page-registry
drift. Mutating content plus its local digest must still fail against the
registered manifest identity.

- [ ] **Step 2: Run the focused RED**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_manifest_ -- --nocapture
```

Expected: missing manifest/loader symbols only.

- [ ] **Step 3: Implement canonical manifest validation and loader**

Deserialize with `deny_unknown_fields` into exact concrete types, reserialize
canonically with one LF, and require byte equality. Validate count arithmetic
before allocating. Read, authenticate, decode, compact, and drop each resident
artifact or directory shard before requesting the next; the source must not
retain returned bodies. Cross-bind global object/fragment/range/page totals
and hierarchy/code-parent roots, require 1..1,024 leaf populations and the
frozen 256 selected-leaf/object caps, compute the actual startup, refresh-overlap,
and steady-state byte projections, and drop each source Arrow shard before
opening the next.

- [ ] **Step 4: Run the manifest and directory gates**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_manifest_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_directory_ -- --nocapture
```

- [ ] **Step 5: Format, diff-check, review, commit, and push**

Commit only the manifest/loader slice after scoped strict Clippy is clean.

### Task 3: Selected-range object matching

**Files:**
- Modify: `crates/borsuk/src/v32_code_objects.rs`
- Modify: `crates/borsuk/src/v32_selective_codes.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit modules in the two files above

**Interfaces:**
- Consumes: decoded `V32CodeObject`, one authenticated directory object row, and sorted selected routing-leaf ordinals.
- Produces:

```rust
pub(crate) struct V32SelectedParentCursor<'a> {
    parent: &'a V32ParentCodes,
    selected_range_indices: &'a [usize],
    selected_position: usize,
    row_in_range: u32,
    local_row: usize,
    base_offset: usize,
    high_offset: usize,
    range_base_offsets: Vec<u32>,
    range_high_offsets: Vec<u32>,
}

impl V32ParentCodes {
    pub(crate) fn selected_cursor<'a>(
        &'a self,
        selected_range_indices: &'a [usize],
    ) -> Result<V32SelectedParentCursor<'a>>;
}

pub(crate) fn match_v32_code_object<'a>(
    object: &'a V32CodeObject,
    identity: &'a V32CodeObjectIdentity,
    parents: &'a [V32CodeParentMetadata],
    leaves: &'a [V32RoutingLeafMetadata],
    selected_leaves: &[u32],
) -> Result<Vec<V32SelectedParent<'a>>>;

pub(crate) struct V32SelectedParent<'a> {
    pub(crate) code_parent_ordinal: u32,
    pub(crate) centroid: &'a [half::f16; 96],
    pub(crate) cursor: V32SelectedParentCursor<'a>,
}
```

- [ ] **Step 1: Write selected-cursor and full-object authority tests**

Use a mixed base24/high48 object with two parents and four ranges. Select only
ranges 1 and 3; assert exact logicals, code widths, byte slices, borrowing, and
repeated EOF. Put an artificially winning code in an unselected range and prove
it never reaches the scorer. Mutate object parent/range order and count,
centroid bits, logical start/count, packed bytes, duplicate selected leaf,
unknown selected leaf, and an object that is internally valid but belongs to a
different authenticated directory row.

- [ ] **Step 2: Run the focused RED**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selected_code_object_ -- --nocapture
```

- [ ] **Step 3: Implement range-aware borrowed traversal**

Validate the parent once, require strictly increasing in-bounds selected range
indices, and compute base/high offsets for each range boundary by checked prefix
popcount over the fidelity bitmap. Seek directly to selected ranges without
walking skipped rows or allocating per-row state, and emit only selected rows. Match every decoded parent and range
against the complete authority row before exposing any cursor.

- [ ] **Step 4: Run selected-object, codec, and shared-scorer gates**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selected_code_object_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_code_object_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_borrowed_parent_scorer_ -- --nocapture
```

- [ ] **Step 5: Format, strict scoped Clippy, review, commit, and push**

Promote `v32_code_objects` from `#[cfg(test)]` to a normal private module only
when this production consumer compiles without dead-code suppression.

### Task 4: Async selective routing and code fetch

**Files:**
- Create: `crates/borsuk/src/v32_selective_search.rs`
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit module in `crates/borsuk/src/v32_selective_search.rs`

**Interfaces:**
- Consumes: `V32SelectiveMetadata`, selected-range matcher, shared parent scorer, and code/page identities.
- Produces:

```rust
#[async_trait::async_trait]
pub trait V32SelectiveStore: Send + Sync {
    async fn read_code_wave(
        &self,
        objects: &[V32CodeObjectIdentity],
    ) -> Result<Vec<bytes::Bytes>>;
    async fn read_page_wave(
        &self,
        pages: &[V27PageIdentity],
    ) -> Result<Vec<bytes::Bytes>>;
}

pub struct V32SelectiveIndex<S> {
    metadata: std::sync::Arc<V32SelectiveMetadata>,
    store: S,
    admission: std::sync::Arc<tokio::sync::Semaphore>,
}

pub(crate) struct V32SelectiveCodeWork {
    pub(crate) roots_scored: u32,
    pub(crate) leaves_scored: u32,
    pub(crate) selected_leaves: u32,
    pub(crate) code_objects: u32,
    pub(crate) code_waves: u32,
    pub(crate) code_bytes: u64,
    pub(crate) codes_scored: u64,
    pub(crate) physical_attempts: u32,
    pub(crate) throttles: u32,
    pub(crate) maximum_connections: u16,
    pub(crate) parent_transitions: u32,
    pub(crate) table_builds: u32,
}

pub(crate) struct V32SelectiveCandidateResult {
    pub(crate) candidates: Vec<(u64, f32)>,
    pub(crate) work: V32SelectiveCodeWork,
}

impl<S: V32SelectiveStore> V32SelectiveIndex<S> {
    pub async fn search_candidates(
        &self,
        query: &[f32; 96],
    ) -> Result<V32SelectiveCandidateResult>;
}
```

- [ ] **Step 1: Write routing, admission, wave, and failure tests**

Use two roots, more than 32 rows per parent, mixed code widths, at least 64
pages, and objects containing selected plus unselected leaves. Assert root and
leaf `(distance, ordinal)` ties, extension to candidate depth or exactly 256
leaves, identical resident-oracle truncation at that ceiling, exact scan/object/
byte/range caps before the first store call, object order, wave sizes at each
frozen width 16/32/64/128/256 including a short final wave, reverse completion
with request-order return, and exact candidate
bits versus an independent eager scorer.

Fault each wave at throttling, truncation, substitution, overlength, decode, directory
match, and scoring. After a failure in wave two, assert no wave three and no page
call. Assert one lazy base/high table pair is reused and no unselected row is
scored. Construct the index with no resident code/fidelity/router argument.

- [ ] **Step 2: Run the focused RED**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_code_fetch_ -- --nocapture
```

- [ ] **Step 3: Extract only the shared crate-private kernel surface**

Make `Candidate`, `BoundedCandidates`, `normalized`, deterministic root/leaf
selection, and the parent scorer crate-private without changing arithmetic or
public exports. Split table preparation from prepared-row scoring so consecutive
fragments of one parent reuse the current base/high tables; retain
`score_parent_codes` as the resident prepare-plus-score wrapper. Do not copy the
scorer into the new module.

- [ ] **Step 4: Implement the async code-only path**

Reserve the exact sum of selected encoded object bytes plus decode/candidate/page
headroom before I/O, select roots/leaves through root-local spans with the exact
shared 256-leaf ceiling, derive and
preflight ordered objects, fetch in chunks of the frozen wave width, authenticate and match each complete wave, score
selected ranges, and drop bodies/decoded arrays before the next wave. Return a
candidate result only after all waves succeed. Preserve delivery-order
independence by forbidding score-dependent early termination.

- [ ] **Step 5: Run focused and resident regression gates**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_code_fetch_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_borrowed_parent_scorer_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_root64_ -- --nocapture
```

- [ ] **Step 6: Format, scoped Clippy, review, commit, and push**

Commit the code-fetch path before page I/O so a reviewer can reject its resource
and error semantics independently.

### Task 5: Exact page reduction and end-to-end parity

**Files:**
- Modify: `crates/borsuk/src/v32_selective_search.rs`
- Modify: `crates/borsuk/src/v30_s3_search.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit module in `crates/borsuk/src/v32_selective_search.rs`

**Interfaces:**
- Consumes: successful selective candidate result, compact page prefix table, async page-wave store, and shared exact reranker.
- Produces: `V32SelectiveIndex::search(&[f32;96], usize) -> Future<Output=Result<V32SearchResult>>`.

- [ ] **Step 1: Write complete resident-versus-selective differential tests**

For three parent/object delivery orders and reversed wave completion, compare
every retained candidate logical/score bit, first 16 and first 64 unique page
prefixes, requested 16 identities, and final ten source ordinals/f64 distance
bits. Cover ties, duplicate page candidates, page byte overflow, wrong response
count/order/body, exact page authentication, and fewer than ten unique decoded
rows. Page reads must remain zero after any code-path failure.

- [ ] **Step 2: Run the end-to-end RED**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_search_ -- --nocapture
```

- [ ] **Step 3: Reuse page reducer and reranker without duplicate arithmetic**

Expose the existing exact page mapping/reduction and `exact_rerank_pages` only
crate-wide. Map logical IDs through the compact page prefix table, preflight 16
unique pages and 3,145,728 bytes, await one page wave, then run the same decoder
and exact top-ten merge.

- [ ] **Step 4: Run end-to-end and existing serving gates**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_search_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_s3_search_ -- --nocapture
```

- [ ] **Step 5: Run fmt, strict workspace Clippy, and locked workspace tests once**

```bash
rtk proxy cargo fmt --all
rtk proxy cargo fmt --all -- --check
CARGO_BUILD_JOBS=1 rtk proxy cargo clippy --locked --workspace --all-targets -- -D warnings
CARGO_BUILD_JOBS=1 rtk proxy cargo test --locked --workspace --all-targets
rtk proxy git diff --check
```

- [ ] **Step 6: Review, commit, and push**

Require independent review to confirm no resident-code construction is reachable
from `V32SelectiveIndex` and no compatibility path was introduced.

### Task 6: Deterministic writer and manifest-last publication

**Files:**
- Create: `crates/borsuk/src/v32_selective_build.rs`
- Modify: `crates/borsuk/src/v32_selective_codes.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: unit module in `crates/borsuk/src/v32_selective_build.rs`

**Interfaces:**
- Consumes: the frozen query-independent V32 construction stream after hierarchy training and fidelity selection.
- Produces:

```rust
pub trait V32SelectiveSink {
    fn put_page(&mut self, generation: &str, identity: &V27PageIdentity, body: &[u8]) -> Result<()>;
    fn put_code_object(&mut self, generation: &str, identity: &V32CodeObjectIdentity, body: &[u8]) -> Result<()>;
    fn put_resident_artifact(&mut self, generation: &str, role: &str, body: &[u8]) -> Result<()>;
    fn publish_generation_manifest(&mut self, generation: &str, body: &[u8]) -> Result<()>;
    fn compare_and_swap_current(&mut self, predecessor: Option<&str>, generation: &str) -> Result<()>;
}

pub struct V32SelectiveConstructionBuilder<S> {
    config: V32SelectiveConstructionConfig,
    sink: S,
}

pub struct V32SelectiveConstructionConfig {
    pub generation: String,
    pub expected_predecessor_generation: Option<String>,
    pub source_rows: u64,
    pub maximum_object_rows: u32,
    pub maximum_object_parent_records: u32,
    pub maximum_object_ranges: u32,
    pub maximum_object_encoded_bytes: u32,
    pub maximum_directory_shard_encoded_bytes: u64,
    pub maximum_page_rows: u16,
    pub maximum_page_encoded_bytes: u32,
    pub upload_concurrency: u16,
    pub leaves_per_object: u8,
    pub code_wave_width: u16,
}
```

- [ ] **Step 1: Write deterministic packing and atomic-publication tests**

Stream a skewed fixture with a code parent larger than 8,192 rows and at least
nine microleaves. Require root-local objects in `(root, leaf)` order, whole
microleaves, 1/2/4 leaves-per-object arms, repeated parent centroid equality, every local object cap, exact
directory/page reconciliation, stable bytes across chunk sizes and worker
completion orders, and maximum bounded live rows/bytes. Inject failure at each
page, code object, registry, directory, and manifest step; assert manifest is
never published early and exactly once on success. Build into two generation
prefixes; prove the old pinned reader remains valid through the conditional
pointer swap, a failed build cannot overwrite either generation, and collection
is rejected while a reader lease exists.
Require pages to pack consecutive rows on the global logical axis across
parent/leaf boundaries so the 2.4M page ceiling is arithmetically attainable.

- [ ] **Step 2: Run the writer RED**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_build_ -- --nocapture
```

- [ ] **Step 3: Implement bounded deterministic emission**

Pack the frozen 1/2/4 count of consecutive complete microleaves into the current
root-local object, flushing
earlier when the next leaf crosses a row, parent, range, or byte cap. Flush, authenticate, and
drop the body before continuing. Emit pages and code objects through bounded
queues to digest-sharded keys below one unique generation; collect only compact
identities and metadata. Validate the complete directory/page/PQ graph, write
resident artifacts, publish the canonical generation manifest, then conditionally
swap the current pointer against the expected predecessor. Never overwrite or
delete an older generation in the build path.

- [ ] **Step 4: Run writer, directory, and selective-search gates**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_build_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_directory_ -- --nocapture
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_selective_search_ -- --nocapture
```

- [ ] **Step 5: Run static/full assurance, review, commit, and push**

Record observed maximum live construction bytes and object/page counts; do not
claim incremental-update throughput from this bulk writer.

### Task 7: Fail-fast performance and quality qualification

**Files:**
- Create: `crates/borsuk/examples/v32_selective_serving.rs`
- Create: `scripts/run_v32_selective_spot.py`
- Create: `scripts/test_run_v32_selective_spot.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

**Interfaces:**
- Consumes: one exact selective manifest generation, frozen query/truth cohort, and a selected Standard or Express serving tier.
- Produces: canonical claim-ineligible local ABBA receipt, then authenticated Spot campaign receipt.

- [ ] **Step 1: Write CLI, receipt, simulator, and controller RED tests**

Require exact manifest/query/truth identities; no corpus or directory-discovery
flag; tier chosen once; 128 fixed queries; ABBA resident/selective order;
candidate/page/result parity; code/page logical and physical attempts; encoded
bytes; cache hits; fetch/decode/score/rerank/outer elapsed; process CPU; RSS;
concurrency; retries; throttles; maximum connections; object/page key-shard
distribution; parent transitions; table builds; and failure reason. Controller tests require `causality`,
Spot, terminal-marker repetition, interruption discard/restart of that cell,
and immediate termination.

- [ ] **Step 2: Run the narrow REDs and implement only the harness**

```bash
CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --example v32_selective_serving v32_selective_ -- --nocapture
rtk proxy python3 -m unittest scripts.test_run_v32_selective_spot
```

- [ ] **Step 3: Run the same-host ABBA fail-fast gate**

Require exact parity before interpreting performance. Report, without tuning,
median/p95/p99 and raw samples for code GET count/bytes, page GET count/bytes,
code/page waves, retry-inclusive attempts, throttles, cache hits, compute,
end-to-end, CPU, and RSS. A failure returns to the narrow failing layer;
do not rerun a full suite after every repair.

- [ ] **Step 4: Preregister and run one 1M Spot experiment**

Use `AWS_PROFILE=causality`, fresh capacity in a preregistered region/AZ and
instance type, one original controller, immutable S3 terminal receipt, and
health/terminal-only monitoring. Stop immediately after terminal. Do not launch
100M when recall parity, 3 GiB RSS, code/page request and byte bounds, or the
preregistered read/write throughput gates fail.

- [ ] **Step 5: Record evidence and decide the next architecture slice**

If selective serving passes, design immutable delta segments and compaction for
incremental write throughput. Separately evaluate centroid versus enclosing
shape/micro-prototype routing on a burned development cohort, then freeze one
query-independent representation before any sealed holdout. Neither experiment
may retune on repeated holdout queries.

### Task 8: Unbiased alternative-cell geometry falsifier

**Files:**
- Create: `crates/borsuk/examples/v32_cell_shape_diagnostic.rs`
- Create: `scripts/run_v32_cell_shape_spot.py`
- Create: `scripts/test_run_v32_cell_shape_spot.py`
- Modify: `docs/research/publication-v3-attempt-ledger.md`

- [ ] **Step 1: Write authority, arithmetic, and leakage RED tests**

Authenticate a fresh source-distribution cohort of about 2,000 queries and exact
GT@10, with one burned development split and one sealed single-use split. Reject
any query appearing in an earlier V32 cohort. Freeze identical root beam,
selected-row budget, candidate depth, sixteen-page reducer, and byte/RSS caps.
Cover these query-independent leaf scores: centroid squared-L2; sphere boundary
distance; quantized diagonal covariance; a preregistered low-rank covariance;
and 1/2/4 micro-prototypes in f16 and int8. A triangle is exactly the
three-prototype case and receives no special geometric privilege in 96D.

- [ ] **Step 2: Implement the oracle-gap and miss-attribution diagnostic first**

Before implementing a production shape scorer, compute baseline truth
containment and offline oracle leaf containment under the identical row/page
budgets. Attribute every missed GT row to anisotropy, multimodality, or a
downstream page-reducer miss using corpus members and exact truth only in the
diagnostic. If the oracle-minus-centroid containment confidence interval is not
material, stop the shape program without opening an arm ladder.

- [ ] **Step 3: Project every arm at 100M and 1B before scoring holdout**

Use checked actual leaf counts and the four-million-leaf envelope. Include
centroid/shape bytes, quantization scales, offsets, allocator padding, scorer
scratch, metadata, cache, active queries, and refresh overlap. Reject any arm
whose complete process projection reaches 3 GiB. In particular, eight f16
prototypes are forbidden; two f16 or four int8 prototypes are candidates only
when the complete checked projection, not the prototype array alone, fits.

- [ ] **Step 4: Burn development once and seal one arm**

Fit shapes from corpus rows only. The development split may select at most one
representation and its fixed hyperparameters. The sealed split runs exactly
once. Adoption requires a positive 95% confidence interval for aggregate and
minimum truth containment, no regression in Recall@10, the exact same selected
row and page limits, code bytes within the selective-serving gate, RSS below
3 GiB, and CPU/throughput within the preregistered production envelope. A large
sphere cannot pass merely by selecting diffuse cells or scanning more rows.

- [ ] **Step 5: Record a rejection or create a new strict format**

Persist all arms and the causal miss breakdown. If one arm passes, introduce a
new format marker and repeat selective-serving parity and Spot qualification;
never mutate the already-qualified centroid format or claim that a diagnostic
oracle is a production result.

# Resident Global PQ Production Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task on `main`. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the recall-qualified GloVe path's 80 cell-object reads (~53.5 MB before rerank) with one compact resident TurboQuant product-code index and exact sidecar reranking, then add compact graph navigation only when it improves the measured CPU/recall frontier.

**Architecture:** Compaction fits a deterministic product quantizer after TurboQuant's structured rotation, writes one content-addressed global candidate object, and references it from the manifest. Opening an index loads and budget-checks this object as search metadata. Unfiltered `pq-scan` ranks global product codes in memory, groups exact candidates by segment, and reads only their vector-sidecar rows. Filtered, text, named-vector, WAL-tail, and legacy-index searches retain the existing cell path until their correctness metadata is represented in the compact object. A compact connected graph is an optional accelerator over the same codes, not a separate public method.

**Tech Stack:** Rust, Arrow/Parquet manifest compatibility, existing object-store/cache abstraction, TurboQuant SRHT rotation, deterministic product quantization, exact vector sidecars, criterion/AWS benchmark harnesses.

---

## Production gates

- GloVe-100 angular recall@10 must meet or exceed the frozen S3-comparable target of 0.95.
- Uncached p95 must be below 150 ms on the frozen Frankfurt c7g.8xlarge setup; the stretch gate is 100 ms.
- Disk-cached p95 must not regress from the frozen pq-scan result.
- Resident bytes must be measured from owned capacities and remain below the configured RAM budget.
- Search must report physical coalesced bytes and physical range GETs, not requested-row bytes or logical `get_ranges` calls.
- Results and non-timing resource columns must be deterministic across two builds with the same seed.
- The existing cell-based `pq-scan` remains the compatibility fallback for old manifests and unsupported query features.

### Task 1: Correct physical range-I/O accounting

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`

- [x] **Step 1: Add a failing coalesced-range accounting test**

The test requests two four-byte rows 512 KiB apart and requires one GET plus `512 KiB + 4` transferred bytes.

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p borsuk --lib storage::tests::range_reads_report_physical_coalesced_bytes_and_gets -- --nocapture
```

Observed before the fix: `left: 8`, `right: 524292`.

- [x] **Step 3: Count merged physical requests and spans**

Call `object_store::coalesce_ranges` through the counted store, increment transferred bytes inside the physical fetch closure, and keep the cached range bundle restricted to requested chunks.

- [x] **Step 4: Verify GREEN and storage regression suite**

Observed: focused test passed; storage suite `13 passed; 0 failed`.

### Task 2: Persisted resident product-code object

**Files:**
- Create: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/rotated_product_quantizer.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [x] **Step 1: Write failing round-trip and byte-accounting tests**

Define a private `PersistedGlobalPq` containing the quantizer, flat codes, compact `(segment,row)` locations, flattened record-id bytes/offsets, optional generation values, and segment summaries. Tests must require deterministic round-trip, one byte per configured subspace, correct id/location recovery, corrupt-object rejection, and exact resident-byte accounting.

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p borsuk --lib global_pq_sidecar::tests -- --nocapture
```

Expected: compile failure because `PersistedGlobalPq` does not exist.

- [ ] **Step 3: Implement a versioned binary payload inside one Parquet binary column**

The payload header stores magic, version, metric geometry, dimensions, seed, subspace count, centroid count, counts and checked section lengths. All integer encoding is little-endian. Decode validates every offset, node count, segment index, row index, code width, finite codebook value, and checksum-covered object length without panicking.

- [x] **Step 4: Add deterministic global ADC top-k**

Expose a crate-private scan that prepares one ADC table, scores the flat code array without allocation per vector, and retains a bounded deterministic heap ordered by `(distance,node_id)`.

- [x] **Step 5: Verify focused tests and quantizer regressions**

Run:

```bash
cargo test -p borsuk --lib global_pq_sidecar::tests -- --nocapture
cargo test -p borsuk --lib rotated_product_quantizer::tests -- --nocapture
```

### Task 3: Manifest lifecycle and RAM-budget enforcement

**Files:**
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/record.rs`
- Test: `crates/borsuk/tests/local_index.rs`
- Test: `crates/borsuk/tests/in_memory_preload.rs`

- [ ] **Step 1: Write failing lifecycle tests**

Tests require compaction to publish a content-addressed global-PQ reference, reload to preserve search results, an old manifest without the reference to open normally, segment-changing compaction to replace the reference, GC to retain the live object and reclaim an obsolete one, and open to fail with `RamBudgetExceeded` when the compact resident object exceeds the configured budget.

- [ ] **Step 2: Verify RED**

Run the named lifecycle tests and confirm failure is due to the missing manifest field/object.

- [ ] **Step 3: Add `GlobalPqRef` with backward-compatible manifest encoding**

The reference contains path, checksum, vector count, code width and decoded resident bytes. Missing fields decode as `None`. Include the live path in GC reachability and the decoded bytes in `Manifest::resident_bytes_estimate`/runtime enforcement.

- [ ] **Step 4: Build the object from compaction-owned vectors**

Fit on a deterministic bounded sample. Use normalized vectors for cosine/angular candidate geometry and raw vectors for Euclidean/inner-product geometry. Choose code width by padded dimensions: `clamp(ceil(padded_dimensions / 8), 16, 64)`, never exceeding padded dimensions. Preserve final segment order and row order in locations.

- [ ] **Step 5: Load the object during open**

The load is startup/search-metadata I/O, not first-query I/O. Validate checksum and resident budget before exposing the handle. A legacy index uses the current cell path; a corrupt referenced object is an explicit storage error rather than silent recall degradation.

- [ ] **Step 6: Verify lifecycle and full library tests**

Run focused integration tests followed by `cargo test -p borsuk --lib`.

### Task 4: Global shortlist and grouped exact rerank

**Files:**
- Modify: `crates/borsuk/src/index.rs`
- Modify: `crates/borsuk/src/record.rs`
- Test: `crates/borsuk/tests/local_index.rs`
- Test: `crates/borsuk/tests/performance_smoke.rs`

- [ ] **Step 1: Write failing search-equivalence and I/O-shape tests**

For metadata-free, unfiltered primary-vector `pq-scan`, require exact-reranked ordering to match a reference shortlist, zero segment-Parquet payload reads, and sidecar reads only for distinct segments represented in the global shortlist. Also require legacy manifests, filters, non-empty metadata, named vectors, text/hybrid queries, guaranteed recall, and live WAL tails to use the existing path.

- [ ] **Step 2: Verify RED**

Run the named tests and observe cell payload reads in the current path.

- [ ] **Step 3: Define global candidate-budget semantics**

Add an optional global rerank budget to `SearchOptions`. Keep `max_candidates_per_segment` unchanged for compatibility. The production benchmark must sweep the global budget independently; it must never multiply by `nprobe`.

- [ ] **Step 4: Implement grouped rerank**

Rank compact codes, group `(segment,row)` candidates by segment, fetch/decode each sidecar group concurrently under the existing global admission cap, exact-score vectors, apply suppression, and form hits from compact id/generation data. Query-local heaps/maps remain bounded by the global candidate budget.

- [ ] **Step 5: Verify correctness, concurrency and full tests**

Run focused search tests, the 16-caller fairness tests, and the full workspace test suite.

### Task 5: Optional compact graph acceleration

**Files:**
- Modify: `crates/borsuk/src/global_graph.rs`
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/index.rs`

- [ ] **Step 1: Add failing connectivity and recall tests**

Require every node to be reachable from the entry point, immutable adjacency to be shared by callers, query-local traversal state, deterministic candidates, and exact rerank recall equal to or better than the flat compact scan at its selected candidate budget.

- [ ] **Step 2: Replace validation HNSW persistence with connected alpha-pruned Vamana**

Build bidirectional candidates, apply robust alpha pruning, repair disconnected components deterministically, flatten adjacency to CSR, and retain no source vectors.

- [ ] **Step 3: Promote graph traversal only on evidence**

Select graph traversal as the internal `pq-scan` accelerator only if it lowers recall-matched memory-preloaded p95 and uncached p95 on every qualifying real dataset. Otherwise keep flat resident code scan as the production default and label graph experimental.

### Task 6: AWS evidence and documentation

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Modify: `scripts/bench_graph_promotion.py`
- Modify: `docs/research/*.md`
- Modify: `docs/web/research.html`
- Modify: `docs/web/assets/benchmarks/**`
- Modify: `docs/web/assets/charts/**`

- [ ] **Step 1: Add separate startup, uncached and disk-cached measurements**

Startup includes the compact metadata load. Uncached begins after open and clears the local disk cache without unloading resident metadata. Disk-cached requires zero physical network GETs.

- [ ] **Step 2: Record complete resource/I/O series**

Every run records p50/p95/p99/max, throughput, recall, resident index bytes, process peak RSS, CPU, disk usage, disk-cache size, physical GETs, physical bytes, and S3 request/transfer cost separately from client compute.

- [ ] **Step 3: Run frozen real and synthetic matrices**

Run Fashion-MNIST, GloVe-100, GIST-960, SIFT, Deep-Image-96, synthetic clustered/uniform/adversarial data, three production repetitions, 1/2/4/8/16 callers, and uncapped research ceiling runs. Compare at recall-matched points only.

- [ ] **Step 4: Publish honest method names and curves**

Public mode remains `pq-scan`; TurboQuant is the default quantizer/configuration. Charts explicitly distinguish cell-based TurboQuant pq-scan, resident-global TurboQuant PQ scan, and experimental Vamana-PQ navigation. Remove the obsolete 28 MB/query and 2 s GloVe claims only after frozen replacement artifacts validate.

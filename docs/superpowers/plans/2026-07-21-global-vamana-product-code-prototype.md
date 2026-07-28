# Global Graph Product-Code Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the smallest reusable global-graph core that can prove whether compact rotated product codes preserve enough recall to justify production persistence and S3 integration.

**Architecture:** Keep the production `pq-scan` path unchanged. Train a deterministic product quantizer on TurboQuant's structured rotation, build one global navigable graph over the compacted vectors, discard the builder's full vectors, navigate on resident product codes and compact adjacency, and exact-rerank the returned ids. This gate intentionally reuses the existing deterministic global HNSW builder to isolate product-code distortion; it does not call that builder Vamana. A true alpha-pruned Vamana builder belongs in the production follow-up only if compact-code navigation passes. The first milestone is internal and opt-in: it produces recall/latency/resident-byte curves on synthetic and GIST data but does not alter manifests, storage format, public configuration, or default search.

**Tech Stack:** Rust, the existing TurboQuant SRHT implementation, the existing deterministic `CentroidHnsw` builder, the shared SIMD squared-distance kernel, standard-library scoped threads, and the existing AWS benchmark/resource wrappers.

---

## Scope boundary

This plan implements the validation gate, not the whole production engine. It deliberately excludes manifest references, graph-object serialization, GC, WAL/L0 merge, language bindings, and production query dispatch. Those become a second plan only if the compact-code graph meets all of these gates:

- recall@10 reaches the dataset target after exact reranking;
- navigation latency is below recall-matched `pq-scan` in the memory-preloaded profile;
- resident graph bytes are reported exactly and remain within the tested budget;
- exact-vector reads are at least 1.5x lower than recall-matched IVF/PQ scan on GIST;
- results are deterministic across two builds with the same seed.

## File map

- Create `crates/borsuk/src/rotated_product_quantizer.rs`: deterministic rotated product-code training, encoding, ADC query tables, and byte accounting.
- Create `crates/borsuk/src/global_graph.rs`: compact resident adjacency, code ownership, deterministic beam traversal, exact-rerank candidate output, and research harness.
- Modify `crates/borsuk/src/turboquant.rs`: derive equality for the already crate-private structured rotation so deterministic fitted artifacts can be compared directly; do not widen visibility.
- Modify `crates/borsuk/src/centroid_hnsw.rs`: export the built entry point and adjacency towers without retaining centroid vectors.
- Modify `crates/borsuk/src/lib.rs`: register both internal modules; do not export a public API.
- Modify `docs/superpowers/specs/2026-07-18-global-vamana-graph-design.md`: replace the product-code caveat with measured outcomes after the experiments complete.
- Create `docs/web/assets/benchmarks/aws-global-graph-product-code-2026-07-21.csv`: consolidated publication data after AWS execution.
- Create `docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code/`: commands, stdout, resource timelines, and source hash.

---

### Task 1: Deterministic rotated product codes

**Files:**
- Create: `crates/borsuk/src/rotated_product_quantizer.rs`
- Modify: `crates/borsuk/src/turboquant.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [ ] **Step 1: Write failing tests for compact width, deterministic fitting, and ADC scoring**

Add tests that define the intended internal API before the type exists:

```rust
#[test]
fn product_code_uses_one_byte_per_subspace() {
    let fit = fixture_vectors(64, 16);
    let pq = RotatedProductQuantizer::fit(ProductQuantizerConfig {
        seed: 7,
        dimensions: 16,
        subspaces: 4,
        centroids: 8,
        sample_limit: 64,
        iterations: 4,
    }, &fit).unwrap();
    assert_eq!(pq.encode(&fit[0]).len(), 4);
    assert_eq!(pq.code_bytes_per_vector(), 4);
}

#[test]
fn product_code_fit_is_deterministic() {
    let fit = fixture_vectors(64, 16);
    let config = test_config();
    let first = RotatedProductQuantizer::fit(config, &fit).unwrap();
    let second = RotatedProductQuantizer::fit(config, &fit).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.encode(&fit[17]), second.encode(&fit[17]));
}

#[test]
fn adc_ranks_the_matching_cluster_first() {
    let fit = separated_cluster_fixture();
    let pq = RotatedProductQuantizer::fit(test_config(), &fit).unwrap();
    let prepared = pq.prepare_query(&fit[0]);
    let near = prepared.distance(&pq.encode(&fit[1]));
    let far = prepared.distance(&pq.encode(fit.last().unwrap()));
    assert!(near < far, "near={near}, far={far}");
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib rotated_product_quantizer::tests -- --nocapture
```

Expected: compilation fails because `RotatedProductQuantizer`, `ProductQuantizerConfig`, and the module do not exist.

- [ ] **Step 3: Reuse the existing crate-private rotation without widening the public API**

`StructuredRotation`, `StructuredRotation::new`, `StructuredRotation::rotate`, and `StructuredRotation::padded_len` are already `pub(crate)`. Add `PartialEq` to its derives so two deterministic fitted artifacts can be compared. Do not expose scalar-code internals or add a public re-export.

- [ ] **Step 4: Implement the minimal product quantizer**

Implement these crate-private types:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProductQuantizerConfig {
    pub(crate) seed: u64,
    pub(crate) dimensions: usize,
    pub(crate) subspaces: usize,
    pub(crate) centroids: usize,
    pub(crate) sample_limit: usize,
    pub(crate) iterations: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RotatedProductQuantizer {
    seed: u64,
    dimensions: usize,
    padded_dimensions: usize,
    subspaces: usize,
    centroids: usize,
    rotation: StructuredRotation,
    subspace_offsets: Vec<usize>,
    codebooks: Vec<Vec<f32>>,
}

pub(crate) struct PreparedAdc {
    subspaces: usize,
    centroids: usize,
    tables: Vec<f32>,
}
```

Validation must reject zero dimensions, empty training data, zero subspaces, more subspaces than padded dimensions, centroid counts outside `1..=256`, zero sample limits, zero iterations, and wrong-width vectors. Training must use a seeded deterministic sample, deterministic centroid initialization, Lloyd assignment with the shared SIMD distance kernel, and deterministic empty-cluster reseeding. `encode` returns exactly `subspaces` bytes. `prepare_query` rotates once and builds `subspaces * centroids` distances. `PreparedAdc::distance` performs only table lookups and additions.

- [ ] **Step 5: Run focused and TurboQuant regression tests and verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib rotated_product_quantizer::tests -- --nocapture
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib turboquant::tests -- --nocapture
```

Expected: every selected test passes with no warnings.

- [ ] **Step 6: Commit the isolated quantizer slice**

```bash
git add crates/borsuk/src/rotated_product_quantizer.rs crates/borsuk/src/turboquant.rs crates/borsuk/src/lib.rs
git commit -m "research: add compact rotated product quantizer"
```

---

### Task 2: Compact resident global graph

**Files:**
- Create: `crates/borsuk/src/global_graph.rs`
- Modify: `crates/borsuk/src/centroid_hnsw.rs`
- Modify: `crates/borsuk/src/lib.rs`

- [ ] **Step 1: Write failing tests for compact ownership and deterministic traversal**

```rust
#[test]
fn built_graph_retains_codes_and_edges_but_not_source_vectors() {
    let vectors = clustered_fixture(8, 32, 256);
    let graph = ResidentGlobalGraph::build(test_graph_config(), &vectors).unwrap();
    assert_eq!(graph.node_count(), vectors.len());
    assert_eq!(graph.code_bytes(), vectors.len() * test_graph_config().pq.subspaces);
    assert!(graph.resident_bytes() < vectors.len() * 256 * size_of::<f32>());
}

#[test]
fn beam_candidates_are_deterministic_and_rerankable() {
    let vectors = clustered_fixture(8, 32, 16);
    let graph = ResidentGlobalGraph::build(test_graph_config(), &vectors).unwrap();
    let first = graph.candidates(&vectors[7], 10, 64).unwrap();
    let second = graph.candidates(&vectors[7], 10, 64).unwrap();
    assert_eq!(first, second);
    assert!(first.contains(&7));
}
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib global_graph::tests -- --nocapture
```

Expected: compilation fails because `ResidentGlobalGraph` and `GlobalGraphConfig` do not exist.

- [ ] **Step 3: Add a one-way compact export to the existing graph builder**

Add a crate-private test-harness method which consumes `CentroidHnsw`, extracts the entry point and adjacency towers, and drops its full vectors:

```rust
pub(crate) fn into_adjacency(self) -> (u32, Vec<Vec<Vec<u32>>>) {
    (self.entry, self.neighbours)
}
```

Add a unit test asserting the exported neighbor ids are in range and the original vectors are not part of the returned type. The first red test demonstrated why the sparse upper layers remain in this validation artifact: the layer-zero graph reached only 32/256 separated-cluster nodes even though the query node's product-code rank was zero. ADC descent through the compact upper layers restored the candidate without retaining full vectors. The production follow-up still replaces this HNSW-specific structure with a connected alpha-pruned Vamana graph.

- [ ] **Step 4: Implement compact CSR adjacency and code-backed beam search**

Use a flat layout rather than `Vec<Vec<u32>>` in the retained object:

```rust
#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalGraph {
    entry: u32,
    node_layer_offsets: Vec<u64>,
    adjacency_offsets: Vec<u64>,
    neighbours: Vec<u32>,
    codes: Vec<u8>,
    quantizer: RotatedProductQuantizer,
}
```

`build` may temporarily construct `CentroidHnsw` from full vectors, but the returned object must contain no `Vec<f32>` per node. This is an HNSW-backed validation graph, not yet the promised Vamana builder. Flatten all exported layers into two-level CSR, encode all vectors into a single `nodes * subspaces` byte array, descend the sparse upper layers using ADC, and run the nearest-first beam on layer zero. Verify every edge is in range, score each discovered layer-zero node once, keep deterministic distance/node-id ordering, and return candidate ids only. Mutable visited/frontier/result state remains query-local, so the resident object is safe to share through `Arc`.

- [ ] **Step 5: Run focused and centroid-HNSW regressions and verify GREEN**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib global_graph::tests -- --nocapture
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib centroid_hnsw::tests -- --nocapture
```

Expected: every non-ignored selected test passes.

- [ ] **Step 6: Commit the graph core**

```bash
git add crates/borsuk/src/global_graph.rs crates/borsuk/src/centroid_hnsw.rs crates/borsuk/src/lib.rs
git commit -m "research: add resident global graph core"
```

---

### Task 3: Recall, latency, and memory validation harness

**Files:**
- Modify: `crates/borsuk/src/global_graph.rs`
- Create: `docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code/README.md`

- [ ] **Step 1: Write a non-ignored exact-rerank correctness test**

```rust
#[test]
fn exact_rerank_restores_distance_order_for_returned_candidates() {
    let vectors = clustered_fixture(8, 32, 16);
    let graph = ResidentGlobalGraph::build(test_graph_config(), &vectors).unwrap();
    let candidates = graph.candidates(&vectors[19], 40, 64).unwrap();
    let got = exact_rerank(&vectors[19], &vectors, &candidates, 10);
    assert_eq!(got[0], 19);
    assert!(got.windows(2).all(|pair| pair[0].1 <= pair[1].1));
}
```

- [ ] **Step 2: Run it and verify RED for the missing rerank helper**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib global_graph::tests::exact_rerank_restores_distance_order_for_returned_candidates -- --nocapture
```

Expected: compilation fails because `exact_rerank` does not exist.

- [ ] **Step 3: Add an ignored, machine-readable curve experiment**

The experiment must accept `GIST_DIR`, `GIST_LIMIT`, `GLOBAL_GRAPH_PQ_M`, `GLOBAL_GRAPH_R`, and `GLOBAL_GRAPH_SAMPLE_LIMIT`. For synthetic and GIST, emit CSV rows with:

```text
dataset,n,dimensions,pq_subspaces,graph_degree,ef,rerank_candidates,recall_at_10,p50_ms,p95_ms,code_bytes_per_vector,adjacency_bytes_per_vector,total_resident_bytes,total_resident_bytes_per_vector,rerank_sectors,rerank_fraction,source_sha
```

Sweep `M={16,32,48,64}`, `R={16,24,32,48}`, `ef={32,64,128,256}`, and rerank widths `{20,40,80}` where valid. Build time and build peak RSS must be captured separately from query metrics.

- [ ] **Step 4: Run the synthetic experiment twice**

Run:

```bash
CARGO_INCREMENTAL=0 cargo test -p borsuk --release --lib global_graph::tests::global_graph_product_code_curve -- --ignored --nocapture
CARGO_INCREMENTAL=0 cargo test -p borsuk --release --lib global_graph::tests::global_graph_product_code_curve -- --ignored --nocapture
```

Expected: both runs produce identical recall, resident-byte, and sector-count columns. Timing may vary and must remain separate.

- [ ] **Step 5: Run the library regression suite**

Run:

```bash
cargo fmt --all -- --check
CARGO_INCREMENTAL=0 cargo test -p borsuk --lib
cargo clippy -p borsuk --all-targets -- -D warnings
git diff --check
```

Expected: formatting, 200+ library tests, clippy, and whitespace checks pass.

- [ ] **Step 6: Commit the validated research harness**

```bash
git add crates/borsuk/src/global_graph.rs docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code/README.md
git commit -m "research: measure global graph product-code curves"
```

---

### Task 4: AWS GIST validation and decision

**Files:**
- Create: `docs/web/assets/benchmarks/aws-global-graph-product-code-2026-07-21.csv`
- Create: `docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code/commands.json`
- Create: `docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code/resources.csv`
- Modify: `docs/superpowers/specs/2026-07-18-global-vamana-graph-design.md`
- Modify: `docs/research/methods.md`

- [ ] **Step 1: Freeze and verify the exact source archive**

Archive `Cargo.toml`, `Cargo.lock`, `crates/`, and the benchmark wrapper; compute SHA-256 locally and remotely. Record the git revision, dirty diff hash, compiler version, instance id/type, architecture, region, dataset checksum, command, and environment in `commands.json`.

- [ ] **Step 2: Wait for the frozen six-dataset campaign to release the AWS host**

Do not overlap this CPU/RSS-intensive graph build with the active publication campaign. Confirm no `production_bench`, index build, or benchmark resource wrapper remains before starting.

- [ ] **Step 3: Execute GIST curves with resource telemetry**

Run the ignored experiment against the real GIST-960 corpus using the same Frankfurt `c7g.8xlarge`. Capture build and query CPU, RSS, VMS, process read/write bytes, cache-disk size, and wall time. Repeat every selected query configuration three times after one untimed initialization.

- [ ] **Step 4: Consolidate only recall-matched rows**

For each `M/R` pair, select the lowest-latency `ef/rerank` point meeting the frozen GIST recall target. Compare it with the frozen `pq-scan` row on p50/p95/p99, rerank GETs/bytes, CPU, peak RSS, and estimated S3 request cost. Keep failed configurations in raw data.

- [ ] **Step 5: Apply the go/no-go gate**

Proceed to a production persistence/search plan only if one configuration meets every scope gate. Otherwise document which axis failed—recall, latency, memory, or reads—and keep `pq-scan` as the sole production default.

- [ ] **Step 6: Update research documentation without promoting the engine**

Replace modeled product-code memory/recall language with measured results. Label the engine `experimental global graph`; do not change README or API defaults. Distinguish the segment-local graph baseline from the new global graph in every table.

- [ ] **Step 7: Commit the evidence and decision**

```bash
git add docs/web/assets/benchmarks/aws-global-graph-product-code-2026-07-21.csv \
  docs/web/assets/benchmarks/raw/2026-07-21/global-graph-product-code \
  docs/superpowers/specs/2026-07-18-global-vamana-graph-design.md \
  docs/research/methods.md
git commit -m "research: validate compact global graph on GIST"
```

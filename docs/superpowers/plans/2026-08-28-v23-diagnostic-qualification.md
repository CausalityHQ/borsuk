# V23 Diagnostic Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Falsify or qualify the V23 quantized-posting-page architecture with authenticated D1 code-fidelity, D2 full-corpus page simulation, and D3 real-S3 one-wave evidence before any persistent production format is built.

**Architecture:** Extend the existing read-only V22 corpus scan with a private V23 diagnostic module that evaluates the production SIMD quantizers, deterministically constructs capped replicated posting pages, and emits immutable page bytes. Route the diagnostic through the existing Publication V3 worker/controller authority, but give it its own claim-ineligible namespace and strict canonical validators. Each paid stage is conditional on the preceding stage's terminal receipt and does not change the authoritative index.

**Tech Stack:** Rust 2024, serde, bytes, crc32fast/SHA-256 utilities already in BORSUK, production SIMD quantizers, BORSUK Storage/S3 Standard, Python 3.12 Publication V3 controller and validators, unittest/pytest, AWS EC2 Spot with profile `causality`.

**Spec:** `docs/superpowers/specs/2026-08-28-standard-s3-v23-code-posting-pages-design.md`

## Global Constraints

- The diagnostic is always `claim_eligible:false`; no 32-query result is a product claim.
- D1 must prove oracle recall@10 `>= 0.990`, routed recall@10 `>= 0.975`, p99 CPU `<= 15 ms`, code width `<= 64` bytes, and four-page projection `<= 983,040` bytes.
- D2 must prove every query uses `<= 4` pages and `<= 983,040` bytes, aggregate recall@10 `>= 0.975`, per-query recall@10 `>= 0.8`, storage amplification `<= 2.0x`, projected process RAM `<= 3 GiB`, and p99 CPU `<= 15 ms`.
- D3 must issue one parallel wave of at least 1,000 strict-cold S3 Standard queries per arm, with disk cache zero, positive query-scoped backing I/O, and cold p50/p95/p99 `<= 60/100/150 ms`.
- Posting objects are content addressed, at most `245,760` encoded bytes each, and four pages total at most `983,040` bytes.
- No S3 Express, CDN, local index replica, persistent disk cache, dependent replacement GET, exact-vector fetch, or query-result cache is permitted in measured V23 queries.
- RAM accounting uses actual encoded lengths and vector capacities; query admission reserves the complete page wave before the first GET.
- The diagnostic is read-only with respect to manifest/index authority. Temporary objects live only under the exact diagnostic attempt prefix and terminal workers are terminated immediately.
- D1 failure stops D2; D2 failure stops D3; no gate may be weakened in response to a negative result.
- Existing sparse, text, late-interaction, point lookup, WAL, and lifecycle paths remain unchanged during this qualification slice.

---

## File Structure

- `crates/borsuk/src/v23_diagnostic.rs`: V23 constants, strict data contracts, D1 scoring, deterministic D2 page construction, private posting-page codec, one-wave accounting, and pure validators.
- `crates/borsuk/src/index.rs`: authenticated read-only corpus scan and hidden D1/D2/D3 entrypoints that reuse the resident V20 authority without publication.
- `crates/borsuk/src/lib.rs`: hidden exports for canonical evidence types consumed by the benchmark binary.
- `crates/borsuk/examples/production_bench.rs`: V23 environment contract, atomic artifacts, local diagnostic execution, and S3 wave telemetry.
- `scripts/run_publication_v3_cell.py`: exclusive V23 execution plan and strict artifact canonicalization.
- `scripts/publication_v3_execution.py`: immutable worker upload and terminal receipt digests.
- `scripts/publication_v3_controller.py`: bounded `diagnose-v23` stage selection, prerequisite receipt checks, Spot launch, and termination.
- `scripts/launch_aws_publication_v3.sh`: clean-source paid preflight and the exact V23 launcher surface.
- `scripts/test_run_publication_v3_cell.py`, `scripts/test_publication_v3_execution.py`, `scripts/test_publication_v3_controller.py`, `scripts/test_launch_aws_publication_v3.py`: paid-boundary and artifact mutation tests.
- `docs/research/cold-read-latency-design.md`: terminal D1/D2/D3 evidence ledger only after receipts exist.

### Task 1: Freeze the V23 Diagnostic Contract

**Files:**
- Create: `crates/borsuk/src/v23_diagnostic.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Test: `crates/borsuk/src/v23_diagnostic.rs`

**Interfaces:**
- Consumes: `VectorMetric`, `GlobalScanQuantizerState`, `RecordId`, and the existing BORSUK error type.
- Produces: `V23QuantizerFamily`, `V23D1ArmKey`, `V23D1Arm`, `V23D1Report`, `V23D2Arm`, `V23D2Report`, `V23PageRef`, `V23WaveSample`, and the constants used by every later task.

- [ ] **Step 1: Write contract and mutation tests**

Add unit tests that construct one valid report and mutate each bound: code width 65, page bytes 245,761, wave pages 5, wave bytes 983,041, non-finite distance, duplicate query index, duplicate page ordinal, storage amplification 2,000,001 ppm, and RAM 3 GiB plus one. The valid constructor must use these exact constants:

```rust
pub(crate) const V23_PAGE_MAX_ENCODED_BYTES: u64 = 245_760;
pub(crate) const V23_WAVE_MAX_PAGES: usize = 4;
pub(crate) const V23_WAVE_MAX_BYTES: u64 = 983_040;
pub(crate) const V23_PROCESS_MAX_BYTES: u64 = 3 * 1024 * 1024 * 1024;
pub(crate) const V23_DIAGNOSTIC_QUERIES: usize = 32;
pub(crate) const V23_D3_WAVES: usize = 1_000;
```

Assert `validate_d1_report`, `validate_d2_report`, and `validate_wave_sample` return `InvalidStorage` for every mutation and accept the canonical value.

- [ ] **Step 2: Run the contract tests and verify RED**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::contract -- --nocapture`

Expected: compilation fails because `v23_diagnostic` and its validators do not exist.

- [ ] **Step 3: Implement concrete serde contracts and validators**

Define enums with `#[serde(rename_all = "kebab-case")]` and structs with exact integer evidence (ppm and nanoseconds, never publication floats). The central wave contract is:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct V23WaveSample {
    pub query_index: u32,
    pub page_ordinals: Vec<u32>,
    pub encoded_bytes: u64,
    pub candidate_rows: u64,
    pub backing_gets: u32,
    pub backing_bytes: u64,
    pub cpu_ns: u64,
    pub elapsed_ns: u64,
}

pub(crate) fn validate_wave_sample(sample: &V23WaveSample) -> Result<()> {
    if sample.page_ordinals.is_empty()
        || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
        || sample.page_ordinals.windows(2).any(|pair| pair[0] >= pair[1])
        || sample.encoded_bytes == 0
        || sample.encoded_bytes > V23_WAVE_MAX_BYTES
        || sample.backing_gets as usize != sample.page_ordinals.len()
        || sample.backing_bytes != sample.encoded_bytes
        || sample.cpu_ns == 0
        || sample.elapsed_ns == 0
    {
        return Err(BorsukError::InvalidStorage("V23 wave authority differs".into()));
    }
    Ok(())
}
```

Export only the report/evidence types through `#[doc(hidden)] pub use v23_diagnostic::{...};`; keep constructors, page internals, and validators crate-private.

- [ ] **Step 4: Run formatter and contract tests**

Run: `cargo fmt --all -- --check && cargo test -p borsuk --lib v23_diagnostic::tests::contract -- --nocapture`

Expected: all V23 contract tests pass.

- [ ] **Step 5: Commit the contract slice**

```bash
git add crates/borsuk/src/v23_diagnostic.rs crates/borsuk/src/lib.rs
git commit -m "Define V23 diagnostic authority"
```

### Task 2: Add Production-Quantizer D1 Replay

**Files:**
- Modify: `crates/borsuk/src/global_pq_sidecar.rs`
- Modify: `crates/borsuk/src/v23_diagnostic.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/v23_diagnostic.rs`
- Test: `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: `V22StageLSpillRow`, authenticated V20 codebook/root, `RotatedProductQuantizer`, `FastTurboQuantMseScanQuantizer`, and `FastTurboQuantProdScanQuantizer`.
- Produces: `BorsukIndex::diagnose_v23_d1(&[Vec<f32>], &[Vec<String>], &SearchOptions, &Path) -> Result<V23D1Report>`.

- [ ] **Step 1: Write scalar/SIMD identity and recall RED tests**

Create a deterministic 64-row, 16-dimensional fixture with ten known nearest IDs. For SRHT-PQ widths 8 and 16 and both relevant Fast-TurboQuant families, assert:

```rust
assert_eq!(arm.oracle.ids, scalar.ids);
for (simd, scalar) in arm.oracle.distances.iter().zip(&scalar.distances) {
    assert!((simd - scalar).abs() <= 1.0e-5_f32.max(scalar.abs() * 1.0e-5));
}
assert_eq!(arm.code_width_bytes as usize, arm.encoded_codes.len() / 64);
```

Add an index test proving D1 rejects a mutable tail, uses exactly 32 distinct query ordinals, leaves manifest bytes unchanged, and returns arms in canonical family/width order.

- [ ] **Step 2: Run the D1 tests and verify RED**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::d1 index::tests::v23_d1 -- --nocapture`

Expected: compilation fails on the absent D1 API.

- [ ] **Step 3: Expose one production scoring seam without copying kernels**

Add a crate-private diagnostic wrapper to `GlobalScanQuantizer`:

```rust
pub(crate) fn score_contiguous_codes(
    &self,
    query: &[f32],
    codes: &[u8],
) -> Result<Vec<f32>> {
    let prepared = self.prepare_query(query)?;
    self.distances_contiguous(&prepared, codes)
}
```

Add `fit_v23_diagnostic_quantizer(family, width, dimensions, sample)` in `v23_diagnostic.rs`. SRHT-PQ uses `ProductQuantizerConfig { rotation: ProductRotation::Srht, subspaces: width, centroids: 256, sample_limit: sample.len(), iterations: 8, seed: 23, dimensions }`. Fast-TurboQuant arms are included only when `packed_code_len()` is one of 8, 16, 32, or 64.

- [ ] **Step 4: Implement one authenticated corpus pass**

Factor the V22 cell-card scan so D1 writes `(source_ordinal, canonical_record_id, primary_cell, exact_vector)` to the existing bounded scratch extent and encodes every passing quantizer in batches. For each query, score both its authenticated exact top-2,048 prefix and its complete registered routed pool. Sort by `(distance.total_cmp, raw_id)` and compute integer hit counts before ppm conversion.

The report must bind root checksum, codebook checksum, dataset rows, query ordinals, quantizer state SHA-256, code width, candidate counts, SIMD CPU nanoseconds, scalar/SIMD agreement, per-query IDs, and aggregate recall.

- [ ] **Step 5: Run D1 tests and unchanged V22 coverage**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::d1 index::tests::v23_d1 index::tests::v22_stage_l -- --nocapture`

Expected: all selected tests pass and V22 artifact authority is unchanged.

- [ ] **Step 6: Commit the D1 slice**

```bash
git add crates/borsuk/src/global_pq_sidecar.rs crates/borsuk/src/v23_diagnostic.rs crates/borsuk/src/index.rs
git commit -m "Add V23 code fidelity replay"
```

### Task 3: Build the Deterministic D2 Page Simulator

**Files:**
- Modify: `crates/borsuk/src/v23_diagnostic.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/v23_diagnostic.rs`
- Test: `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: one D1-passing quantizer state, authenticated corpus scratch rows, page targets `{512,1024,2048}`, assignment limits `{1,2,3}`, and the existing resident centroid router.
- Produces: `BorsukIndex::diagnose_v23_d2(&V23D1ArmKey, &[Vec<f32>], &[Vec<String>], &Path) -> Result<V23D2Report>` plus deterministic `V23PagePlan` values used by Task 4.

- [ ] **Step 1: Write deterministic balance, closure, and four-page RED tests**

Use a 24-row, three-cluster fixture where boundary rows are known. Assert two independent builds are byte-identical, every row has exactly one primary page, replicas never evict primaries, page ordinals are contiguous, and replica ties resolve by `(distance_ratio.total_cmp, page_ordinal, source_ordinal)`.

Add rejection tests for encoded-size overflow, assignment count zero/four, amplification above 2,000,000 ppm, query-specific/GT-informed assignment, five selected pages, and a query below 800,000 recall ppm.

- [ ] **Step 2: Run D2 tests and verify RED**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::d2 index::tests::v23_d2 -- --nocapture`

Expected: compilation fails on missing page planning types and D2 entrypoint.

- [ ] **Step 3: Implement balanced primary assignment**

Fit page centroids deterministically from the registered sample, assign rows by nearest centroid, then rebalance overflow using the smallest distance penalty. Compare every candidate with this exact closure so no float-ordering dependency is introduced:

```rust
left_penalty
    .total_cmp(&right_penalty)
    .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
    .then_with(|| left.target_page.cmp(&right.target_page))
```

Do not add a new dependency for ordering; implement the comparison with `f32::total_cmp`. Compute the primary row capacity from `V23_PAGE_MAX_ENCODED_BYTES`, actual ID bytes, fixed offsets, header bytes, and code width before assignment.

- [ ] **Step 4: Implement capped boundary closure**

For each row, score only the registered nearest centroid set, calculate `secondary_distance / max(primary_distance, f32::MIN_POSITIVE)`, and push candidates into per-page bounded heaps. Materialize the strongest candidates in deterministic order until either the assignment limit or the page byte cap is reached. Compute amplification from actual total assignments and unique live rows.

- [ ] **Step 5: Implement exact production-router simulation**

Prepare each query once, choose its best one-to-four pages before code scoring, concatenate only those page codes, deduplicate raw IDs, and rank via the production contiguous SIMD method. Emit page ordinals, bytes, rows, GT page coverage, hit count, recall ppm, CPU nanoseconds, and limiting bound. The validator recomputes all aggregate minima/maxima and admits at most three nondominated arms sorted by `(-recall_ppm, bytes, pages, amplification_ppm, projected_ram_bytes, cpu_p99_ns)`.

- [ ] **Step 6: Run D2 tests and deterministic replay twice**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::d2 index::tests::v23_d2 -- --nocapture`

Expected: all tests pass; repeated fixture report bytes and page checksums match exactly.

- [ ] **Step 7: Commit the D2 slice**

```bash
git add crates/borsuk/src/v23_diagnostic.rs crates/borsuk/src/index.rs
git commit -m "Simulate V23 replicated posting pages"
```

### Task 4: Encode and Decode Private Posting Pages

**Files:**
- Modify: `crates/borsuk/src/v23_diagnostic.rs`
- Test: `crates/borsuk/src/v23_diagnostic.rs`

**Interfaces:**
- Consumes: `V23PagePlan`, fixed quantizer family/width, generation checksum, metric, dimensions, and raw IDs/codes.
- Produces: `encode_v23_page(&V23PageInput) -> Result<Bytes>` and `decode_v23_page(Bytes, &V23PageRef) -> Result<V23DecodedPage>`.

- [ ] **Step 1: Write canonical codec and exhaustive mutation RED tests**

Encode a page containing one primary and one replica with variable-length IDs. Assert round-trip equality and canonical bytes. Mutate magic, version, metric, dimensions, family, width, generation, ordinal, counts, each offset, ID order, code length, encoded length, reference checksum, and body checksum. Assert every mutation fails before slices are exposed.

- [ ] **Step 2: Run codec tests and verify RED**

Run: `cargo test -p borsuk --lib v23_diagnostic::tests::page_codec -- --nocapture`

Expected: compilation fails on absent encoder/decoder.

- [ ] **Step 3: Implement checked binary codec**

Define the input and decoded ownership explicitly:

```rust
pub(crate) struct V23PageInput {
    pub generation_checksum: [u8; 32],
    pub page_ordinal: u32,
    pub metric: VectorMetric,
    pub dimensions: u32,
    pub family: V23QuantizerFamily,
    pub code_width: u8,
    pub primary_rows: u32,
    pub ids: Vec<RecordId>,
    pub codes: Vec<u8>,
}

pub(crate) struct V23DecodedPage {
    bytes: Bytes,
    id_offsets: Range<usize>,
    ids: Range<usize>,
    codes: Range<usize>,
    row_count: usize,
    code_width: usize,
}
```

Use a fixed 96-byte little-endian header with magic `BRSKV23P`, version `23`, and checked `u32/u64` lengths. Append `(n + 1)` `u32` ID offsets, raw ID bytes, and exactly `n * code_width` code bytes. Hash the body using the repository's existing SHA-256 helper and reject any complete encoded length above 245,760 bytes. Decoder arithmetic uses `checked_add`/`checked_mul`; it authenticates the reference before returning ranges into the owned `Bytes`.

- [ ] **Step 4: Run codec tests under Miri-compatible safe Rust constraints**

Run: `cargo fmt --all -- --check && cargo test -p borsuk --lib v23_diagnostic::tests::page_codec -- --nocapture && cargo clippy -p borsuk --lib --tests -- -D warnings`

Expected: codec mutation matrix passes and Clippy is clean.

- [ ] **Step 5: Commit the page-codec slice**

```bash
git add crates/borsuk/src/v23_diagnostic.rs
git commit -m "Add authenticated V23 posting pages"
```

### Task 5: Add Canonical Benchmark Artifacts

**Files:**
- Modify: `crates/borsuk/examples/production_bench.rs`
- Test: `crates/borsuk/examples/production_bench.rs`

**Interfaces:**
- Consumes: the hidden D1/D2 report APIs and page encoder from Tasks 2–4.
- Produces: `bench_v23_d1_report.json`, `bench_v23_d2_report.json`, `bench_v23_d3_waves.csv`, `bench_v23_pages.json`, and `bench_v23_summary.json`.

- [ ] **Step 1: Write exclusive-mode and artifact mutation RED tests**

Add tests proving `BORSUK_BENCH_V23_STAGE` accepts exactly `d1`, `d2`, or `d3`; every mode rejects existing output before opening the index; atomic publication validates bytes both before link and after reload; D3 rejects disk cache, fewer than 1,000 samples, non-positive backing I/O, dependent waves, and any request/byte/latency/RAM breach. Fault-injection cases must also prove a missing page, short page, checksum failure, permission failure, and timeout retain their typed storage errors; cancellation releases the complete aggregate permit; and two concurrent four-page waves never exceed the configured transient capacity.

- [ ] **Step 2: Run benchmark tests and verify RED**

Run: `cargo test -p borsuk --example production_bench v23_ -- --nocapture`

Expected: tests fail because no V23 mode or artifacts exist.

- [ ] **Step 3: Implement stage-exclusive benchmark modes**

Parse and bind these exact variables:

```text
BORSUK_BENCH_V23_STAGE=d1|d2|d3
BORSUK_BENCH_V23_SOURCE_ARCHIVE_SHA256=[0-9a-f]{64}
BORSUK_BENCH_V23_INDEX_ID=[A-Za-z0-9._-]+
BORSUK_BENCH_V23_DATASET_ID=deep-image-96
BORSUK_BENCH_V23_D1_REPORT_SHA256=[0-9a-f]{64}  # required for d2 and d3
BORSUK_BENCH_V23_D2_REPORT_SHA256=[0-9a-f]{64}  # required for d3
```

Reject all ordinary build/read/concurrency flags while a V23 stage is active. Use the same frozen 32 query/source ordinals as V22 for D1/D2.

- [ ] **Step 4: Implement D3 one-wave execution**

Upload encoded pages under `diagnostics/v23/{attempt_id}/pages/{sha256}` with immutable conditional writes. For each registered query shape, open a fresh cache-disabled handle, acquire one aggregate transient permit from the sum of referenced page bytes plus decoded/dedup/query scratch, issue all GETs through the shared I/O pool, join the wave, decode/score, and record query-scoped backing telemetry. Do not read the manifest, router, codebook, or footer inside the timed interval.

- [ ] **Step 5: Persist canonical evidence atomically**

Reuse `publish_exclusive_file_set`. Serialize JSON with canonical struct field order and newline termination; CSV has one fixed header and query-major then repetition-major rows. Reload every file and compare exact bytes before returning success.

- [ ] **Step 6: Run benchmark tests and a tiny local object-store integration**

Run: `cargo test -p borsuk --example production_bench v23_ -- --nocapture`

Expected: all V23 benchmark and one-wave fixture tests pass.

- [ ] **Step 7: Commit the benchmark slice**

```bash
git add crates/borsuk/examples/production_bench.rs
git commit -m "Emit V23 qualification evidence"
```

### Task 6: Validate V23 Evidence in the Publication Worker

**Files:**
- Modify: `scripts/run_publication_v3_cell.py`
- Modify: `scripts/test_run_publication_v3_cell.py`

**Interfaces:**
- Consumes: canonical Rust artifacts and the frozen Publication V3 cell/index authority.
- Produces: `build_v23_diagnostic_plan(...)`, `validate_v23_d1_artifacts(...)`, `validate_v23_d2_artifacts(...)`, and `validate_v23_d3_artifacts(...)`.

- [ ] **Step 1: Write Python RED tests for stage exclusivity and recursive mutation**

Build one canonical fixture for each stage. Recursively mutate every mapping key, list item, integer, boolean, string, checksum, query ordinal, arm ordinal, page ordinal, aggregate, gate result, and receipt binding. Assert validation fails. Prove a below-threshold scientific D1/D2 result is a valid terminal diagnostic with `passed:false`, while malformed evidence is an infrastructure failure.

- [ ] **Step 2: Run worker tests and verify RED**

Run: `.venv/bin/pytest -q scripts/test_run_publication_v3_cell.py -k v23`

Expected: collection or assertion failure on absent V23 planning/validation.

- [ ] **Step 3: Implement exact plan construction**

Require `runtime_profile="recall"`, `cache_state="cold"`, 32 queries, disk cache zero, 3-GiB RAM cap, and one stage tag. D2 requires the exact completed D1 SHA-256; D3 requires both exact D1 and D2 SHA-256 values. Return `publishable=False`, `claim_eligible=False`, and a distinct `runtime-v23-{stage}` output namespace.

- [ ] **Step 4: Implement independent validators**

Parse the raw CSV/JSON without trusting Rust summaries. Recompute hit counts, ppm recalls, quantiles by nearest rank, page/request/byte maxima, storage amplification, projected RAM, and nondominated arm ordering. Enforce concrete Python types with `type(value) is ...` so bool/int and int/float substitutions fail. Validate every sample set is exactly `range(32)` for D1/D2 and exactly 1,000 repetitions per D3 arm.

- [ ] **Step 5: Run all V23 worker tests and static gates**

Run: `.venv/bin/pytest -q scripts/test_run_publication_v3_cell.py -k v23 && .venv/bin/ruff check scripts/run_publication_v3_cell.py scripts/test_run_publication_v3_cell.py && .venv/bin/python -m py_compile scripts/run_publication_v3_cell.py`

Expected: V23 tests and static checks pass.

- [ ] **Step 6: Commit the worker-validator slice**

```bash
git add scripts/run_publication_v3_cell.py scripts/test_run_publication_v3_cell.py
git commit -m "Validate V23 qualification evidence"
```

### Task 7: Add Immutable AWS Stage Orchestration

**Files:**
- Modify: `scripts/publication_v3_execution.py`
- Modify: `scripts/test_publication_v3_execution.py`
- Modify: `scripts/publication_v3_controller.py`
- Modify: `scripts/test_publication_v3_controller.py`
- Modify: `scripts/launch_aws_publication_v3.sh`
- Modify: `scripts/test_launch_aws_publication_v3.py`

**Interfaces:**
- Consumes: exact frozen source/archive/manifest/protocol/index authority and prior-stage terminal receipts.
- Produces: `diagnose-v23 --stage d1|d2|d3`, immutable attempt namespaces, terminal marker digests, and automatic Spot termination.

- [ ] **Step 1: Write paid-boundary RED tests**

Assert D1 reuses the completed Deep Image 10M index; D2 refuses absent/failed/mismatched D1; D3 refuses absent/failed/mismatched D1 or D2; all stages use Spot, preserve the registered r7g/c7g instance and timeout contracts, terminate on complete/fail/timeout, and upload exactly the artifacts for their stage. Add marker-conflict, markerless-termination, interruption-attempt, and immutable conditional-write races using the existing controller observation mechanism.

- [ ] **Step 2: Run execution/controller tests and verify RED**

Run: `.venv/bin/pytest -q scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py -k v23`

Expected: tests fail on absent `diagnose-v23` operation and receipt fields.

- [ ] **Step 3: Implement worker upload and terminal receipts**

Upload each artifact with its length, metadata SHA-256, and immutable conditional PUT. The terminal complete document contains `claim_eligible:false`, stage, source archive, manifest, protocol, index ID, instance identity, purchase option, prior-stage receipt hashes, and every artifact SHA-256. A scientific `passed:false` still emits complete; malformed output emits failed.

- [ ] **Step 4: Implement controller stage selection**

Add one controller operation with `--stage {d1,d2,d3}` and bounded automatic attempt selection. Add the launcher form `--diagnose-v23 <stage> <base-build-terminal-uri> <base-build-terminal-sha256>` with strict shell validation, clean-source freezing, and passthrough to that controller operation. Resolve prior-stage receipts before `RunInstances`. Use AWS profile `causality`, Spot purchase option, existing idempotent ClientToken logic, and `finally` termination. D3 selects only the at-most-three frozen nondominated D2 arms bound in its prerequisite receipt.

- [ ] **Step 5: Run affected Python tests and shell syntax checks**

Run: `.venv/bin/pytest -q scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py scripts/test_run_publication_v3_cell.py scripts/test_launch_aws_publication_v3.py -k v23 && .venv/bin/ruff check scripts/publication_v3_execution.py scripts/publication_v3_controller.py && bash -n scripts/launch_aws_publication_v3.sh`

Expected: all V23 orchestration tests pass and user-data remains below the existing 16-KiB bound.

- [ ] **Step 6: Commit and push the complete local qualification implementation**

```bash
git add scripts/publication_v3_execution.py scripts/test_publication_v3_execution.py scripts/publication_v3_controller.py scripts/test_publication_v3_controller.py scripts/launch_aws_publication_v3.sh scripts/test_launch_aws_publication_v3.py
git commit -m "Orchestrate V23 qualification stages"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 8: Run Local Assurance and the Paid D1–D3 Funnel

**Files:**
- Modify after terminal evidence: `docs/research/cold-read-latency-design.md`

**Interfaces:**
- Consumes: committed V23 diagnostic source and the authenticated V22 Deep Image 10M build authority.
- Produces: terminal immutable D1/D2/D3 receipts or a precise rejection of the architecture.

- [ ] **Step 1: Run the focused Rust and Python assurance serially**

Run, one process at a time and only while memory PSI is healthy:

```bash
cargo fmt --all -- --check
cargo clippy -p borsuk --all-targets -- -D warnings
cargo test -p borsuk --lib v23_ -- --nocapture
cargo test -p borsuk --example production_bench v23_ -- --nocapture
.venv/bin/pytest -q scripts/test_run_publication_v3_cell.py scripts/test_publication_v3_execution.py scripts/test_publication_v3_controller.py -k v23
```

Expected: every focused gate passes with no ignored V23 test.

- [ ] **Step 2: Run one repository assurance gate**

Run: `scripts/check-all.sh`

Expected: formatting, strict Clippy, Rust tests, Python tests, and repository validators pass. If the process hits the registered pressure criterion, terminate the original process, retain cancellation evidence, and move the gate to the intended remote build host rather than starting a replacement locally.

- [ ] **Step 3: Freeze and launch D1 only**

Freeze the current clean commit/archive through the existing launcher, then run:

```bash
AWS_PROFILE=causality scripts/launch_aws_publication_v3.sh --diagnose-v23 d1 \
  s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
  "$(aws s3api head-object --profile causality --bucket borsuk-bench-453182569524-euc1 \
      --key publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
      --query 'Metadata.sha256' --output text)"
```

Expected: one Spot worker reaches a terminal D1 receipt and is terminated. Inspect only terminal artifacts. If no arm passes all D1 gates, record V23 rejected and stop.

- [ ] **Step 4: Launch D2 only from a passing D1 receipt**

Run:

```bash
AWS_PROFILE=causality scripts/launch_aws_publication_v3.sh --diagnose-v23 d2 \
  s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
  "$(aws s3api head-object --profile causality --bucket borsuk-bench-453182569524-euc1 \
      --key publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
      --query 'Metadata.sha256' --output text)"
```

The controller resolves and pins the exact D1 terminal receipt; it must refuse a manual or mismatched digest.

Expected: one Spot worker reaches a terminal D2 receipt and is terminated. If no arm passes all D2 gates, record the exact limiting distributions and stop.

- [ ] **Step 5: Launch D3 only for frozen D2 winners**

Run:

```bash
AWS_PROFILE=causality scripts/launch_aws_publication_v3.sh --diagnose-v23 d3 \
  s3://borsuk-bench-453182569524-euc1/publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
  "$(aws s3api head-object --profile causality --bucket borsuk-bench-453182569524-euc1 \
      --key publication/v3/20260812/results/r01-46d286fd1e2290c1cb8b8645/build/attempts/0001/BUILD_TERMINAL_COMPLETE.json \
      --query 'Metadata.sha256' --output text)"
```

The controller binds the exact D1/D2 receipts and the at-most-three nondominated arms.

Expected: at least 1,000 strict-cold waves per arm, exact request/byte/RAM invariants, terminal receipt, and immediate instance termination. If every arm misses 60/100/150 ms, reject the architecture without changing the latency gate.

- [ ] **Step 6: Record terminal evidence and choose the next plan**

Append exact source/archive/index/instance/receipt identities, gate outcomes, and termination evidence to `docs/research/cold-read-latency-design.md`. If D1–D3 pass, write the separate persistent V23 implementation plan against the single frozen winning arm. If a stage fails, return to architecture brainstorming using its measured limiting bound; do not implement a persistent V23 format.

- [ ] **Step 7: Commit and push the evidence ledger**

```bash
git add docs/research/cold-read-latency-design.md
git commit -m "Record V23 qualification evidence"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

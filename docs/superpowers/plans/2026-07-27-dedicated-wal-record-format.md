# Dedicated WAL Record Format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the normal-segment-derived WAL table with a lossless, typed record-only schema that omits routing and product-quantization data unused by WAL search.

**Architecture:** `format.rs` will build and decode a dedicated Arrow `RecordBatch` directly from immutable WAL records plus dimensions; foreground WAL publication will not construct a derived `Segment`. Parquet and Vortex will consume the same logical batch so the layout comparison remains fair; the existing physical-format dispatch remains unchanged. The schema will carry record IDs, metadata, optional sparse/text/generation fields, nullable typed primary vectors, named payload extras, and constant vector type/dimension columns, but no segment header, routing code, or PQ code.

**Tech Stack:** Rust, Arrow, Parquet, Vortex 0.81, existing Borsuk format and end-to-end test suites.

---

### Task 1: Specify the dedicated schema with failing tests

**Files:**
- Modify: `crates/borsuk/src/format.rs`

- [x] **Step 1: Write a failing schema test**

Add a test that serializes one WAL run through both physical formats, reads its Arrow batch, and requires the exact record-only fields:

```rust
#[test]
fn wal_uses_record_only_schema_in_both_table_formats() {
    let segment = valid_segment();
    for format in [crate::PhysicalFormat::Parquet, crate::PhysicalFormat::Vortex] {
        let bytes = wal_object_to_table(&segment, VectorElementType::Float32, format).unwrap();
        let batch = match format {
            crate::PhysicalFormat::Parquet => first_batch(&bytes, "WAL").unwrap(),
            crate::PhysicalFormat::Vortex => read_vortex_table_sync(bytes).unwrap(),
            _ => unreachable!(),
        };
        let names = batch
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "record_id",
                "metadata",
                "vector",
                "wal_record_extras",
                "wal_vector_element_type",
                "wal_vector_dimensions",
            ]
        );
        assert!(!names.contains(&"segment_header"));
        assert!(!names.contains(&"routing_code"));
        assert!(!names.contains(&"pq_code"));
    }
}
```

- [x] **Step 2: Run the test and verify RED**

Run:

```bash
cargo test --locked -p borsuk --lib wal_uses_record_only_schema_in_both_table_formats -- --nocapture
```

Expected: FAIL because current WAL batches still contain `segment_header`, `routing_code`, and `pq_code`.

- [x] **Step 3: Add schema and batch builders**

Add private `wal_records_schema` and `wal_records_to_batch` helpers that accept
`&[VectorRecord]` and dimensions without a `Segment`. Reuse the existing
typed-vector, sparse-list, text-term, metadata, and `WalRecordExtras` builders.
Add a non-null `UInt32` `wal_vector_dimensions` constant column and validate
that the dimension fits in `u32`.

- [x] **Step 4: Route both writers through the dedicated batch**

Add `wal_records_to_table`, which calls `wal_records_to_batch` once and then
dispatches that batch to `write_batch_with_row_groups` for Parquet or compact
Vortex for Vortex. Route `BorsukIndex::wal_object_bytes` directly through this
function and remove its `Segment::from_records` call. Do not alter
normal-segment serialization.

- [x] **Step 5: Run the schema test and verify GREEN**

Run the exact command from Step 2. Expected: PASS.

### Task 2: Decode the dedicated schema losslessly

**Files:**
- Modify: `crates/borsuk/src/format.rs`

- [x] **Step 1: Extend the existing all-types round-trip test and verify RED**

Run every primary vector type through both Parquet and Vortex, including metadata, named dense/sparse/late-interaction payloads, text terms, generation, and forced storage. Add a binary vector whose logical dimensions are not byte-aligned:

```rust
for format in [crate::PhysicalFormat::Parquet, crate::PhysicalFormat::Vortex] {
    for element_type in ALL_VECTOR_ELEMENT_TYPES {
        // Existing rich payload fixture.
        let bytes = wal_object_to_table(&segment, element_type, format).unwrap();
        let decoded =
            wal_records_from_table(bytes, &format!("wal/run.{}", format.extension())).unwrap();
        assert_eq!(decoded, segment.records);
    }
}
```

Run:

```bash
cargo test --locked -p borsuk --lib wal_round_trips -- --nocapture
cargo test --locked -p borsuk --lib wal_round_trips_every_primary_type_and_payload_in_both_formats -- --nocapture
```

Expected: FAIL after Task 1 because `wal_records_from_table` still invokes the normal-segment decoder.

- [x] **Step 2: Implement `wal_records_from_batch`**

Decode and validate:

- the constant `wal_vector_element_type` and `wal_vector_dimensions` values on every row;
- record IDs and metadata;
- optional sparse indices/values as an all-or-nothing pair;
- optional text term IDs/frequencies as an all-or-nothing pair;
- optional generation;
- nullable typed dense vectors via `decode_segment_vector`;
- `WalRecordExtras`, restoring named payloads and requested storage.

Reject zero or inconsistent dimensions, inconsistent type codes, malformed sparse/text pairs, and a row containing both dense and sparse primary encodings.

- [x] **Step 3: Route WAL reads through the dedicated decoder**

Keep extension-based Parquet/Vortex container dispatch in `wal_records_from_table`, but pass the resulting batch directly to `wal_records_from_batch`.

- [x] **Step 4: Verify GREEN**

Run the commands from Step 1 plus:

```bash
cargo test --locked -p borsuk --test fp8_vectors
cargo test --locked -p borsuk --test feature_matrix
cargo test --locked -p borsuk --test local_index
```

Expected: all PASS.

### Task 3: Harden malformed-input behavior

**Files:**
- Modify: `crates/borsuk/src/format.rs`

- [x] **Step 1: Write failing malformed-schema tests**

Construct Parquet batches from a valid dedicated WAL batch after independently removing:

- `wal_vector_element_type`;
- `wal_vector_dimensions`;
- one side of each sparse/text pair.

Also alter one row’s dimension/type constant in a two-row batch. Require a specific `InvalidStorage` error naming the malformed field.

- [x] **Step 2: Run and verify RED**

Run:

```bash
cargo test --locked -p borsuk --lib wal_reader_rejects_ -- --nocapture
```

Expected: the new cases fail before the decoder validations exist.

- [x] **Step 3: Add the minimal validations**

Centralize constant-column checks in small private helpers and preserve existing finite-vector, sparse-index-order, and text-frequency validation paths.

- [x] **Step 4: Run and verify GREEN**

Run the command from Step 2. Expected: all matching tests PASS.

### Task 4: Re-measure storage and freeze the next qualification

**Files:**
- Modify: `docs/research/wal-layout-qualification-protocol.json`
- Modify: `docs/storage-format.md`
- Modify: `docs/architecture.md`
- Modify: `docs/production-readiness.md`
- Modify: `scripts/test_bench_wal_layout_qualification_aws.py`

- [x] **Step 1: Complete and independently reproduce v3**

Wait for all 220 immutable v3 cases and `WAL_LAYOUT_QUALIFICATION_COMPLETE`. Sync to a fresh directory, validate every case, regenerate `qualification-cases.csv` and `wal-layout-decisions.csv` from the frozen source archive, and compare hashes/bytes with the remote outputs.

- [x] **Step 2: Keep the released default unchanged**

Record v3’s decision. Parquet remains the WAL production default unless every promotion gate passes; preliminary or diagnostic measurements cannot promote Vortex.

- [x] **Step 3: Freeze a new v4 source and protocol**

Create a fresh campaign ID and source hash. Record v3 as the predecessor and state that no v3 cases are reused because the logical WAL schema changed. Preserve the same paired schedule, real dataset identities, hardware, repetitions, and strict gates.

- [x] **Step 4: Execute and independently reproduce the current campaign**

The frozen v4 source was invalidated before the complete campaign ran because
v16 changed generation allocation and same-lane publication. Launch v5 in
detached remote `tmux`, monitor completion, then repeat the independent
assembly/analysis reproduction. Promote only a policy supported by all gates;
otherwise keep Parquet.

Campaign `wal-layout-qualification-20260728-v5` completed all 220 cases. A
fresh download of source and results regenerated the case and decision CSVs
byte-for-byte (SHA-256 `c78f8dd5feb4bd5147831eaddd6ba4c3fc6638981444a6598b1256051f728322`
and `963ee72d803b5d4a7f6bb1b34a298ac52dabd38855ad669c0877642ef38bdfd8`).
The global promotion decision is false, so the production default remains
Parquet.

### Task 5: Run final release gates

**Files:**
- Modify only if a gate exposes a scoped defect.

- [ ] **Step 1: Run Rust correctness and lint gates**

```bash
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
scripts/check_rust_test_build.sh
```

- [ ] **Step 2: Run binding and methodology gates**

Run the repository’s Node, Python clean-install, storage inventory, format qualification, research-document validation, and audit commands documented in `docs/production-readiness.md`.

- [ ] **Step 3: Verify final artifacts**

Require exact source/dataset/hardware identities, complete case counts, no overwritten prefixes, and publication tables sourced only from official commercial numbers or cited papers plus Borsuk’s independently reproducible measurements.

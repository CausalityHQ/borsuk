# Production Storage Format Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Select and freeze the fastest correct physical layout for every BORSUK object role using real access traces and end-to-end evidence, after establishing the cell-sharded write boundary.

**Architecture:** Stable logical cells own independent WAL lanes and replaceable physical segments. A versioned role-based layout policy resolves Parquet, Vortex, Arrow IPC, or packed encoding at write time; readers dispatch from persisted references. Trace replay filters candidates, while fresh-index end-to-end runs make the promotion decision.

**Tech Stack:** Rust, object_store, Arrow/Parquet, Vortex, Python, Bash, CSV/JSON evidence.

---

### Task 1: Persisted-object inventory

**Files:**
- Create: `scripts/storage_layout_inventory.py`
- Create: `scripts/test_storage_layout_inventory.py`
- Create: `docs/research/storage-object-roles.csv`

- [x] **Step 1: Write failing inventory tests**

Require one row for each role declared in the design and reject duplicate roles,
missing current codecs, or missing read/write access patterns:

```python
required = {
    "catalog", "wal_run", "lane_head", "commit_marker", "routing_page",
    "normal_segment", "product_codes", "exact_vectors", "lexical_block",
    "filter_index", "late_interaction", "tombstone", "id_directory",
}
self.assertEqual({row["object_role"] for row in rows}, required)
self.assertTrue(all(row["current_format"] for row in rows))
self.assertTrue(all(row["access_patterns"] for row in rows))
```

- [x] **Step 2: Verify RED**

Run:

```bash
python3 -m unittest scripts.test_storage_layout_inventory -v
```

Expected: import/file failure because the inventory does not exist.

- [x] **Step 3: Implement checked inventory**

`storage_layout_inventory.py` validates the CSV and emits stable JSON with:

```text
object_role,current_format,path_family,writer,reader,access_patterns,
conditional_write,checksum,range_read,format_candidates,qualification_status
```

Populate every role from the current Rust writers/readers. Use `not-implemented`
for the future lane/commit/directory roles rather than pretending they ship.

- [x] **Step 4: Verify GREEN**

Run the test and:

```bash
python3 scripts/storage_layout_inventory.py validate \
  docs/research/storage-object-roles.csv
```

Expected: all roles valid and zero unknown fields.

### Task 2: Format-independent access tracing

**Files:**
- Create: `crates/borsuk/src/storage_trace.rs`
- Modify: `crates/borsuk/src/lib.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`
- Create: `crates/borsuk/tests/storage_access_trace.rs`

- [x] **Step 1: Write failing trace tests**

Create a small index and assert that build, approximate, exact, filtered, BM25,
sparse, and late-interaction operations emit rows with:

```rust
assert_eq!(row.object_role, PhysicalObjectRole::NormalSegment);
assert!(row.logical_rows_requested <= row.logical_rows_decoded);
assert!(row.bytes_fetched <= row.object_bytes);
assert!(!row.physical_format.is_empty());
```

The test also proves tracing disabled performs no file writes.

- [x] **Step 2: Verify RED**

Run:

```bash
cargo test -p borsuk --test storage_access_trace
```

Expected: missing trace module and types.

- [x] **Step 3: Implement tracing**

Add `PhysicalObjectRole` and an opt-in `BORSUK_STORAGE_TRACE` CSV sink. Instrument
the common storage read/write/range methods plus decode boundaries. Record:

```text
operation,object_role,path,physical_format,object_bytes,request_count,
bytes_fetched,logical_projection,row_selection,logical_rows_requested,
logical_rows_decoded,decode_cpu_ns,cache_state,status
```

Use a bounded mutex-protected writer only when tracing is enabled.

- [x] **Step 4: Verify GREEN**

Run the selected test, full storage tests, and `cargo clippy -p borsuk -- -D warnings`.

### Task 3: Cell WAL lane foundation

**Files:**
- Create: `crates/borsuk/src/cell_wal.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/index.rs`
- Create: `crates/borsuk/tests/cell_wal.rs`

- [x] **Step 1: Write failing concurrency tests**

Test eight lanes, 32 writers, one hot cell, distributed cells, CAS rebasing,
idempotency keys, prepared-run invisibility, atomic commit visibility, crash
recovery, and flush overlap.

- [x] **Step 2: Verify RED**

Run `cargo test -p borsuk --test cell_wal`; expect missing cell-WAL APIs.

- [x] **Step 3: Implement immutable runs and lane heads**

Add stable `(routing_epoch, cell_ordinal)` IDs, configurable 1–64 lanes with
default eight, content-addressed runs, conditional lane heads, transaction
descriptors, commit markers, and double-collected read snapshots. Replace the
single `wal_frontier` publication path; do not retain a compatibility reader.

- [x] **Step 4: Verify GREEN**

Run cell-WAL, crash-recovery, WAL, mutation, feature-matrix, and full library
tests.

### Task 4: Role-based layout policy

**Files:**
- Create: `crates/borsuk/src/physical_layout.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Modify: `crates/borsuk/src/format.rs`
- Modify: `crates/borsuk/src/index.rs`
- Create: `crates/borsuk/tests/physical_layout.rs`

- [x] **Step 1: Write failing policy tests**

Require each object reference to persist `object_role`, `physical_format`, and
`layout_policy_version`. Prove a single index reads mixed Parquet/Vortex/Arrow
objects and rejects a reference whose declared codec differs from its bytes.

- [x] **Step 2: Verify RED**

Run `cargo test -p borsuk --test physical_layout`; expect missing policy types.

- [x] **Step 3: Implement policy dispatch**

Define fixed and adaptive policies. Resolve a format only at write time and
dispatch reads from persisted references. Remove the global
`segment_table_format` assumption and old-format compatibility branches.

- [x] **Step 4: Verify GREEN**

Run physical-layout, format, lifecycle, compaction, GC, Python, Node, and CLI
tests.

### Task 5: Range-aware Vortex reader

**Files:**
- Modify: `crates/borsuk/src/vortex_table.rs`
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/format.rs`
- Create: `crates/borsuk/tests/vortex_range_reader.rs`

- [x] **Step 1: Write failing range tests**

Use a counting object store and prove a projection, point take, selective
filter, and bounded row range fetch less than the full object while returning
the same Arrow values as complete decode. Include cancellation, checksum, tail
dimensions, and corrupt-range cases.

- [x] **Step 2: Verify RED**

Run `cargo test -p borsuk --test vortex_range_reader`; expect full-object reads.

- [x] **Step 3: Implement object-store-backed Vortex I/O**

Provide Vortex with size and asynchronous range reads instead of a preloaded
`Vec<u8>`. Preserve one process-wide bounded runtime and charge every request,
byte, decode, and cache event to the storage trace.

- [x] **Step 4: Verify GREEN**

Run Vortex range, format, S3-compatible, memory-budget, and concurrency tests.

### Task 6: Checked trace replay

**Files:**
- Create: `scripts/replay_storage_access_traces.py`
- Create: `scripts/test_replay_storage_access_traces.py`
- Modify: `scripts/benchmark_borsuk_table_formats.py`
- Create: `docs/research/storage-format-qualification-manifest.json`

- [x] **Step 1: Write failing replay tests**

Reject unpaired operations, unequal logical values, missing materialization,
different cache states, fewer than 30 samples, or changed source checksums.

- [x] **Step 2: Verify RED**

Run `python3 -m unittest scripts.test_replay_storage_access_traces -v`.

- [x] **Step 3: Implement replay**

Recreate each traced object in every eligible format, execute the identical
projection/selection, materialize the actual downstream Arrow boundary, and
emit raw latency, requests, bytes, CPU, RSS, and logical-value checksums.

- [x] **Step 4: Verify GREEN**

Run replay tests and a local real-index smoke trace.

### Task 7: AWS end-to-end qualification and default freeze

**Files:**
- Create: `scripts/bench_storage_layout_qualification_aws.sh`
- Create: `scripts/test_bench_storage_layout_qualification_aws.py`
- Create: `scripts/analyze_storage_layout_qualification.py`
- Create: `scripts/test_analyze_storage_layout_qualification.py`
- Modify: `docs/research/storage-object-roles.csv`

- [x] **Step 1: Write failing runner and decision tests**

Require fresh prefixes, content-addressed source, identical seeds, alternating
arm order, at least two datasets per promoted role, raw samples, resource
telemetry, and an explicit no-promotion decision when confidence or correctness
gates fail.

- [x] **Step 2: Verify RED**

Run both Python test modules; expect missing runner/analyzer.

- [x] **Step 3: Implement qualification**

Run fixed Parquet, fixed Vortex, mixed, range-aware mixed, and justified packed
arms on local NVMe and S3. Analyze paired p95/p99, build/compaction throughput,
RSS, CPU, requests, bytes, storage size, and recall.

- [x] **Step 4: Freeze defaults**

Write only accepted role mappings into a new layout-policy version, regenerate
all indexes, run the full Rust/Node/Python/CLI/S3 gates, and then freeze the
large-scale publication manifest.

Both frozen qualifications rejected automatic Vortex placement. The all-
Parquet role-policy v3 baseline is therefore the frozen automatic production
default; Vortex remains an explicit research override. The v5 WAL decision is
recorded in `docs/research/wal-layout-qualification-v5-decision.json`.

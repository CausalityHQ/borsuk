# Benchmark Evidence Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an evidence-first benchmark dashboard and a separate local 1B-vector attempt artifact path with practical stop-policy reporting.

**Architecture:** Preserve existing benchmark artifacts and the million-vector release gate. Add a separate `billion-attempt.csv` artifact and renderer so partial 1B attempts cannot be confused with measured release-gate results. Extend docs and tests around the existing static docs webpage.

**Tech Stack:** Rust tests and ignored benchmark gates, static HTML/CSS/JS, CSV artifacts, Node VM-based docs-web test.

---

### Task 1: Add Billion-Attempt Artifact Tests

**Files:**
- Modify: `crates/borsuk/tests/large_scale.rs`

- [ ] **Step 1: Write failing Rust tests**

Add tests that require a new `BillionAttemptSummary`, `billion_attempt_csv`, and stop-policy formatting:

```rust
#[test]
fn billion_attempt_csv_records_partial_stop_policy() {
    let summary = BillionAttemptSummary {
        requested_records: 1_000_000_000,
        completed_records: 2_000_000,
        dimensions: 16,
        segment_max_vectors: 128,
        batch_records: 8_192,
        max_elapsed_seconds: 14_400,
        max_temp_bytes: 250_000_000_000,
        elapsed_ms: 61_000,
        temp_bytes_observed: 12_345_678,
        stop_reason: "max_elapsed_seconds".to_string(),
        completed_target: false,
        pre_segments: 15_625,
        rss_before: Some(1000),
        rss_peak: Some(2500),
        rss_after: Some(1500),
    };

    let csv = billion_attempt_csv(&summary);

    assert!(csv.starts_with("requested_records,completed_records,dimensions,segment_max_vectors,batch_records,max_elapsed_seconds,max_temp_bytes,elapsed_ms,temp_bytes_observed,stop_reason,completed_target,pre_segments,rss_before,rss_peak,rss_after,rss_peak_delta\n"));
    assert!(csv.contains("\n1000000000,2000000,16,128,8192,14400,250000000000,61000,12345678,max_elapsed_seconds,false,15625,1000,2500,1500,1500\n"));
}
```

- [ ] **Step 2: Verify the test fails**

Run: `cargo test --locked -p borsuk --test large_scale billion_attempt_csv_records_partial_stop_policy`

Expected: FAIL because the new summary type/function does not exist.

- [ ] **Step 3: Implement the minimal CSV type and formatter**

Add `BillionAttemptSummary`, `write_billion_attempt_csv`, and `billion_attempt_csv` beside the existing large-scale CSV helpers.

- [ ] **Step 4: Verify the Rust test passes**

Run: `cargo test --locked -p borsuk --test large_scale billion_attempt_csv_records_partial_stop_policy`

Expected: PASS.

### Task 2: Add Local 1B Attempt Gate

**Files:**
- Modify: `crates/borsuk/tests/large_scale.rs`

- [ ] **Step 1: Write a failing stop-policy test**

Add a non-ignored test that sets tiny limits through helper functions and proves the attempt stops before the requested target while still producing a partial summary.

- [ ] **Step 2: Verify the test fails**

Run: `cargo test --locked -p borsuk --test large_scale billion_attempt_stops_when_elapsed_limit_is_reached`

Expected: FAIL because the attempt runner does not exist.

- [ ] **Step 3: Implement the attempt runner**

Create reusable logic for `run_billion_vector_local_attempt` that:

- reads env vars `BORSUK_BILLION_ATTEMPT_RECORDS`, `BORSUK_BILLION_ATTEMPT_DIMENSIONS`, `BORSUK_BILLION_ATTEMPT_SEGMENT_MAX_VECTORS`, `BORSUK_BILLION_ATTEMPT_BATCH_RECORDS`, `BORSUK_BILLION_ATTEMPT_MAX_ELAPSED_SECONDS`, `BORSUK_BILLION_ATTEMPT_MAX_TEMP_BYTES`, `BORSUK_BILLION_ATTEMPT_WORKDIR`, and `BORSUK_BILLION_ATTEMPT_OUTPUT`;
- defaults to 1B records, 16D, 128 segment size, 8192 batch size, 14400 seconds, and 250 GB;
- checks elapsed time and temp bytes after each inserted batch;
- writes a partial CSV when stopped;
- logs completed records, elapsed ms, temp bytes, and stop reason;
- does not change the existing million-vector release gate.

- [ ] **Step 4: Verify the stop-policy test passes**

Run: `cargo test --locked -p borsuk --test large_scale billion_attempt_stops_when_elapsed_limit_is_reached`

Expected: PASS.

### Task 3: Extend Web Test And Static Artifact

**Files:**
- Modify: `scripts/test_docs_web.mjs`
- Create: `docs/web/assets/benchmarks/billion-attempt.csv`

- [ ] **Step 1: Write failing docs-web assertions**

Require the web app to fetch `assets/benchmarks/billion-attempt.csv`, render a billion-attempt chart/table, and expose stop-policy fields.

- [ ] **Step 2: Verify the docs-web test fails**

Run: `node scripts/test_docs_web.mjs`

Expected: FAIL because the app does not fetch or render the new artifact.

- [ ] **Step 3: Add a seed partial artifact**

Create a conservative seed CSV row showing `requested_records=1000000000`, `completed_records=0`, `stop_reason=not-run`, and `completed_target=false`.

- [ ] **Step 4: Verify the test still fails for renderer behavior**

Run: `node scripts/test_docs_web.mjs`

Expected: FAIL because renderer support is still missing.

### Task 4: Implement Web Dashboard Renderer

**Files:**
- Modify: `docs/web/app.js`
- Modify: `docs/web/docs.html`
- Modify: `docs/web/styles.css`

- [ ] **Step 1: Add `billion-attempt.csv` loading and renderer**

Add metrics for completed records, elapsed time, temp bytes observed, pre-segments, and RSS delta. Render the artifact in a separate `#billion-attempt` section.

- [ ] **Step 2: Add evidence summary panels**

Render measured large-scale highlights and 1B attempt status as distinct panels. Use wording that separates measured result from target/attempt.

- [ ] **Step 3: Restyle benchmark areas**

Apply the industrial systems dashboard direction with dense evidence panels, stable chart dimensions, responsive grids, and no unsupported visual claims.

- [ ] **Step 4: Verify docs-web tests pass**

Run: `node scripts/test_docs_web.mjs`

Expected: PASS.

### Task 5: Update Markdown Docs

**Files:**
- Modify: `docs/benchmarks.md`
- Modify: `README.md`

- [ ] **Step 1: Document the 1B attempt command**

Add exact commands and env vars for the 4h/250GB local attempt.

- [ ] **Step 2: Document artifact interpretation**

Explain `completed_target`, partial measured attempts, and why 1B is not a result until the artifact proves completion.

- [ ] **Step 3: Verify benchmark docs references**

Run: `rg -n "BORSUK_BILLION_ATTEMPT|billion-attempt|1,000,000,000|250 GB|4 hour" README.md docs/benchmarks.md docs/web/docs.html`

Expected: all references are present and consistently worded.

### Task 6: Verification And Local Attempt

**Files:**
- Modify generated artifact: `/tmp/borsuk-billion-attempt/billion-attempt.csv`
- Optionally copy completed artifact to `docs/web/assets/benchmarks/billion-attempt.csv`

- [ ] **Step 1: Run focused verification**

Run:

```bash
cargo test --locked -p borsuk --test large_scale billion_attempt_csv_records_partial_stop_policy billion_attempt_stops_when_elapsed_limit_is_reached
node scripts/test_docs_web.mjs
```

Expected: both commands PASS.

- [ ] **Step 2: Run the practical local attempt**

Run:

```bash
rm -rf /tmp/borsuk-billion-attempt
mkdir -p /tmp/borsuk-billion-attempt
BORSUK_BILLION_ATTEMPT_OUTPUT=/tmp/borsuk-billion-attempt/billion-attempt.csv \
BORSUK_BILLION_ATTEMPT_WORKDIR=/tmp/borsuk-billion-attempt/index \
BORSUK_BILLION_ATTEMPT_RECORDS=1000000000 \
BORSUK_BILLION_ATTEMPT_DIMENSIONS=16 \
BORSUK_BILLION_ATTEMPT_MAX_ELAPSED_SECONDS=14400 \
BORSUK_BILLION_ATTEMPT_MAX_TEMP_BYTES=250000000000 \
cargo test --locked --release -p borsuk --test large_scale \
  billion_vector_local_attempt_gate -- --ignored --nocapture
```

Expected: the run either reaches 1B or writes a partial artifact with the largest completed measured scale and stop reason.

- [ ] **Step 3: Copy artifact only if it is meaningful**

If the run completes at least one inserted batch, copy `/tmp/borsuk-billion-attempt/billion-attempt.csv` to `docs/web/assets/benchmarks/billion-attempt.csv`. If it does not, keep the seed `not-run` artifact and report why.

- [ ] **Step 4: Run final verification**

Run:

```bash
cargo fmt --check
cargo test --locked -p borsuk --test large_scale billion_attempt_csv_records_partial_stop_policy billion_attempt_stops_when_elapsed_limit_is_reached
node scripts/test_docs_web.mjs
```

Expected: all commands PASS.

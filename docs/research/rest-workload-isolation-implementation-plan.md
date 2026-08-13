# REST Workload Isolation Implementation Plan

**Goal:** Produce frozen AWS evidence that an S3-backed BORSUK search service on
a four-vCPU application host preserves cheap REST endpoint tail latency under
normal and overloaded vector traffic.

**Architecture:** An Axum example embeds one `BorsukIndex` handle and isolates
blocking search behind non-queueing application admission plus BORSUK's own
decode/search admission. A separate Python standard-library generator emits
open-loop HTTP arrivals and writes raw JSONL; a validator computes gates and a
Spot launcher freezes identities and terminal receipts.

**Spec:** `docs/research/rest-workload-isolation-benchmark.md`

## Task 1: Same-process REST application

**Files:**

- Modify `crates/borsuk/Cargo.toml` for example-only Axum/Tokio dependencies.
- Create `crates/borsuk/examples/rest_app_bench.rs`.

1. Add tests that reject unsupported V12 page budgets and prove a saturated
   search admission returns immediately rather than queues.
2. Run the exact example tests and capture the missing-contract RED.
3. Implement environment parsing, BORSUK open options, `/health`,
   `/api/item/:id`, `/api/search`, and `/metrics`.
4. Add a local HTTP smoke test proving cheap endpoints respond while the search
   permit is held and a second search receives 429.
5. Run the example tests, Clippy, and formatting.

## Task 2: Open-loop generator and result gate

**Files:**

- Create `scripts/rest_coexistence_load.py`.
- Create `scripts/test_rest_coexistence_load.py`.

1. Test deterministic absolute-deadline scheduling, scheduling-lag inclusion,
   percentile interpolation, endpoint classification, and gate failures.
2. Implement cheap baseline, search staircase, mixed normal, and mixed overload
   schedules using only the Python standard library.
3. Emit canonical JSONL samples and one canonical summary containing offered,
   accepted, error, 429, latency, scheduling-lag, and recall fields.
4. Validate the relative cheap-p99 and vector-recall gates from the frozen spec.

## Task 3: AWS attempt controller

**Files:**

- Create `scripts/launch_rest_coexistence_spot.py`.
- Create `scripts/test_launch_rest_coexistence_spot.py`.
- Extend `scripts/publication_v3_protocol.py` only after the diagnostic method
  is green and frozen.

1. Test canonical server/generator launch requests, Spot defaults, separate
   instance identities, no-swap cgroup contract, and terminal-marker-only
   reconciliation.
2. Implement immutable source/binary/index/dataset receipts and user-data for
   both instances.
3. Upload raw samples, summaries, telemetry, and terminal marker; terminate both
   instances immediately at terminal state.
4. Run one SIFT smoke, inspect only after the terminal marker, and repair the
   first failing gate before any full repetitions.

## Task 4: Freeze, run, and publish

1. Freeze the exact source, binary, index, dataset, instance, and workload
   identities after the SIFT smoke passes recall and isolation.
2. Run three rotated repetitions on SIFT, then the realistic-dimension and 100M
   datasets with the identical method.
3. Validate all cells without reading incomplete measurements.
4. Update README, architecture/API/benchmark docs, and the website only from
   complete frozen results; include the workload diagram and explicit limits.


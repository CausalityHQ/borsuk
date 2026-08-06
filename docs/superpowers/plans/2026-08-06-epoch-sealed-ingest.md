# Epoch-Sealed Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the unreleased inline lane HEAD with a scalable epoch-sealed immutable WAL that preserves sub-200 ms durable writes, high-throughput batched ingest, immediate visibility, and crash-safe last-write-wins ordering.

**Architecture:** One create-only immutable extent PUT is the acknowledgement boundary. A bounded lane HEAD owns epochs and publishes off-path durable/materialized watermarks; readers recover acknowledged extents independently of watermark freshness. Group commit batches before lane fan-out, uploads lane extents concurrently, and applies admission backpressure against materialization lag rather than HEAD size.

**Tech Stack:** Rust, `object_store`, Arrow/Parquet WAL framing, process-local group commit, Python fail-closed validators, AWS S3 profile `causality`.

## Global Constraints

- Replace format v26 outright with format v28; do not retain a v26 reader or migration path.
- Acknowledge only a checksum-verified immutable extent created inside the owning lease.
- Keep lane HEAD payload-free and bounded independently of extent count.
- Preserve partial-lane receipts, last-write-wins order, reopen visibility, and fail-closed recovery.
- Apply soft and hard tail bounds before durable group admission.
- Scalar and 16-record bulk workload points are separate; never infer throughput from scalar latency.
- Require `32 writers * 4 pipeline depth * 16 records = 2,048` outstanding records for the 10,000 records/s bulk gate.
- Never inspect incomplete campaign measurement CSVs; use terminal markers and infrastructure health until completion.
- Commit verified slices and fast-forward push directly to `origin/main`; never force push or create a pull request.

---

### Task 1: Encode bounded format-v28 HEADs and immutable extents

**Files:**
- Modify: `crates/borsuk/src/lane_log.rs`
- Modify: `crates/borsuk/src/manifest.rs`
- Test: `crates/borsuk/src/lane_log.rs`

**Interfaces:**
- Produces: `LaneEpochHead`, `LaneExtent`, `extent_path(lane, epoch, sequence)`, `extent_bytes`, and `extent_from_bytes`.
- Removes: inline bytes and per-block descriptors from persisted lane HEADs.

- [ ] **Step 1: Write RED codec tests**

  Add tests named `v28_head_size_is_constant_across_extent_counts`,
  `v28_extent_round_trips_identity_and_records`, and
  `v28_extent_rejects_path_or_checksum_identity_mismatch`. Construct heads with
  durable sequences 1 and 1,000,000 and require equal encoded length. Require
  corrupted lane, epoch, sequence, checksum, truncation, and trailing bytes to
  fail.

- [ ] **Step 2: Verify RED**

  Run: `rtk cargo test -p borsuk --lib lane_log::tests::v28_`

  Expected: compilation fails because the v28 types and codec do not exist.

- [ ] **Step 3: Implement the minimal codec**

  Set the lane-log format marker to 28. Define a fixed HEAD containing lane,
  epoch owner/expiry, durable sequence, materialized sequence, generation base,
  and a bounded prior-epoch seal. Define a self-describing extent containing
  lane, epoch, sequence, first generation, records, payload bytes, and checksum.
  Use the existing fenced envelope helpers and reject all format-v26 objects.

- [ ] **Step 4: Verify GREEN**

  Run: `rtk cargo test -p borsuk --lib lane_log::tests::v28_`

  Run: `rtk cargo fmt --all -- --check`

- [ ] **Step 5: Commit and fast-forward push**

  Commit: `storage: define epoch-sealed lane log v28`

### Task 2: Make immutable extent creation the durability boundary

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/lane_log.rs`
- Test: `crates/borsuk/src/lane_log.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`

**Interfaces:**
- Produces: `Storage::create_bytes_verified(path, bytes, checksum) -> Result<CreateOutcome>`.
- Produces: `LaneLogWriter::append_extent(payload, records, completed_at_ms) -> Result<LaneLogReceipt>`.

- [ ] **Step 1: Write RED create/idempotency tests**

  Require first create to issue one PUT, a replay with identical bytes to return
  success without creating a second object, and the same key with different
  bytes to return a fencing error. Inject accept-then-timeout and prove retry
  verifies the existing checksum before acknowledging.

- [ ] **Step 2: Write RED lease-completion test**

  Add `extent_completing_after_lease_guard_is_not_acknowledged`. Start inside the
  lease, make the store complete after expiry, and require a fencing error even
  though the immutable object exists.

- [ ] **Step 3: Verify RED**

  Run: `rtk cargo test -p borsuk --lib create_bytes_verified`

  Run: `rtk cargo test -p borsuk --test fault_injection extent_completing_after_lease_guard_is_not_acknowledged -- --exact`

- [ ] **Step 4: Implement create-only acknowledgement**

  Add create-only storage publication with exact checksum reconciliation.
  Reserve epoch sequence and generation locally, encode one extent, publish it,
  recheck the lease using the completion timestamp, and construct the receipt.
  Do not update HEAD synchronously and do not acknowledge an expired completion.

- [ ] **Step 5: Verify GREEN and adjacent safety**

  Run the two RED commands, then:

  `rtk cargo test -p borsuk --test fault_injection lane_`

- [ ] **Step 6: Commit and fast-forward push**

  Commit: `storage: acknowledge immutable epoch extents`

### Task 3: Recover and read independently of watermark freshness

**Files:**
- Modify: `crates/borsuk/src/lane_log.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/lane_log.rs`
- Test: `crates/borsuk/tests/group_commit.rs`

**Interfaces:**
- Produces: `LaneReadConsistency::{Committed, Linearizable}`.
- Produces: `LaneLogReader::read_lane(lane, consistency)` and bounded parallel multi-lane search refresh.

- [ ] **Step 1: Write RED stale-watermark visibility test**

  Publish two extents while leaving HEAD durable sequence at zero. Require
  `Linearizable` reopen to return both records and `Committed` to return the
  explicitly documented watermark view.

- [ ] **Step 2: Write RED epoch-sealing and zombie tests**

  Acquire epoch `E+1`, seal `E`, then inject a late `E` extent. Require every
  acknowledged extent at or below the seal to remain visible and the late
  zombie to remain excluded. Require `(epoch, sequence, ordinal)` ordering to
  make an `E+1` upsert dominate all `E` values.

- [ ] **Step 3: Write RED point-read routing test**

  Trace storage and assert `get_record` reads exactly the ID's computed ownership
  lane rather than all lane HEADs or prefixes.

- [ ] **Step 4: Verify RED**

  Run: `rtk cargo test -p borsuk --lib lane_log::tests::stale_watermark`

  Run: `rtk cargo test -p borsuk --test group_commit epoch_seal -- --nocapture`

- [ ] **Step 5: Implement recovery and readers**

  Seal only after the prior lease plus skew guard. Discover deterministic extent
  keys with bounded prefix enumeration during sealing and a bounded post-watermark
  probe during linearizable refresh. Validate every extent identity and checksum.
  Route point reads to one lane; retain bounded parallel fan-out for search.

- [ ] **Step 6: Verify GREEN and reopen invariants**

  Run the RED commands, then:

  `rtk cargo test -p borsuk --test group_commit`

- [ ] **Step 7: Commit and fast-forward push**

  Commit: `storage: recover epoch extents without fresh watermarks`

### Task 4: Pipeline group commit and decouple progress maintenance

**Files:**
- Modify: `crates/borsuk/src/group_commit.rs`
- Modify: `crates/borsuk/src/index.rs`
- Test: `crates/borsuk/src/group_commit.rs`
- Test: `crates/borsuk/tests/group_commit.rs`
- Test: `crates/borsuk/tests/fault_injection.rs`

**Interfaces:**
- Produces: pre-fan-out `PendingGroup`, per-lane concurrent `ExtentUpload`, and admission `TailBudget`.
- Produces: owner-only watermark/checkpoint messages; materializers never CAS lane HEAD directly.

- [ ] **Step 1: Write RED grouping test**

  Submit four concurrent one-record tickets with eight ownership lanes and one
  worker. Assert the dispatch group reports four committed records before its
  per-lane sub-batches upload concurrently. This prevents hashing from reducing
  the measured mean group to the v31 value of 1.196.

- [ ] **Step 2: Write RED blocked-maintenance test**

  Block watermark and materialization work in the fault store. Require subsequent
  extent acknowledgements to finish until the soft/hard lag thresholds are
  reached, then require a typed retryable admission error before a new ticket is
  issued.

- [ ] **Step 3: Write RED partial-lane and drain-frontier tests**

  Fail one lane extent while other lane extents succeed. Require exact committed
  lane receipts. During drain, capture the admitted frontier, wait for those
  uploads, publish an immutable delta search artifact, advance checkpoints, and
  prove later extents remain outside that drain.

- [ ] **Step 4: Verify RED**

  Run: `rtk cargo test -p borsuk --test group_commit grouped_before_lane_fanout -- --exact`

  Run: `rtk cargo test -p borsuk --test fault_injection epoch_extent -- --nocapture`

- [ ] **Step 5: Implement pipelined workers**

  Form groups on the shared request queue, fan records into lane sub-batches only
  after the grouping deadline, and run independent extent uploads concurrently.
  Track per-lane lag at admission. Move watermark publication and immutable delta
  construction to dedicated maintenance workers that return monotonic progress
  to the owning worker; remove corpus-wide global-PQ rebuild from ordinary drain.

- [ ] **Step 6: Verify GREEN and sustained local correctness**

  Run the RED commands, then:

  `rtk cargo test -p borsuk --test group_commit`

  `rtk cargo test -p borsuk --test fault_injection`

- [ ] **Step 7: Commit and fast-forward push**

  Commit: `ingest: pipeline epoch extents and delta drains`

### Task 5: Make the sustained campaign mathematically valid

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`
- Create: `docs/research/realistic-group-commit-v32-epoch-campaign.json`
- Modify: `scripts/bench_group_commit_scalability.sh`
- Modify: `scripts/validate_group_commit_scalability.py`
- Test: `scripts/test_bench_group_commit_scalability_runner.py`
- Test: `scripts/test_validate_group_commit_scalability.py`

**Interfaces:**
- Consumes: `records_per_operation=1` for scalar and `16` for bulk.
- Produces: raw `batch_records`, `operations_per_second`, `records_per_second`, and a Little's-Law preflight.

- [ ] **Step 1: Write RED campaign tests**

  Reject a throughput cell when
  `writers * pipeline_depth * records_per_operation < min_records_per_second * max_write_p95_ms / 1000`.
  Require distinct scalar and bulk workload IDs and raw batch lengths.

- [ ] **Step 2: Verify RED**

  Run: `PYTHONPATH=scripts rtk python3 -m unittest scripts.test_bench_group_commit_scalability_runner scripts.test_validate_group_commit_scalability`

- [ ] **Step 3: Implement valid scalar/bulk factors**

  Keep scalar batches at one record for the latency gate. Configure bulk batches
  at 16 records, yielding 2,048 outstanding records for 32 writers and depth
  four. Recompute operation and record rates from raw samples; reject any batch
  identity or record-count mismatch. Preserve the terminal v31 manifest and all
  of its artifacts unchanged.

- [ ] **Step 4: Replace obsolete correctness gates**

  Remove inline spill gates. Require extent idempotency, post-completion lease
  fencing, stale-watermark reopen, epoch zombie exclusion, owner-only HEAD
  mutation, tail backpressure, and delta-drain frontier safety.

- [ ] **Step 5: Run local structural smoke**

  Run the repository runner in smoke mode for scalar and 16-record bulk cells,
  require terminal markers, then apply the fail-closed validator.

- [ ] **Step 6: Commit and fast-forward push**

  Commit: `bench: qualify scalable epoch ingest`

### Task 6: Full verification and AWS architecture qualification

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/research/market-benchmark-matrix.md`
- Modify: `docs/superpowers/plans/2026-08-05-realistic-ingest-read-qualification.md`

**Interfaces:**
- Consumes: one exact committed source revision and frozen v32 manifest.
- Produces: terminal raw samples, summaries, storage traces, resource telemetry, and production-gate decisions.

- [ ] **Step 1: Run the full repository assurance gate once**

  Run library, integration, Python validator, policy, formatting, and strict
  all-target/all-feature Clippy gates using the isolated Cargo target. Do not
  repeat passing layers unless relevant code changes.

- [ ] **Step 2: Verify AWS preconditions**

  With profile `causality`, require an idle host, no benchmark service/process,
  sufficient memory/disk, exact source/manifest/dataset hashes, and unused S3
  prefixes.

- [ ] **Step 3: Run five paired repetitions**

  Cover 2K/16K logical cells, 1/8/32 writers, 1/2/4/8 worker lanes, scalar and
  16-record bulk. Preserve raw artifacts and resource telemetry. While active,
  monitor terminal markers and infrastructure health only in attached waits of
  at most 55 seconds; do not inspect measurement CSVs.

- [ ] **Step 4: Validate before inspection**

  Require root completion, no failure marker, successful process exit, every
  planned cell, exact identities, raw-to-summary reconciliation, write/read
  p95 below 200 ms, inserted-ID visibility, bounded physical amplification,
  10,000 acknowledgement records/s, and 10,000 drain-inclusive records/s for
  bulk 32-writer cells.

- [ ] **Step 5: Re-run realistic read qualification from the exact revision**

  Require Cohere 1M/768D recall@10 at least 0.95 and read p95 below 200 ms across
  the declared concurrency/cache scope. Then run DBpedia 1M/1536D with pinned
  ground truth before freezing defaults.

- [ ] **Step 6: Record terminal evidence and fast-forward push**

  Commit: `docs: record epoch ingest qualification`

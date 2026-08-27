# Cold object-store read latency implementation plan

**Goal:** Reduce honest fresh-handle cold search to p50 <=160 ms and p99
<200 ms at recall@10 >=97.41%, with <=30 GETs and <=32 MiB/query.

**Architecture:** First remove cache-motivated full-plane work from explicitly
cacheless handles and add bounded phase/read telemetry. Use a claim-ineligible
32-vs-64 width diagnostic on the existing index. Then introduce a new
authenticated cell-card format whose deterministic physical packing follows
coarse-centroid locality while logical IDs and ranking remain unchanged.

## Task 1: Expose a bounded code-plane retention policy

Files:

- Modify `crates/borsuk/src/index.rs`.
- Modify `crates/borsuk/examples/production_bench.rs`.
- Modify publication worker/parser tests under `scripts/` only if the selected
  option is emitted as result authority.

Steps:

1. Add failing unit tests for `OpenOptions::cell_card_code_plane_cache_max_bytes`
   defaulting to the current bounded budget and for zero disabling complete
   plane promotion/retention despite a nonzero retained pool.
2. Add a behavioral test with two cards in a multi-megabyte group: cacheless
   execution fetches only authenticated selected ranges, while cache-enabled
   execution may promote within the existing byte/amplification gates.
3. Add the option, propagate it into `CollectionReadRuntime`, and replace
   `retained_pool.is_some()` cache-policy inference with the explicit capacity.
4. Set the publication `uncached` serving profile to zero and keep warm at the
   bounded default. Assert cold/warm result identity on the same fixture.
5. Run the narrow Rust tests, the production benchmark test target, strict
   Clippy, formatting, and diff-check.

## Task 2: Add bounded head and exact physical-read telemetry

Files:

- Modify `crates/borsuk/src/index.rs`.
- Modify `crates/borsuk/src/storage.rs` only if request service timestamps must
  be captured below the query scheduler.
- Modify `crates/borsuk/examples/production_bench.rs`.
- Modify `scripts/run_publication_v3_cell.py` and its tests.

Steps:

1. Add RED tests for fixed-size telemetry aggregates covering count, bytes,
   service sum/max, queue sum/max, and threshold counts.
2. Timestamp each physical range operation immediately before scheduler
   admission and immediately around `read_range`; fold completion into query
   aggregates without retaining per-read records.
3. Make each physical range read return its own attempt/response-byte/service
   counters and fold distinct head/exact aggregates through `SearchReport` and
   the raw query CSV. Do not derive query evidence by deltaing shared-handle
   counters under concurrency.
4. Make the publication parser reject missing, negative, inconsistent, or
   noncanonical telemetry while preserving the ordinary quality gate.
5. Verify success, read failure, admission failure, and early termination all
   release permits and publish no partial result.

## Task 3: Complete the frozen baseline and preflight the rebuilt index

Files:

- Reuse the existing publication namespace for the old-format baseline and a
  distinct claim-ineligible namespace for the rebuilt-format preflight.

Steps:

1. Finish the five immutable old-format cold repetitions. R01 and R02 already
   reproduce identical recall, GET, and byte counts; interrupted or
   capacity-rejected launches advance only through the registered attempt
   ledger.
2. Before a paid rebuilt campaign, run the production planner on a shuffled
   locality fixture and require fewer physical head requests with identical
   selected logical cells.
3. Build one immutable v14 index, then run a small claim-ineligible replay of
   frozen queries at the registered width 32. Require average total backing
   GETs <=30, recall >=97.41%, bytes <=32 MiB/query, and exact query-owned
   head/exact telemetry reconciliation before launching all five repetitions.
4. If that preflight misses only because queue/service overlap is binding, add
   a separately registered width-64 tuning cell. Do not build a width matrix
   into the ordinary publication arm or weaken the quality gate.
5. Terminate every instance at terminal marker and record it in the attempt
   ledger. Do not use the preflight as publication evidence.

## Task 4: Introduce authenticated locality packing

Files:

- Modify `crates/borsuk/src/rotated_product_quantizer.rs`.
- Modify `crates/borsuk/src/global_pq_sidecar.rs`.
- Increment the relevant global cell-card format/version constant and update
  fixtures; do not add a legacy reader.

Steps:

1. Add RED tests for a deterministic logical-to-physical permutation over a
   shuffled centroid fixture. Require bijection, stable logical-ID tie breaks,
   and identical output across repeated builds.
2. Replace the current Morton key in parent and child centroid reordering with
   a deterministic nearest-neighbour chain. Each call is bounded to at most 256
   centroids; use stable centroid-value and prior-ordinal tie breaks.
3. Bump the codebook layout/version so its existing content checksum binds the
   new renumbering and old experimental indexes fail closed.
4. Keep the existing ascending cell spool, canonical root ordering, and reader
   binary search unchanged. Add tests explicitly proving those invariants.
5. Add an adversarial shuffled-neighbour test proving selected cells coalesce
   within the existing <=2x amplification and <=4 MiB range limits. Assert
   lower request count without wider logical selection or changed exact IDs.
6. Add format round-trip, incompatible-layout, interrupted build,
   bounded-memory, and deterministic-byte tests.

## Task 5: Qualify bounded V20 range hedging

Files:

- Modify `crates/borsuk/src/storage.rs`.
- Modify `crates/borsuk/src/index.rs`.
- Extend the existing global-range hedge validator/launcher without changing a
  publication arm.

Steps:

1. Add RED tests for primary win, hedge win, primary error, hedge error,
   cancellation, exact attempt/response-byte accounting, and permit release.
2. Expose a bounded optional V20 range hedge delay in `OpenOptions`; every
   attempt must acquire the shared backing-GET gate and remain scoped.
3. Return query-owned physical-read telemetry rather than shared counter deltas.
4. Run control/75/35/20 ms on the disjoint tuning split after locality packing.
   Select only an arm that improves p99 and stays inside request, byte, memory,
   and cost gates.

## Task 6: Review, assurance, and paid qualification

1. Ask Claude for a read-only adversarial review of the exact diff, especially
   permutation authority, bounded construction, cache-policy semantics, and
   telemetry accounting. Independently reproduce every actionable finding.
2. Run focused tests, strict workspace Clippy, formatting, and one full
   repository assurance command with no overlap.
3. Commit coherent slices and fast-forward `origin/main` only after verifying
   ancestry and the configured operator identity.
4. Freeze source/archive/manifest/protocol, build one new immutable index on
   Spot, and run five serial cold repetitions. Stop each host after its terminal
   marker.
5. Accept only if all recall, p50, p99, GET, byte, RSS, swap, and OOM gates pass.
   Otherwise use the new telemetry to choose one bounded follow-up; speculative
   exact overlap remains a contingency rather than an automatic next step.
6. Only after BORSUK passes, run real paired S3 Vectors and Turbopuffer adapters
   on the same queries and disclose service configuration, concurrency, cost,
   and artifact authority in the paper.

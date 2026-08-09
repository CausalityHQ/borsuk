# Task 3 report: bounded leaf routing and exact scoring

## Outcome

- Replaced the V10 reader's fail-closed stub with a resident coarse router and
  compact directory root whose selected-cell directory shards remain paged and
  authenticated.
- Added deterministic centroid-code leaf ranking for 4/8/16/32-page production
  budgets, canonical tie breaking, duplicate-page suppression, and base/delta
  fusion with one reserved page per non-empty layer and delta-before-base ties.
- Added exact-contiguous bundle-range coalescing and one shared, process-wide
  rerank-admitted GET wave per initial or continuation batch. No V9 code scan,
  identity fetch wave, random exact-row wave, or per-row GET/cache dependency is
  used by the V10 path.
- Decode and verify every selected Arrow batch and row, reconstruct canonical
  typed vectors, resolve duplicate IDs/newest mutation stamps/delta ties and
  tombstones, and exact-score all live rows through the existing SIMD metric
  kernel.
- Continue through ranked pages when MVCC suppression leaves fewer than `k`
  rows. Page, encoded-byte, and request envelopes stay bounded; short results
  terminate explicitly with `MaxBytes` or `MaxSegments`.
- Added truthful leaf-directory read/byte, logical page read/byte, exact-score,
  continuation, and wave counters. Generic segment counters remain in segment
  units (`0` segment payloads fetched on V10) instead of mixing in page counts.
- Preserved and tested explicit segment-engine fallbacks for filtered and
  nonmatching dense searches. Existing sparse, text, late-interaction, and
  exact-fringe dispatch remains outside the V10 eligibility predicate.
- Included the retained directory root/cell/shard/bundle allocation in the
  persisted resident-memory estimate and updated all `SearchReport`
  constructors, including the performance smoke integration fixture.

## TDD evidence

All Rust commands used this exact environment prefix:

`CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-task3 CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS='-C codegen-units=8' rtk cargo ...`

1. **Bounded deterministic routing**

   - RED: the new ranking tests failed to compile with eight missing
     routing/fusion symbols.
   - GREEN: `cargo test --locked -p borsuk leaf_ranking --lib` -> 3 passed,
     589 filtered out.
   - Covers budgets 4/8/16/32, selected-cell restriction, no duplicates,
     `budget * 128 KiB`, canonical ties, layer reservation, and delta tie order.

2. **Authenticated typed-row decode**

   - RED: `decode_global_leaf_rows` was absent.
   - GREEN: the focused decoder regression passed; the fresh module suite
     `cargo test --locked -p borsuk global_leaf::tests --lib` -> 16 passed,
     576 filtered out.

3. **V10 search without segment payloads or dependent GET waves**

   - RED: the production search regression failed to compile because the new
     leaf report counters were absent.
   - GREEN: the regression deletes every segment vector payload, returns the
     exact expected hit from V10 leaves, observes one leaf wave, at most four
     pages and 512 KiB, and proves physical GETs do not exceed directory reads
     plus logical pages.

4. **MVCC continuation and budget exhaustion**

   - RED: the first fixture retained multiple rows per page and reported zero
     continuations; the corrected one-row-page fixture then exercised the
     missing continuation behavior.
   - GREEN: one tombstoned nearest row produces the next live row in two waves.
     With every candidate tombstoned, exactly four pages produce three
     continuations and terminate `MaxSegments`.
   - Additional RED: a byte frontier that admitted one page but rejected the
     next returned `Complete` because consumed bytes remained numerically below
     the limit.
   - GREEN: candidate-prefix truncation is now tracked explicitly; the same
     query reads one page within the hard byte limit and terminates `MaxBytes`.

5. **Fallbacks, codecs, and integration surface**

   - `cargo test --locked -p borsuk resident_global_ --lib` -> 11 passed,
     581 filtered out.
   - `cargo test --locked -p borsuk every_named_global_scan_codec_routes_its_v10_artifact --lib`
     -> 1 passed, 591 filtered out.
   - `cargo test --locked -p borsuk --test performance_smoke approx_report_accepts_equal_distance_hits_with_different_ids`
     -> 1 passed, 2 filtered out.

## Fresh final verification

- `cargo test --locked -p borsuk leaf_ranking --lib` -> 3 passed, 589
  filtered out.
- `cargo test --locked -p borsuk global_leaf::tests --lib` -> 16 passed, 576
  filtered out.
- `cargo test --locked -p borsuk resident_global_ --lib` -> 11 passed, 581
  filtered out.
- `cargo test --locked -p borsuk every_named_global_scan_codec_routes_its_v10_artifact --lib`
  -> 1 passed, 591 filtered out.
- Focused performance-smoke integration regression -> 1 passed, 2 filtered
  out.
- `cargo check --locked -p borsuk --lib` -> exit 0.
- `cargo clippy --locked -p borsuk --lib -- -D warnings` ->
  `cargo clippy: No issues found`.
- `cargo fmt --all --check` and `git diff --check` -> exit 0.

No full workspace suite, AWS operation, benchmark artifact access, push, or PR
was performed.

## Review fix round 1/5

The native read-only review found no Critical issues and four Important issues:
an omitted integration-test report initializer, short byte-bounded results
misreported as complete, use of an optional decode gate instead of the shared
rerank admission gate, and page counts mixed into segment telemetry. All four
were corrected and covered by the focused verification above.
The scoped fix-round re-review marked all four findings addressed and found no
new Critical or Important issues.

The repository-requested cross-provider Claude review could not run because the
provider had reached its weekly usage limit; the queued consultation was
canceled rather than left pending.

## Remaining sequencing concern

Task 4 still owns removal of the now-unused V9 resident-global query
implementation and its legacy helpers. The production dispatch is V10-first and
does not call that path.

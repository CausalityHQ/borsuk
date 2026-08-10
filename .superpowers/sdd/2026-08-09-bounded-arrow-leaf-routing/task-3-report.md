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

## Post-Task-3 independent review fix round 1/5

### Root cause and architecture correction

The independent review found that the V10 directory and leaf reads already
reached the correct process-wide physical GET-admission seam in
`CountingObjectStore::get_opts`, but MVCC resolution did not. After each admitted
leaf wave, `resolve_global_leaf_rows` called the ordinary query-paged
`mutation_states` path. A cold stable tombstone page could therefore start a
dependent GET wave, repeat across continuations when tombstone retention was
disabled, and add backing bytes that were absent from V10's logical
directory/leaf byte telemetry.

The fix keeps the physical GET seam unchanged and gives V10 an explicit bounded
MVCC eligibility contract:

- Open and full refresh may prepare one complete mutation view containing the
  stable tombstone pages, manifest frontier, and visible cell-WAL tombstone runs.
- Preparation first rejects count-derived lower bounds above 32 MiB, then holds
  a full 32 MiB permit from the collection-shared `RetainedBytePool` before
  decoding, and finally rejects an actual decoded view above that cap.
- The view is keyed by the manifest, lane-head, and cell-WAL snapshot digest.
  V10 uses it only on an exact key match. Missing, oversized, unreserved, or
  stale views cause segment-engine fallback before any V10 directory or leaf
  read; ordinary paged MVCC remains available to that fallback.
- `resolve_global_leaf_rows` now performs only resident hash lookups and cannot
  issue storage reads. Search telemetry reports the retained-pool reservation.
- A 100M-ID declared overlay is rejected before any object GET or allocation.
- `max_segments` dispatch now admits only the qualified page budgets 4, 8, 16,
  and 32. Every other explicit positive maximum falls back unchanged, so the
  caller's segment maximum remains authoritative.

### RED evidence

All fix-round Rust commands used this exact isolated environment:

`CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-task3-fix1 RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper SCCACHE_DIR=/data/cache/sccache CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS='-C codegen-units=8' rtk cargo ...`

1. `resident_global_v10_cold_stable_tombstone_has_no_dependent_get_wave`
   used a shared real `object_store::memory::InMemory`, no disk cache, a zero-byte
   decoded tombstone cache, and a flushed stable tombstone page. Before the fix
   it failed with `backing_bytes_read=226890` versus reported
   `bytes_read=213506`; it issued 5 GETs while one directory read plus two leaf
   pages allowed exactly 3. The two extra GETs were repeated post-decode reads of
   the same cold tombstone page across the initial and continuation waves.
2. `resident_global_v10_dispatch_accepts_only_qualified_page_budgets` failed at
   the first unsupported value: budget 1 reported `bounded-arrow-leaf-v10`
   instead of the required `srht-pq-scan` fallback.
3. The dedicated retained-view lifecycle regression was mutation-tested against
   both required invalidations. Removing the pre-replacement release from full
   `refresh` made the deliberately tight 48 MiB retained pool reject the second
   32 MiB view and the test failed on a missing replacement. Removing the
   `refresh_wal_tail` invalidation failed its explicit assertion that the stale
   resident view was gone before search.

### GREEN and focused regression evidence

- Cold stable-tombstone no-dependent-GET regression: 1 passed, 594 filtered out.
  It returns the next live ID through V10, requires backing bytes to equal
  reported V10 bytes, requires physical GETs to equal directory plus leaf reads,
  and observes a nonzero retained-byte reservation.
- Exact supported/unsupported budget matrix: 1 passed, 594 filtered out.
- 100M mutation-overlay refusal before object reads: 1 passed, 594 filtered out.
- Full-refresh and lane-tail-refresh lifecycle regression: 1 passed, 595
  filtered out. Two successive mutation refreshes replace both the exact
  snapshot key and resident object while retained bytes remain exactly 32 MiB;
  unchanged refreshes do not grow the pool. A lane-tail refresh removes the
  view/reservation, after which a supported-budget search explicitly uses
  `srht-pq-scan` with zero V10 directory and leaf reads.
- `cargo test --locked -p borsuk resident_global_ --lib`: 15 passed, 581
  filtered out, covering base/delta fusion, moved upserts/generations,
  tombstones, continuation, WAL merge, and explicit fallbacks.
- `cargo test --locked -p borsuk leaf_ranking --lib`: 3 passed, 592 filtered
  out.
- `cargo test --locked -p borsuk global_leaf::tests --lib`: 16 passed, 579
  filtered out.
- All named global scan codecs through V10: 1 passed, 594 filtered out.
- `cargo test --locked -p borsuk physical_get_admission_ --lib`: 11 passed,
  584 filtered out. These cover single admission below isolated read scopes,
  response-body permit lifetime, exactly-once forwarded counting, and queued or
  cancelled attempts remaining uncounted.
- Focused performance-smoke report regression: 1 passed, 2 filtered out.
- `cargo check --locked -p borsuk --lib`: exit 0.
- `cargo clippy --locked -p borsuk --lib -- -D warnings`: no issues.
- `cargo fmt --all -- --check` and `git diff --check`: exit 0.

The repository-requested read-only cross-provider Claude review was started
after the diff stabilized, but Claude reported its weekly usage limit and
queued the job until reset. The obsolete queued job was canceled; it produced
no code-review result.

No full workspace suite, AWS operation, benchmark artifact access, push, or PR
was performed in this fix round.

## Post-Task-3 independent review fix round 2/5

### Root cause and dataflow correction

The first fix made fetched-leaf MVCC resident-only, but dense search still
resolved the live WAL before V10 dispatch. On a cache miss,
`dense_live_wal_records` built `live_wal_snapshot`, whose per-record suppression
called the ordinary query-paged `mutation_state`. A V10 query with a non-empty
cell WAL could therefore cold-read the stable page and the visible cell-WAL
tombstone run before reading its leaf. WAL telemetry counted only record-run
bytes, so both MVCC reads were unreported.

The corrected dataflow computes the side-effect-free V10 context before WAL
resolution. A statically supported V10 query obtains the exact-key frozen
mutation view or has no V10 context at all. With a context, both WAL record
suppression and leaf-row suppression use the same resident lookup. Missing,
stale, oversized, or unreserved views use the ordinary paged WAL resolver and
segment engine; the later V10 dispatch sees the same missing context and returns
before directory or leaf reads. Unrelated engines are not made dependent on the
view.

The ordinary complete-WAL snapshot builder also uses an exact resident view
opportunistically when one is available. This matters because its cache and
single-flight are shared: for a key eligible for V10, an overlapping unrelated
query cannot start a paged-MVCC snapshot build that the V10 query then joins.
Selected-cell V10 WAL resolution has its own explicitly resident-only wrapper.
The resident call chain is therefore:

`resident_global_v10_context -> dense_live_wal_records_with_resident_mutations -> live_wal_snapshot_with_resident_mutations -> live_wal_tail_records_for_cells_with_resident_mutations -> state_suppresses_record`.

Neither that chain nor `resolve_global_leaf_rows` calls `mutation_state` or
`mutation_states`; the only storage work on the branch is the explicitly
reported WAL record run, selected directory shards, and selected leaf pages.

### Honest frozen heap representation

The retained view no longer stores a `HashMap`. It consumes the temporary merge
map into an immutable sorted boxed slice. Each entry contains a 64-bit BLAKE3
routing key, an exact boxed record ID, and `MutationState`. Lookup binary-searches
the numeric routing key first and always verifies the complete ID, including on
numeric-key collisions.

Post-decode admission uses checked arithmetic over every requested retained
allocation: the outer `Arc` payload and two reference counters, the exact boxed
entry slice, and every exact boxed-ID length. There is no spare hash capacity or
arbitrary bytes-per-bucket estimate. The full 32 MiB retained-pool permit remains
pinned for the view lifetime, while the existing count-derived lower bound still
rejects a 100M-ID view before loading any tombstone object.

The separate paged tombstone-cache estimator now also charges conservatively
for hash-table load-factor slack, control bytes, and each ID buffer's capacity;
that cache is not part of the retained V10 view.

### RED evidence

All Rust commands used the isolated round-2 target and required wrapper/cache:

`CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-task3-fix2 RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper SCCACHE_DIR=/data/cache/sccache CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS='-C codegen-units=8' rtk cargo ...`

`resident_global_v10_cold_nonempty_wal_uses_only_reported_gets_and_bytes` uses a
shared real `object_store::memory::InMemory`, no disk cache, zero tombstone-page
retention, zero WAL-run retention, a flushed stable tombstone page, and a live
record+tombstone cell-WAL transaction for the same ID. Mutation-testing the old
pre-dispatch paged resolver produced `backing_bytes_read=19252` versus reported
`bytes_read=15988`, and 7 physical GETs versus the 5 explicitly represented by
one WAL record run, one directory read, and three leaf pages. The extra 3,264
bytes and two GETs were the stable and cell-WAL tombstone objects.

### GREEN and focused verification

- Cold non-empty-WAL exact byte/GET regression: 1 passed, 597 filtered out.
- Frozen exact-heap/collision-safe lookup regression: 1 passed, 597 filtered
  out.
- Cold empty-WAL stable-tombstone regression: 1 passed, 597 filtered out.
- Refresh/replacement/lane-tail invalidation regression: 1 passed, 597 filtered
  out.
- 100M pre-read refusal regression: 1 passed, 597 filtered out.
- Qualified/unsupported page-budget matrix: 1 passed, 597 filtered out.
- `cargo test --locked -p borsuk resident_global_ --lib`: 16 passed, 582
  filtered out.
- `cargo test --locked -p borsuk leaf_ranking --lib`: 3 passed, 595 filtered
  out.
- `cargo test --locked -p borsuk global_leaf::tests --lib`: 16 passed, 582
  filtered out.
- Every named global scan codec through V10: 1 passed, 597 filtered out.
- `cargo test --locked -p borsuk physical_get_admission_ --lib`: 11 passed,
  587 filtered out.
- Focused performance-smoke report regression: 1 passed, 2 filtered out.
- `cargo check --locked -p borsuk --lib`: exit 0.
- `cargo clippy --locked -p borsuk --lib -- -D warnings`: no issues.
- `cargo fmt --all -- --check` and `git diff --check`: exit 0.

No full workspace suite, AWS operation, benchmark artifact access, push, or PR
was performed in this fix round.

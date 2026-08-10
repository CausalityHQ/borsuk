# Task 4 report: remove V9 and prove V10 correctness

## Outcome

- Removed the unreleased V9 bundle, identity, location, code-scan, random
  exact-row, exact-bound, graph, range-planning, hedge, and cache paths. V10's
  authenticated bounded Arrow leaf directory/bundles are now the only global
  ANN artifact and query implementation.
- Deleted the custom global-cell graph module, global range planner, hedge
  trace test, storage range planner/cache helpers, and their production and
  benchmark controls. Public Rust, CLI, Python, and Node configuration no
  longer exposes those knobs; compile-fail examples prevent their accidental
  return.
- V7 and V9 descriptor layouts now fail with an explicit instruction to
  rebuild the unreleased index. No legacy reader, migration layer, alias, or
  dual writer remains.
- Preserved V10 base/delta fusion, typed exact scoring, WAL and tombstone
  visibility, deterministic result semantics, manifest atomicity, and explicit
  segment fallback for an unmaterialized exact fringe.
- Fixed one lifecycle issue found by the final workspace tests: open now
  preloads the V10 delta descriptor/root with the base, so the first query does
  not perform unreported delta setup reads. A cold threshold-materialized
  base+delta query now reports backing bytes equal to its logical V10
  directory/page bytes.

## Removed surface

- Deleted `global_graph.rs`, `global_read_planner.rs`, and
  `tests/hedge_trace.rs`.
- Removed V9 row/location/chunk/candidate structures, bundle parsing and
  codecs, resident code caches, graph caches/warmers, identity and exact range
  caches, random exact-row planners, certificate-bound machinery, and obsolete
  parallel/storage helpers.
- Removed `GlobalCellGraphConfig`, graph construction settings,
  `global_exact_rerank`, `global_exact_bound_shadow`, prefetch-stripe and
  slow-read-hedge options, graph-cache sizing, and corresponding benchmark
  protocols.
- Updated all bindings, examples, report initializers, and integration
  fixtures to the V10-only API and telemetry.

## TDD and focused regression evidence

Every Rust command used:

`CARGO_TARGET_DIR=/home/rb/worktrees/borsuk-prod-ready-v9/target-task4 RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper SCCACHE_DIR=/data/cache/sccache CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0 RUSTFLAGS='-C codegen-units=8' rtk cargo ...`

1. **Explicit V7/V9 rebuild errors**

   - RED: the legacy-layout regression received the ambiguous V10 layout
     metadata error.
   - GREEN: `index::tests`/descriptor regression
     `v7_and_v9_descriptors_require_an_explicit_rebuild` passed and requires
     `rebuild the unreleased index` for both legacy markers.

2. **Deleted public controls**

   - RED: three new compile-fail examples compiled because the V9 open/build/
     search fields still existed.
   - GREEN: all three doctests passed after the fields and their consumers were
     removed.

3. **V10 format/query correctness**

   - `resident_global_v10` family: 8 passed.
   - `resident_global_base_and_delta_fuse_v10_leaves`: passed.
   - `fused_resident_global_upsert`: passed.
   - Selected missing/corrupt/substituted/truncated global-leaf rejection
     family: 6 passed.
   - Every primary dense element type feature-matrix regression: passed,
     covering all seven scalar/binary representations.
   - Refresh snapshot, foreground delta, direct-add fringe, and group-commit
     regressions were rerun by fully qualified exact name and passed.

4. **Failures found by the first full workspace run**

   - Two tests still asserted deleted V9 scan-chunk telemetry. They now require
     the exact hit plus V10 engine, authenticated directory/page reads and
     bytes, and exact scoring.
   - A resident mutation refresh test compared allocator addresses. The
     allocator legitimately reused a released address; the test now proves
     semantic replacement through a new snapshot key, the newly visible
     delete state, and exactly one retained permit.
   - Three group-commit tests still expected V9 `global-pq` delta objects and
     phase telemetry. Threshold drains now prove authenticated `global-leaf`
     publication and V10 reads; publication failure is injected at the current
     manifest boundary; cold base+delta reads prove fused V10 leaf behavior.
   - The threshold regression exposed the lazy delta-descriptor read described
     above. It was RED with `backing_bytes_read=26126` versus
     `bytes_read=14946`, then GREEN after delta preload.

## Complete assurance

- `cargo fmt --all -- --check`: passed. The first attempt found one import
  wrapping difference; `cargo fmt --all` repaired it and the check passed.
- `cargo clippy --locked --workspace --all-features --all-targets -- -D warnings`:
  passed after removing stale binding initializers and one orphaned RPQ memory
  helper. A final incremental rerun after the V10 delta-preload lifecycle fix
  also reported no issues.
- `cargo test --locked --workspace --all-features --all-targets`: final run
  passed with zero failures. The core library reported 550 passed / 4 ignored;
  group commit reported 43 passed; every remaining workspace target passed.
- `uv run --python 3.12 --with-requirements
  scripts/requirements-format-bench.txt python -m unittest discover -s scripts
  -p 'test_*.py'`: 542 passed. The system `python3` run first exposed the
  missing pinned NumPy/PyArrow environment; rerunning the same layer with the
  checked-in requirements passed.
- `python3 scripts/check_repo_policy.py`: passed.
- `node scripts/test_docs_web.mjs`: passed.
- `node scripts/sync_docs_examples.mjs --check`: passed.
- `git diff --check`: passed.

`python3 scripts/validate_research_docs.py` remains an environmental gate
blocker: this worktree has no ignored historical `raw/` artifact tree and no
`resource-schema.csv`. Task 4 changes no validator, research, web-asset, or
workflow file. A fresh `git archive` of base commit `4b27892` reproduced the
same failures, and Git contains neither the required raw directories nor the
schema. The checked-in CI/publish workflow does not provide a fetch/bootstrap
step, and no documented full immutable archive restore command exists. No
historical or incomplete measurement CSV was inspected, fabricated, or
rewritten.

## Self-review and external review handoff

- `git diff --ignore-all-space` was audited across the V10 descriptor, query,
  storage, public API, bindings, benchmark, and test surfaces.
- Repository-wide searches found no callable removed API or V9 graph/range
  implementation references. Remaining removed-name matches are compile-fail
  guards, immutable historical campaign policy names, or negative path-role
  assertions.
- `git diff --check` is clean and the final full workspace suite covers the
  final production tree.
- Per controller instruction, the bounded read-only adversarial review is
  controller-owned and will be dispatched from the committed review package;
  this implementer did not launch a competing consultation.

## Remaining production concern

Correctness is preserved for small direct or drained writes that have not yet
crossed the incremental V10 leaf-materialization threshold, but those reads use
the explicit segment engine because the exact fringe is nonempty. A follow-on
incremental/resident-write design should materialize those rows into V10 leaves
without waiting for the threshold. Threshold-sized drains already publish and
query authenticated V10 base/delta leaves.

## Fix round 1/5 — latency, refresh/cache, and benchmark schema

### Root causes and repairs

1. The resident V10 scheduler bypassed the segment path's stop helper, so it
   could begin directory reads, the initial page wave, and tombstone-driven
   continuations after `max_latency_ms` had expired. V10 now checks the shared
   absolute deadline before every base/delta directory wave, before the initial
   leaf wave, and before every continuation. A wave that crosses the deadline
   makes `MaxLatency` terminal even if it completed `k`; latency takes precedence
   over `MaxBytes`, `MaxSegments`, and `Complete`.
2. Resident descriptor/root setup used an unlocked-miss, strong, unbounded
   `HashMap`. Refresh also swapped manifests without validating the next base
   and delta references. It is now a four-generation recency cache with the
   existing `InFlightReads` primitive coalescing descriptor plus three-root
   setup. Values are immutable `Arc`s, so eviction cannot invalidate an active
   query and unpinned evicted generations are reclaimable. Refresh validates
   and preloads target-manifest base/delta references for primary and named
   modalities before any manifest handoff; target element type is passed
   explicitly because `self.manifest` is intentionally still the old snapshot.
3. Production CSVs were unversioned and still emitted literal-zero graph
   configuration plus removed graph/scan telemetry. Query-related outputs now
   declare `borsuk-production-bench-v10` and emit V10 directory reads/bytes,
   page reads/bytes, waves, continuations, exact scores, and backing reads/bytes.
   Compatibility columns and `graph_config_columns` were deleted. The
   storage-layout assembler accepts only this exact schema. The immutable V9
   physical-GET validator explicitly rejects versioned V10 rows so historical
   V9 results cannot be compared as though they came from this architecture.

### Strict TDD evidence

- Latency RED (the first command accidentally omitted `--locked`, with no
  dependency resolution change): 0 passed / 1 failed / 43 filtered. The delayed
  store returned a hit from a continuation scheduled after the deadline.
  GREEN with `--locked`: 1 passed / 43 filtered; zero post-deadline
  continuations and exactly one bundle GET.
- Bounded/single-flight cache RED: compilation failed because
  `BoundedResidentCache` did not exist. GREEN: 1 passed / 554 filtered, proving
  one loader invocation for concurrent callers, the hard retention bound,
  active-`Arc` safety, and reclaim after eviction/drop.
- Transactional refresh RED with preload removed: 0 passed / 1 failed / 44
  filtered because a corrupted next descriptor was published. GREEN: 1 passed /
  44 filtered; the refresh errors while retaining/querying the old manifest.
- Concurrent refresh GREEN: 1 passed / 45 filtered. Refresh performs exactly
  one new descriptor plus three root GETs; two concurrent first queries perform
  zero descriptor/root setup GETs, and a pre-refresh clone still queries its old
  V10 snapshot.
- Benchmark RED: seven missing V10 `QuerySample` fields. GREEN: focused field
  test 1 passed / 41 filtered, header test 1 passed / 41 filtered, and the full
  `production_bench` example target 42 passed. All six row formats have the same
  arity as their respective headers.
- Focused Python consumers: storage-layout assembler 9 passed; frozen
  physical-GET validator 6 passed.
- Existing V10 continuation regression: 1 passed / 554 filtered.

### Fresh verification

- `cargo --locked fmt --all -- --check` first found formatting differences;
  `cargo --locked fmt --all` applied them. `git diff --check` is clean.
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`:
  passed with no issues.
- `cargo test --locked --workspace --all-targets --all-features --no-run`:
  exited 0 with no diagnostics. This was compilation only; no broad test suite
  or qualification arm was run in the fix round.

### Remaining concern

The V10 schema is a deliberate break. Existing unversioned V9 campaign
artifacts remain immutable historical evidence and are intentionally rejected
by current-schema consumers; they must not be used for old/new comparisons.

## Fix round 2/5 — terminal deadline sampling, snapshot pins, and fail-closed consumers

### Root causes and repairs

1. The first resident V10 deadline gate still followed coverage resolution and
   descriptor/root setup, and the final latency sample preceded CPU scoring,
   sorting, hit construction, and optional vector materialization. An expired
   request now returns before descriptor/root setup with zero resident work,
   and termination is sampled again after result construction. `MaxLatency`
   has precedence over byte, segment, and complete outcomes.
2. The four-generation cache retained metadata only by recency; index clones
   did not own immutable references, so cache churn could reintroduce four
   setup GETs. Each primary or named snapshot now pins its validated base and
   optional delta `Arc`. Open and refresh prepare pins before handoff, and
   every writer publication that replaces the resident PQ artifact repins the
   new manifest. Cache and pin hits are both revalidated against declared
   vector count, subspace count, and vector element type.
3. Current query CSV consumers did not all enforce the new contract, and the
   storage-layout assembler filtered rows before validation. A shared schema
   module now requires the exact `borsuk-production-bench-v10` version and all
   nine V10 telemetry fields on every source row. The artifact validator,
   publication validator, and assembler validate the full input before any
   phase/cohort selection; missing, unknown, V9, mixed, and off-cohort stale
   rows fail closed.

### Strict TDD evidence

- Pre-setup/final deadline RED first failed to compile because the planned
  termination helper did not exist. With a temporary test stub, both assertions
  failed: 0 passed / 2 failed / 555 filtered; the expired setup path performed
  four GETs and the post-materialization result incorrectly reported
  `Complete`. GREEN: 2 passed / 555 filtered with zero setup GETs and terminal
  `MaxLatency`.
- Cached-reference validation RED: 0 passed / 1 failed / 557 filtered because
  a cached checksum accepted a mismatched vector count. GREEN: 1 passed / 558
  filtered, covering vector count, subspaces, and element type.
- Snapshot-pin RED: 0 passed / 1 failed / 558 filtered because an evicted
  primary snapshot repeated four setup GETs. GREEN: 1 passed / 558 filtered;
  primary and named snapshots perform zero setup GETs after cache clear and an
  old clone remains queryable through V10.
- Artifact-consumer RED: four table cases failed to raise for missing/unknown/
  V9 schema or missing telemetry. Publication-consumer RED: three cases failed
  to raise for unknown/V9-mixed/missing telemetry. Assembler RED failed because
  no pre-selection schema hook existed. GREEN: the combined three-module
  Python consumer suite passed 29 tests, including off-cohort stale rows.

The first deadline compile check and the following assertion run briefly
appeared as overlapping process groups in controller telemetry even though the
first command had returned; both groups were gone when inspected. Every later
Cargo launch used an explicit process and memory-pressure guard, with only one
Cargo command active at a time.

### Focused verification

- Delayed resident deadline integration: 1 passed / 45 filtered.
- Invalid-next refresh rejection: 1 passed / 45 filtered.
- Refresh preload, concurrent first queries, and old snapshot: 1 passed / 45
  filtered.
- Full paged compaction resident-PQ rebuild: 1 passed / 558 filtered.
- Purge/re-add publication path: 1 passed / 155 filtered.
- Python direct-consumer suites: 29 passed.
- `cargo fmt --all -- --check`: passed after applying the formatting delta
  reported by its first check.
- `git diff --check`: passed.

One initially attempted namespaced library test used `--exact` without the
module prefix and therefore selected 0 tests / 559 filtered; it was immediately
rerun with the correct filter and produced the compaction result above. Per the
round controller, no broad workspace test, Clippy, or no-run gate was launched.

### Remaining concern

The exact V10 schema requirement is intentionally incompatible with historical
V9/unversioned query samples. Those artifacts remain immutable, but current
assemblers and publication validators will reject them rather than silently
mix architectures.

## Fix round 3/5 — public finalization and transactional resident pins

### Root causes and repairs

1. Resident V10 correctly sampled its own work, but the public search path could
   subsequently merge and re-sort a live-WAL execution, construct replacement
   hits/vectors, and return after the deadline while retaining `Complete`.
   Every public vector-search return now passes through one authoritative final
   sample after all result construction and WAL observation. Expiration changes
   the terminal reason to `MaxLatency` and recall to degraded; storage/decode
   errors still return as errors before a report exists. Named-vector routing
   recursively invokes this same finalizer.
2. Incremental maintenance published a rebuilt delta without installing its
   pins. In addition, an unchanged refresh preloaded primary/named candidates
   but returned before assigning them. Maintenance now installs the prepared
   base/delta pair after its CAS, and a no-op refresh repairs pins for every
   modality before returning `false`.
3. Purge, compaction, lane drain, delta refresh, exact-fringe publication, and
   full resident rebuild published first and then performed a fallible preload.
   Candidate manifests now preload and validate descriptor plus roots before
   any publish. After a successful publish the prepared `Arc`s are installed
   without I/O. A failed preparation leaves both the handle and `CURRENT`
   unchanged; a concurrent maintenance loser discards its candidates and
   retries from refreshed state.

### Strict TDD evidence

- Public deadline fixture exploration first exposed two invalid segment-path
  fixtures (each 0 passed / 1 failed / 559 filtered). After keeping the V10
  artifact current and appending a true live-WAL record, the accepted RED was
  0 passed / 1 failed / 559 filtered: the report was V10, contained two merged
  hits, observed WAL records, exceeded 100 ms at the controlled merge boundary,
  and failed only because it returned `Complete` instead of `MaxLatency`.
  GREEN: 1 passed / 561 filtered. The complete resident-deadline family then
  passed 3 / 559.
- No-op refresh RED repeated four descriptor/root GETs after candidate preload
  and cache eviction. The first maintenance fixture changed segment topology
  without changing deterministic descriptor content, so it was rejected as a
  pin-generation test. The corrected delete-driven maintenance RED changed the
  delta checksum and failed 0 passed / 1 failed / 561 filtered with four setup
  GETs after eviction. Combined GREEN: 2 passed / 560; the current maintenance
  snapshot and its old clone both remained zero-GET queryable.
- Transaction RED injected the setup GET after purge construction and failed
  0 passed / 1 failed / 47 filtered: the error was returned after the handle had
  already advanced from version 5 to 10. The final isolated maintenance fault
  fixture fails descriptor GET three (after opening the current base and delta)
  before the CAS. GREEN: 1 passed / 47 filtered, with both the handle and a
  clean reopen of `CURRENT` retaining the pre-maintenance version.
- Successful purge pin installation passed 1 / 47 before and after the repair:
  exactly one descriptor plus three roots are validated during the operation,
  and the first post-publish V10 query performs zero setup GETs.

### Focused verification

- Lane-drain base/delta publication: 1 passed / 47 filtered.
- Invalid-next refresh rejection: 1 passed / 47 filtered.
- Refresh preload, concurrent queries, and old snapshot: 1 passed / 47 filtered.
- Full paged-compaction resident rebuild: 1 passed / 561 filtered.
- Cached-reference metadata validation: 1 passed / 561 filtered.
- `cargo fmt --all -- --check`: passed after applying its reported formatting
  delta.
- `git diff --check`: passed.

Every Cargo launch after the required process and memory-pressure guard ran
alone in `target-task4-fix3`. Per controller instruction, no broad workspace
suite, Clippy, no-run gate, AWS operation, or benchmark CSV inspection ran.

### Remaining concern

The final deadline sample is intentionally terminal reporting, not
preemptive cancellation of CPU work already in progress. It prevents a late
result from being mislabeled and does not start any new work after observing
expiration; hard CPU cancellation would require a separate cooperative scoring
design.

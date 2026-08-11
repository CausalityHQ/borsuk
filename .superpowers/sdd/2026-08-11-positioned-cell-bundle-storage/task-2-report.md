# Task 2 evidence report: pure shared row-bundle packer

Date: 2026-08-11

Base: `3a6674e` (`devbox/prod-ready-v9`, equal to `origin/main` at task start)

Status: implementation and scoped verification complete; uncommitted pending controller review

## Scope and production boundary

This slice adds pure construction, authentication, reopen, and point-read code only. It does not edit `index.rs`, `manifest.rs`, or the production flush path. The existing live hash-owner fallback in `index.rs` remains a correctness blocker that Task 4 must delete at the atomic switch to the durable directory. No legacy format reader or compatibility path was added.

Owned source changes:

- `crates/borsuk/src/row_bundle.rs` (new)
- `crates/borsuk/src/lib.rs` (module declaration; documented dead-code boundary until Task 4)
- `crates/borsuk/src/format.rs` (`validate_positioned_route_plan` exposed crate-locally for exact packer validation)

The modified plan/spec documents shown by `git status` are controller-owned and were neither edited nor reverted by this task.

## Implemented authority and APIs

- Canonical schema-homogeneous row batches validate exact system columns, one modality/projection/assignment identity, global canonical ordering, and exact one-for-one coverage of catalog-routed and analyzer-routed route-plan data rows. Extra, missing, duplicated, or inconsistent routes fail closed.
- Row bundles are stock Parquet 2.0 with ZSTD level 3, 64 MiB production target, 128 MiB format hard cap, 4 MiB row-group target, 8 MiB row-group hard cap, and at most 16 row groups. Record-ID bloom FPP/NDV, data-page limits, bloom defaults, and writer version are pinned.
- Every bundle records a full-object BLAKE3 checksum plus independently authenticated footer, contiguous row-group data, record-ID bloom, and discovered page-index ranges. External Parquet `file_path` is forbidden. Page indexes remain authenticated authority but are not fetched because page-index decoding is disabled.
- `VerifiedRange` stores a checked end; `BoundedChunkReader` translates offsets into verified buffers and rejects overflow, overlap, escape, or unauthenticated reads. Footer interpretation requires authenticated trailing `PAR1`.
- Summary shards, summary roots, rosters, generation roots, and directory roots are stock Parquet with exact schemas and explicit row/object caps checked from footer metadata before batch construction. Summary/root schemas contain no centroid.
- Summary authority persists assignment kind/checksum, nullable route identity, global and exact canonical record-ID bounds, row count, mutation bounds, bundle length, footer authority, and row-group range authority.
- Immutable per-run summary shards are reused byte-for-byte. The generation root is O(active levels), rejects duplicate/overflowing levels, persists exact role-byte totals, and carries the authenticated directory-root reference, making it a complete recovery point.
- Production construction emits each completed bundle, summary shard, summary root, roster, and generation root through `RowBundleObjectSink`. Returned production structs retain references/metadata, not run-sized byte bodies. Peak accounting is maximum staged encoded object bytes, not total retained corpus bytes or OS RSS.
- The run pack API takes an explicit target level. Consecutive immutable publications are possible; merge/promotion policy remains Task 5 scope.
- `DirectoryPartition(u8)` is fixed at 256 partitions and independent of logical-cell count. Each partition/level is an immutable, bounded Arrow IPC V5 ZSTD run; its Parquet root persists exact footer and batch authorities. A point read transfers at most one independently authenticated IPC batch per active level and never needs a full-object read or cache for correctness.
- Directory whole-object overflow is a non-retryable split error. Only an authenticated batch-range overflow can reduce rows per batch.
- Directory MVCC persists the complete `MutationState` stamp and operation, chooses the greatest canonical state, preserves original owner, and rejects equal-version/different-digest or equal-version/different-owner conflicts. Unknown IDs return `DirectoryLookup::Unknown`; there is no synthesized-owner fallback in the new API.
- `MaterializedLookupAuthority` is typed: catalog lookup requires explicit modality/projection/assignment checksum plus stable `DirectoryOwnerState`; analyzer lookup requires explicit modality/projection/assignment checksum plus mutation state and cannot fabricate epoch/cell authority. Sparse and text analyzer rows are readable, and wrong assignment kinds fail rather than silently returning `None`.
- `open_row_bundle_generation` authenticates the directory root and every run roster/summary root once in one batched object fetch. `lookup_materialized_row_opened` uses the retained verified authority, selects at most one exact non-overlapping summary shard per run by the full canonical boundary, batch-fetches intersecting shards, then performs at most two batched range phases (all footers first; data/bloom only after footer row-count/schema validation). It never refetches roots/rosters or depends on a cache.
- Compaction emits rewritten physical row-bundle objects while observing zero directory-object writes and preserving directory references byte-for-byte.

## TDD evidence

All Cargo commands used `RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper` and `SCCACHE_DIR=/data/cache/sccache`; the global Cargo lane was checked before each invocation and only one Cargo process ran at a time.

Important honest REDs and complete logs:

- Initial/cold API RED: missing durable directory/generation/root/roster decoders and reopened lookup, session `39488`, full log `~/.local/share/rtk/tee/1786486928_cargo_test.log`.
- Exact non-overlap and footer row-cap RED/first compile stderr: session `57069`, full log `~/.local/share/rtk/tee/1786487465_cargo_test.log`. The complete stderr was read; it contained the four local issues subsequently fixed (missing validators, invalid `MutationStamp` ordering use, decoder cap argument mismatch, and test borrow conflict).
- First runtime RED: session `34569`, full log `~/.local/share/rtk/tee/1786487595_cargo_test.log` (exact Arrow list child nullability).
- Second runtime RED: session `45294`, full log `~/.local/share/rtk/tee/1786487644_cargo_test.log` (two fixture/authority mismatches).
- Format-cap and footer-first rejection RED: session `65619`, 16 passed / 2 intended failures, full log `~/.local/share/rtk/tee/1786488279_cargo_test.log`.
- Opened-generation/typed-authority RED: session `92273`, full log `~/.local/share/rtk/tee/1786488770_cargo_test.log`.
- First opened implementation compile failure: session `69665`, exactly two test `Bytes` mismatches, full log `~/.local/share/rtk/tee/1786489185_cargo_test.log`; same no-run compile then passed in session `86474`.
- Adversarial review RED batch: session `95526`, 17 passed / 6 failed, full log `~/.local/share/rtk/tee/1786489445_cargo_test.log`. It proved missing metadata sink emission, unused page-index fetches, missing explicit `PAR1` rejection, missing prefetch authority cap, wrong directory overflow classification, and the opened range-cache interaction.
- Exact-shard pruning RED: session `68896`, one point incorrectly selected 7 same-modality shards, full log `~/.local/share/rtk/tee/1786489827_cargo_test.log`; the exact canonical-boundary implementation passed in session `5917`.
- First strict Clippy RED: session `83366`, full log `~/.local/share/rtk/tee/1786487913_cargo_clippy.log`. Final review-fix Clippy RED: session `66835`, exactly two local lints, full log `~/.local/share/rtk/tee/1786490054_cargo_clippy.log`.

The tests cover the requested 3/2K/16K logical-cell scaling, real constructed 1K/8K/18K prior-summary authorities, fixed directory partition stability across 1/2K/16K catalogs, run-root O(active runs), exact role totals, no centroids, sparse/text analyzer round trips and reads, range overflow/escape/corruption, one nonzero-offset IPC batch per active directory level, directory root round trip/corruption/overlap, MVCC newest/delete/conflict precedence, physical bundle rewrite with observed zero directory writes, generation level-cap rejection, opened cold recovery without construction state, footer-first declared row-count rejection, hard format caps, no unused page-index GET, and trailing `PAR1` rejection.

## Independent review disposition

The controller-provided Opus review was treated as untrusted and each finding was checked against current code.

- Accepted and fixed: C1 opened/cache-independent authority and batched hot path; C2 explicit modality authority; C3 analyzer readback; I1 observed physical rewrite/zero directory writes; I2 real prior summaries and removal of the fabricated GET metric; I3 explicit target level; I4 sink-streamed metadata; I5 no unused page-index fetch; I6 generation directory-root reachability; I7 non-retryable whole-object overflow.
- Accepted and fixed minor findings: writer/decoder cap symmetry, trailing `PAR1`, redundant decoded-row comparison, and uncorrelated high-entropy directory overflow IDs.
- No substantive finding was rejected. Task 5 merge/promotion policy was deliberately not pulled into this slice.

## Final scoped verification

- `cargo test -p borsuk row_bundle::tests --lib` — session `71977`, **23 passed, 578 filtered**, exit 0.
- `cargo test -p borsuk positioned_route_plan --lib` — session `46987`, **7 passed, 594 filtered**, exit 0.
- `cargo test -p borsuk mutation::tests --lib` — **9 passed, 592 filtered**, exit 0.
- `cargo clippy -p borsuk --lib --tests -- -D warnings` — final session `97519`, **no issues**, exit 0.
- `python3 scripts/check_repo_policy.py` — exit 0. (`python` is unavailable on this devbox; the first literal `python` invocation exited 127 before the command was rerun with `python3`.)
- `python3 -m unittest scripts/test_check_repo_policy.py` — **31 passed**, exit 0.
- `cargo fmt --all -- --check` — exit 0.
- `git diff --check` and `git diff --no-index --check /dev/null crates/borsuk/src/row_bundle.rs` — exit 0, including the untracked new file.

Per controller instruction, no broad/full repository gate, commit, push, benchmark, AWS action, PR, or incomplete CSV read was performed.

## Remaining switch-time concerns

1. Task 4 must delete the existing `index.rs` hash-owner fallback when the durable directory becomes authoritative and atomically wire generation/directory authority into the manifest/flush path. Until then the repository is intentionally not production-ready.
2. Task 5 owns leveled merge/promotion selection. This slice only validates and persists explicit target levels and immutable per-level authority.
3. Production flush remains disabled, so `local_index.rs` integration was intentionally not changed in this pure construction slice.
4. No commit exists yet; controller review/approval is required before committing, and pushing is prohibited for this task handoff.

# Task 2 report — authenticated identity projection before exact vectors

## Status

DONE

## Commits

- `45577e8` — `search: authenticate identities before global rerank`
- `922fdb4` — `storage: split global exact vectors into V6 objects`

## Implementation

- Split resident global rerank into an authenticated identity/MVCC phase and a winner-only exact-vector phase for both fused and non-fused global paths.
- Fused base and delta still contribute at most `C` approximate candidates per layer. Identity rows are authenticated and decoded, greatest mutation stamps are resolved per `RecordId`, tombstones are applied, and live candidates are ordered by approximate distance then `RecordId` and truncated to `C` before exact-vector I/O.
- Removed the global rerank full-object vector-cache shortcut. Exact reads now fetch only selected fixed-width rows, validate each row against its authenticated row-integrity digest, decode it, and exact-score at most `C` winners.
- Added required `GlobalPqChunkRef::identity_checksum`, now using domain `borsuk.global-pq.identity.v6\0` and u64-length-prefixed raw Arrow buffers in the required order. Added required `identity_values_size_bytes` so the exact raw values buffer is authenticated without including Arrow padding or parsing unauthenticated offsets.
- Identity envelopes are checksum-verified before validation, parsing, or caching. Cache keys include the identity checksum. Offset and fixed-width envelope validation fails closed.
- Advanced descriptors exclusively to `typed-columns-v6-dual-arrow`; V5, missing identity checksums, and missing/empty/equal exact-object paths are rejected with no compatibility reader/default/alias.
- V6 writes two standard Arrow IPC files per content-addressed bundle: scan codes, ordinals, and authenticated identity/MVCC columns in one object; exact-vector record batches in a distinct object with independent offsets. Persistence, reads, storage accounting, and GC all track both objects.
- Identity prefetch ends at the row-integrity buffer. With exact vectors removed from the scan object, the range planner can safely coalesce nearby scan/identity batches again while preserving the existing 8 MiB/16-stripe query-local and 32-read/32 MiB wave envelopes.
- Added serde-default `SearchReport` counters `global_identity_rows_resolved` and `global_exact_vectors_fetched`, including zero initialization, tracing, hybrid summation, and execution-merge summation.
- Updated benchmark-report test fixtures for the two required `SearchReport` counters.

## TDD evidence

- Initial RED: `fused_resident_global_search_keeps_a_moved_upsert_visible` failed because `global_identity_rows_resolved` serialized as `Null` instead of `2`.
- GREEN: the same regression passes with two identity rows resolved, one exact row fetched, `records_scored <= 1`, aligned current vector output, and identical behavior under scan-only cache policy.
- Review RED: `global_pq_code_reads_never_coalesce_distinct_arrow_batches` failed because the old planner returned two groups instead of three and could span an earlier exact buffer.
- GREEN: each Arrow batch now receives an independent code/identity range, and every planned range ends no later than that batch's exact-vector start.
- Follow-up RED: `resident_global_pq_search_returns_identity_without_physical_vector_sidecars` observed 9 object-store GETs against its unchanged `<= 4` ceiling after per-batch grouping disabled scan coalescing.
- GREEN: V6 dual Arrow objects remove exact bytes from scan ranges, restore safe cross-batch scan/identity coalescing, and pass the original request ceiling without exact prefetch or a weakened assertion.
- Review RED: a V6 descriptor could name the same path for the independent scan and exact coordinate spaces.
- GREEN: descriptor construction now rejects equal paths, with missing, empty, and equal exact-path regressions; targeted re-review found the issue resolved.
- Added fail-closed corruption coverage for an authenticated stale base candidate that would lose to the current delta generation.
- Added exact-raw-buffer checksum coverage proving Arrow padding changes do not alter `identity_checksum`.
- Added signed/monotonic/final-offset and fixed-width identity-envelope validation coverage.
- Added V6 acceptance and V5/missing required-field rejection coverage, plus independent standard-Arrow readability for both objects and inter-batch padding coverage.

## Verification

All commands used the required `RUSTC_WRAPPER`, `SCCACHE_DIR`, and `CARGO_TARGET_DIR` environment for Rust compilation.

- `cargo test -p borsuk --lib fused_ -- --nocapture` — 4 passed, 0 failed.
- `cargo test -p borsuk --lib resident_global_pq_search_returns_identity_without_physical_vector_sidecars -- --nocapture` — 1 passed, 0 failed with the original request ceiling.
- `cargo test -p borsuk --lib global_pq_code_read -- --nocapture` — 6 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_bundle_ -- --nocapture` — 3 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_identity_validation_enforces_signed_offsets_and_fixed_widths -- --nocapture` — 1 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_chunk_references_accept_standard_arrow_inter_batch_padding -- --nocapture` — 1 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_sidecar::tests -- --nocapture` — 19 passed, 0 failed.
- `cargo test -p borsuk --test group_commit cold_search_overlaps_independent_global_base_and_delta_reads -- --nocapture` — 1 passed, 0 failed.
- `cargo clippy -p borsuk --all-features --all-targets -- -D warnings` — passed with no issues.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

Independent read-only review found the earlier cross-batch exact-byte prefetch and identity-padding checksum issues, then found the V6 equal-path issue. All three were fixed, covered by regressions, and confirmed resolved by targeted re-review.

## Concerns

- The required per-chunk exact-object path raises the 100M-row descriptor fixture from below 11 MiB to 12,111,332 bytes, still below its updated 12 MiB cap.
- Per the task brief, no full workspace gate, AWS inspection, or campaign CSV inspection was performed.

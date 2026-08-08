# Task 2 report — authenticated identity projection before exact vectors

## Status

DONE

## Commits

- `45577e8` — `search: authenticate identities before global rerank`

## Implementation

- Split resident global rerank into an authenticated identity/MVCC phase and a winner-only exact-vector phase for both fused and non-fused global paths.
- Fused base and delta still contribute at most `C` approximate candidates per layer. Identity rows are authenticated and decoded, greatest mutation stamps are resolved per `RecordId`, tombstones are applied, and live candidates are ordered by approximate distance then `RecordId` and truncated to `C` before exact-vector I/O.
- Removed the global rerank full-object vector-cache shortcut. Exact reads now fetch only selected fixed-width rows, validate each row against its authenticated row-integrity digest, decode it, and exact-score at most `C` winners.
- Added required `GlobalPqChunkRef::identity_checksum`, using domain `borsuk.global-pq.identity.v5\0` and u64-length-prefixed raw Arrow buffers in the required order. Added required `identity_values_size_bytes` so the exact raw values buffer is authenticated without including Arrow padding or parsing unauthenticated offsets.
- Identity envelopes are checksum-verified before validation, parsing, or caching. Cache keys include the identity checksum. Offset and fixed-width envelope validation fails closed.
- Advanced descriptors exclusively to `typed-columns-v5`; V4 and missing checksum fields are rejected with no compatibility reader/default/alias.
- Identity prefetch ends at the row-integrity buffer. Code-read groups are restricted to one Arrow record batch so a multi-batch contiguous range cannot cross an earlier batch's exact-vector bytes. Existing 8 MiB/16-stripe query-local and 32-read/32 MiB wave envelopes remain in place.
- Added serde-default `SearchReport` counters `global_identity_rows_resolved` and `global_exact_vectors_fetched`, including zero initialization, tracing, hybrid summation, and execution-merge summation.

## TDD evidence

- Initial RED: `fused_resident_global_search_keeps_a_moved_upsert_visible` failed because `global_identity_rows_resolved` serialized as `Null` instead of `2`.
- GREEN: the same regression passes with two identity rows resolved, one exact row fetched, `records_scored <= 1`, aligned current vector output, and identical behavior under scan-only cache policy.
- Review RED: `global_pq_code_reads_never_coalesce_distinct_arrow_batches` failed because the old planner returned two groups instead of three and could span an earlier exact buffer.
- GREEN: each Arrow batch now receives an independent code/identity range, and every planned range ends no later than that batch's exact-vector start.
- Added fail-closed corruption coverage for an authenticated stale base candidate that would lose to the current delta generation.
- Added exact-raw-buffer checksum coverage proving Arrow padding changes do not alter `identity_checksum`.
- Added signed/monotonic/final-offset and fixed-width identity-envelope validation coverage.
- Added V5 acceptance and V4/missing/nonempty identity-checksum rejection coverage.

## Verification

All commands used the required `RUSTC_WRAPPER`, `SCCACHE_DIR`, and `CARGO_TARGET_DIR` environment for Rust compilation.

- `cargo test -p borsuk --lib fused_ -- --nocapture` — 4 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_code_read -- --nocapture` — 6 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_bundle_ -- --nocapture` — 3 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_identity_validation_enforces_signed_offsets_and_fixed_widths -- --nocapture` — 1 passed, 0 failed.
- `cargo test -p borsuk --lib global_pq_sidecar::tests -- --nocapture` — 19 passed, 0 failed.
- `cargo test -p borsuk --test group_commit cold_search_overlaps_independent_global_base_and_delta_reads -- --nocapture` — 1 passed, 0 failed.
- `cargo clippy -p borsuk --all-features --all-targets -- -D warnings` — passed with no issues.
- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.

An independent read-only review found two Important issues before commit: cross-batch exact-byte prefetch and padding included in the identity-values checksum input. Both were fixed, covered by regressions, and the targeted re-review confirmed both resolved with no remaining issue in those areas.

## Concerns

- None in the implemented slice.
- Per the task brief, no full workspace gate, AWS inspection, or campaign CSV inspection was performed.

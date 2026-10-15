I recommend a narrow HOLD: one repair before running your gates. The checks the new reader adds are sound. The problem is a side effect: it also lets an old 256-row loss reducer accept v9 files, and the test that should catch this is too small to see it.

This was static only. I verified the patch hash `ad4faeb9…` and read the git blobs inside bounded scopes. I ran no Cargo, rustc or native code and edited nothing in the repo; I did save one review note to my memory. Line numbers below refer to `02044f35:crates/borsuk/examples/compare_native_replay.rs` unless noted.

## What checks out
- **Range oracle matches production.** The library's `cover_pages` (`budgeted_page_rank.rs:30`) sorts gaps by `(gap, index)` and merges the smallest ones. That is the same as merging the leftmost smallest gap repeatedly. Byte gaps and page gaps rank the same way, because only the last range can end on a partial block.
  - Row width is `D+12` and offsets start at 0 (`sq8_page_authority.rs:121`).
  - The SQ8 fetch makes one GET per range and counts exactly the range bytes (`sq8_s3_range.rs:1099-1105`), so the exact charge equality holds.
  - The baseline planner always sets `primary_pages_retained: 0`, and `target_pages` is at most 256 (`two_bit_generation.rs:820-843`).
  - The covered-page and bridge counts agree with the runner's per-range calculation, including a partial last block.
- **Stripping fields does not bypass any check.** File headers are still compared byte-for-byte as JSON against the expected config. The renamed `v8` view exists only in memory. The query view skips the old scale query branch, but every check from that branch is either repeated in the v9 code or done by `validate_geometry`. A `diagnostic` field must be present exactly when the run is a traced panel, and the slot order is still checked.
- **Real v9 output has the right shape.** The runner's identity and bound-input rows (`check_cohere_native_baseline.rs:2609-2660`), its `QueryDiagnostic` fields (`:1618`) and its terminal summary all match what the reader expects.
- **Pre-open admission is real.** Every check in `read_candidate` runs before `read_run_with`. The refusal test asserts the `"v9 panel admission:"` prefix, so a reader that opened the file anyway would fail it with "No such file".

## Defects

**1. MEDIUM, fix before the gates: the shared reader now lets `reduce_loss` accept v9.**
- **Cause:** `current` depends only on the expected identity schema (`:1618-1622`), so every reducer using the v4 scale config now accepts v9 results. `loss_coverage` (`:5918-5990`) still assumes 256-row SQ8 pages:
  - `page = width*256` with ranges aligned to it
  - the selected blocks must be a subset of the SOURCE pages
  - the selected blocks must contain `primary_page`
- **Counterexample:** a 1M v9 plan with `selected_pages=[5]` and `ranges=[[165760,198912]]` fails with "loss SQ8 aligned ordered cover". That failure is safe, because the row mapping comes from byte offsets, so a coincidental pass would not produce wrong rows. But the error is misleading.
- **The green test proves nothing about v9:** `sealed_loss_actual_native_panel_reconciles_recall_and_refuses_before_truth` runs on `Shape::tiny(32)`, so rows=32. That is one block, where 32-row and 256-row pages are the same thing. `actual_native_scale_runner_reducer_seal_count_mismatches` has the same limit, so neither real-runner test ever exercises merging, bridges or a partial tail.
- **Same pattern elsewhere:** `source_utilization_query` (`:4284-4293`, and the `"baseline_page_rows":256` label at `:4435`) has no identity-schema gate either.
- **Smallest fix:** make `reduce_loss` (and source-utilization) refuse a v9 expected identity with an explicit "historical 256-row v7/v8 only" error. Then change that test to assert the refusal, or relabel its fixture as v8 like the leaf-coverage helper does. Optionally add one real-runner fixture at `Shape::tiny(257)`, which `tiny` allows: 9 blocks with a 1-row tail.

**2. LOW: production pins are incomplete.** `read_candidate` pins only the five input digests. It does not pin `generation_root_sha256`, the identity's binary, runner and component source SHAs, `producer_authority`, the backend etag or `fetch_parallelism`. A panel from a different router generation or a modified runner would pass admission. Add these from the frozen launch receipt before any CLI uses this helper.

**3. LOW: the admission row is not checked, and the fixture contains values the producer cannot emit.**
- For the 30-GET baseline, the producer always emits `trace_ranges == sq8_get_budget` and `host_read_cap_bytes == 131072`. The fixture has 128 and 16384.
- The reader checks neither, nor `trace_peak_charged_by_library_at_traced_admission == trace`, nor `diagnostic_bytes_per_active_query <= diagnostic_cap_bytes`.
- Fix: add those equalities in current mode and correct the fixture.

**4. LOW: the 6 MiB memory model is not an upper bound.**
- The reader allows lines up to `LINE_CAP` (256 KiB). `validate_row` keeps one parsed JSON tree alive while `validate_v2_row` builds a second.
- A 256 KiB line of small integers makes roughly a 4 MiB tree, so two trees exceed 8 MiB.
- The model holds only for lines within the producer's 64 KiB `PANEL_LINE_CAP`. Either enforce that limit in current mode or state the assumption.

## Test and code notes
- Four of the 15 "semantic negatives" (row 0 schema, row 1 rows/byte-budget/truth) are rejected by pre-open admission, not by the reader. None of them tests a file whose headers differ from the caller's expected headers.
- The two current-mode cap checks (`:1810-1823`) and the `validate_geometry` call re-read `sq8_get_budget`/`sq8_byte_budget` from the config four times. Parse them once.
- `underfilled_queries == 0` in `read_candidate` repeats the `returned.len()==K` check.

## Still unverified
- **Pass/fail of the targeted tests:** whether the five targeted and four new tests pass needs native execution. The fixture's 1 SQ8 GET per query is consistent with its terminal totals, but I could not confirm the base fixture statically.
- **Behaviour against real producer output:** non-trivial ranges have only been checked against the hand-built `geometry2` cases and the library's own oracle test, never against real producer output.
- **Remaining gates:** the full suite, Clippy, the workspace test build and release are still unrun.

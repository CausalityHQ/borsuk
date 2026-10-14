**Verdict: hold. Not ready for native qualification at a304e580.** Two problems need fixing first. Both fixes go in the same small edit, which then needs a new revision and new gate runs. I compiled nothing; finding 1 comes from reading the source, and the gates already running will confirm or refute it.

## Blocker 1: the change should break two of the seven running gates

- **Cause:** `crates/borsuk/examples/compare_native_replay.rs:38-41` pulls `budgeted_page_rank.rs` into the example as `mod source_cover`. Inside the example, `crate` means the example itself.
- **What breaks:** the new code refers to `crate::sq8_page_authority::PageAuthority` at `budgeted_page_rank.rs:67` (and `:568` in the tests). The example has no `sq8_page_authority` module and no import that could stand in for one. That path can't resolve there, so the example fails with error E0433. `#[allow(dead_code)]` doesn't help with this kind of error.
- **Which gates:** `scripts/check_rust_test_build.sh` runs `cargo test --workspace --all-targets --no-run`, and workspace Clippy also uses `--all-targets`. Both compile this example, so I expect both to exit non-zero. The lib-only gates (compile, the two exact tests, the 12 planner tests, release-lib) can pass, but they don't qualify the revision on their own.
- **Why the test-only RED commit didn't catch it:** at 4558b11b the new references sat only in `#[cfg(test)]` code. A normal build of the example skipped that code.
- **Smallest falsifier:** a bounded `cargo check --locked -p borsuk --example compare_native_replay` on a304 should report E0433 at line 67.

## Blocker 2: a two-bit authority gets the wrong geometry and still returns `Ok`

- **Cause:** `PageAuthority::load_two_bit` builds the same `PageAuthority` type, with `sq8: false` and `record_bytes = padded/4 + 8`. The new planner never calls `is_sq8()`, and at `budgeted_page_rank.rs:83` it rebuilds row width as `dimensions + 12` instead of using the authority's `record_bytes`. So "authority-derived geometry" is only true for SQ8 authorities.
- **Existing practice:** the current SQ8 consumer already refuses non-SQ8 authorities at `sq8_s3_range.rs:851`.
- **Falsifier:** use the existing two-bit test fixture (`sq8_page_authority.rs:316`: 33 rows, D=5, 10-byte records, 32-row pages, `object_bytes == 330`). Planning row `[32]` with `max_gets` 1 and a large byte cap returns `Ok(ranges == [544..561])`. That is past the end of the object, and the byte charge is 17 bytes against the real 10.

## Minimal fix (one edit covers both blockers)

1. Move `SelectedSq8Limits`, `SelectedSq8Plan`, `selected_sq8_planner_payload`, `plan_selected_sq8_rows` and the `selected_sq8_rows_contract` tests out of `budgeted_page_rank.rs` and into `sq8_page_authority.rs`.
   - Nothing pulls `sq8_page_authority.rs` in by path; I checked with `git grep`.
   - From there the code can call `crate::budgeted_page_rank::cover_pages` and return `BudgetedPageError`.
   - `budgeted_page_rank.rs` goes back to its base content, so the example's `planner_source_sha256` and `shared_cover_source_sha256` (`:3852`, `:4351`) match the historical receipts again.
2. Add `if !authority.is_sq8() { return Err(InvalidGeometry) }` and use `authority.record_bytes`.
3. Add two test lines:
   - the two-bit fixture above asserts `Err(InvalidGeometry)`;
   - every planned range equals `authority.byte_range(start/page_bytes, (end-1)/page_bytes)`.

   The consumer takes inclusive page pairs (`ranges: &[(usize, usize)]` at `sq8_s3_range.rs:828`) and rebuilds byte ranges through `byte_range`. The plan returns byte ranges, so the Task 2 driver has to convert back to page pairs. The parity line pins that conversion.

## Areas checked and sound

- **Admission order:** zero limits, then roster checks (no allocation), then the checked `512N + 4096` model, all before the first allocation (the `BTreeSet`). The byte cap can only be checked after the cover is built. The GET cap holds by construction: `cover_pages` merges down to `min(runs, max_gets)` ranges.
- **Scratch bound:** the bound holds, with about 4× headroom. My worst-case derivation per input ordinal:
  - Building the `BTreeSet`: Rust's `BTreeSet::from_iter` first collects into a Vec (8N) and sorts it with up to 8N of scratch (on the heap when N > 512), then bulk-builds full nodes (about 10–25 bytes per page).
  - Inside `cover_pages`: the run list (≤32R with doubling), the gap table (16R, held while the join set is collected), the join hash set (≤ about 21 bytes per bridge), and the merged and output range lists (≤48·min(R, G)).
  - Output: the final `selected_pages` list (8P).
  - Peak is roughly 125 bytes per ordinal, plus allocator headers that the 4 KiB fixed reserve covers.
  - The code comment doesn't mention the Vec and sort scratch from `from_iter`, but the margin absorbs them. The tests check only the formula boundary (6,143 vs 6,144), not real allocations. The doc comment says this correctly.
- **Hostile geometry and overflow:** `load` checks that `rows*(D+12)` fits. In `cover_pages`, `end*page_rows` cannot overflow: with one page, `end` is 1; otherwise `page_rows < rows`, so the product stays under 2·rows. Dividing by `page_rows` is safe because it is positive.
- **Valid authority:** a `PageAuthority` can only be built through `load` or `load_two_bit`. That leaves the SQ8 vs two-bit split in blocker 2 as the only trust gap.
- **Dropped or over-bridged nominees:** every selected page stays inside a run, and runs only ever grow. Exactly `runs − max_gets` of the smallest page gaps get bridged. Bridged pages are always full pages (only the last page can be partial), so the smallest page gap is also the smallest byte gap. That makes the greedy choice byte-optimal for the GET cap. Equal gaps are broken by lower index, which the tie test confirms.
- **Hand-derived expectations:** I re-derived 9,804, 9,728, 19,456 and 3,328..3,341, and all four are correct.

## Smaller points (not blocking)

- **Refusals lose information.** The planner-memory refusal and the physical-byte refusal both return `InsufficientBudget`, and a refusal throws away `planned_bytes`. The spec wants resource-exceeding candidates recorded with their geometric cost, so the Task 2 driver has to replan with an uncapped byte limit to get it.
- **Missing evidence for Task 3.**
  - When the 30-GET cap binds, bridged gaps are charged whole no matter the block size.
  - Moving from 256-row to 32-row blocks saves at most `2·G·(256−32)·row_bytes` (about 13.9 MB at D=1024 and G=30), all at run edges.
  - Any bigger saving has to come from the shortlist narrowing the physical span and the run count.
  - So Task 3 should log, per query, the run count before the cap is applied and the bridge-byte share. Without them, block granularity and the bridge cost can't be told apart. This is a structural limit, not a measured result.
- **Test gaps left after the fix:** no case with several runs where nothing needs bridging (`max_gets` ≥ runs), and no counting-allocator check of the 512-bytes-per-ordinal model.

I make no speed, quality or lifecycle claim here. I did not touch or poll the running EC2 jobs, and changed no repository files. I added one review note to my notes file for future sessions.

# V32 borrowed parent scorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make existing PQ scoring consume bounded borrowed parent rows without changing candidates or pages.

**Architecture:** Validate an immutable parent once and stream its packed codes through a constant-state cursor. Extract the existing resident scoring body into one private helper used by resident routing and object differential tests.

**Tech Stack:** Rust, existing Arrow codec, f16 centroids, V30LazyQueryTable.

**Spec:** `docs/superpowers/specs/2026-09-05-v32-borrowed-parent-scorer-design.md`

## Global Constraints

- No routing, PQ arithmetic, S3, schema or default changes.
- Preserve original f16 centroids, width24/48 and deterministic score/logical ordering.
- Blocks at most32; root64 scan524288/candidate12288 limits unchanged.
- Codec remains test-gated until a real production object consumer exists.
- One narrow local gate at a time; no scientific dataset downloads.

## Task1: Sequential validated cursor

**Files:** modify `crates/borsuk/src/v32_code_objects.rs` and its unit tests.

**Interfaces:** `V32ParentCodes::cursor(&self)->Result<V32ParentCursor<'_>>`;
`V32ParentCursor<'a>: Iterator<Item=(u64,V30PqWidth,&'a [u8])>`.
Private cursor state: borrowed parent, range ordinal, row within range,
local row, base byte offset, high byte offset. No public fields.

- [x] Add `v32_code_object_cursor_borrows_mixed_gapped_rows` using the existing literal parent and these assertions:

```rust
let mut cursor = parent.cursor().unwrap();
let first = cursor.next().unwrap();
assert_eq!(first, (10, V30PqWidth::Base24, &[1_u8;24][..]));
assert_eq!(first.2.as_ptr(), parent.base_codes.as_ptr());
assert_eq!(cursor.next().unwrap(), (11, V30PqWidth::High48, &[2_u8;48][..]));
assert_eq!(cursor.next().unwrap(), (20, V30PqWidth::Base24, &[3_u8;24][..]));
assert_eq!(cursor.next().unwrap(), (21, V30PqWidth::High48, &[4_u8;48][..]));
assert!(cursor.next().is_none());
assert!(cursor.next().is_none());
```

- [x] Add `v32_code_object_cursor_boundaries_and_rejections`: literal9-row alternating-width fixture with ranges(100,3),(200,6), bitmap0xAA,0; exact logical values100,101,102,200..205 and per-row byte labels0..8. Check empty ranges/bad packed lengths/padding reject construction, and8192-row all-base/all-high endpoints.
- [x] Run `CARGO_BUILD_JOBS=1 rtk proxy cargo test -p borsuk --lib v32_code_object_cursor_ -- --nocapture`; preserve missing cursor RED.
- [x] Implement constructor calling `validate()` once and zeroing cursor offsets. `next` reads current range and fidelity bit, borrows24/48 bytes from the matching plane, advances that byte offset and row/range positions, and returns None at exhausted ranges. Do not call random `code`/`logical` helpers per row.
- [x] Rerun the exact selector, then the codec selector; scoped fmt/Clippy/diff, review and commit the cursor slice.

Cursor checkpoint: intended missing-method RED (six E0599 diagnostics), then
2/2 focused GREEN and10/10 codec GREEN with the separate interchange test
explicitly ignored. Strict scoped library/test Clippy, fmt, docs and diff
checks passed. Astra's read-only cursor/plan review reported READY. The
cursor adds only constant state and borrows original code slices.

## Task2: Shared parent scorer and equivalence

**Files:** modify `crates/borsuk/src/v30_s3_search.rs`, private helper and tests.

**Interface:** private helper using existing Candidate/BoundedCandidates:

```rust
fn score_parent_codes<'a>(
    query: &[f32;96], centroid: &[f16;96],
    rows: impl Iterator<Item=Result<(u64,V30PqWidth,&'a [u8])>>,
    base_table: &mut V30LazyQueryTable,
    high_table: &mut V30LazyQueryTable,
    candidates: &mut BoundedCandidates,
) -> Result<()>
```

- [x] Stage `v32_borrowed_parent_scorer_matches_scalar`: use existing coherent small codebook fixtures; enumerate scalar base/high ADC per logical row using original residual centroid, sort `(score.total_cmp,logical)`, truncate to the same candidate depth. Assert exact score bits and IDs from the missing helper. Include mixed widths/gaps/ties and33+ rows.
- [x] Run `rtk proxy cargo test -p borsuk --lib v32_borrowed_parent_scorer_ -- --nocapture` for missing-helper RED.
- [x] Extract the existing loop into the declared helper: begin both tables, take32 fallible borrowed rows into fixed logical/score arrays and reused base/high refs/slot buffers; score, restore slots and insert. Propagate row/table errors without a partial successful result.
- [x] Replace resident inner loop with a fallible adapter over selected leaf ranges and `self.codes.code`, retaining range-entry observer invocation. Keep one scratch pair outside the parent loop and existing work accounting.
- [x] Add `v32_borrowed_parent_scorer_object_delivery_is_equivalent`: materialize only a coherent synthetic parent family into Arrow, decode/cursor/map(Ok), compare complete candidate bits/IDs against resident rows and independently scored rows. Use64+ pages, two roots/multiple parents, original distinct centroids; compare16/64 physical page prefixes and reversed whole-parent delivery.
- [x] Run the narrow scorer gate, then existing `v32_root64_` and `v32_s3_search_reuses_one_live_query_table_pair` gates. Fix the failing layer only, with regression tests for any discovered bug.
- [x] Run affected library checks and strict scoped Clippy, fmt/diff; request Astra read-only review; commit/push the verified slice fast-forward. Do not claim global code-plane residency has been removed.

## Shared scorer checkpoint

Missing-helper RED was the sole E0425. After extraction, one missing borrow
at the resident callsite was corrected; the final scorer gate passed3/3 in
0.05s. The independent eager test scores74 mixed-width/gapped rows and
compares40 retained candidates exactly. The object test uses256 rows,
two roots/four original parents and128 physical pages; three whole-parent
delivery orders agree with eager and production resident candidates and
physical16/64-page prefixes. Error tests cover a row-read failure and short
code after a complete32-row block, plus unused high-width overflow rejection.

Existing root64 tests passed3/3, lazy-PQ tests6/6 and serving/search tests17/17.
Strict scoped library/test Clippy passed; Astra read-only review reported
READY. Blocks may cross leaf boundaries, so the next leaf's entry observer
can run before the prior partial block is scored, while traversal order and
the observer's range-entry contract remain unchanged. No S3 latency, write
throughput or new dataset-scale result is asserted by these unit gates.

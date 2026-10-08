# research_critic

Review group: dd1e505d37bc425a
Consultation: 72b457a0d5c443c7
Reviewed source: 64d519b0da5d0d0d9eb0c4e415ec8a9f244d50ef
Status: completed; exit 0
Native execution: not performed.

# Verdict: GO for compilation, no blocker found

I reviewed only the exact commits (`git diff 7c11957d..64d519b0`, which changes only `crates/borsuk/examples/compare_native_replay.rs`), plus the producer code at `fc8a23d`. Nothing was compiled, run or edited, and no real traces were opened. Compilation, the affected tests, Clippy and a trusted replay are all still unproved.

## What I checked and found correct

- **Source closure and charge parity.** At the producer, the walked set is the semantic units plus the seed-page additions. `seed_walk` adds the units of the lowest page that are not already semantic, and `semantic_walks` records `units` in BTreeSet order. The closure is the walked units divided by 8, and the source cover is `plan_two_bit_source_cover` → `cover_pages(closure, rows, record_bytes, 256, 128)` with a 64 MiB check. The producer test (`two_bit_generation.rs:2988`) asserts `source_stats == {cover.len(), bytes}`. Cache-off is required. The replay mirrors all of this at `compare_native_replay.rs:1541–1630`. The Native100k caps also match (1024 semantic units, 16 leaves, 7 additions, walk 1031, closure 1024 pages, completion limit 2544).
- **Scored subset and ordered completion guard.** The walked units must form an ascending prefix, which matches `admit_source_walks` sorting before scoring. Each completion page must then consume its non-walked units in ascending order. A partial page is accepted only at the 2544 cap, and the cap can only cut the final page. Finally the count must equal `min(closure_units, 2544)`. This is equivalent to `rank_walked_source_with_limit` (producer lines 812–828); checking the cap before or after the walked-skip makes no difference. The page-visit order depends on scores and cannot be verified here, and the report says so. The "repeated completion page" check can never fire because unit uniqueness already prevents it; it is harmless.
- **The 32-row hindsight bound.** `cover_pages` merges the smallest (runs − allowance) gaps, which is the optimal minimum-byte cover for a fixed GET allowance. The scored units are a subset of the closure units, so the `ideal ≤ baseline` assertion is a theorem, not a hope. Calling it an "optimistic hindsight lower bound" is correct. Note that `ideal_gets` can be higher than `baseline_gets`; it is reported.
- **Direct closure SQ8 cost.** It uses the same closure, the same helper, 256-row pages, row width D+12 and 32 GETs. The SQ8 row offsets equal the source row order (producer test line 2919). The labels say cost only, no score or recall evaluated, and no new byte envelope, so no cap is silently raised.
- **Source record padding.** The replay's padding formula is identical to `RotatedTwoBitCodec::padded_dimensions` and `record_bytes` at `fc8a23d`: whole 256-dimension blocks, plus the tail rounded up to a power of two, divided by 4, plus 8. That gives 264 bytes at D1024. `budgeted_page_rank.rs`, `two_bit_generation.rs`, `semantic_unit_router.rs` and `rotated_two_bit.rs` are unchanged between `fc8a23d` and `64d519b0`.
- **Input boundary.** Duplicate keys are rejected recursively (`UniqueJson`) for both the config and every row. The config uses `deny_unknown_fields`, and the trace struct uses it too; its seven fields exactly match the producer's `TwoBitPlanTrace`. The config's identity and input rows must match the JSONL byte-for-value. The existing full SHA, end-of-file, descriptor, seal and terminal checks all still run after the callback, and the input identity and config are re-checked before publication. The report cap is enforced before the create-only write. My estimate is about 0.9 MB of report against the 2 MiB cap. Work per query is bounded at 8192 units or fewer.
- **Old command-line behaviour is preserved.** `read_run_with` now calls `read_run_observed` with a no-op callback, and the callback runs after all the existing per-query checks. The `#[path]` include brings in `budgeted_page_rank.rs` unchanged. It has no `crate::` dependencies, the let-chains are fine because it is the same package and edition, and Clippy's `duplicate_mod` lint works per crate, so it won't fire.
- **Test arithmetic.** I recomputed every hand count by hand: 6144/4608/3456, the 6156/4236/1920 tail case, 396300 for the tied gaps, 236572/229404/7168 and −2 GETs for the fragmented SQ8 case, 3072 at the cap, and the record-width table. All of them hold.

## Problems found, none blocking (ranked)

1. **The result is partly predetermined.** If a query was not completion-limited, its scored set is every unit of every closure page. Unit gaps are then exactly 8× the page gaps, so the 32-row cover is byte-for-byte the 256-row cover and the saving is zero. **A positive saving is only possible for queries whose closure exceeds 318 pages.**
   - The aggregate is therefore driven by how many queries have closures that large.
   - Suggested fix: add the free invariant `!completion_limited ⇒ ideal_bytes == baseline_bytes` next to `:1637`. If it ever fails, the arithmetic is wrong.
2. **The direct closure SQ8 cost is not compared with the historical SQ8 limits.**
   - At `fc8a23d`, `check_cohere_native_baseline.rs:615` sets the SQ8 per-query byte cap `max_query_bytes` to 16,773,120 bytes. The direct cost exceeds that whenever the closure is larger than about 63 pages, and can reach about 272 MB per query.
   - Neither the config nor the report pins this cap or counts the queries that exceed it. The "−N GETs" figure can therefore read as a favourable trade.
   - Suggested fix: pin `historical_sq8_query_byte_cap` in `validate` (`:1421`), and report a per-query exceedance flag and a total count.
   - Related: the historical SQ8 charge check at `:1650–1657` borrows `direct_sq8_get_cap` as the historical cap and bounds bytes only by the whole population. Bounding by 16,773,120 would be tighter.
3. **The cost of the actual two-wave design is missing.** The only causal way to realise any saving is: read the walked units, score them, then read completion units for the best pages. The trace already contains enough to compute the walked-only unit cover and the completion-only cover with their GET counts. Without them, a positive bound sends the next design decision back for another replay. This is optional but cheap, and the planning result already says the second wave's charges must be qualified.
4. **The "unwalked winner" test proves nothing about the code.** The toy-score assertion at `:2578–2582` tests an array literal. The meaningful check in that test is `walked_only < ideal`. Keep it as documentation only.
5. **The run could fail closed on retried requests.** The strict `failed_gets == 0` check (`:1630`, `:1650`) makes the whole replay INVALID if B1 recorded even one retried GET. Before running, confirm from B1's already-verified terminal summary that source and SQ8 `failed_gets` are both 0.
6. **Minor points:**
   - `semantic_leaves` is only sanity-bounded by the unit count.
   - The `budgeted_page_rank` unit tests now also run in the example's test binary. That is harmless duplication.
   - The only borrow pattern I could not fully confirm by eye is `max_by_key(|(_, score)| score)` over `slice.iter()`. I believe it compiles because the borrow passes through a shared-reference dereference.

## Prior rejections are not repeated

This mode adds no packing heuristic, so it does not repeat the co-selection packing failure (50/32 fitting plans → 48/33). It drops no rows from the SQ8-scored population, so it does not repeat the page-centroid loss; the direct variant scores a superset. It does not touch the SHA backend (5.26%) or the rejected four-row primitive (16–18%). The cover helper is byte-identical to the frozen producer's.

## Required before a trusted numerical replay

Items 1, 2 and 5 should be done, item 3 is optional, and none of them blocks the remote compile. After that, the root-owned gates still apply: the affected example tests, the release example build, workspace Clippy with correctness and suspicious lints, and `check_rust_test_build.sh` on `64d519b0`. The root also still has to admit the frozen inputs and bind the producer limits (`producer_provenance_verified=false`). Nothing here supports a claim about performance, quality or latency.

I also saved a short memory note of these findings for later utilization reviews.

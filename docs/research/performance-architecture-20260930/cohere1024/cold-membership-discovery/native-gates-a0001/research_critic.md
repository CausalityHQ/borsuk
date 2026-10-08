I found no correctness or authentication blocker in 811c4217. The library change is sound. The weak point is the new `--membership-pair-v2` reducer: it can't by itself support the planned ABBA performance verdict. I read everything via `git show` and ran nothing (no cargo, native, data or network), so all native tests remain unrun.

## Findings, most important first

**1. The reducer doesn't decide the ABBA result** (`examples/compare_native_replay.rs:1507-1658`)
- **One pair per reduction.** `reduce_membership_pair` reduces a single pair. The four-run ABBA becomes two separate reductions, and nothing checks that both pairs used the same control and candidate binaries.
- **Run order isn't checked.** Both configs list control first, so the reducer can't tell AB from BA. The counterbalancing claim depends entirely on outside receipts.
- **No comparison is computed.** It produces per-arm statistics only. It never calculates the preregistered p90/p95 drops (at least 5%) or the serial-QPS change. As with the earlier width test, the decision would be worked out by hand in prose.
- **An old run could pass as the control.** The runtime config holds no field unique to this run, and both arms must share a config SHA. So if the old width32 config file is reused, a historical B1/B2 run would be accepted as the control. That compares runs from different days without any check catching it.
- **Fix:** use one config that names A1, B1, B2 and A2 together. Require identical identities for runs of the same arm, and give the config a field unique to this attempt (for example an attempt-specific `scratch_parent`). Compute the threshold checks inside the reducer.
- `producer_source_commits` and the archive SHAs are only checked for format and for being different. Nothing ties them to the run evidence.
- I did verify that between the qualified producer 31eca734 and 811c4217, only `semantic_unit_router.rs` and `two_bit_generation.rs` change in the library (plus the example). So the binary difference is attributable, but only if the external freeze gate records that file list.

**2. The planned fix at `bin/check_semantic_router_scorer.rs:593` is weaker than it needs to be.** Use `assert_eq!(error.router_stats(), Sq8ReadStats::default())` instead of `verified_bytes == 0`. A router GET that failed would also show `verified_bytes == 0`; the full-struct check also rules out submitted and failed GETs, and it's the same size. Keep the existing source `failed_gets > 0` and zero-SQ8 assertions. Line 608 is now vacuous (both sides are default) but harmless.

**3. Leftover code to delete after the measurement closes** (pre-release policy says don't keep it)
- `RouterCharged` and `with_router` (`two_bit_generation.rs:67,111`) now do nothing, because router stats are always zero.
- The fixture helpers `arm_leaves`, `release_error` and `release_sibling`, plus the leaf branch in `sq8_s3_range.rs:808-823,939`, are now unused or dead. They will produce dead-code warnings in test builds; the Clippy gate doesn't deny those.
- Keep the always-zero `leaf_peak_inflight` and `router_head_*` result fields until the reducer has used them, then remove them.

**4. A redundant copy of membership stays in memory** (optional, not needed for this slice). The router keeps `membership: Vec<usize>` (8 bytes per unit), but only `validate_leaf` on the publication path uses it. The directory slice can answer that lookup with a binary search, and open can make two passes over the authenticated bytes. At 10M rows that removes about 2.5 MB, more than the new directory adds.

**5. Minor**
- The router's own check, `directory_peak <= modeled_allocation_limit_bytes` (`semantic_unit_router.rs:913-918`), compares against the construction limit and isn't cumulative, so it effectively never triggers. The real bound is the generation-level check, which is correct.
- The report text in `completed_report` (`compare_native_replay.rs:1353`) still says discovery "includes preparation/router reads", which is wrong for the candidate arm.

## Confirmed sound
- **Authentication:** the generation root pins the router root's SHA and length, the root pins membership's SHA, and each leaf's unit count and row count must match the membership-derived counts. The directory is built only from data that has already passed those checks.
- **Same results as before:** for a valid publication, the new path picks exactly the units the old leaf path would have. `validate_leaf` requires `membership[unit] == leaf`, no duplicates, and matching counts and rows. Both paths then share the seed-page completion and page-closure code.
- **The dropped byte cap is harmless:** the old `selected_leaf_bytes` limit is already implied by the leaf-count limit times the fixed record size, for both profiles. Rejections of invalid, duplicate, too-many and too-few (Fresh1m needs exactly 48) leaf selections still happen before any source read.
- **Memory is checked before allocating:** the generation-level check runs before the plane and router are opened. The leaf count is derived from `root_bytes`, which `Discovery::valid` has already confirmed is at least 512 and divides evenly, so the subtraction can't underflow and the count matches the decoded root. The arithmetic is right: 13,116 / 14,332 bytes for 3,125 units and 76 leaves.
- **Leaf validation still exists where it matters:** publication still checks leaves (`two_bit_store.rs:1289`, `two_bit_build.rs:155`), and the corrupt-leaf publish test is still there.
- No Python controller for the Cohere baseline asserts anything about router charges.

## Things the experiment can't show as designed
- **The 5% bar is close to a foregone conclusion.** At width32, discovery averaged about 34.5 ms per query, and most of that is the 16-GET leaf wave. That is about 23% of a roughly 150 ms p90. A better pre-registered prediction is per query: candidate time ≈ control time minus the removed discovery time. A large miss either way would point to an interaction, such as connection-pool warmup or a change in S3 concurrency, which is the informative result.
- Startup no longer includes the leaf HEAD request, so report startup separately; the reducer doesn't compare it.
- The membership-pair reducer's tests are also unrun. The earlier ABBA attempt failed at reduction (`EXECUTION_INVALID_CONFIGURATION`), so the reducer should pass native tests before launch.

I saved a project memory note of this review.

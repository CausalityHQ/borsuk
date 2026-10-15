# SOURCE shortlist library implementation plan

> For agentic workers: use superpowers:executing-plans inline. Root owns all native execution, source review, integration and push.

**Goal:** Make the measured256 selection a coherent Rust generation/search/lifecycle implementation.
**Architecture:** Generation v9 with32-row SQ8 authority and one existing serving path; bounded streaming top256 followed by the canonical greedy block cover.
**Tech Stack:** Existing Rust, object_store and SHA256 helpers; no dependencies or Python/TS features.
**Spec:** design.md plus integration-plan.045fe423.md; measured decision in predecessor0f46108a.

## Global constraints

Protected checkout/index unchanged. Source area is authenticated475-file native baseline6fb; delivery parent0f46108a has identical native code. No local Cargo/rustc/native/data or network experiments. All native checks on causality/eu-central-1 bounded Spot; one original handle and cleanup per job. Preserve historical/source artifacts. No default/vendor win until actual cold evidence.

## Review focus

- SOURCE256 is not SQ8 proof32: explicit plan geometry and native tail/conversion tests.
- Streaming ties/unique row visits: compare exact heap selection to independent full-sort oracle, including signed zero and negative finite scores.
- Greedy scratch refusal must not look like byte unaffordability: pre-admit simultaneous planner space and test model-minus-one before I/O.
- Remote import/retained publication/canonical ownership: native publish/reopen/corruption and old-root refusal.
- Retired generations/mutations/shared window: old pins, update/delete merge, held error drain and concurrency tests.

## Task1: Write and observe behavior failures

Files: src/two_bit_build.rs tests; src/two_bit_generation.rs source_walk_tests.
- [ ] Add source_shortlist_v9_authenticates_32_row_sq8_blocks using real builder, partial final block and authenticated manifest/sidecar; expect32, SCHEMAv9 and exact digests.
- [ ] Add source_shortlist_v9_uses_score_then_physical_ordinal using existing public planner/scorer and tied SOURCE rows; require exactly first256 physical rows' cover, not old whole-page candidate population.
- [ ] Freeze tests-only source; remote affected compile then exact tests. Record expected assertion failures; compile/environment failures are not behavioral RED.
- [ ] Preserve RED commit/receipts. Do not write production implementation before observed failures.

## Task2: Implement v9 proof and selection path

Files: src/{two_bit_generation,sq8_page_authority,budgeted_page_rank,two_bit_build,object_native_generation,two_bit_store,two_bit_index,two_bit_compaction,two_bit_gc}.rs and affected constructors/fixtures/bins/examples.
- [ ] Preserve discovery/SOURCE completion/scorer; collect top256 at actual score call, once per visited row, no bridge-only SOURCE nominations.
- [ ] Add bounded block admission by reusing canonical plan_selected_sq8_rows; pre-admit scratch before greedy trials; return precise geometry and selected ranges.
- [ ] Change generation schema9/SQ8proof32, sidecar writes/counts/remote/pub/builder/repackage allowances; keep SOURCE geometry unchanged.
- [ ] Attach explicit BudgetedPagePlan.page_rows to every constructor and update every consumer; range-to-proof conversion uses authority geometry.
- [ ] Remove semantic SQ8 clamps, preserve explicit caller caps and source caps; charge concurrent heap/planner/responses/import/retired state, add index forwarding for existing shared window.
- [ ] Add independent4097/D5 native oracle, partial-block/corruption/cap/exclusion/drain/concurrency/restart/compaction/GC coverage; update RED call signatures without weakening expected behavior.

## Task3: Qualify and measure the real library

- [ ] Stable-diff full source review reconciled against both measured algorithm and lifecycle requirements; preserve old missing-review history.
- [ ] Remote affected compile/tests and regressions; cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious; env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh; release artifacts with exact rosters/source hashes/exit statuses.
- [ ] Real-input8-query oracle parity and separate disposable smoke through the actual library. Rebuild proofs using authenticated unchanged SQ8 and original SOURCE/router identities; no production legacy converter.
- [ ] Freeze paired cold protocol and exact retained baseline/candidate binaries. Run once per declaredcell; terminal repetitions only; terminate compute at terminal. Keep false product/vendor claims until end-to-end measurements prove them.
- [ ] Commit coherent verified source/evidence and FF-push origin/main after fresh ancestry verification. Full objective remains active after this increment.

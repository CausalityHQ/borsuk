# Authenticated SQ8 Shortlist Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans. The root implements this bounded slice; existing workers/jobs remain preserved.

**Goal:** Add bounded authenticated selected-row planning and run one native quality/cost falsifier before persistentformat changes.
**Architecture:** Reuse production nomination, shortestgap cover, PageAuthority and returned-SQ8 kernels. A native diagnostic captures row scores through the existing record callback; no second routing algorithm or Python controller.
**Tech Stack:** Existing Rust/SHA256/object_store/serde; minimal existing Bash remote transport only.
**Spec:** ../specs/2026-10-14-sq8-subpage-shortlist-design.md

## Global Constraints
- Base b20abe258723635de0abf804811ab9e24ff78dda; protected realindex/worktree unchanged.
- No localCargo/rustc/native/data/GT processing; causalityEC2 only, compilerjobs1.
- Exact97.80% baseline reconstruction,97.60% prospectivefloor, no2x byte gate.
- Shortlist sizes1024/2048/4096/8192; page_rows32; SOURCE128/width64/windowOFF/cacheOFF unchanged.
- Candidate30GET/32MiB SQ8 envelope explicitly distinct frombaseline30GET/16MiB.
- One original nativejob/receipt per gate; Spot/default/immediatetermination/volumeclosure.

## Review Focus
1. Logical IDs mistaken for physicalordinals: validate actualSQ8records/sourceorder; no impliedidentity mapping.
2. SOURCEbridge rows leaked into candidateeligibility: recordexactproductioncallbackroster and refuseextras.
3. Authenticationpartialblock/bridgebytes omitted: authority-derived geometry, wholeblocks and exactbyteledger.
4. Allocation before scratchadmission or changing geometry overflows: checked preallocation model and explicitlimit refusals.
5. Candidate setup accepted withoutbaselineparity or corruptpins: authenticate allinputs, nativebaselineexactness gate first.

### Task1: Bounded selected-row SQ8 library planner
**Files:** Modify/test crates/borsuk/src/budgeted_page_rank.rs only.
**Interface:** `SelectedSq8Limits { max_gets: usize, max_bytes: usize, max_planner_bytes: usize }`; `SelectedSq8Plan { selected_pages: Vec<usize>, ranges: Vec<Range<usize>>, planned_bytes: usize, modeled_planner_bytes: usize }`; `plan_selected_sq8_rows(authority: &PageAuthority, selected_ordinals: &[usize], limits: SelectedSq8Limits) -> Result<SelectedSq8Plan, BudgetedPageError>`.
- [ ] Write tests `selected_sq8_rows_authenticated_geometry_parity_and_bridges` and `selected_sq8_rows_refuse_roster_overflow_and_planner_budget`. Cover32/256-authority, partialtail, deterministicequalgap, allselectedrows retained, exactbytes, unsorted/duplicate/out-of-range/empty rows,0limits and below/exact allocation boundary.
- [ ] Execute affectedtarget on frozen test-onlysource oncausalityEC2, retainRED cause before implementation.
- [ ] Implement authority-derived geometry and reuse cover_pages. Conservative ownedplanner bound512bytes perinputordinal+4096 fixedreserve, checked beforecreatingBTreeSet/Vectors. Borrowedinput/authority, caller shortlistheap and finalretainedpayload must be separatelycharged. Audit actualinternal capacities against thismodel.
- [ ] Run exacttests then affectedmodule tests; checkpoint onlywithnative evidence, sourceUNVERIFIED untilallrequiredgates.

### Task2: Native falsifier and decisivefixtures
**Files:** Create crates/borsuk/examples/compare_native_replay/sq8_subpage_shortlist.rs; modify parent compare_native_replay.rs onlymodule+CLIentry. Keepexisting198 tests/functions unchanged.
**Interface:** `run(config_path: &Path, config_sha: &str) -> super::Result<serde_json::Value>`, exposed as `--sq8-subpage-shortlist CONFIG SHA NEW_REPORT` through existingexclusive reportwriter. Strictpinnedconfig binds baselinecompleted/runtime/metadata/SOURCE/SQ8/request/truth/order and explicitpayloadlimits; report alwayslabels offline/no cold/vendor/productclaim.
- [ ] Author fixtures proving baselineparity first, separate shortlist/bridge/quantization loss, deterministicties, nonidentityapplication IDs, 32/256blockonlyparity and invalidinput earlyrefusal. Preserve exacttestnames in contract before remoteexecution.
- [ ] ExecuteRED then minimalimplementation using libraryplanner, current record callback, PreparedTwoBit and auth/scoring kernels. Capture no extraeligibleSOURCErows; SHA-pinnedSQ8 stream emits ephemeral32digests withoutnewpersistentroot.
- [ ] Run affectedcompile/exactfixtures/fullcomparatorregressions/release/workspacecorrectness+suspiciousClippy/actualenv-unsetjobs1fulltestbuild on exactrevision. Independentreview newlychanged admission/scoring scope beforeintegration.

### Task3: Actual-input native quality/cost falsifier
- [ ] Freeze rootactualinputpins and boundedSpot envelope frommetadata; admission beforecandidateexecution and disposableCLI/cleanupsmoke separately. Do not use syntheticPASS asactualdataproof.
- [ ] Run unchanged128nativebaseline reconstruction first, then single-pass shortlist sweep. Stopinterpretation onbaseline mismatch; retain allvalidrecall/GET/byte tradeoffs and explicitresource-exceeding candidates.
- [ ] Verify native/source/resource/instance+volumeclosure, publishcode+receipt/decision FFtomain. Choose persistentformat/lifecycle work onlyfromactualParetoresult; no freshcoldrun untilits ownqualification/admission/canary.

## Self-review
Scope covers the selectedresearch candidate while preserving currentbenchmark andfailedarm authority. Generic library API has no dataset/dimension naming. The caller-funded512P+4096 model still needs native boundary tests and independentallocation review. Driver must reproduce the oldphysicalSQ8expansion before interpretingloss; the candidate mustnot silentlygrowSOURCEeligibility. No completedclaim is made by thisplan.

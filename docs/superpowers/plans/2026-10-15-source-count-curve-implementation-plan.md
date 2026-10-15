# SOURCE shortlist-count curve implementation plan

> Execute inline with the existing autonomous root authority; use executing-plans/TDD. Preserve protected checkout/index and qualified6459993d; author private snapshots/refs. No new worker/research consultation.

**Goal:** measure the recall versus planned GET/byte frontier for smaller SOURCE nominee prefixes, original physical layout.
**Architecture:** reuse the shared engine, SOURCE score capture, greedy block planner, scorer and quality code. A private example module handles bounded four-count configuration, planning and control/report checks.
**Tech stack:** existing Rust/serde/sha2; minimal existing Bash/Perl exact-experiment glue only.
**Spec:** design.md and closed prior experiment launch-virtual-full-a0001/decision.json.

## Constraints

1M Cohere/D1024/cosine/k10, same1,000 requests/truth and source/order/SQ8/router pins; SOURCE128 unchanged; each arm30GET/32MiB; countsfirst256/512/768/1024; fixed9760floor; control1024 exact closed-selector parity. No hypothetical allocation/cold/vendor win. All native work remote/jobs1/testthreads1. No changes to protected HEAD793a0699/indexeea8b798.

## Task1 — configuration and four-arm planning, native RED then Green

Files: create examples/compare_native_replay/sq8_count_curve.rs; modify parent sq8_subpage_shortlist.rs module declaration and test-helper visibility.
Interfaces: Config{schema,shortlist:ShortlistConfig,selector_control:CompletedInput}; read_config(Path,SHA)->Result<(Config,u64)>; plan_arms(PageAuthority,&[(usize,f64)],&[usize;4],SelectedSq8Limits,payload_cap)->Result<Vec<ShortlistArm>>.
- [ ] Write curve_config_accepts_lower_counts_without_opening_payloads: valid lower-count cases succeed with absent payload files; unsorted/duplicate/out-of-bound/control-not-last/unknown fields/bad pin refused.
- [ ] Write curve_plans_all_source_prefixes_and_preserves_control: real planner,4,096rows/D2, SOURCE scores for0..1024, literal selected counts256/512/768/1024 and contiguous bytes3584/7168/10752/14336;1024 matches the unchanged old primitive. No fake source scorer or I/O.
- [ ] Freeze scaffold retaining unsupported-count/single-arm behavior; remote compile--no-run, then two exact fixtures. Require compile0 and both causal native failures; preserve/log/close. No full suite on intentional RED.
- [ ] Only after RED, implement bounded count validation and four invocations of existing shortlist/block admission/canonical planner; run affected Green.

## Task2 — actual runtime mode and evidence

Files: parent engine and main CLI/output; new module config/control/report helpers. Keep original wrappers/schemas unchanged.
- [ ] Use explicit Selection enum (Unbudgeted/Block/CountCurve) replacing only internal bool; virtual mode remains Block+context, forbid incompatible combinations. Read curve config directly; avoid accepting new config through old CLI alias.
- [ ] Authenticate pinned closed control as opaque bytes before inputs/queries; parse quality after all arm plans/scores. Recheck file identity; compare query ordinal/baseline hits and arm3 to old arm0 exactly.
- [ ] For each captured SOURCE roster, evaluate4 prefixes with unchanged ranking/tie rules; rank each real SQ8 cover; preserve returnedIDs/scorebits/ordinals plus counts/quality/miss buckets. Charge bounded retained report/control memory explicitly.
- [ ] Add separate admit/smoke/full schema/CLI. Admission validates all real inputs with0 candidates; smoke executes1 query/all4 arms, truth unopened; full requires4,000 cells, exact totals/parity, and records all points even belowfloor.16MiB new full output cap, old caps unchanged.
- [ ] Test final-query/missing-arm/inconsistent-total/wrong-control failures and valid belowfloor report; output durability/create-only behavior remains shared.
- [ ] Commit coherent source UNVERIFIED; remote affected compile/tests + unchanged regressions + workspace testbuild(actual shim unset), workspace correctness/suspicious Clippy, release tests/bin. Record exact commands/source/exits. One independent review of stable whole diff; no per-helper consultations.

## Task3 — one frozen curve

- [ ] Use qualified ELF, original input/control pins and preregister first counts. Run real actual-input admission and separate disposable one-query smoke; verify collection/compute closure.
- [ ] Full1,000/all4 arms on fresh Spot host.30min/8GiB/noSwap/CPU0-3 native envelope or revise from measured source/stage memory before launch, whole3600+120s and<=0.42USD/host. No partial science reads, no duplicate on observation timeouts.
- [ ] Native aggregates only locally; report recall/GET/bytes/per-arm floor and1024 control parity, gain/loss evidence retained; close cloud. Choose an actually useful Pareto point for production32row-proof/source-row admission integration and cold S3. No cold run for a dominated point.

## Review focus

- New counts must not alter SOURCE capture/routing or scorer (test exact original baseline and1024 controls).
- Count bounds/ordering and schema isolation (config test and CLI refusal cases).
- Admission/control before expensive execution, quality read after decisions (native real-input/staging evidence and snapshot negatives).
- Last query/arm missing or wrong totals (native report validator fixture).
- Partial/belowfloor results must not become a false win or execution INVALID (belowfloor positive, explicit advancement flags).

Self-review: source interfaces consistent, first two fixtures have literal geometry expectations; no native verification claimed before actual results. Remaining task2 APIs are implementation details within the shared engine, no product/public ABI change.

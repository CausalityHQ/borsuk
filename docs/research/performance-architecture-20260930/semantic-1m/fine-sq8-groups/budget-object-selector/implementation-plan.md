# Budget Object Selector Implementation Plan

> For agentic workers: use superpowers:executing-plans inside the managed specialist. Root owns remote execution and integration.

**Goal:** Implement the reusable bounded selector/membership evaluator specified in native-spec.md, with independent loss diagnostics.

**Architecture:** One new Rust module plus lib registration; immutable model and membership, explicit budgeted receipts, independent small exhaustive tests. No fitting or I/O.

**Tech Stack:** Existing Rust/sha2; standard library. No dependencies.

**Spec:** native-spec.md in this directory.

## Global Constraints

Only crates/borsuk/src/budget_object_selector.rs and lib.rs registration. Exact64 hidden/four mixtures/8x8 enumeration/15 objects. Preserve existing scoring/router source. Child source-only, CPU1/256MiB/noSwap formatting; root remote Cargo jobs1. Remain UNVERIFIED until native gates pass.

## Review Focus

- Empty candidates and tail groups: no phantom reads or rows.
- A global mixture winner outside the shortlist: report algorithm loss.
- Mutation identity change: old plan cannot be reused.
- Overflow and transient allocation coexistence: admit before allocate.
- Legitimate resource refusal: failed gate, distinct from malformed input.

## Task1: Pure selector and membership evaluation

Files: new module and one lib registration. Inline tests.

- [ ] Define immutable validated Model/Membership, Limits/Budget, bound Plan/Receipt and explicit Refused/Underfill dispositions; publish exact API in /tmp contract before handoff.
- [ ] Write independent tests for all seven oracle groups in native-spec.md. Tests construct model probabilities/weights independently and do not call production coverage/selection helpers for expected answers; the seventh uses nonzero input-dependent weights to test the actual MLP rather than constant logits alone.
- [ ] Implement query normalization, finite MLP/softmax, actual Cartesian selection, empty filtering, stable ties and modeled reservations.
- [ ] Implement separately charged full-grid model diagnostic and tiny exhaustive legal/candidate-restricted cover diagnostic, with strict size ceilings.
- [ ] Implement pure move/swap before/after coverage receipts and checkpoint acceptance; no actual fitting.
- [ ] Verify deterministic identity binding and all preallocation arithmetic/owned capacities. No production fallback scan.
- [ ] Source-only formatting and diff checks; commit owned2 with configured Roman attribution, no trailers. Record exact source hashes and mandatory names in contract. Hold same worker for compiler feedback.

## Task2: Root exact-source verification/integration

- [ ] Freeze remote manifest/archive for exact candidate, preserve original handles.
- [ ] Run locked library test filter budget_object_selector:: serially, then release library build.
- [ ] Run cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious.
- [ ] Run env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh with jobs1.
- [ ] Authenticate exact source before/after, test-name counts, logs/exits/resources and terminated original compute.
- [ ] Integrate only verified native bytes and receipts; ancestor-check then push origin/main. No mechanism/recall claim from compilation.

The subsequent256train/64held fitter in native-spec.md is a separate next slice after this evaluator is qualified; no controller feature work or paid scale follows automatically.

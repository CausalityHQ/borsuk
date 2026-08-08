# Global Cell Stripe Confirmation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a fail-closed, higher-sample 1 MiB versus 4 MiB paired confirmation without changing the frozen v68 result or production default prematurely.

**Architecture:** Add a distinct exact Rust benchmark protocol for the 500-query confirmation, drive it from a new immutable manifest and production runner, and extend the shared artifact validator with campaign-specific schemas and selection rules. Historical v1 inputs retain their exact three-arm behavior; confirmation v1 accepts only two arms and applies the preregistered effect-size and median guards.

**Tech Stack:** Rust, Bash, Python 3.9+, Cargo, unittest, AWS CLI/SSM/S3, tmux.

## Global Constraints

- Use AWS profile `causality`; use only S3 for durable BORSUK data and standard Arrow/Parquet formats.
- Do not inspect measurement CSVs until root terminality makes the fail-closed validator eligible.
- Confirmation shape is exactly 768 dimensions, 8 writers, 1,000 operations/writer, 16 records/operation, 500 queries/arm, 4 maximum read segments, five repetitions, and 1 MiB/4 MiB arms.
- Every production arm uses a fresh process and a unique nonexistent disk-cache path.
- Promotion requires recall@10 1.0, zero PUT/DELETE, identical paired query IDs and logical bytes, pooled and worst-repeat p95 below 200 ms, at least four of five paired p95 wins/ties, at least 10% pooled-p95 improvement, and no more than 5% pooled-p50 regression.
- Use `RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper` and `SCCACHE_DIR=/data/cache/sccache` for Rust builds.
- Commit coherent verified slices; fast-forward push directly to `origin/main`; never create a PR or force push.

---

### Task 1: Exact Rust Confirmation Protocol

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`

**Interfaces:**
- Consumes: existing `ReadQualificationShape`, `run_read_qualification`, deterministic sample-to-dataset reconstruction, and `measure_reads`.
- Produces: protocol value `read-stripe-confirmation`; helper `read_confirmation_order_position(repetition, stripe_bytes)`; exact confirmation shape validation.

- [x] **Step 1: Write failing protocol tests**

Add tests proving that production confirmation accepts only:

```rust
ReadQualificationShape {
    writers: 8,
    operations: 1_000,
    records_per_operation: 16,
    dimensions: 768,
    query_count: 500,
    max_read_segments: 4,
    stripe_bytes: MIB, // and separately 4 * MIB
    repetition: 1,    // through 5
}
```

Also prove that 2 MiB, 100 queries, 499 queries, repetition 0/6, or another
shape fails, and that paired orders are `[1,4]`, `[4,1]`, `[1,4]`, `[4,1]`,
`[1,4]` MiB. Preserve the exact existing 2-writer, 8-dimensional, four-query
structural-smoke shape for the new protocol, but allow only its 1 MiB or 4 MiB
arm and require order position zero.

- [x] **Step 2: Run the focused Rust test and observe RED**

Run:

```bash
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper \
SCCACHE_DIR=/data/cache/sccache \
cargo test -p borsuk --example group_commit_bench read_confirmation -- --nocapture
```

Expected: compile failure because the new confirmation validator/order helper
does not exist.

- [x] **Step 3: Implement minimal campaign-specific dispatch**

Parse the protocol before shape validation. Keep `read-qualification` bound to
its existing 100-query/three-arm validator. Bind `read-stripe-confirmation` to
the exact 500-query/two-arm validator and alternating order helper. Both modes
continue through the existing immutable sample validation, dataset reconstruction,
search measurement, raw artifact writing, and marker creation. Emit the protocol
kind in `summary.csv` so the validator can distinguish confirmation artifacts.

- [x] **Step 4: Run focused tests and formatting**

Run the focused command from Step 2 and:

```bash
cargo fmt --all -- --check
```

Expected: all focused tests pass and formatting exits zero.

- [x] **Step 5: Commit and fast-forward push**

```bash
git add crates/borsuk/examples/group_commit_bench.rs
git commit -m "bench: add exact stripe confirmation reads"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 2: Confirmation Manifest, Runner, and Launcher

**Files:**
- Create: `docs/research/global-cell-stripe-confirmation.json`
- Create: `scripts/bench_global_cell_stripe_confirmation.sh`
- Create: `scripts/launch_aws_global_cell_stripe_confirmation.sh`
- Create: `scripts/test_bench_global_cell_stripe_confirmation.py`

**Interfaces:**
- Consumes: `read-stripe-confirmation`, immutable v67 identities, `benchmark_with_resources.py`, and `validate_global_cell_stripes.py`.
- Produces: immutable two-arm campaign configuration, root markers `GLOBAL_CELL_STRIPE_CONFIRMATION_COMPLETE` / `GLOBAL_CELL_STRIPE_CONFIRMATION_FAILED`, retained AWS tmux launch, and policy coverage.

- [x] **Step 1: Write failing structural and policy tests**

Assert exact manifest values: 500 queries, five repetitions, arms
`[1048576, 4194304]`, alternating order, 10% p95 improvement, 5% p50 regression,
and both 200 ms limits. Assert the runner sets
`BORSUK_GROUP_COMMIT_PROTOCOL=read-stripe-confirmation`, unique cache/output paths,
telemetry, terminal markers, empty-prefix protection, and terminal validator.
Assert the launcher uses `causality`, `c7g.8xlarge`, retained tmux, process/tmux
contention checks, and no force option. The existing repository policy discovers
the shared production runner and needed no path-list change.

- [x] **Step 2: Run tests and observe RED**

```bash
python3 -m unittest scripts.test_bench_global_cell_stripe_confirmation \
  scripts.test_check_repo_policy
```

Expected: failures for missing files and policy entries.

- [x] **Step 3: Implement the manifest and scripts**

Derive runner/launcher lifecycle from the v68 scripts, but use confirmation-only
paths, markers, variables, and protocol. Generate the ten arms solely from the
new manifest. Copy the manifest into output, preserve the source archive identity,
sync after every completed arm, and run the validator only after writing the root
complete marker. Refuse nonempty S3 prefixes and any active BORSUK workload.

- [x] **Step 4: Run focused tests and shell syntax**

```bash
python3 -m unittest scripts.test_bench_global_cell_stripe_confirmation \
  scripts.test_check_repo_policy
bash -n scripts/bench_global_cell_stripe_confirmation.sh \
  scripts/launch_aws_global_cell_stripe_confirmation.sh
python3 scripts/check_repo_policy.py
```

Expected: every command exits zero.

- [x] **Step 5: Commit and fast-forward push**

```bash
git add docs/research/global-cell-stripe-confirmation.json \
  scripts/bench_global_cell_stripe_confirmation.sh \
  scripts/launch_aws_global_cell_stripe_confirmation.sh \
  scripts/test_bench_global_cell_stripe_confirmation.py
git commit -m "bench: preregister stripe confirmation campaign"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 3: Campaign-Specific Terminal Validation

**Files:**
- Modify: `scripts/validate_global_cell_stripes.py`
- Modify: `scripts/test_validate_global_cell_stripes.py`
- Create: `scripts/test_validate_global_cell_stripe_confirmation.py`

**Interfaces:**
- Consumes: either exact historical v1 manifest or exact confirmation v1 manifest.
- Produces: reconciled JSON containing p50/p95/worst-repeat/GET/byte metrics and an explicit `winner` only if that campaign's frozen rule passes.

- [x] **Step 1: Write failing confirmation validator tests**

Generate a synthetic ten-arm terminal matrix from the confirmation manifest.
Test a passing 4 MiB candidate and independently reject: incomplete roots before
CSV parsing, unexpected arm/order, missing artifact, reused cache, changed query
IDs, recall below 1.0, any PUT/DELETE, unequal logical bytes, fewer than four
paired wins, pooled improvement below 10%, pooled p50 regression above 5%, pooled
p95 at/above 200 ms, and worst-repeat p95 at/above 200 ms. Keep all historical
v1 tests green.

- [x] **Step 2: Run the validator tests and observe RED**

```bash
python3 -m unittest scripts.test_validate_global_cell_stripes
```

Expected: confirmation manifest/schema is rejected or new selection fields are
missing.

- [x] **Step 3: Implement exact schema dispatch and selection**

Select expected markers, arms, orders, protocol kind, query count, and thresholds
from the exact `campaign_id`. Reject unknown campaign IDs. Preserve the terminality
check before manifest or CSV measurement parsing. Reconcile every query and
summary counter as v1 does. Add pooled p50. For confirmation, compare only `s4m`
to `s1m` and require every criterion in the design; return the individual boolean
criteria so failure is auditable. Preserve v1 winner behavior byte-for-byte in
its tests.

- [x] **Step 4: Run validator and harness tests**

```bash
python3 -m unittest scripts.test_validate_global_cell_stripes \
  scripts.test_bench_global_cell_stripes \
  scripts.test_bench_global_cell_stripe_confirmation
```

Expected: all focused tests pass.

- [x] **Step 5: Commit and fast-forward push**

```bash
git add scripts/validate_global_cell_stripes.py \
  scripts/test_validate_global_cell_stripes.py \
  scripts/test_validate_global_cell_stripe_confirmation.py
git commit -m "bench: validate stripe confirmation evidence"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

### Task 4: Local Structure, Full Assurance, and AWS Launch

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: `docs/superpowers/plans/2026-08-08-global-cell-stripe-confirmation.md`

**Interfaces:**
- Consumes: Tasks 1-3 and the dedicated Causality worker.
- Produces: verified source revision, launch identity, immutable terminal artifacts, and a defensible promote/reject decision.

- [x] **Step 1: Run a local structurally valid confirmation smoke**

Use a temporary local index and samples with the existing structural-smoke shape,
then invoke `read-stripe-confirmation` and verify nonempty summary, reads, resource
telemetry, storage trace, zero process exit, and terminal markers. Record this as
structural evidence only; do not report its latency as production data.

- [x] **Step 2: Run exact full assurance once**

```bash
cargo fmt --all -- --check
git diff --check
python3 scripts/check_repo_policy.py
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper \
SCCACHE_DIR=/data/cache/sccache \
cargo clippy --locked --workspace --all-features --all-targets -- -D warnings
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper \
SCCACHE_DIR=/data/cache/sccache \
cargo test --locked --workspace --all-features --all-targets
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
```

Expected: all commands exit zero. If a layer fails, repair only that layer and
then rerun one final full gate.

- [x] **Step 3: Record assurance, commit, and fast-forward push**

Record exact command counts/timings and structural artifact paths, then:

```bash
git add docs/research/group-commit-scalability-attempt-ledger.md \
  docs/superpowers/plans/2026-08-08-global-cell-stripe-confirmation.md
git commit -m "docs: record stripe confirmation assurance"
git fetch origin main
git merge-base --is-ancestor origin/main HEAD
git push origin HEAD:main
```

- [x] **Step 4: Preflight and launch exactly one AWS campaign**

Verify Causality account `453182569524`, EC2 instance/SSM/EBS health, absence of
active BORSUK benchmark processes or non-shell tmux panes, clean/equal main, and
empty destination prefix. Launch through
`scripts/launch_aws_global_cell_stripe_confirmation.sh`, preserve run/source/
manifest identities in the ledger, commit, and fast-forward push that launch
record.

- [x] **Step 5: Monitor markers and infrastructure health periodically**

Retain one 15-minute background timer/session and wait on that same handle in
at-most-55-second polls. At each timer boundary inspect only root/arm markers,
process/tmux state, and EC2/SSM/EBS health. Restart the timer if healthy and
incomplete. On a terminal failure, run the root validator before opening any
measurement CSV; then use terminal logs and eligible artifacts to debug.

- [x] **Step 6: Make the frozen decision**

At terminal completion, run the validator before any CSV inspection, preserve
its JSON and SHA-256 separately from raw results, and record its exact decision.
Change the default to 4 MiB only if `winner == "s4m"`; otherwise leave 1 MiB and
return to architectural diagnosis. Any default change requires focused TDD,
strict Clippy, full Rust/Python assurance, a coherent commit, and a fast-forward
push before resuming the 1/8/32-writer matrix.

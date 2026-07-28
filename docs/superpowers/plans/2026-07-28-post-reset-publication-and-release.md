# Post-reset Publication and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover the frozen BORSUK work after the laptop reset, correct the cache-lifecycle defect that failed publication v7, complete and independently audit a fresh publication campaign, then execute the remaining production-scale and datatype-SIMD evidence gates without selective reporting.

**Architecture:** Git is the durable source of code, tests, compact evidence, and methodology; S3 is the durable source of paid raw evidence. Failed publication v7 remains immutable and fail-closed. Its rows are never reused. The corrected campaign, scale tests, and SIMD experiments run only from fully identified source revisions and write to fresh, non-overlapping prefixes.

**Tech Stack:** Rust/Cargo, Python 3, Node.js, AWS CLI/SSO, EC2/SSM, S3, Bash, Arrow IPC, Parquet, Vortex experimental controls.

---

### Task 1: Restore and identify the exact repository state

**Files:**
- Read: `AGENTS.md`
- Read: `docs/handoff-2026-07-28.md`
- Read: `docs/research/release-readiness-2026-07-26.md`

- [ ] **Step 1: Clone and fast-forward the durable branch**

```bash
git clone git@github.com:CausalityHQ/borsuk.git
cd borsuk
git checkout main
git pull --ff-only origin main
```

Expected: checkout succeeds with no local divergence.

- [ ] **Step 2: Record the restored revision**

```bash
git status --short --branch
git log -1 --date=iso-strict --format='%H%n%ad%n%s'
```

Expected: `main` tracks `origin/main`, and the working tree is clean.

- [ ] **Step 3: Restore AWS SSO separately**

```bash
aws sso login --profile causality
AWS_PROFILE=causality aws sts get-caller-identity
```

Expected account: `453182569524`. Stop if the account differs.

### Task 2: Confirm and preserve publication v7 failure without selecting outcomes

**Files:**
- Read: `docs/research/publication-v2-attempt-ledger.md`
- Read: `docs/research/publication-v2-manifest.json`
- Read: `docs/research/reproducibility.md`

- [ ] **Step 1: List only terminal and repetition markers**

```bash
AWS_PROFILE=causality aws s3api list-objects-v2 \
  --bucket borsuk-bench-453182569524-euc1 \
  --prefix publication/v2/confirmatory-20260728-v7/results/ \
  --query 'Contents[].Key' --output text |
  tr '\t' '\n' |
  grep -E 'PUBLICATION_V2_(COMPLETE|FAILED)|REPETITION_COMPLETE'
```

Expected: `PUBLICATION_V2_FAILED` exists, `PUBLICATION_V2_COMPLETE` does not,
and no repetition marker exists. Do not download or compare partial numerical
outcomes.

- [ ] **Step 2: Verify the recorded failure boundary**

The preserved EC2 pane and attempt ledger must agree on the boundary: SciFact
`dense+text`, `hot-1`, repetition 1 failed while writing a range-bundle cache
file with OS error 28 (`No space left on device`). The S3 tree must contain 208
objects at the terminal snapshot and no repetition marker.

- [ ] **Step 3: Keep the failed tree immutable**

The v7 ledger row already records the hashes and exact boundary. Never merge
any v7 measurement into a later attempt. The corrected launch must use a fresh
v8 prefix and newly frozen source, manifest, and schedule hashes.

### Task 3: Correct cache lifecycle and freeze a fresh v8 campaign

**Files:**
- Modify: `scripts/bench_hybrid_retrieval_matrix.sh`
- Modify: `scripts/bench_publication_v2_aws.sh`
- Modify: `scripts/test_bench_hybrid_retrieval_matrix.py`
- Modify: `scripts/test_bench_publication_v2_aws.py`

- [ ] **Step 1: Add failing cache-lifecycle contracts**

Add tests proving that paid hybrid execution deletes each query cell's `cache`
and `scratch` directories only after `hybrid_queries.csv`,
`hybrid_summary.csv`, `hybrid_startup.csv`, and `resources.csv` have all been
validated. Add a publication-runner test proving that a free-disk preflight
runs before each repetition and before the hybrid phase.

Run:

```bash
python3 -m unittest discover -s scripts \
  -p 'test_bench_hybrid_retrieval_matrix.py' -v
python3 -m unittest discover -s scripts \
  -p 'test_bench_publication_v2_aws.py' -v
```

Expected before implementation: both new contracts fail for the v7 lifecycle.

- [ ] **Step 2: Implement bounded cache cleanup and disk preflight**

After each measured hybrid cell validates all four durable artifacts, remove
only that cell's `cache` and `scratch` directories. Do not delete CSV evidence.
Before each repetition and immediately before hybrid execution, compare free
bytes on the result filesystem with
`BORSUK_PUBLICATION_MIN_FREE_BYTES=34359738368` (32 GiB), record that bound in
`environment.txt`, and fail and sync the marker before starting work when
headroom is insufficient.

- [ ] **Step 3: Verify the lifecycle fix**

Run the two focused suites, then all 320 methodology tests in the pinned format
environment. Expected: all tests pass, and a local fixture proves completed
cell evidence survives while cache and scratch bytes do not accumulate.

- [ ] **Step 4: Freeze and launch v8**

Update the manifest campaign ID and fresh result/index/cache prefixes, generate
a newly balanced schedule, archive the exact source, and record all three
SHA-256 values in a new v8 ledger row before measurement. Launch only after the
dry run and local gates pass. Do not reuse v7 indexes, caches, or measurements.

### Task 4: Download and independently audit a completed fresh campaign

**Files:**
- Run: `scripts/validate_publication_v2_results.py`
- Run: `scripts/analyze_publication_claims.py`
- Run: `scripts/validate_reported_comparisons.py`

- [ ] **Step 1: Download the complete immutable fresh result prefix**

Use the exact v8 result prefix and source SHA-256 printed by its launcher.
Expected: the local tree contains `PUBLICATION_V2_COMPLETE`, five repetition
markers, `manifest.json`, `schedule.csv`, and `source-archive.tar.gz`.

- [ ] **Step 2: Run the independent validator**

Run `validate_publication_v2_results.py` with the downloaded root and exact
v8 source SHA-256. Any missing row, identity mismatch, schedule drift, sample
mismatch, or recomputed-claim mismatch makes the campaign ineligible.

- [ ] **Step 3: Validate external context separately**

```bash
python3 scripts/validate_reported_comparisons.py
```

Expected: exit status zero; every commercial or paper number remains
`context-only`.

- [ ] **Step 4: Record the validator decision without stronger wording**

Use only the machine-readable decision produced by the validator. If it is
`no-superiority-claim`, publish that result plainly. If it authorizes lower
latency at matched recall, retain the exact same-client/managed-compute
disclosure and confidence intervals.

### Task 5: Re-run correctness gates on the restored source

**Files:**
- Run: `scripts/check_rust_test_build.sh`
- Run: `scripts/check_repo_policy.py`
- Run: `scripts/test_docs_web.mjs`

- [ ] **Step 1: Verify formatting and lint**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
```

Expected: both commands exit zero.

- [ ] **Step 2: Verify the complete Rust target matrix**

```bash
cargo test --locked --workspace --all-targets -j2
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
```

Expected: both commands exit zero; explicit research/scale tests may remain
ignored by design.

- [ ] **Step 3: Verify methodology and documentation contracts**

```bash
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/check_repo_policy.py
node scripts/test_docs_web.mjs
node scripts/sync_docs_examples.mjs --check
```

Expected: every command exits zero.

### Task 6: Execute the frozen production lifecycle and scale gates

**Files:**
- Run: `scripts/bench_production_lifecycle_aws.sh`
- Run: `scripts/bench_100m_code_ranges.sh`
- Update only after validation: `docs/research/release-readiness-2026-07-26.md`

- [ ] **Step 1: Inspect each paid runner's dry-run and safety gates**

```bash
bash -n scripts/bench_production_lifecycle_aws.sh
bash -n scripts/bench_100m_code_ranges.sh
rg -n 'EXECUTE|RUN_|source|sha256|COMPLETE|FAILED' \
  scripts/bench_production_lifecycle_aws.sh \
  scripts/bench_100m_code_ranges.sh
```

Expected: syntax is valid and paid execution requires explicit gates, frozen
source identity, and fresh result prefixes.

- [ ] **Step 2: Launch lifecycle evidence from the restored commit**

Use a new date-stamped S3 prefix and explicitly archive the Git SHA, source
archive SHA-256, AWS account, region, instance type, AMI, storage class,
toolchain, and command environment. Do not reuse v7 indexes or result rows.

- [ ] **Step 3: Run the 100M bounded-memory/code-range gate**

Run only after the lifecycle campaign is terminal and validated. Require
bounded peak RSS, finite request/byte telemetry, complete correctness markers,
and no fallback to a corpus-sized resident vector or code matrix.

- [ ] **Step 4: Update readiness evidence**

Commit only compact validated manifests, aggregate CSVs, and decision records.
Keep raw data in S3 with checksums and immutable prefixes.

### Task 7: Qualify SIMD end to end across physical datatypes

**Files:**
- Read: `docs/research/simd-kernels.md`
- Modify after evidence: `docs/research/simd-kernels.md`
- Create after protocol review: `docs/research/simd-e2e-manifest.json`
- Create after protocol review: `scripts/bench_simd_datatype_matrix.sh`
- Create tests with the runner: `scripts/test_bench_simd_datatype_matrix.py`

- [ ] **Step 1: Freeze the comparison matrix before measurements**

The manifest must include:

```text
architectures: AWS Graviton arm64 and AWS x86-64
builds: SIMD enabled and explicitly compiled scalar control
primary dense types: float32, float16, bfloat16, E4M3FN, E5M2, int8
other paths: packed binary, sparse float32/float16, late interaction float32/float16, BM25
states: uncached, disk-cached, mixed 0/10/25/50/75/90/100%, memory-preloaded where valid
clients: 1, 2, 4, 8, 16
metrics: recall/exact agreement, p50/p90/p95/p99/max/mean/stddev, QPS, CPU/query, RSS, storage bytes/requests
```

Every cell must use a freshly recreated index from the same source revision.

- [ ] **Step 2: Add fail-closed runner tests**

Test that the runner rejects architecture drift, missing scalar-control build
identity, missing datatype cells, cache-coverage drift, unequal query cohorts,
non-finite timings, and incomplete raw per-query evidence.

- [ ] **Step 3: Execute both architecture campaigns**

Launch fresh prefixes only after local tests pass. Do not compare an ARM SIMD
run to an x86 scalar run or to historical results.

- [ ] **Step 4: Promote only measured wins**

A datatype/path may be called SIMD-accelerated only when end-to-end CPU/query or
latency improves without correctness or recall regression on both
architectures. Otherwise document it as a correctness-preserving SIMD
implementation without a speedup claim.

### Task 8: Final evidence, cost, and repository closure

**Files:**
- Modify: `docs/research/publication-v2-attempt-ledger.md`
- Modify: `docs/research/release-readiness-2026-07-26.md`
- Modify: `docs/research/systems-comparison.md`
- Modify: `docs/benchmarks.md`

- [ ] **Step 1: Verify all remote evidence is durable**

For every paid campaign, list its terminal marker, source hash, manifest hash,
schedule hash where applicable, object count, and S3 prefix. Confirm local
compact artifacts reproduce from the raw S3 tree.

- [ ] **Step 2: Stop only owned temporary research instances**

```bash
AWS_PROFILE=causality aws ec2 describe-instances \
  --region eu-central-1 \
  --filters Name=tag:Purpose,Values=temporary-research \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name,Name:Tags[?Key==`Name`]|[0].Value}' \
  --output table
```

Resolve each instance to a terminal campaign before stopping it. Never target
unrelated EKS or development instances.

- [ ] **Step 3: Run the final repository gate**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -j2 -- -D warnings
cargo test --locked --workspace --all-targets -j2
python3 -m unittest discover -s scripts -p 'test_*.py'
python3 scripts/check_repo_policy.py
node scripts/test_docs_web.mjs
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 4: Commit and push compact evidence**

```bash
git status --short
git add --all
git diff --cached --check
git commit -m "research: record validated publication and scale evidence"
git push origin main
```

Before committing, confirm that `.borsuk-scratch/` and raw benchmark trees are
not staged.

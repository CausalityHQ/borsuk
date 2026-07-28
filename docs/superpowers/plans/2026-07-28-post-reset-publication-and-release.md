# Post-reset Publication and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recover the frozen BORSUK work after the laptop reset, complete and independently audit publication v2, then execute the remaining production-scale and datatype-SIMD evidence gates without selective reporting.

**Architecture:** Git is the durable source of code, tests, compact evidence, and methodology; S3 is the durable source of paid raw evidence. Publication v7 remains immutable and fail-closed. Scale and SIMD experiments run only from a fully identified source revision and write to fresh, non-overlapping prefixes.

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

### Task 2: Determine publication v7 terminal state without selecting outcomes

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

Expected while running: zero or more repetition markers and no terminal marker.
Do not download or compare partial numerical outcomes.

- [ ] **Step 2: Wait for exactly one terminal state**

Repeat Step 1 at a reasonable interval. Continue only after exactly one of
`PUBLICATION_V2_COMPLETE` or `PUBLICATION_V2_FAILED` exists.

- [ ] **Step 3: Handle failure without artifact reuse**

If the failure marker exists, download only the failure/protocol logs needed to
identify the boundary, append a v7 row with hashes and exact boundary to
`docs/research/publication-v2-attempt-ledger.md`, and commit that ledger update.
Never merge any v7 measurement into a later attempt. A corrected launch must
use a fresh v8 prefix and newly frozen source, manifest, and schedule hashes.

### Task 3: Download and independently audit a completed v7 tree

**Files:**
- Run: `scripts/validate_publication_v2_results.py`
- Run: `scripts/analyze_publication_claims.py`
- Run: `scripts/validate_reported_comparisons.py`

- [ ] **Step 1: Download the complete immutable result prefix**

```bash
mkdir -p "$PWD/.borsuk-scratch/publication-v2-v7-download"
AWS_PROFILE=causality aws s3 sync \
  s3://borsuk-bench-453182569524-euc1/publication/v2/confirmatory-20260728-v7/results/ \
  "$PWD/.borsuk-scratch/publication-v2-v7-download/"
```

Expected: the local tree contains `PUBLICATION_V2_COMPLETE`, five repetition
markers, `manifest.json`, `schedule.csv`, and `source-archive.tar.gz`.

- [ ] **Step 2: Run the independent validator**

```bash
python3 scripts/validate_publication_v2_results.py \
  "$PWD/.borsuk-scratch/publication-v2-v7-download" \
  --expected-source-sha256 \
  9805df95efd9c3fa38b22e8b720e435e6c0b852694c6e8777b13ff955b8124c8
```

Expected: exit status zero. Any missing row, identity mismatch, schedule drift,
sample mismatch, or recomputed-claim mismatch makes the campaign ineligible.

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

### Task 4: Re-run correctness gates on the restored source

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

### Task 5: Execute the frozen production lifecycle and scale gates

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

### Task 6: Qualify SIMD end to end across physical datatypes

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

### Task 7: Final evidence, cost, and repository closure

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

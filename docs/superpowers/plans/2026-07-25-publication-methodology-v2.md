# Publication Methodology v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a confirmatory benchmark pipeline that can support a lower-p95-at-matched-recall claim from independent BORSUK and Amazon S3 Vectors repetitions while keeping all other commercial and paper figures explicitly reported and non-controlled.

**Architecture:** A Python protocol module validates the frozen campaign manifest and emits a deterministic repetition schedule. Existing benchmark executables remain responsible for measurement, but shell orchestration moves repetitions outside measured processes and assigns fresh storage/cache paths. A separate analysis module consumes normalized raw samples, performs deterministic hierarchical bootstrap analysis, and emits claim decisions; reported figures pass through a strict evidence registry.

**Tech Stack:** Python 3 standard library, Bash, Rust, `unittest`, Cargo.

## Status (2026-07-26)

- Tasks 1-7 are implemented and verified.
- Task 8 steps 1-4 are complete: the Python, Rust, shell, repository, and
  frozen-schedule checks pass.
- Task 8 step 5 is pending external authentication. The `causality` AWS SSO
  token is expired, so the prior pilot cannot yet be confirmed terminal. No
  publication-v2 resources have been launched.

---

### Task 1: Confirmatory manifest and deterministic schedule

**Files:**
- Create: `scripts/publication_protocol.py`
- Create: `scripts/test_publication_protocol.py`
- Create: `docs/research/publication-v2-manifest.json`

- [ ] **Step 1: Write failing protocol tests**

Add tests that construct minimal manifests and assert:

```python
manifest = valid_manifest(repetitions=5, queries=1000)
validated = validate_manifest(manifest)
self.assertEqual(validated["campaign_kind"], "confirmatory")
self.assertEqual(len(build_schedule(validated)), 10)
```

The tests must also reject `pilot`, fewer than three repetitions, fewer than
1,000 queries when `publish_p99` is true, non-frozen search configurations,
and duplicate repetition identifiers. A schedule test must assert that
`borsuk` and `amazon-s3-vectors` alternate order for the direct dataset and
that repeated calls with the same seed are byte-for-byte equal.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest scripts.test_publication_protocol -v
```

Expected: import failure because `scripts.publication_protocol` does not exist.

- [ ] **Step 3: Implement manifest validation and scheduling**

Implement:

```python
def validate_manifest(value: dict[str, object]) -> dict[str, object]:
    normalized = dict(value)
    if normalized.get("campaign_kind") != "confirmatory":
        raise ValueError("campaign_kind must be confirmatory")
    repetitions = int(normalized["repetitions"])
    if repetitions < 3:
        raise ValueError("confirmatory campaigns require at least 3 repetitions")
    return normalized

def build_schedule(manifest: dict[str, object]) -> list[dict[str, object]]:
    seed = int(manifest["master_seed"])
    rows = []
    for repetition in range(1, int(manifest["repetitions"]) + 1):
        rows.append({
            "repetition_id": f"r{repetition:02d}",
            "query_seed": seed + repetition,
            "system_order": (
                "borsuk amazon-s3-vectors"
                if repetition % 2 else
                "amazon-s3-vectors borsuk"
            ),
        })
    return rows

def hardware_relation(
    borsuk: dict[str, object],
    external: dict[str, object],
) -> str:
    required = ("logical_cpus", "ram_bytes", "accelerator", "storage_class")
    if any(key not in borsuk or key not in external for key in required):
        return "unknown"
    return "weaker-or-equal" if (
        int(borsuk["logical_cpus"]) <= int(external["logical_cpus"])
        and int(borsuk["ram_bytes"]) <= int(external["ram_bytes"])
        and borsuk["accelerator"] == external["accelerator"]
        and borsuk["storage_class"] == external["storage_class"]
    ) else "stronger-or-incomparable"
```

Use `random.Random(master_seed)` and emit explicit `repetition_id`,
`query_seed`, `dataset_order`, `system_order`, `result_prefix`,
`index_prefix`, and `cache_key` fields. The CLI must support:

```bash
python3 scripts/publication_protocol.py validate MANIFEST
python3 scripts/publication_protocol.py schedule MANIFEST --output schedule.csv
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the command from Step 2. Expected: all protocol tests pass.

- [ ] **Step 5: Add and validate the frozen manifest**

Create a checked manifest that declares five repetitions, 1,000 queries,
Fashion-MNIST as the direct S3 Vectors dataset, six internal dense datasets,
the existing three BEIR datasets, p95 as primary, p99 as secondary, and
`srht-pq-scan` as the frozen production codec.

Run:

```bash
python3 scripts/publication_protocol.py validate docs/research/publication-v2-manifest.json
```

Expected: `valid confirmatory manifest`.

### Task 2: Seeded hybrid query cohorts

**Files:**
- Modify: `crates/borsuk/examples/hybrid_retrieval_bench.rs`
- Modify: `scripts/test_hybrid_retrieval_bench.py`

- [ ] **Step 1: Write a failing Rust source contract test**

Extend the Python source contract to require
`BORSUK_HYBRID_QUERY_SEED`, `query_seed`, and a deterministic
`permute_queries` helper. Add Rust unit tests:

```rust
#[test]
fn query_permutation_is_seeded_and_membership_preserving() {
    let first = permuted_positions(8, 17);
    assert_eq!(first, permuted_positions(8, 17));
    assert_ne!(first, permuted_positions(8, 23));
    let mut sorted = first;
    sorted.sort_unstable();
    assert_eq!(sorted, (0..8).collect::<Vec<_>>());
}
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest scripts.test_hybrid_retrieval_bench -v
cargo test -p borsuk --example hybrid_retrieval_bench query_permutation_is_seeded_and_membership_preserving
```

Expected: the source contract or Rust compilation fails because seed support
is absent.

- [ ] **Step 3: Implement deterministic permutation**

Load all selected queries, derive a stable Fisher-Yates permutation with a
small local PRNG from `BORSUK_HYBRID_QUERY_SEED`, and reorder before computing
the hot cohort. Add `query_seed` to raw and summary CSV output. Do not add a new
crate dependency.

- [ ] **Step 4: Run tests and verify GREEN**

Run both commands from Step 2. Expected: all selected tests pass.

### Task 3: Independent hybrid repetitions

**Files:**
- Modify: `scripts/bench_hybrid_retrieval_matrix.sh`
- Modify: `scripts/test_bench_hybrid_retrieval_matrix.py`
- Modify: `scripts/render_hybrid_retrieval_charts.py`
- Modify: `scripts/test_render_hybrid_retrieval_charts.py`

- [ ] **Step 1: Write failing shell-runner tests**

Add a dry-run test with two repetitions. Assert that `coverage.csv` contains
`campaign_repetition` and `query_seed`, each query row has a unique
`artifact_dir`, and each artifact directory ends in `repetition-1` or
`repetition-2`.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest scripts.test_bench_hybrid_retrieval_matrix -v
```

Expected: missing repetition columns or repeated artifact directories.

- [ ] **Step 3: Move repetitions outside the benchmark process**

Wrap each query point in:

```bash
for campaign_repetition in $(seq 1 "$REPETITIONS"); do
  query_seed=$((MASTER_SEED + campaign_repetition))
  query_out="$base_query_out/repetition-$campaign_repetition"
  cache_dir="$query_out/cache"
  mkdir -p "$cache_dir" "$query_out/scratch"
  env \
    BORSUK_HYBRID_QUERY_SEED="$query_seed" \
    BORSUK_HYBRID_REPETITIONS=1 \
    BORSUK_HYBRID_PRIME_TARGET_HOT_SET=1 \
    "$BENCH_BINARY" query
done
```

Set `BORSUK_HYBRID_REPETITIONS=1`,
`BORSUK_HYBRID_QUERY_SEED`, and
`BORSUK_HYBRID_PRIME_TARGET_HOT_SET=1` in the measured process. Remove the
separate priming process so metadata and the primed data cache belong to the
same process/cache condition.

- [ ] **Step 4: Update chart aggregation**

Teach the chart loader to pool raw rows for visualization while retaining
`campaign_repetition` in each record. Change chart subtitles from per-query
sample SD to independent-repetition language where applicable.

- [ ] **Step 5: Run tests and verify GREEN**

Run:

```bash
python3 -m unittest \
  scripts.test_bench_hybrid_retrieval_matrix \
  scripts.test_render_hybrid_retrieval_charts -v
```

Expected: all selected tests pass.

### Task 4: Raw S3 Vectors query evidence and seeded order

**Files:**
- Modify: `scripts/benchmark_s3_vectors.py`
- Modify: `scripts/test_benchmark_s3_vectors.py`
- Modify: `scripts/bench_s3_vectors_matrix.sh`
- Modify: `scripts/test_bench_s3_vectors_matrix.py`

- [ ] **Step 1: Write failing raw-sample tests**

Add unit tests for a deterministic `permuted_positions(count, seed)` helper
and a source/fixture test requiring `query_samples.csv` columns:

```text
dataset,engine,pass,repetition_id,query_seed,query_position,query_source_index,latency_ms,recall_at_10,status,cache_state,resource_scope
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest \
  scripts.test_benchmark_s3_vectors \
  scripts.test_bench_s3_vectors_matrix -v
```

Expected: missing permutation helper and raw sample artifact.

- [ ] **Step 3: Emit raw S3 Vectors samples**

Add CLI arguments `--query-seed` and `--repetition-id`, deterministically
permute query/truth pairs, retain source indexes, and emit one raw row per
request. Summary `query.csv` must be computed from those raw rows.

- [ ] **Step 4: Make matrix repetitions fresh and explicit**

Add `BORSUK_S3V_REPETITIONS` and `BORSUK_S3V_MASTER_SEED`. Each repetition gets
a fresh bucket, output directory, query seed, and coverage row. Validate
`build.csv,query.csv,query_samples.csv,resources.csv`.

- [ ] **Step 5: Run tests and verify GREEN**

Run the command from Step 2. Expected: all selected tests pass.

### Task 5: Confirmatory AWS orchestration

**Files:**
- Create: `scripts/bench_publication_v2_aws.sh`
- Create: `scripts/test_bench_publication_v2_aws.py`
- Create: `scripts/launch_aws_publication_v2.sh`
- Create: `scripts/test_launch_aws_publication_v2.py`

- [ ] **Step 1: Write failing static and dry-run tests**

Tests must assert:

- explicit `BORSUK_RUN_PUBLICATION_V2=1` paid gate;
- manifest validation happens before AWS writes;
- output/index prefixes contain the repetition ID;
- dense runs receive `BORSUK_BENCH_QUERIES=1000`;
- S3 Vectors is limited to Fashion-MNIST;
- no external-control runner is invoked;
- source archive, environment, schedule, and manifest are copied into results;
- an active `pilot` prefix is never reused.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest \
  scripts.test_bench_publication_v2_aws \
  scripts.test_launch_aws_publication_v2 -v
```

Expected: missing scripts.

- [ ] **Step 3: Implement the remote runner**

The runner validates the manifest, writes `schedule.csv`, records environment
metadata, and executes each schedule row with fresh roots. It invokes
`bench_s3_full.sh`, `bench_s3_vectors_matrix.sh`, and the frozen hybrid runner
only for their declared datasets. It syncs after every repetition and writes
immutable per-repetition checkpoints.

- [ ] **Step 4: Implement the content-addressed launcher**

Copy the content-addressed archive, account guard, instance lookup, SSM wait,
and detached tmux startup structure from
`scripts/launch_aws_publication_benchmarks.sh`. Set
`runner='scripts/bench_publication_v2_aws.sh'`, upload
`docs/research/publication-v2-manifest.json` beside the archive, include its
SHA-256 in `campaign_argv`, and reject any active tmux session whose name starts
with `borsuk-`.

- [ ] **Step 5: Run tests and verify GREEN**

Run the command from Step 2. Expected: all selected tests pass.

### Task 6: Hierarchical bootstrap and claim decisions

**Files:**
- Create: `scripts/analyze_publication_claims.py`
- Create: `scripts/test_analyze_publication_claims.py`

- [ ] **Step 1: Write failing statistical tests**

Use synthetic repetition/query rows to assert:

```python
decision = compare_direct(borsuk_rows, s3_rows, seed=17, bootstrap_samples=2000)
self.assertLess(decision.latency_ratio_ci_high, 1.0)
self.assertGreaterEqual(decision.recall_difference_ci_low, 0.0)
self.assertEqual(decision.claim, "lower-latency-at-matched-recall")
```

Add rejection cases for recall loss, latency CI crossing one, fewer than three
repetitions, mismatched query IDs, and failed requests.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest scripts.test_analyze_publication_claims -v
```

Expected: import failure because the analysis module does not exist.

- [ ] **Step 3: Implement deterministic hierarchical bootstrap**

Resample repetition IDs with replacement, then resample paired query IDs
within each selected repetition. Compute nearest-rank p95 per resample, the
BORSUK/S3 latency ratio, and mean recall difference. Emit:

```text
dataset,cache_pair,repetitions,queries_per_repetition,borsuk_p95_ms,s3_p95_ms,latency_ratio,latency_ratio_ci_low,latency_ratio_ci_high,recall_difference,recall_difference_ci_low,recall_difference_ci_high,claim
```

- [ ] **Step 4: Run tests and verify GREEN**

Run the command from Step 2. Expected: all selected tests pass.

### Task 7: Reported evidence registry and claim wording gate

**Files:**
- Create: `docs/research/reported-comparisons.csv`
- Create: `scripts/validate_reported_comparisons.py`
- Create: `scripts/test_validate_reported_comparisons.py`
- Modify: `docs/research/market-benchmark-matrix.md`
- Modify: `docs/publication-notes.md`

- [ ] **Step 1: Write failing registry tests**

Test that every row has an evidence class, primary source URL, access date,
dataset/metric/k/cache/latency scope, hardware fields, mismatch reasons, and
permitted wording. Reject `direct-controlled` registry rows and reject a
superiority phrase when any comparability field is unknown.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
python3 -m unittest scripts.test_validate_reported_comparisons -v
```

Expected: missing validator.

- [ ] **Step 3: Implement and populate the registry**

Use only first-party vendor pages and papers. Store reported values verbatim as
numeric fields plus units, but paraphrase surrounding claims. Set
`permitted_wording=context-only` unless every comparison gate is satisfied.

- [ ] **Step 4: Update methodology documentation**

Document the evidence classes, primary p95/recall claim, hardware definition,
S3 Vectors managed-service limitation, confirmatory repetition policy, and
prohibition on plotting reported values as direct measurements.

- [ ] **Step 5: Run tests and verify GREEN**

Run:

```bash
python3 -m unittest \
  scripts.test_validate_reported_comparisons \
  scripts.test_validate_research_docs -v
```

Expected: all selected tests pass.

### Task 8: Full verification and launch readiness

**Files:**
- Modify only if verification exposes a defect.

- [ ] **Step 1: Run Python benchmark tests**

Run:

```bash
python3 -m unittest discover -s scripts -p 'test_*.py' -v
```

Expected: zero failures and zero errors.

- [ ] **Step 2: Run Rust benchmark tests**

Run:

```bash
cargo test -p borsuk --example hybrid_retrieval_bench
cargo test -p borsuk --tests
```

Expected: zero test failures.

- [ ] **Step 3: Run shell and repository checks**

Run:

```bash
bash -n \
  scripts/bench_publication_v2_aws.sh \
  scripts/launch_aws_publication_v2.sh \
  scripts/bench_hybrid_retrieval_matrix.sh \
  scripts/bench_s3_vectors_matrix.sh
git diff --check
```

Expected: every command exits zero.

- [ ] **Step 4: Generate and inspect the frozen schedule**

Run:

```bash
python3 scripts/publication_protocol.py schedule \
  docs/research/publication-v2-manifest.json \
  --output /tmp/borsuk-publication-v2-schedule.csv
```

Expected: five unique repetition IDs, alternating direct-system order, unique
result/index/cache keys, and no pilot identifiers.

- [ ] **Step 5: Check pilot state before paid launch**

Use a read-only AWS status check. Launch v2 only after the pilot tmux campaign
has exited and its result sync is terminal. If the pilot is still active, leave
the verified v2 launcher ready and do not create resource contention.

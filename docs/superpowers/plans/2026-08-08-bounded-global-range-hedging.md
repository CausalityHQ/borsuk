# Bounded Global Range Hedging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound uncached global-PQ S3 range-wave tail latency without changing candidates, recall, logical bytes, or durable Arrow/Parquet artifacts.

**Architecture:** Add a generic one-hedge asynchronous fetch primitive inside storage, thread an optional hedge delay from `OpenOptions` into the collection read runtime, and apply it only to immutable global-PQ striped ranges. Keep the default disabled until a terminal five-repeat AWS comparison proves the 75 ms candidate. A dedicated writer-0 cohort protocol runs with disk cache disabled and a fail-closed validator measures latency plus physical amplification.

**Tech Stack:** Rust, Tokio/futures, `object_store`, Arrow/Parquet, Bash, Python `unittest`, AWS CLI/SSM/S3.

## Global Constraints

- Preserve inserted-ID recall@10 1.0 and the existing candidate/rerank path.
- Durable data remains standard Arrow/Parquet in S3; no new service or coordination dependency.
- The option defaults to `None` until terminal evidence separately promotes 75 ms.
- Start at most one hedge per slow physical stripe; count every issued backing GET.
- No disk cache in qualification; do not use cache to mask core performance.
- Never inspect incomplete campaign CSV files; run the fail-closed validator after all terminal markers and before measurements.
- Commit coherent verified slices directly to `origin/main`; no PR and no force push.

---

### Task 1: One-hedge asynchronous fetch primitive

**Files:**
- Modify: `crates/borsuk/src/storage.rs`

**Interfaces:**
- Consumes: two owned futures produced by the same `FnMut() -> Fut` request factory.
- Produces: `async fn fetch_with_optional_hedge<F, Fut, T, E>(fetch: F, hedge_after: Option<Duration>) -> Result<T, E>`.

- [ ] **Step 1: Write the failing behavior tests**

Add storage-module tests named:

```rust
#[test]
fn slow_fetch_is_hedged_once_and_fast_fetch_is_not_hedged() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let started = Instant::now();
    let value = runtime.block_on(fetch_with_optional_hedge(
        {
            let attempts = Arc::clone(&attempts);
            move || {
                let ordinal = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(if ordinal == 0 {
                        Duration::from_millis(100)
                    } else {
                        Duration::from_millis(5)
                    })
                    .await;
                    Ok::<&'static [u8], &'static str>(if ordinal == 0 {
                        b"primary".as_slice()
                    } else {
                        b"hedge".as_slice()
                    })
                }
            }
        },
        Some(Duration::from_millis(20)),
    )).unwrap();
    assert_eq!(value, b"hedge");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(started.elapsed() < Duration::from_millis(80));

    let fast_attempts = Arc::new(AtomicUsize::new(0));
    let fast = runtime.block_on(fetch_with_optional_hedge(
        {
            let fast_attempts = Arc::clone(&fast_attempts);
            move || {
                fast_attempts.fetch_add(1, Ordering::SeqCst);
                async {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Ok::<_, &'static str>(b"fast")
                }
            }
        },
        Some(Duration::from_millis(20)),
    )).unwrap();
    assert_eq!(fast, b"fast");
    assert_eq!(fast_attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn hedged_fetch_uses_the_other_success_and_fails_only_after_both_fail() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let success = runtime.block_on(fetch_with_optional_hedge(
        {
            let attempts = Arc::clone(&attempts);
            move || {
                let ordinal = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(if ordinal == 0 {
                        Duration::from_millis(30)
                    } else {
                        Duration::from_millis(5)
                    })
                    .await;
                    if ordinal == 0 {
                        Ok::<&'static [u8], &'static str>(b"primary".as_slice())
                    } else {
                        Err("hedge")
                    }
                }
            }
        },
        Some(Duration::from_millis(20)),
    )).unwrap();
    assert_eq!(success, b"primary");

    let attempts = Arc::new(AtomicUsize::new(0));
    let error = runtime.block_on(fetch_with_optional_hedge(
        {
            let attempts = Arc::clone(&attempts);
            move || {
                let ordinal = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    tokio::time::sleep(if ordinal == 0 {
                        Duration::from_millis(30)
                    } else {
                        Duration::from_millis(5)
                    })
                    .await;
                    Err::<&'static [u8], _>(if ordinal == 0 { "primary" } else { "hedge" })
                }
            }
        },
        Some(Duration::from_millis(20)),
    )).unwrap_err();
    assert_eq!(error, "hedge");
}
```

The first test uses an atomic attempt ordinal: attempt zero sleeps 100 ms,
attempt one sleeps 5 ms, and the hedge delay is 20 ms. Assert result `b"hedge"`,
two attempts, and elapsed time below 80 ms. Its fast subcase sleeps 5 ms and
asserts exactly one attempt.

- [ ] **Step 2: Observe RED**

Run:

```bash
RUSTC_WRAPPER=/usr/local/libexec/devbox-rustc-wrapper \
SCCACHE_DIR=/data/cache/sccache \
CARGO_TARGET_DIR=/data/target/borsuk-prod-ready-v9 \
cargo test --locked -p borsuk storage::tests::slow_fetch_is_hedged_once_and_fast_fetch_is_not_hedged -- --exact
```

Expected: compile failure because `fetch_with_optional_hedge` does not exist.

- [ ] **Step 3: Implement the minimal primitive**

Implement this state machine in `storage.rs`:

```rust
async fn fetch_with_optional_hedge<F, Fut, T, E>(
    mut fetch: F,
    hedge_after: Option<Duration>,
) -> std::result::Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let Some(delay) = hedge_after else { return fetch().await };
    let primary = fetch();
    tokio::pin!(primary);
    if delay.is_zero() { return primary.await; }
    tokio::select! {
        result = &mut primary => result,
        () = tokio::time::sleep(delay) => {
            let hedge = fetch();
            tokio::pin!(hedge);
            tokio::select! {
                result = &mut primary => match result {
                    Ok(value) => Ok(value),
                    Err(primary_error) => hedge.await.or(Err(primary_error)),
                },
                result = &mut hedge => match result {
                    Ok(value) => Ok(value),
                    Err(hedge_error) => primary.await.or(Err(hedge_error)),
                },
            }
        }
    }
}
```

Use the actual error-handling form accepted by the compiler without changing
the contract: one failure waits for the other request and two failures return
an error.

- [ ] **Step 4: Observe GREEN and run neighboring storage tests**

```bash
cargo test --locked -p borsuk storage::tests::slow_fetch_is_hedged_once_and_fast_fetch_is_not_hedged -- --exact
cargo test --locked -p borsuk storage::tests::hedged_fetch_uses_the_other_success_and_fails_only_after_both_fail -- --exact
cargo test --locked -p borsuk storage::tests::global_rerank_range_wave_does_not_serialize_twenty_small_gets -- --exact
```

Expected: all pass with no warning.

### Task 2: Apply the hedge only to global-PQ stripes

**Files:**
- Modify: `crates/borsuk/src/storage.rs`
- Modify: `crates/borsuk/src/index.rs`

**Interfaces:**
- Consumes: `OpenOptions::global_pq_slow_read_hedge_after: Option<Duration>`.
- Produces: `Storage::read_striped_range(relative, range, stripe_bytes, max_parallel, hedge_after)` and the same ordered `ReadBytes` as today.

- [ ] **Step 1: Write the failing ordered multi-stripe test**

Add a real object-store integration test that writes literal bytes
`b"abcdefghijkl"`, delays only the first attempt for one requested range, calls
the public-in-crate striped helper with four-byte stripes and a 20 ms hedge,
then asserts the returned bytes equal the literal source and that only the slow
stripe issued two GET attempts. Name the production mutation it catches:
`hedged_striped_read_preserves_order_and_hedges_only_the_slow_stripe`.

- [ ] **Step 2: Observe RED**

```bash
cargo test --locked -p borsuk storage::tests::hedged_striped_read_preserves_order_and_hedges_only_the_slow_stripe -- --exact
```

Expected: compile failure because `read_striped_range` has no hedge parameter.

- [ ] **Step 3: Thread the option through production code**

Add the field to `OpenOptions`, its `None` default, and
`CollectionReadRuntime`. Pass it at `index.rs`'s sole global-PQ striped call:

```rust
self.storage.read_striped_range(
    path,
    start as u64..end as u64,
    self.read_runtime.global_pq_prefetch_stripe_bytes as u64,
    DEFAULT_GLOBAL_PQ_PREFETCH_STRIPES,
    self.read_runtime.global_pq_slow_read_hedge_after,
)?
```

Inside `read_ranges_with_policy`, accept the optional hedge only from the
striped caller and wrap each physical GET factory with
`fetch_with_optional_hedge`. Ordinary sidecar, WAL, manifest, and coordination
reads pass `None`.

- [ ] **Step 4: Observe GREEN and mutation coverage**

Run the ordered hedge test, all `storage::tests::read_striped_range*` tests, and
the resident global-PQ test group. Then temporarily pass `None` at the global
call and verify the delayed integration test fails before restoring the hedge.

- [ ] **Step 5: Format and commit the production slice**

```bash
cargo fmt --all -- --check
git diff --check
git add crates/borsuk/src/storage.rs crates/borsuk/src/index.rs
git commit -m "perf: hedge slow global range reads"
```

### Task 3: Exact uncached writer-cohort benchmark protocol

**Files:**
- Modify: `crates/borsuk/examples/group_commit_bench.rs`

**Interfaces:**
- Consumes environment variables `BORSUK_GROUP_COMMIT_READ_WRITER=0`,
  `BORSUK_GROUP_COMMIT_READ_QUERIES=500`, and
  `BORSUK_GROUP_COMMIT_HEDGE_AFTER_MS=none|75`.
- Produces protocol `read-hedge-qualification`, `reads.csv`, `summary.csv`,
  `environment.txt`, and terminal arm marker.

- [ ] **Step 1: Add failing shape and selection tests**

Tests must hand-check that 500 selections for writer zero span operation 0
through 998, never select another writer, and remain identical across control
and candidate. Add invalid-shape cases for cache directory presence, any writer
other than zero, query count other than 500, and hedge values other than
`none`/`75`.

- [ ] **Step 2: Observe RED**

```bash
cargo test --locked -p borsuk --example group_commit_bench read_hedge_qualification -- --nocapture
```

Expected: failure because the protocol and selector are absent.

- [ ] **Step 3: Implement exact protocol**

Open with `cache_dir: None`, 1 MiB stripes, and the parsed optional 75 ms hedge.
Select each operation with literal integer sampling:

```rust
let operation = query * operations / query_count;
let position = writer * operations + operation;
samples[position].clone()
```

Reuse `measure_reads` unchanged so candidates, `k=10`, four probes, 16 rerank
candidates, logical bytes, physical requests, and phase telemetry stay honest.
Reject every shape outside 8 writers, 1,000 operations, 16 records/operation,
768 dimensions, writer zero, 500 queries, and repetitions 1..=5.

- [ ] **Step 4: Observe GREEN**

Run the example tests and an in-memory structural smoke with 2 writers, 2
operations, 8 dimensions, 4 queries, hedge disabled and 5 ms candidate. The
smoke must issue zero writes during reads and preserve every expected ID.

- [ ] **Step 5: Commit the benchmark-binary slice**

```bash
git add crates/borsuk/examples/group_commit_bench.rs
git commit -m "bench: add uncached range hedge reads"
```

### Task 4: Preregister, run, and validate the paired campaign

**Files:**
- Create: `docs/research/global-range-hedge-qualification.json`
- Create: `scripts/bench_global_range_hedge_qualification.sh`
- Create: `scripts/launch_aws_global_range_hedge_qualification.sh`
- Create: `scripts/validate_global_range_hedge_qualification.py`
- Create: `scripts/test_validate_global_range_hedge_qualification.py`
- Modify: `scripts/check_repo_policy.py`
- Modify: `scripts/test_check_repo_policy.py`
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: this plan

**Interfaces:**
- Consumes: immutable v67 index/samples URIs and the exact protocol from Task 3.
- Produces: ten terminal arms, root `GLOBAL_RANGE_HEDGE_QUALIFICATION_COMPLETE`,
  immutable raw/resource/storage artifacts, and a validator JSON decision.

- [ ] **Step 1: Write RED validator fixtures**

Create one literal terminal fixture that passes every invariant and mutations
for missing markers, wrong hedge, cache enabled, query mismatch, recall below
1.0, logical byte mismatch, writes, p95/worst-repeat gate, fewer than 4/5 paired
wins, p50 regression, GET amplification, and backing-byte amplification.

- [ ] **Step 2: Observe RED**

```bash
python3 -m unittest scripts.test_validate_global_range_hedge_qualification
```

Expected: import failure because the validator does not exist.

- [ ] **Step 3: Implement manifest, runner, launcher, validator, and policy discovery**

The validator checks root and all arm markers before opening any CSV. It then
requires the exact v67 identities and shape, reconciles raw rows to summaries,
and computes the six promotion gates from the design. The launcher must check
AWS account `453182569524`, profile `causality`, instance health, SSM health,
free disk, and absence of another benchmark process before starting one
retained tmux session.

- [ ] **Step 4: Run validator and harness tests**

```bash
python3 -m unittest scripts.test_validate_global_range_hedge_qualification scripts.test_check_repo_policy
python3 scripts/check_repo_policy.py
bash -n scripts/bench_global_range_hedge_qualification.sh scripts/launch_aws_global_range_hedge_qualification.sh
```

- [ ] **Step 5: Run a local structurally valid smoke**

Use an isolated `/data/home/rb/borsuk-local-qual/<commit>/global-range-hedge-smoke` directory and
the canonical terminal local fixture. Record structure only; do not claim local
latency as S3 evidence.

- [ ] **Step 6: Run exact assurance once**

```bash
cargo fmt --all -- --check
git diff --check
python3 scripts/check_repo_policy.py
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets --all-features
uv run --python 3.12 --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
```

- [ ] **Step 7: Commit and fast-forward push**

Fetch `origin/main`, require it to be an ancestor of `HEAD`, commit the complete
harness slice, and push `HEAD:main` without force.

- [ ] **Step 8: Launch and monitor terminally**

Launch exactly once if no workload competes. Retain the execution handle; wait
in at most 55-second observations around one 15-minute background timer. Check
only terminal markers and EC2/SSM/EBS health while active. On terminal state,
run the repository validator before reading measurements, preserve raw
artifacts, record only defensible results, and either promote the 75 ms default
or leave it disabled.

### Task 5: Resume scalability from the evidence-selected revision

**Files:**
- Modify: `docs/research/group-commit-scalability-attempt-ledger.md`
- Modify: relevant production plan checkboxes

**Interfaces:**
- Consumes: terminal hedge decision and its exact source revision.
- Produces: a frozen source revision eligible for the 2K/16K × 1/8/32 writer
  matrix or a new root-cause iteration.

- [ ] **Step 1: Apply the terminal decision without goal substitution**

If all hedge gates pass, change the `OpenOptions` default to 75 ms with a
focused test, complete gate, and separate commit. If any gate fails, retain
`None`, record the failure, and preregister the fused base-plus-delta scheduler.

- [ ] **Step 2: Run the scalability matrix only from a verified frozen revision**

Preserve five repetitions, raw/resource/storage telemetry, recall@10 1.0,
write/read p95 below 200 ms, and the existing throughput gates. Do not inspect
partial CSVs. A passing 128k experiment is not evidence for 100M scale; larger
dataset and concurrency qualifications remain required.

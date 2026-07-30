# Gate-Free Explicit-ID Claims Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the collection-wide explicit-ID claim gate while preserving deadlock freedom, duplicate exclusion, crash recovery, and version-safe rollback.

**Architecture:** Explicit-ID batches continue hashing IDs into the existing fixed shard set. Every writer acquires its sorted shard paths sequentially, releases a partial set on contention or error, and retries with deterministic jitter; the existing transaction state and version-fenced release remain the recovery authority.

**Tech Stack:** Rust, object_store coordination CAS, BORSUK cell WAL, Cargo integration tests, Markdown architecture documentation.

---

### Task 1: Prove the collection-wide gate is observable

**Files:**
- Modify: `crates/borsuk/tests/cell_wal.rs`

- [ ] **Step 1: Add the failing filesystem regression**

Add this test beside `explicit_id_appends_do_not_touch_the_collection_wide_generated_id_counter`:

```rust
#[test]
fn explicit_id_appends_do_not_create_a_collection_wide_claim_gate() {
    let directory = tempfile::tempdir().unwrap();
    let uri = directory.path().to_string_lossy().into_owned();
    let mut index = BorsukIndex::create(index_config(uri)).unwrap();

    index
        .add(vec![VectorRecord::new("caller-owned-id", vec![1.0, 2.0])])
        .unwrap();

    assert!(
        !directory
            .path()
            .join("id-directory/claim-shards/GATE")
            .exists(),
        "disjoint explicit-ID batches must not serialize through a collection-wide gate"
    );
}
```

- [ ] **Step 2: Run the regression and verify RED**

Run:

```bash
cargo test -p borsuk --test cell_wal \
  explicit_id_appends_do_not_create_a_collection_wide_claim_gate -- --exact
```

Expected: the assertion fails because the current acquisition path leaves an
`Available` value at `id-directory/claim-shards/GATE`.

- [ ] **Step 3: Commit the red test**

```bash
git add crates/borsuk/tests/cell_wal.rs
git commit -m "test: expose explicit ID claim gate"
```

### Task 2: Acquire claim shards in deterministic order

**Files:**
- Modify: `crates/borsuk/src/cell_wal.rs`

- [ ] **Step 1: Remove the gate path and gate acquisition helper**

Delete `claim_gate_path` and `acquire_claim_gate`. Do not change shard hashing,
transaction state, owner reclamation, claim encoding, or release fencing.

- [ ] **Step 2: Replace `acquire_claim_shards` with ordered acquisition**

Use the already sorted `BTreeSet<u8>` to build shard paths. On each retry,
acquire paths in ascending order through `try_acquire_claim`. Stop at the first
contended path, release the partial set with `release_claims`, back off with
`claim_retry_delay`, and retry. On an error, release the partial set before
returning the original error.

The function must have this shape:

```rust
fn acquire_claim_shards(
    storage: &Storage,
    transaction_id: &str,
    shards: &BTreeSet<u8>,
) -> Result<Vec<CellWalHeldClaim>> {
    const MAX_ATTEMPTS: usize = 10_000;
    let paths = shards
        .iter()
        .map(|&shard| claim_shard_path(shard))
        .collect::<Vec<_>>();
    let mut last_contended_path = paths.first().cloned().unwrap_or_else(|| {
        "id-directory/claim-shards".to_string()
    });

    for attempt in 0..MAX_ATTEMPTS {
        let mut acquired = Vec::with_capacity(paths.len());
        let mut contended = false;
        for path in &paths {
            match try_acquire_claim(storage, transaction_id, path) {
                Ok(ClaimAcquireAttempt::Acquired(claim)) => acquired.push(claim),
                Ok(ClaimAcquireAttempt::Contended) => {
                    last_contended_path = path.clone();
                    contended = true;
                    break;
                }
                Err(error) => {
                    let _ = release_claims(storage, transaction_id, acquired);
                    return Err(error);
                }
            }
        }
        if !contended {
            return Ok(acquired);
        }
        let _ = release_claims(storage, transaction_id, acquired);
        std::thread::sleep(claim_retry_delay(transaction_id, attempt));
    }

    Err(BorsukError::ConcurrentModification {
        path: last_contended_path,
    })
}
```

An empty shard set returns an empty guard immediately. All non-empty batches
use a single total order, so circular wait is impossible.

- [ ] **Step 3: Run focused tests and verify GREEN**

Run:

```bash
cargo test -p borsuk --test cell_wal \
  explicit_id_appends_do_not_create_a_collection_wide_claim_gate -- --exact
cargo test -p borsuk --test cell_wal -j2
```

Expected: the new regression and the complete cell-WAL suite pass.

- [ ] **Step 4: Commit the implementation**

```bash
git add crates/borsuk/src/cell_wal.rs
git commit -m "perf: acquire explicit ID shards without a global gate"
```

### Task 3: Stress distinct and overlapping writers

**Files:**
- Modify: `crates/borsuk/tests/cell_wal.rs`

- [ ] **Step 1: Add a 32-writer distinct-ID regression**

Create one memory-backed index, open 32 independent handles, synchronize them
with `Barrier::new(32)`, and have writer `n` insert
`disjoint-writer-{n:02}`. Join every writer, reopen the index, and require all
32 IDs exactly once.

```rust
#[test]
fn gate_free_distinct_explicit_id_writers_all_commit() {
    const WRITERS: usize = 32;
    let object_store = store();
    let uri = "memory:///gate-free-distinct-explicit-ids";
    BorsukIndex::create_with_object_store(
        Arc::clone(&object_store),
        IndexConfig {
            uri: uri.to_string(),
            metric: VectorMetric::Euclidean,
            dimensions: 2,
            segment_max_vectors: 1_000,
            ram_budget_bytes: None,
            text: false,
            named_vectors: Default::default(),
        },
    )
    .unwrap();
    let barrier = Arc::new(Barrier::new(WRITERS));
    let handles = (0..WRITERS)
        .map(|writer| {
            let object_store = Arc::clone(&object_store);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let mut index = BorsukIndex::open_with_object_store(
                    object_store,
                    "memory:///gate-free-distinct-explicit-ids",
                )
                .unwrap();
                barrier.wait();
                index.add(vec![VectorRecord::new(
                    format!("disjoint-writer-{writer:02}"),
                    vec![writer as f32, 0.0],
                )])
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().unwrap().unwrap();
    }

    let reopened = BorsukIndex::open_with_object_store(object_store, uri).unwrap();
    let ids = reopened
        .list_records(0, WRITERS)
        .unwrap()
        .into_iter()
        .map(|record| record.0.to_string())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), WRITERS);
}
```

- [ ] **Step 2: Run concurrency regressions**

Run:

```bash
cargo test -p borsuk --test cell_wal \
  concurrent_index_handles_append_without_collection_wide_current_contention \
  -- --exact
cargo test -p borsuk --test cell_wal \
  concurrent_insert_only_batches_commit_a_shared_id_once -- --exact
cargo test -p borsuk --test cell_wal \
  gate_free_disjoint_explicit_id_writers_all_commit -- --exact
```

Expected: disjoint writers all commit; the shared-ID race still has exactly one
success and one duplicate failure.

- [ ] **Step 3: Run failure and recovery suites**

Run:

```bash
cargo test -p borsuk --test crash_recovery -j2
cargo test -p borsuk --test fault_injection -j2
cargo test -p borsuk --test concurrency_stress -j2
```

Expected: all suites pass without leaked locks, resurrected transactions, or
partial visibility.

- [ ] **Step 4: Commit stress coverage**

```bash
git add crates/borsuk/tests/cell_wal.rs
git commit -m "test: stress gate-free explicit ID writers"
```

### Task 4: Correct the durable architecture description

**Files:**
- Modify: `docs/architecture.md`
- Modify: `docs/storage-format.md`
- Modify: `docs/production-readiness.md`
- Modify: `docs/research/production-hardening-audit-2026-07-28.md`

- [ ] **Step 1: Remove the gate from storage diagrams**

Delete `claim-shards/GATE` from the architecture tree. Replace prose that says
the gate makes parallel acquisition all-or-none with the deterministic ordered
acquisition and version-fenced partial release contract.

Use this contract consistently in all three public architecture documents:

```text
Explicit-ID batches hash IDs into the fixed claim shards and acquire the
deduplicated shard paths in ascending order. Contention releases only the
caller's version-fenced partial set before a jittered retry. The total order
prevents circular wait, and disjoint shard sets share no coordination object.
```

- [ ] **Step 2: Record implementation versus promotion status**

In the hardening audit, state that the collection-wide gate implementation
blocker is closed by ordered shard acquisition, while the 1/8/32/128-writer
production measurement remains open. Do not claim improved throughput before
that matrix exists.

Add this dated implementation update:

```text
Implementation update (2026-07-30): explicit-ID batches no longer acquire the
collection-wide claim GATE. Writers acquire their fixed claim shards in
ascending order and version-safely release partial acquisitions on contention
or error. Duplicate-race, failed-batch release, stale-checkpoint, crash, and
fault suites remain mandatory. The performance promotion gate remains open
until the frozen 1/8/32/128-writer matrix completes.
```

- [ ] **Step 3: Run documentation contracts**

Run:

```bash
node scripts/test_docs_web.mjs
node scripts/sync_docs_examples.mjs --check
python3 scripts/validate_research_docs.py
python3 scripts/check_repo_policy.py
```

Expected: all commands exit zero.

- [ ] **Step 4: Commit documentation**

```bash
git add docs/architecture.md docs/storage-format.md \
  docs/production-readiness.md \
  docs/research/production-hardening-audit-2026-07-28.md
git commit -m "docs: record gate-free explicit ID coordination"
```

### Task 5: Run the complete local gate and push

**Files:**
- Verify only.

- [ ] **Step 1: Run formatting and lint**

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets -- -D warnings
```

Expected: both exit zero.

- [ ] **Step 2: Run complete Rust and methodology gates**

```bash
cargo test --locked --workspace --all-targets -j2
BORSUK_TEST_BUILD_JOBS=2 bash scripts/check_rust_test_build.sh
uv run --python 3.12 \
  --with-requirements scripts/requirements-format-bench.txt \
  python -m unittest discover -s scripts -p 'test_*.py'
```

Expected: every non-ignored target passes.

- [ ] **Step 3: Check diff hygiene and push**

```bash
git diff --check
git status --short --branch
git push origin codex/prod-v8
```

Expected: the branch is clean and the remote advances to the verified commits.

# Borsuk Production Readiness Implementation Plan (rev 3 — review-approved)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. In this session the plan is executed by a Workflow of Codex/Claude agents, one task at a time, sequentially, in the main worktree.

**Goal:** Close the evidence-backed gaps between borsuk's current state and the product goal: production-ready blob-storage vector search with near-zero RAM, guaranteed recall semantics, and high write/read performance.

**Architecture:** All durable state stays binary Parquet + content-addressed immutable blobs behind an `object_store` backend; changes harden the publish/GC/read paths and add explicit guarantee reporting rather than restructuring the format. No new storage formats; no JSON manifests (repo policy forbids them).

**Tech Stack:** Rust (crates/borsuk), object_store 0.14 (conditional PUT, multipart), Parquet/Arrow, PyO3 bindings (crates/borsuk-python + python/), napi-rs bindings (crates/borsuk-node + packages/borsuk), tracing (feature-gated).

**Review status:** Rev 1 reviewed by two independent reviewers (Fable, GPT-5.5/Codex); all blocker/major findings incorporated below. Key corrections: portable CAS is `PutMode::Create` on versioned objects (LocalFileSystem does NOT support `PutMode::Update` in object_store 0.14 — local.rs:398 returns NotImplemented); publish today writes CURRENT before routing layer indexes (storage.rs:119-121) and must be reordered; GC needs a retention policy; `Degraded` must cover routing preselection; `add()` signatures must stay additive; checksum-verified cache hits already exist (storage.rs:572-606).

---

## Ground rules for every task

- TDD: write the failing test first, watch it fail, implement, watch it pass.
- After each task run: `cargo fmt --all -- --check && cargo clippy --locked --workspace --all-targets -- -D warnings && cargo test --locked -p borsuk <targeted tests>`.
- Commit after each task with a focused message (imperative mood, match repo style e.g. "Guard concurrent index publishes"). `git add` only the files you touched — never `git add -A` (work.md and this plan are untracked on purpose).
- API additions must keep Rust/Python/TypeScript parity where the task says so. Python tests: `python/tests/`; Node tests: `packages/borsuk/`.
- Docs live in `docs/` and `README.md`; `python3 scripts/check_repo_policy.py` must stay green after any doc change. Never reference `design.md`/`multimode.md`; never introduce JSON manifests or `payload_refs`.
- The full gate suite (fmt, clippy, workspace tests, python, node, repo policy, docs smoke, actionlint if workflows changed) runs at the milestones marked below, not after every task.

Key files:
- `crates/borsuk/src/storage.rs` — object-store wrapper; publish path `publish_manifest_metadata` (~142, full publish sequence ~112-163); cache + checksum-verified reads (`read_bytes_with_cache_status_and_checksum` ~572-606); `write_bytes` ~531; `from_uri` ~73, ~855-875.
- `crates/borsuk/src/index.rs` — search paths, budgets (~1799-1911, ~3313, ~3373), routing preselection (~1885, ~2140-2165), GC (`gc_obsolete_segments` ~1594-1650), compaction, reports, `LOCAL_GRAPH_NEIGHBORS` (~35), add paths (~484-541).
- `crates/borsuk/src/manifest.rs` (version allocation ~59, ~101, ~135), `format.rs` — Parquet tables, versioning.
- `crates/borsuk/src/error.rs` — `BorsukError` taxonomy.
- Bindings: `crates/borsuk-python/src/lib.rs` (single exception class ~1431), `crates/borsuk-node/src/lib.rs` (add ~181), `python/src/borsuk/__init__.pyi`.
- Tests: `crates/borsuk/tests/local_index.rs`, `large_scale.rs`, `s3_compatible.rs`.

---

### Task 0: Store-injection test seam

Several later tasks (1B, 3, 8, 12) need to inject a wrapped/mock/shared store, which is impossible today: `Index` is constructible only from URI strings (index.rs:229-309), `Storage` is `pub(crate)`, and `memory:///` URIs create a fresh `InMemory` per parse (object_store parse.rs:203), so two handles can never share a memory store.

**Files:** Modify `crates/borsuk/src/index.rs`, `crates/borsuk/src/storage.rs`, `crates/borsuk/src/lib.rs`. Test: `crates/borsuk/tests/local_index.rs`.

- [ ] Add a `#[doc(hidden)]` constructor (e.g. `Index::open_with_object_store(store: Arc<dyn ObjectStore>, ...)` plus create equivalent) that bypasses URI parsing. Not part of the public documented API; mark `#[doc(hidden)]` and comment it as a test seam.
- [ ] Test: two `Index` handles constructed over one shared `InMemory` store both see the same data after one publishes.
- [ ] Add a small fault-injecting `ObjectStore` wrapper (fail Nth operation matching a path predicate, then optionally recover; injectable latency). Used by Tasks 1-3, 8, 12. Placement: `crates/borsuk/tests/common/mod.rs` (shared by integration tests) — NOT a `#[cfg(test)]` module in `src/`, which integration tests cannot see. If the wrapper needs crate internals, use a `#[doc(hidden)]` lib module instead.
- [ ] Verify targeted tests; commit.

### Task 1: Safe concurrent publish — conditional CREATE on all versioned objects, CURRENT truly last — BLOCKER

Conditional PUT on CURRENT alone is NOT sufficient: `publish_manifest_metadata` writes versioned manifest/routing/pivot tables with plain overwrite `put` before CURRENT, so a CAS loser would clobber the winner's already-published table bytes (checksum corruption on next open). Additionally the publish sequence currently writes CURRENT *before* the routing layer page indexes (storage.rs:119-121) — a reader can observe a version whose routing pages are missing. Portable CAS primitive is `PutMode::Create` (works on every backend including local FS).

**Files:** Modify `crates/borsuk/src/storage.rs` (publish sequence), `crates/borsuk/src/error.rs`, `crates/borsuk/src/index.rs` (publish call sites, refresh-on-conflict). Test: `crates/borsuk/tests/local_index.rs` (using Task 0 seam). Docs: `docs/storage-format.md`, README.

- [ ] Failing test A (shared InMemory store): two handles at version N both `add()`; exactly one succeeds, the loser gets `BorsukError::ConcurrentModification`; a fresh open sees the winner's consistent manifest; no object referenced by CURRENT has been overwritten by the loser.
- [ ] Failing test B (fault wrapper): inject failure after metadata tables are written but before CURRENT; reopen; assert old version loads cleanly and search works; **then assert a subsequent `add()` on a fresh handle succeeds** — it must skip the orphaned N+1 namespace and publish as N+2 (see version-skip recovery below). Without this recovery the crash state leaves the index permanently unwritable (every writer recomputes N+1, hits `AlreadyExists`, refreshes an unchanged CURRENT, retries N+1 forever).
- [ ] Implement publish ordering: (1) segment/graph payloads, (2) routing page content, (3) routing layer page indexes, (4) versioned manifest/routing/pivot tables, (5) CURRENT — strictly last.
- [ ] Implement conditional writes: every versioned object for candidate version N+1 (manifest/routing/pivot tables, routing layer indexes under `routing/layers/{version}/...`) is written with `PutMode::Create`; `AlreadyExists`/`Precondition` maps to `BorsukError::ConcurrentModification` (the first creator of the versioned table namespace wins; the first Create in the fixed write order — the L0 layer index — is the linearization point). Segment/graph payloads are UUIDv4-named (index.rs:545) so their plain puts cannot collide. Content-addressed routing *pages* keep overwrite semantics (identical content, benign). CURRENT itself: attempt `PutMode::Update{etag}`, fall back to plain put on `Error::NotImplemented` — do NOT sniff URI schemes (Update is supported by S3/Azure/GCS/InMemory; only `LocalFileSystem` lacks it, local.rs:398). This is safe against same-version clobbering (version-namespace Create serialized same-version winners and CURRENT content for a given version is deterministic); cross-version arbitration — e.g. between a version-skipped writer and a slower in-flight one — relies on the CURRENT etag CAS where supported.
- [ ] Version-skip recovery: on `ConcurrentModification`, the handle refreshes CURRENT. If the refreshed CURRENT version is UNCHANGED (meaning the collision came from an orphaned/in-flight namespace, not a completed publish), advance the candidate version past the occupied namespace — probe `PutMode::Create` at incrementing versions (or list `manifests/`) until one succeeds. Document the retry contract, and document that after a version skip, strict pointer safety needs a CAS-capable CURRENT backend (S3/Azure/GCS etag — the etag CAS is what arbitrates a version-skipped writer racing a live in-flight one to CURRENT). On backends without `Update` (local FS), the auto-skip must only fire after CURRENT is confirmed unchanged across a re-check delay (or surface a typed error instead of skipping), because plain-put CURRENT is last-writer-wins across versions; document concurrent multi-process writing on local FS as unsupported for production (single-winner-per-version + best-effort pointer only).
- [ ] Bindings: expose the failure mode distinguishably — add a stable error-code attribute (e.g. `code == "concurrent_modification"`) on the Python exception and TS error rather than string matching; one parity test each (two handles over one local dir, using public URI API — local FS honors `PutMode::Create`).
- [ ] Docs: concurrency model (optimistic single-winner publish; losers refresh and retry) in `docs/storage-format.md` + README production caveats.
- [ ] Verify: targeted tests green; commit.

### Task 2: Complete GC — routing pages, layer indexes, old table versions, retention policy

Immediate deletion of older-than-CURRENT objects is unsafe: open handles keep reading their pinned manifest snapshot (index.rs:1829, 2038, 2064, 2383), and a writer mid-publish has written objects not yet referenced by CURRENT.

**Files:** Modify `crates/borsuk/src/index.rs` (GC ~1594-1650), report types; bindings for new report fields. Test: `crates/borsuk/tests/local_index.rs`.

- [ ] Failing test: create → add → compact ×2 → GC (delete mode, retention 0 for test). Enumerate storage dir: routing/ and manifest/pivot/layer object counts equal exactly the live set referenced by CURRENT; reopen paged; search correctly. Also: the fault-injection orphans from Task 1 test B are reported by dry-run GC.
- [ ] Implement: traverse CURRENT's routing layer indexes across all levels to collect live routing-page checksums; list `routing/pages/`, `routing/layers/`, versioned manifest/routing/pivot tables. Candidate set for deletion = versioned tables/layer indexes and routing pages **not referenced by CURRENT's live set, in either version direction** (both older-than-CURRENT leftovers AND newer-than-CURRENT orphans from crashed publishes — Task 1 test B's orphans are newer than CURRENT and must be reported/reclaimed; `min_age` is what protects legitimately in-flight newer-than-CURRENT publishes). This GC is also the eventual backstop that reclaims skipped version namespaces from Task 1's version-skip recovery. Stream/chunk listings — never hold a full listing in memory.
- [ ] Retention policy: GC takes a `min_age` (default e.g. 24h, configurable to 0) and only deletes objects older than it, so pinned readers and in-flight publishes are protected by a documented grace interval. Document explicitly: GC with `min_age=0` requires external quiescence (no concurrent readers/writers).
- [ ] Extend `GarbageCollectionReport` with `routing_objects_deleted`, `tables_deleted` (`bytes_reclaimed`/`bytes_reclaimable` already exist — index.rs:1615-1650); mirror new fields in Python/TS report types with parity tests.
- [ ] Verify: targeted + existing GC tests green; commit.

### Task 3: Object-store fault behavior — borsuk-level error mapping, multipart, S3 caveats

`RetryConfig` has no string config key in object_store 0.14 (builder-only, aws/builder.rs:945), so it cannot be threaded through `parse_url_opts`; and trait-level mocks bypass object_store's internal HTTP retries entirely. Scope decision: rely on object_store's built-in cloud retries (do NOT rebuild per-scheme builder plumbing this cycle); what borsuk owns is correct error mapping and surviving/failing-fast appropriately.

**Files:** Modify `crates/borsuk/src/storage.rs` (`write_bytes`, error mapping). Test: new `crates/borsuk/tests/fault_injection.rs` (Task 0 wrapper) + extend `s3_compatible.rs`. Docs: `docs/storage-format.md`, README.

- [ ] Failing test (fault wrapper over shared store): transient store errors on GET during search surface as typed retryable errors (or succeed if borsuk already retries at call sites); permanent NotFound/permission errors fail fast with the right `BorsukError` variant; a mid-search transient failure never returns silently-partial results (either error or complete result).
- [ ] Implement whatever error-mapping gaps the test exposes (transient vs permanent classification in the storage layer).
- [ ] Implement multipart in `write_bytes` above 64 MiB via `put_multipart`; single PUT below. Extend S3 smoke with a >64 MiB round-trip (guarded by `BORSUK_S3_TEST_URI`).
- [ ] Docs: "S3 assumptions and caveats" — required consistency (read-after-write, list), optimistic-concurrency publish (Task 1), retries are delegated to object_store's built-ins (state defaults), transient-vs-permanent taxonomy, GC retention interaction. Link from README.
- [ ] Verify: `python3 scripts/check_repo_policy.py`; targeted tests; commit.

### Task 4: Format forward-compatibility

**Files:** Modify `crates/borsuk/src/manifest.rs` / `format.rs` (Parquet readers). Docs: `docs/storage-format.md`. Test: format unit tests or `local_index.rs`.

- [ ] Failing test: a manifest table containing an extra unknown column round-trips through the manifest reader (unknown column ignored, all known fields intact). Same for routing/pivot tables if readers are separate.
- [ ] Implement: readers select known columns by name and ignore unknowns.
- [ ] Docs: versioning policy — what bumps pointer version vs table version; additive columns must be ignorable by same-major readers. Anchor in `scripts/check_repo_policy.py` per existing `assert_contains` pattern (+ update `scripts/test_check_repo_policy.py`).
- [ ] Verify: `python3 scripts/check_repo_policy.py && python3 -m unittest scripts/test_check_repo_policy.py`; commit.

### Task 5: Recall guarantee semantics

`Degraded`-detection must cover ALL silent recall-loss paths, not just the segment-loop budget stops. Routing preselection prunes candidates before the segment loop (`segments_skipped` starts as `segments_total - candidates_total`, index.rs:~1886, driven by `routing_page_overfetch`), so a search can lose true neighbours while every in-loop budget check passes.

**Files:** Modify `crates/borsuk/src/index.rs` (search reports, budgets, preselection), API types; Python + TS bindings. Tests: `crates/borsuk/tests/local_index.rs`, `python/tests/`, `packages/borsuk/`.

- [ ] Failing tests: (a) tiny budgets → `recall_guarantee == Degraded` and measured recall < 1.0 vs exact oracle; (b) approximate mode where routing preselection pruned segments (small overfetch) → `Degraded` even though no in-loop budget fired; (c) full coverage (no preselection pruning, no skips, no truncation, termination reason Complete) → `BudgetComplete`; (d) `SearchMode::Exact` → `Exact`. Note for (c): the loop's lower-bound early exit (`search_stop_reason_before_segment`, index.rs:~1897) inflates `segments_skipped` even for provably-lossless bound pruning, which under this classification reads as `Degraded` (conservative, never unsafe). Keep the (c) fixture small enough that no early stop fires; refining the classification so bound-proven skips retain `BudgetComplete` is optional follow-up, not required this task.
- [ ] Implement `recall_guarantee: RecallGuarantee { Exact, BudgetComplete, Degraded }` on `SearchReport`. `BudgetComplete` iff: termination reason Complete AND `segments_skipped == 0` (including preselection skips) AND no per-segment candidate truncation below segment length. Routing preselection skips, epsilon/byte/latency/max-segment stops, and truncation all classify as `Degraded`.
- [ ] Implement guaranteed search option (per existing options style): disables per-segment truncation AND routing preselection pruning (full page enumeration of routed level), forces exact rerank over all candidates, returns a typed error instead of silently degrading if a hard budget would violate the guarantee. Failing tests: tiny-budget guaranteed → typed error; ample-budget guaranteed → recall == 1.0 vs oracle on a dataset where default approximate search is < 1.0.
- [ ] Bindings parity: enum + option in Python and TS, one test each.
- [ ] Docs: `docs/api.md` + README — table of which mode+option combinations are guaranteed vs empirical; formal statement that Exact returns true k-NN under the index metric. Add "recall guarantee semantics" to `docs/production-readiness.md`.
- [ ] Verify: targeted rust+python+node tests; repo policy; commit.

### Task 6: Ingest reporting (AddReport) + graph-build knob — additive API only

Current shapes must not break: Rust `add -> Result<()>`, `add_vectors -> Result<Vec<String>>` (index.rs:484-508); Python `add() -> list[RecordId]`; Node `add() -> Vec<String>`.

**Files:** Modify `crates/borsuk/src/index.rs` (add paths), config; bindings. Test: `crates/borsuk/tests/local_index.rs`.

- [ ] Failing test: new additive API `add_with_report(...) -> Result<(Vec<String>, AddReport)>` (Rust; keep existing methods delegating to it). `AddReport { segments_written, graph_payloads_written, manifest_tables_written, routing_pages_written, total_bytes_written, bytes_per_vector }`; counters match objects enumerated on local storage; content-addressed pages that were reused are not counted as written.
- [ ] Bindings: additive `add_with_report` (Python returns `(ids, AddReport)`, TS returns `{ids, report}`); existing `add` signatures unchanged; one parity test each.
- [ ] Make graph neighbor count (`LOCAL_GRAPH_NEIGHBORS`, index.rs:35) configurable at index create, validated, persisted in config table; default unchanged.
- [ ] Verify: targeted tests; commit.

**MILESTONE A — full local gate suite** (workspace tests, python, node, repo policy, docs smoke) must be green before proceeding.

### Task 7: Zero-resident RAM lifecycle + deep-routing tests

**Files:** Test: `crates/borsuk/tests/local_index.rs`, `crates/borsuk/tests/large_scale.rs`. Docs: `docs/production-readiness.md`.

- [ ] Lifecycle test: create with small `routing_page_fanout` → batch add → compact → close → reopen with non-resident routing and a ram_budget fitting only config+pivots → 10+ searches spanning segments; assert resident segment summaries stay empty and `resident_bytes_estimate` does not grow across queries.
- [ ] Deep-routing test: `routing_page_fanout=4`, enough vectors to force `routing_max_level >= 2`; compact L0→L1 then within L1; assert level correctness, content-addressed page reuse (compaction bytes_read excludes untouched parents), and search correctness after each step. Keep default-suite runtime reasonable (<60s) — scale vector count down as long as depth ≥ 2 holds.
- [ ] Headroom test in `large_scale.rs` (ignored): budget = estimate + margin, 8 parallel searches at max budgets; record RSS peak vs budget in gate output.
- [ ] Docs: state precisely that `resident_bytes_estimate` covers metadata only; recommend headroom for concurrent queries.
- [ ] Verify: default-suite tests green; commit.

### Task 8: Read-path prefetch (bounded concurrent blob fetches)

The search loop is sync (sync-over-tokio via the existing runtime); budgets are checked before each serial read and `bytes_read` counts consumed reads only. Keep those semantics: reserve budget before scheduling a prefetch; `bytes_read` stays equal to consumed bytes; prefetched-but-unused bytes go in a NEW separate field (`prefetched_bytes_unused`), not into `bytes_read` — do not force report equality with the serial path on that field.

**Files:** Modify `crates/borsuk/src/storage.rs` (async bounded prefetch primitive on the existing Tokio runtime), `crates/borsuk/src/index.rs` (consume in candidate order). Test: `local_index.rs` + fault/latency wrapper from Task 0.

- [ ] Failing equality test: pipelined path (prefetch depth 8) returns identical hits, identical termination reasons, and identical `bytes_read` vs serial path on a multi-segment index; `prefetched_bytes_unused` may differ and is reported.
- [ ] Implement bounded prefetch (semaphore, default depth 8, configurable; depth 1 == serial behavior) with budget reservation before scheduling; consume strictly in candidate order so early-termination decisions are identical.
- [ ] Batch path: request-scoped routing-page cache in `search_batch_with_report` so top/parent pages are fetched once per batch. Test WITHOUT a cache_dir (or by counting store GETs, not `object_cache_hits` — on-disk cache already dedupes when configured): store GETs for routing pages do not scale linearly with batch size.
- [ ] Latency A/B using the Task 0 latency wrapper: depth 8 beats depth 1 wall-clock on an emulated-latency store (sanity, not a strict ratio gate).
- [ ] Verify: targeted tests; commit. (Benchmark artifact regeneration at Milestone C.)

### Task 9: Cache hardening — LRU bound + repair telemetry (NOT re-implementing verification)

Checksum-verified cache hits with delete+refetch ALREADY EXIST (`read_bytes_with_cache_status_and_checksum`, storage.rs:572-606; used at index.rs:2422, 2501, 2672, 2687; tested around local_index.rs:3600). Do not re-implement.

**Files:** Modify `crates/borsuk/src/storage.rs` (cache layer), report types. Test: extend existing corruption tests in `local_index.rs`.

- [ ] Failing test: corrupt a cached segment file → search returns correct results AND reports new counter `cache_repairs == 1` (extend the existing corruption test with the counter assert).
- [ ] Failing test: with `cache_max_bytes` set small, exceeding it evicts oldest objects; subsequent reads re-fetch and still succeed.
- [ ] Implement `cache_repairs` counter surfaced in `SearchReport`; optional `cache_max_bytes` LRU eviction (content-addressed immutable objects — safe to evict anytime). Document default (unbounded) behavior.
- [ ] Verify: targeted tests; commit.

### Task 10: Observability — feature-gated tracing (direct-dependency scoped)

`object_store` already depends transitively on `tracing` (Cargo.lock), so "keep tracing out of the dep graph" is impossible. The requirement is: borsuk itself does not directly depend on or emit `tracing` unless the `tracing` feature is enabled.

**Files:** Modify `crates/borsuk/Cargo.toml` (optional dep + feature), spans in `index.rs`/`storage.rs`. Test: new `crates/borsuk/tests/tracing_smoke.rs` (feature-gated).

- [ ] Failing test (feature on, capturing subscriber): a search and a compaction emit expected spans (open/add/compact/publish/gc/search) with report counters as span fields; segment-skip events carry a reason.
- [ ] Implement behind default-off feature; verify `cargo clippy -p borsuk` (no feature) and `--features tracing` both clean; assert borsuk's direct deps exclude tracing without the feature (Cargo.toml optional dep).
- [ ] Docs: Observability section in `docs/api.md`; note for Python/TS that spans surface via the Rust subscriber.
- [ ] Verify: `cargo test -p borsuk --features tracing --test tracing_smoke`; commit.

**MILESTONE B — full local gate suite green.**

### Task 11: Docs consolidation — mutation model, exact-mode contract

**Files:** `docs/api.md`, `README.md`, `scripts/check_repo_policy.py` (+ its test).

- [ ] Write "Updates and deletes" section: append-only model, supported workflow (or explicit non-support until tombstones), rebuild+GC recipe with runnable example.
- [ ] Anchor the section in the repo-policy script; update `scripts/test_check_repo_policy.py`.
- [ ] Verify: `python3 scripts/check_repo_policy.py && python3 -m unittest scripts/test_check_repo_policy.py && node scripts/test_docs_web.mjs`; commit.

### Task 12: Cold/warm read benchmark stratification

**Files:** Benchmark example (per existing `benchmark_report` structure), `docs/web/assets/benchmarks/`, docs-site loader (`docs/web/`).

- [ ] Extend the benchmark report to emit cold (empty cache dir) vs warm p50/p95 columns using existing cache-hit counters; include prefetch depth 1 vs 8 A/B (Task 8) via the latency wrapper where feasible.
- [ ] Render new columns/CSV on the docs site; `node scripts/test_docs_web.mjs` green.
- [ ] Verify + commit. (Full artifact regeneration at Milestone C.)

**MILESTONE C — release-candidate evidence run:**
- Full gate suite (fmt, clippy, workspace tests, python wheel + unittest, node build + test, repo policy, policy unittest, docs smoke, `git diff --check`).
- `cargo test --locked -p borsuk --test performance_smoke`.
- Regenerate benchmark artifacts incl. `BORSUK_LARGE_SCALE_OUTPUT=... cargo test --locked --release -p borsuk --test large_scale million_vector_local_search_scale_gate -- --ignored --nocapture`; refresh `docs/web/assets/benchmarks/large-scale.csv` (now with rss headroom / gc columns).
- SeaweedFS smoke locally if docker available: `./examples/seaweedfs/run-smoke.sh`; otherwise rely on the CI SeaweedFS job (already wired in ci.yml) and verify it green on the release commit.
- Push; watch CI to completion; fix failures root-cause-first.

## Deferred (explicitly out of scope this cycle)

- SIFT1M adversarial recall benchmark — dataset download; only if time remains after Milestone C.
- 100M-vector gate — machine-scale dependent; deep-routing functional test + honest cost-model note cover the readiness claim.
- Parquet row-group range reads — only if Milestone C cold/warm data shows bytes-per-query dominates.
- Per-scheme object_store builder plumbing for custom RetryConfig — object_store built-in cloud retries are the documented behavior this cycle.
- Cross-platform CI matrix evidence — produced by CI; monitored, not implemented.

## Sequencing rationale

Tasks run **sequentially in one worktree** (index.rs/storage.rs shared by most tasks; parallel worktrees would merge-conflict on 150KB+ files). Task 0 seam first (unblocks 1-3, 8, 12); correctness/durability (1-4); guarantee semantics and reporting (5-6); RAM/scale proof (7); performance (8-9); observability/docs/bench (10-12). Each task is a self-contained brief: a fresh agent with zero context can execute it from this file plus the repo.

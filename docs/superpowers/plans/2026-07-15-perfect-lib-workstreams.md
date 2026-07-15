# Top Vector Lib — Remaining Workstreams (design, turnkey for execution)

**Goal (user, 2026-07-15):** perfect recall, perfect write latency (per-segment WAL),
perfect read latency (DONE — Arrow sidecar), fast compaction, fast GC. Execute after the
benchmark rerun finishes (concurrent cargo would skew benchmark timings). Each is a
separate commit; full gate green per commit (fmt, clippy -D, `cargo test -p borsuk`, ruff,
prettier, check_repo_policy, test_docs_web). Branch `ivf-hnsw-coarse-quantizer`, don't push.

Priority order: **fast compaction → fast GC → per-segment WAL → perfect-recall guarantee**
(cheapest/safest first; WAL changes the durability model so it goes late).

---

## 1. Fast compaction (parallelize the single-threaded PQ/code work)

**Problem:** compaction wall-clock is dominated by per-record PQ/code computation in
`Segment::from_records` (single-threaded); k-means is NOT the bottleneck (see
[[borsuk-curse-of-dim-ivf-hnsw]]: 50k×960 ≈ 177–189s). PQ encoding is embarrassingly
parallel and deterministic per record.

**Approach:**
- Parallelize the per-record PQ/signature/code loop in `crates/borsuk/src/segment.rs`
  (`pq_codes_for_records`, `from_records`). Use `std::thread::scope` chunking (no new dep) or
  add `rayon` if cleaner — each record's code is independent of the others, so results are
  **order-preserving** (collect into a pre-sized `Vec` by index → deterministic, identical
  bytes). Do NOT parallelize anything order- or RNG-dependent (k-means seeding stays serial).
- Gate parallelism on a threshold (e.g. `records.len() > 2048`) so small segments stay serial.
- Keep the centroid/quantizer build serial (it's cheap relative to PQ and seeded).

**Files:** `segment.rs` (PQ loop), maybe `index.rs` compaction call sites.
**Risk:** determinism. **Mitigation:** index-into-preallocated-Vec (not push-from-threads);
the existing reproducibility/compaction tests must stay green byte-for-byte.
**Acceptance:** all compaction tests green; a release micro-bench shows from_records scaling
with cores; segment bytes identical to serial (add a test: serial vs parallel PQ codes equal).

---

## 2. Fast GC (less listing/scanning)

**Problem:** GC builds a keep-set of referenced object paths (segments + graph + filter/bm25/
sparse/**vector** sidecars) from active summaries, then LISTs the bucket and deletes anything
not kept. On object storage, the full LIST + per-object HEAD/age checks dominate.

**Approach (measure first, then cut):**
- The keep-set is already O(active segments). The cost is the bucket-wide LIST + age gating.
  Options: (a) LIST only the known object prefixes (`segments/`, `graphs/`, `vectors/`,
  `fidx/`, `bidx/`, `svidx/`) instead of a full recursive list — fewer/scoped LISTs;
  (b) skip HEAD-for-age when `min_age_ms == 0` (delete-immediately path); (c) parallelize the
  prefix LISTs with the existing runtime.
- Confirm the vector sidecar (`vectors/<cs>.arrow`, added this session) is in BOTH the keep-set
  (it is — index.rs ~3844) AND the GC scan prefixes (so orphaned sidecars are actually
  collected). **Add a test:** GC deletes an orphaned `vectors/…arrow` and keeps live ones.

**Files:** `index.rs` (`gc_obsolete_segments` / GC impl), `storage.rs` (scoped `for_each_object`
per prefix).
**Risk:** deleting a live object (data loss) — the keep-set must be complete. **Mitigation:**
the existing GC tests + a new sidecar-GC test; dry-run mode already exists.
**Acceptance:** GC tests green + new sidecar-orphan test; fewer LIST requests in a report.

---

## 3. Per-segment WAL (write latency)

**Problem:** `add()` writes an immutable segment (+ sidecars + manifest publish) per call —
fine for batches, high per-call latency for small/streaming writes.

**Approach (durability-preserving):**
- A per-index **write-ahead log**: `add()` appends records to an open WAL object
  (`wal/<gen>.arrow`, Arrow IPC stream — append-friendly) and returns once the WAL append is
  durable, WITHOUT building a segment. A background/threshold flush converts accumulated WAL
  records into a real segment + sidecars + manifest publish (the current path). Reads union
  the published segments **and** the un-flushed WAL tail (so read-your-writes holds).
- Crash recovery: on open, replay any un-flushed WAL into memory (or trigger a flush).
- Config: `wal: bool` (index-creation) + flush threshold (records or bytes). Default on for
  low-latency writes; off = today's synchronous segment-per-add.

**Files:** new `wal.rs`; `index.rs` (`add`/`upsert` append path, read-union, open-recovery,
flush); manifest (track WAL generation/offset for recovery).
**Risk:** HIGH — durability + read-your-writes + crash recovery + consistency
([[borsuk-production-roadmap-workmd]] consistency tests must still pass). This is the biggest,
riskiest item; do it last, with its own consistency tests (append-durable, read-tail,
recover-on-open, flush-idempotent).
**Acceptance:** new WAL tests + all existing consistency/upsert/durability tests green; a
write-latency micro-bench shows per-add latency drop with WAL on.

---

## 4. Perfect-recall guarantee (recall = 1.0, not just achievable)

**Problem:** recall = 1.0 is *achievable* (full per-cell scoring at nprobe covering the true
top-k span) but not *guaranteed* at low reads on real high-dim data — centroid+radius bounds
are too loose to prune to exactly the true neighbours ([[borsuk-curse-of-dim-ivf-hnsw]]).

**Approach (make it a first-class, opt-in guarantee):**
- `SearchOptions::guaranteed_recall` already exists (exact/lower-bound path). Extend the
  *approx* path with a **recall-target** mode: keep probing cells (raising effective nprobe)
  until the k-th best distance is provably ≤ the next cell's lower bound (the adaptive-stop
  machinery, inverted: stop only when the bound *guarantees* nothing better remains). This
  gives recall=1.0 with query-adaptive reads — cheap queries stop early, hard ones read more,
  none miss a neighbour.
- Wire as a typesafe read-time config (building block, like `with_adaptive_stop`):
  `with_recall_target(1.0)` / recallTarget. Off by default (perf), on = provable recall.

**Files:** `record.rs` (SearchOptions field + builder), `index.rs` (search loop stop
condition using the centroid lower bound vs running k-th distance), bindings.
**Risk:** the lower-bound must be a true lower bound for the metric (cosine/euclidean geometry
already handles this in the exact path — reuse it). **Acceptance:** a test on real-ish high-dim
data: `with_recall_target(1.0)` returns exactly the brute-force top-k for every query, reading
≤ full nprobe; recall asserted == 1.0.

---

## Cross-cutting
- After all four: rerun benchmarks again (recall + write/compaction/GC latency) and refresh
  docs/web/README with the new numbers + the two-formats / beat-Lance positioning.
- Each workstream is independently shippable; land in priority order, gate green, commit per.

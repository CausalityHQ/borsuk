# work_state.md — status of the work.md roadmap

Companion to `work.md` (the strategic gap analysis vs Pinecone/Qdrant/Milvus/
turbopuffer/S3-Vectors). This file tracks **what has been built, how, why, and
what is left**. Every commit below is on `main` and passed the full gate (fmt,
`clippy --workspace --all-targets -D warnings`, tests, repo policy, docs-web,
tsc). `format_version` stays `1`; the lib is unreleased, so breaking changes are
fine and there is no migration.

Last updated: 2026-07-13.

---

## TL;DR

The top of the roadmap is done: **versioned upserts across all four clients and
five adapters, consistency guarantees, a real production workload benchmark, a
query cost/explain planner, a reranking hook, and confirmed-complete metadata
filtering** — plus **two pre-existing data-loss bugs found and fixed**.
`explain` and sparse named vectors are now in **all four clients** (Rust,
Python, TypeScript, CLI), the **Qdrant and Pinecone sparse adapters** are
drop-ins (sparse config + upsert + sparse/hybrid query), there are **runnable
cookbook examples** (Python + TypeScript) covering every retrieval mode and mix,
and the **docs, README, and examples were refreshed** to match (upsert, explain,
reranking, sparse named vectors; the stale "no in-place update" and "use BM25 for
lexical" guidance removed). The remaining work is the larger roadmap tail: the
APIs, sparse-vector drop-in adapters, ColBERT, and the Tier-3/4 research items.

---

## Done

### 1. Versioned upserts (MVCC) — work.md Tier-1 #1, the "essential prerequisite"

The single biggest functional gap in `work.md`. `add` was insert-only (it
rejected existing ids); there was no overwrite. Now `upsert` inserts-or-replaces
by id, atomically, across the whole stack.

**How.** Per-record MVCC generation rather than a per-segment version (which
would have needed a routing-page schema change) or a segment-id-owner tombstone
(which would have needed compaction rewrites). A record is suppressed iff
`record.generation < live_generation(id)`. Compaction preserves generations, so
it just drops suppressed rows — no rewrite of the overlay needed.

- `2e088b1` **U1** — `VectorRecord.generation: u64`, written as a *conditional*
  segment-parquet column (only when some record is non-zero), so dense/plain
  data round-trips byte-for-byte and all prior tests are unchanged.
- `d4e3ac7` **U2** — the tombstone became an overlay `id -> min_visible_generation`
  (parquet gains a `min_visible_generation` column). `is_deleted` split into
  `id_is_tombstoned(id)` (presence) and `is_suppressed(&record)` (generation).
  All read/compaction/purge paths skip suppressed records; the get fast-path
  early-out was removed so a re-upserted live id is not hidden. `delete` stays
  idempotent (only bumps when a still-visible copy exists).
- `8aba06e` **U3** — `BorsukIndex::upsert(records)`: stamps each id a strictly
  higher generation and publishes the new record + overlay bump in **one**
  manifest (threaded through both the segment-append and top-routing-page add
  paths). Revives a previously deleted id. Named + sparse-named vectors are
  replaced in lockstep. `tests/upsert.rs` (6 tests).
- `de554b3` **Python + all 5 adapters** — `Index.upsert(...)`; Pinecone/Qdrant/
  S3Vectors/turbopuffer/Chroma now use native atomic upsert instead of the
  delete→purge→add anti-pattern `work.md` explicitly warns against.
- `c437f4f` **TypeScript/Node**, `504412f` **CLI** — upsert parity.

Result: upsert works identically from **Rust, Python, TypeScript, and CLI**.

### 2. Consistency guarantees + multi-node story — work.md Tier-1 #2 / #5

`ef63b0c`. The machinery already existed (immutable content-addressed manifests
+ atomic `CURRENT` compare-and-swap via conditional PUT); it was undocumented and
under-tested. Added `docs/consistency.md` (atomic snapshot publication,
snapshot-isolated readers, read-your-writes, crash recovery, optimistic
multi-writer CAS, and the many-readers / bring-your-own-bucket deployment story)
and `tests/consistency.rs` (4 tests pinning the observable contract).

### 3. Production workload benchmark — work.md Tier-1 #3

`c41a326`. `tests/production_workload.rs` runs the mix a real deployment runs —
upserts (insert + overwrite), deletes, metadata-filtered search, compaction, and
a restart — and **asserts correctness the whole way** (every live record resolves
to its newest value; deletes are gone; a bucket filter returns exactly the live
matching set). `production_workload_is_sound` is a fast gate;
`production_workload_gate` (ignored) writes a CSV. Real numbers in
`docs/benchmarks.md`: 2,189 live records after churn, 83/105 ms p50/p95 filtered
search, ~225 GET/query. (Latency reflects a deliberately fragmented worst-case
read shape, not a tuned serving config — documented as such.)

### 4. Query cost / explain planner — work.md Tier-3 #11/#12 (differentiator)

`f41e50e` (engine) + `daa553d` (Python). `index.explain(query, options, cost)`
returns object-store GET/HEAD requests, bytes read, routing pruning
(total/searched/skipped/pruned-by-filter), cache-hit ratio, latency, and an
estimated **dollar cost** under a `QueryCostModel` (default = AWS S3 Standard
pricing). Object-storage engines can make $/query legible where RAM-first engines
can't; this surfaces it directly. Python `index.explain(...)` returns a dict.

### 5. Reranking hook — work.md Tier-2 #7/#8

`83093a2`. `index.search_rerank(query, candidate_options, final_k, rerank_fn)` —
the retrieve → rerank → top-k pipeline every RAG stack runs, as one call. The
closure returns one score per candidate; hits come back sorted by that score.

### 6. Sparse inverted index + named vectors + hybrid (earlier this push)

`f643562`→`30df29f` (RA1a, INV1–2c, BENCH) and `b35a136` (Python binding).
Representation-aware vector math (never densify), an in-memory `SparseIndex`
inverted index, a persisted `SparseNamedStore`, `search_sparse_named`, fusion of
sparse legs into `search_hybrid`, a benchmark showing the inverted index gets
*faster* as the vocabulary grows (211 µs → 10 µs from 10k → 5M terms while a
densify backend would need 74.5 GiB), and Python `create(kind="sparse")` +
`add(named_vectors=[{name: {indices, values}}])` + `search_sparse_named`. This
makes high-dimensional lexical/SPLADE vectors a drop-in from Python.

### 7. Metadata filtering — work.md Tier-2 #6 (confirmed already complete)

`Filter` = And/Or/Not/Cmp/Exists; `Op` = Eq/Ne/Gt/Gte/Lt/Lte/In/Nin/**Contains**
(array-contains); dotted **nested paths**; **Exists** (present/absent). The
doc's asks (nested, exists, array-contains, range, in/not-in, boolean combos) are
all present. Only geo and regex are missing (advanced; deferred).

### 8. Binding parity, cookbooks, and docs refresh (the appended work.md ask)

`explain` and sparse named vectors (`create(kind="sparse")` + `searchSparseNamed`)
are now in **both the Python and TypeScript** bindings. Runnable **cookbook**
examples — `python/examples/cookbook.py` and
`packages/borsuk/examples/cookbook.ts`, both exercised in CI — cover dense search,
upsert, filtering, BM25, sparse lexical, hybrid (RRF + weighted), a RAG
retrieve-then-rerank pattern, and query cost. `docs/api.md`, `README.md`, and the
examples were rewritten to match: upsert leads the update semantics, `explain`,
reranking, and sparse named vectors are documented, and the obsolete "no in-place
value update" and "use BM25 text for lexical retrieval" guidance is gone.

### 9. Docs UX: compatibility matrix + feature-complete Quickstart ladder

Two docs-readability passes over the "redo docs/webpage/examples" ask:

- **Compatibility matrix** (`docs/drop-in.md`, `e9ad0bd`) — a per-adapter
  capability table (create / upsert / query / filter / fetch / delete / list /
  named / sparse / count) exercised by the compat tests, plus an explicit "not
  emulated" list (control plane, async clients, integrated embedding). Corrected
  the stale "sparse vectors … not emulated" honest-limits note now that Qdrant
  and Pinecone sparse are drop-ins.
- **Feature rungs on the web Quickstart ladder** (`f0b725d`) — the ladder covered
  the basics and the ops arc (report → s3 → tuning → production) but skipped the
  retrieval features. Inserted three CI-run rungs between them: **3. Filter by
  metadata**, **4. Update and delete** (atomic upsert), **5. Full-text & hybrid**
  (BM25 + RRF/weighted). Each is a real `docs:` marker region in the three ladder
  examples (`docs_ladder.rs/.py`, `docs-ladder.ts`), extracted verbatim by
  `sync_docs_examples.mjs`, so the page can't drift from code that compiles and
  runs. Renumbered ops rungs to 6/7/8; reframed the intro ("Eight rungs … a
  full-featured production deployment"). Gives an honest easy→advanced arc: first
  search → read the report → the feature set → object storage → tuning → serving.
- **Durability & SLA docs** (`bf66123`) — work.md asked us to "mention SLA in the
  docs/readme." Added a Durability & SLA treatment to `README.md`,
  `docs/consistency.md`, and the web docs S3 section: BORSUK is an embedded
  library with no data or always-on tier of its own, so an index's durability and
  availability are, by construction, the SLA of the backing store — no separate
  BORSUK SLA. Cites S3 Standard (eleven nines designed durability, 99.9%
  availability commitment / designed for 99.99%); GCS + Azure comparable. What
  BORSUK adds is the correctness contract (atomic publication, snapshot isolation,
  read-your-writes, crash recovery).

---

## Bugs found and fixed (pre-existing, not from this work)

Building the production benchmark surfaced two serious pre-existing bugs, both in
**paged** (routing-tree) code paths that assumed segments live in
`manifest.segments`. Both reproduced at `2e088b1` (before the upsert work) and
escaped existing tests because those tests don't delete on a paged index.

- `80a0ed8` **Data loss: `delete` on a paged index wiped every record.**
  `publish_tombstone` republished via `publish_manifest_reusing_routing_pages`,
  which rebuilds routing pages from `manifest.segments` — empty for a paged index
  — so any delete published an empty routing tree. Fix: a tombstone-only publish
  now re-publishes referencing the existing routing pages when the index has
  paged. Regression guard: `tests/paged_delete_compaction.rs`.
- `c41a326` **Deleted records leaked back.** `has_live_record` scanned the empty
  `manifest.segments`, so a delete of an upserted id didn't find its live copy
  and never suppressed it. Fix: use `active_segment_summaries()` (resolves
  segments from routing pages).

Audit of the other `publish_manifest_reusing_routing_pages` callers:
`purge_impl` repopulates `manifest.segments` first (safe), `publish_segment_delta`
(incremental maintenance) loads the resident manifest first (safe), the flat
`compact_impl` runs only when not paged (safe). So `delete` was the only
vulnerable caller.

---

## What's left (roadmap tail)

Priority order; each is its own gated-green increment.

1. ~~Binding parity for `explain` + sparse named vectors~~ — **done** across all
   four clients (Rust, Python, TypeScript, CLI). `search_rerank` is Rust-only (a
   closure can't cross FFI); from Python/TS/CLI it's the documented
   retrieve-then-rerank pattern shown in the cookbooks.
2. ~~Sparse-vector drop-in adapters~~ — **done**. Qdrant `sparse_vectors_config`
   and Pinecone `sparse_values` now map onto BORSUK sparse named vectors
   (create + upsert + sparse/hybrid query); the old Qdrant `NotImplementedError`
   is gone.
3. **95% API-compat adapters + contract tests + a published compatibility
   matrix** (work.md's central "change the import" goal). Pinecone first, then
   S3-Vectors, turbopuffer, Qdrant, MilvusClient.
4. **ColBERT / late interaction** (work.md Tier-2 #10) — multi-vector storage +
   MaxSim reranking. Large; named vectors are the foundation.
5. **Tier-3/4 differentiators** — automatic tuning (RAM/QPS/vectors → config),
   cost-aware ANN (optimize quality vs $ vs latency), native archival hot/warm/
   cold tiers, native LLM memory (episodic/semantic/working). Large, research-y.
6. **Filtering polish** — ~~geo~~ done (`$geoRadius`, haversine, all clients).
   Only `regex` remains (deferred: needs the `regex` crate + a compiled-context
   design to avoid per-row recompilation).

Not planned (out of scope for an embedded library, per `work.md` itself): global
endpoints, auth/orgs, billing, autoscaling, managed replicas, SLAs — i.e. the
control plane. BORSUK is API-compatible with a service's *data plane*, not a
replacement for the whole product.

---

## How to work in here

- **Gate before every push:** `cargo fmt --all`,
  `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test -p borsuk`,
  then the pre-push hook runs the workspace compile + policy + docs-web + tsc.
- **Python wheel/tests locally:**
  `python/.venv/bin/maturin develop --manifest-path ../crates/borsuk-python/Cargo.toml`,
  then copy `.venv/.../site-packages/_borsuk/_borsuk*.so` into `python/src/borsuk/`
  (maturin installs it top-level but the code does `from ._borsuk import`), run
  `python -m unittest discover python/tests`, and **`rm python/src/borsuk/_borsuk*.so`
  before committing** (the repo policy check flags the stray `.so`).
- **Node native + tests:** `cd packages/borsuk && npm run build:native && npm run build && node --test dist/test/*.test.js`.
  `native.d.ts`, the `.node` binary, and `dist/` are gitignored (regenerated).
- **Paged-path gotcha:** any op that mutates the index from the lazy
  `self.manifest` (whose `segments` is empty on a non-resident handle) must not
  rebuild routing from `manifest.segments` — reference the existing routing pages
  instead. This is what caused both data-loss bugs above.
- **Debugging the routing tree:** instrument with `eprintln!` read-backs
  (`routing_max_level` + `active_segment_summaries()?.len()`) right after each
  publish to see exactly where the segment set changes.

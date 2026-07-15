# Hierarchical Paged PQ + Graph Fragments + Adaptive Stopping — Implementation Plan

**Goal:** Bounded resident RAM (tens of MB regardless of N), 4–8× fewer bytes/query, and
DiskANN-style few-fragment reads at scale — while keeping BORSUK's blob-native,
paged model.

**Architecture:** A resident coarse quantizer (fixed budget) routes to regions; PQ codes
live in paged per-region sidecars loaded on demand + LRU-cached; a Vamana-style graph is
laid out so a node co-locates with its neighbours (DiskANN sectors); beam search navigates
in loaded PQ, reranks full vectors for the top few; adaptive stopping caps reads per query.

**Reuses:** `Segment.pq_codes` / `pq_min` / `pq_max` already exist; the projected-read path
(`query_projectable`, read the pq_codes column) already reads codes not full vectors;
`CentroidHnsw` is the resident coarse quantizer; segment-local `SegmentGraph` exists.

---

## Phase 0 — Validate the make-or-break assumption (before any build)

The whole graph-fragment value hinges on: *does a graph laid out with neighbour-co-located
fragments read FEWER fragments than IVF cells, at our N?* Extend the ignored
`centroid_hnsw::tests::gist_cell_graph_experiment`:
- [ ] Lay out fragments by graph locality (each fragment = a node + its out-neighbours, or a
  balanced graph partition of the Vamana graph into fragment-sized groups).
- [ ] Beam search counting DISTINCT fragments touched vs recall, at N = 10k / 100k / 1M.
- [ ] Compare fragment-reads to IVF cell-reads at matched recall.
- **Gate:** if graph fragments do not beat IVF cell-reads by ≥1.5× at N≥100k, stop — the
  bounded-RAM + fewer-bytes wins (Phases 1–2) still stand, but skip the graph (Phase 3).

## Phase 1 — Object-store-ranged reads (fewer bytes/query, existing Parquet format)

**Approach (2026-07-15, user: "use existing formats", "no backward compat"):** no new
format — exploit Parquet's own column projection + row selection, which BORSUK already
used *in memory over whole objects* (saving decode, not I/O). The fix reads through an
async **range-fetching** Parquet reader so scoring pulls only the `pq_code`/code columns
and rerank pulls only the chosen rows' row groups, over the object store.

- [x] Type-safe read-time toggle `with_projected_reads(bool)` / `projectedReads`,
  supersedes the `BORSUK_DISABLE_PROJECTED_SCORING` env kill-switch. (commit 612f603)
- [x] `Storage::read_parquet_columns_ranged` — a custom `AsyncFileReader` over BORSUK's own
  range reads (sidesteps parquet's bundled object_store 0.13 vs our 0.14), footer
  pre-fetched, projects columns (`Keep`/`DropVector`) + optional row selection, and reports
  exact bytes fetched. Unit-tested: id-only ≪ whole object; row-selective ≪ full scan.
- [x] Search path wired: cold `PqScan`/`SqScan` scoring uses `read_segment_lean_ranged`
  (non-vector columns only); rerank uses `segment_vectors_for_rows_ranged` (chosen rows).
  Old whole-object lean path removed. All suites green; results identical.
- [x] Segment writer emits small row groups (`SEGMENT_ROW_GROUP_ROWS = 32`) so row-selective
  rerank prunes to the touched groups. Net win verified (`projected.bytes_read <
  full.bytes_read`); margin grows with embedding width.
- **Measured:** ~11% fewer bytes at 256-dim random; larger at 960-dim (vector column
  dominates more). This is the *modest* Parquet-granularity win.
- [ ] **The 4–8× "two formats" win (next):** a per-row **raw vector blob** sidecar
  (fixed-stride f32, row `i` at `i·dim·4`) so rerank of *scattered* rows range-reads exactly
  `k·dim·4` bytes — Parquet row groups can't (scattered rows touch most groups). Columnar
  Parquet for codes/scan + row-major blob for random-access vectors = the user's "two
  formats". Resident fixed-budget coarse codebook + LRU sidecar cache ride on top.
- **Validate (blob):** bytes/query drops 4–8× on gist-960; resident bytes flat across sizes.

## Phase 2 — Adaptive stopping (free ~10–15%) — DONE (commit 3bec2e6)

- [x] Per-query early-stop: stop when the top-k has been stale for `patience` segments,
  capped by max_segments. `SearchTerminationReason::AdaptiveStop`.
- [x] Type-safe read-time toggle: `SearchOptions::with_adaptive_stop(patience)` (Rust),
  `adaptiveStop` (Node `SearchOptionsJs`). Off by default. Wired CLI/python/node.
- [x] Test asserts fewer segments read while keeping the exact match; full gate green.

## Phase 3 — Graph-laid-out fragments (few reads at scale; gated by Phase 0)

- [ ] Build a global Vamana graph over vectors (α-RobustPrune) at compaction.
- [ ] Fragment layout co-locating each node with its neighbours (DiskANN sector).
- [ ] Beam search: PQ-navigate (from loaded fragments), follow edges loading fragments on
  demand, rerank full vectors.
- **Validate:** fragment-reads ≪ IVF at N≥1M; recall→1.0; sub-few-ms warm.

---

## Notes / risks
- Full-vector rerank must stay exact (recall=1.0 achievable) — PQ only prunes/navigates.
- Determinism: keep compaction reproducible (seeded).
- Every phase passes the full gate (fmt, clippy -D, cargo test -p borsuk, ruff, prettier,
  check_repo_policy, test_docs_web, tsc). Commit per phase; branch off main.

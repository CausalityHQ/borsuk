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

## Phase 1 — Paged PQ sidecars + resident coarse codebook (bounded RAM + fewer bytes)

**Reality check (2026-07-15):** the existing projected-read path (`read_segment_lean` →
`segment_vectors_for_rows`) reads the *whole* segment object off storage and only avoids
*decoding* non-candidate vectors — so it saves CPU/memory, **not** storage bytes. The 4–8×
fewer-bytes win requires the PQ codes to live in a **separate sidecar object** so a probed
segment fetches only the codes (route → PQ-score → range-fetch full vectors for the rerank
set). That sidecar is the real remaining work here; the projected path is its decode half.

- [x] Type-safe read-time toggle for the projected/lean path: `with_projected_reads(bool)`
  (Rust), `projectedReads` (Node `SearchOptionsJs`). Supersedes the untyped
  `BORSUK_DISABLE_PROJECTED_SCORING` env kill-switch. Off = engine default. — commit below.
- [ ] Persist per-segment PQ codes as a standalone sidecar object (like the sparse/filter
  sidecars) so they load independently of the full-vector column. **← the byte-savings work**
- [ ] Resident coarse codebook: a fixed-budget quantizer (cap resident bytes, not √N).
- [ ] Search path: route (resident) → load only the probed segments' PQ sidecars → score in
  PQ → fetch full vectors for the top rerank set only.
- [ ] LRU cache for PQ sidecars with a fixed byte budget.
- **Validate:** bytes/query drops 4–8× on gist-960; resident bytes flat across 50k/500k.

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

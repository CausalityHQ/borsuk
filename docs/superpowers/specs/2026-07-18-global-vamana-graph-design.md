# BORSUK — Global DiskANN/Vamana graph (research/design, OFF by default)

**Status (2026-07-18):** RESEARCH + DESIGN + feasibility prototype. NOT shipped, NOT a
default. Branch `global-graph-design` off `eff41d5` (the `ivf-hnsw-coarse-quantizer`
line, NOT main). The prototype is an `#[ignore]`d harness
(`centroid_hnsw::tests::global_vamana_reads_experiment`) that measures the crux
number — **distinct sectors touched (≈ blob GETs) vs recall** — so we can decide
go/no-go before building the real thing. Honest-negative results are a valid outcome.

**Why:** Today's high-dim routing is IVF Voronoi cells + a `CentroidHnsw` coarse
quantizer (warm and cold/paged). Prior research (git history, `[[borsuk-curse-of-dim-ivf-hnsw]]`,
the existing `centroid_hnsw::tests::gist_cell_graph_experiment`) established the ceiling:
on real gist-960 exact search prunes ZERO cells (curse of dimensionality), and
centroid-IVF reaches recall≈1.0 only at **~40% cell reads**. A graph over vectors gives
sub-2ms/0.94 recall *in RAM*, but on blob storage a naive graph beam-search touches
**2–3× MORE** cells than IVF because scattered graph nodes land in scattered blocks. The
known step-change is DiskANN/Vamana: a single global navigable graph + a **resident
compressed PQ code per vector** for the beam navigation, reading full vectors (from the
Arrow sidecars) only for the final rerank → recall≈1.0 at **~1% reads**. The catch is
O(N) resident RAM (tens of MB per million), which departs from BORSUK's near-zero-RAM
niche. So this is proposed as a **typed, OFF-by-default** config: the default stays the
IVF path (near-zero RAM); operators who can spend the RAM opt into the global graph for
the read-count step-change.

BORSUK already has the per-SEGMENT pieces (`SegmentGraph`, `LeafMode::VamanaPq/Graph`,
`TurboQuantizer`, the persisted-quantizer object pattern). The **missing** piece is a
GLOBAL cross-segment/cross-cell graph with fragment-locality layout and a resident PQ
codebook that navigates it cold.

---

## 1. Architecture

Four cooperating parts, three of them already exist in per-segment form:

1. **Global Vamana graph** over *all* live vectors (across every cell/segment). Node id =
   a stable global vector id (the MVCC record id). Edges are Vamana `α`-pruned
   (diversified) directed adjacency, degree `R` (≈ 32–64). One graph for the whole index,
   not one per segment — this is the new object.

2. **Resident PQ codes** — one compact code per live vector, held in RAM, used to estimate
   beam-navigation distances without any blob read. Reuse `TurboQuantizer` as the
   rotation + scalar-quant kernel (SRHT/FWHT rotation already ships and is the default
   quantizer), but scored as a **product code** (see §4 for the byte-budget subtlety — the
   current TurboQuant scalar code is 1 byte/coord, which is too big; the resident code
   must be a *product* code of `M` subvectors, `M≈32–64` bytes/vector).

3. **Fragment (sector) layout on object storage** — the graph's layer-0 adjacency laid out
   in a locality order (BFS/METIS-style) and chunked into fixed-size *sectors* (e.g. 64
   nodes' full vectors + their edge lists per sector, one sector = one blob range). A beam
   step that expands a frontier node reads the *sector* it lives in — which, because of the
   locality layout, also contains most of that node's graph neighbours — so a whole
   frontier expands in a handful of GETs instead of one-GET-per-node.

4. **Full-vector rerank from the existing Arrow sidecars.** The beam navigates on resident
   PQ estimates only; the final top-`k'` (`k' ≈ 2–4·k`) candidate ids are reranked by
   reading their *exact* vectors from the per-segment ZSTD-dict-compressed Arrow sidecars
   (`vector_sidecar.rs`, `~919 B/row`, random-access by row) — the same rerank path the
   IVF leaf already uses. No new exact-vector store; the sidecars are already random-access.

**Query flow (cold/paged):**

```
load global graph header + resident PQ codes (one object read, cached)   [amortized]
beam := {entry point}
while beam improves:
    for each unexpanded frontier node n (lowest PQ-estimated distance first):
        edges(n)  := from resident graph adjacency (in RAM — see §2 sizing)
        for each neighbour m of n not yet seen:
            est(m) := TurboQuant coarse_distance(rotated_query, pq_code[m])   [RAM only]
            push m into beam candidate heap
    (NO blob read on the navigation hot loop — codes+edges are resident)
select top-k' candidate ids from the beam by PQ estimate
rerank: read those k' exact vectors from the Arrow sidecars (few GETs, batched by segment)
return true top-k
```

The blob GETs are **only** the rerank reads (a few) plus, in the memory-bounded variant
(§2), the sector reads for edges/codes not held resident. The navigation itself is RAM.

---

## 2. The RAM tradeoff, quantified

This is the whole decision. Two resident structures scale as O(N):

- **PQ codes:** `M` bytes/vector (product code, `M` subquantizers × 1 byte). Plus a shared
  codebook (`M · 256 · (d/M) · 4` bytes — a few MB, N-independent).
- **Graph adjacency:** `R` × 4 bytes/vector (`u32` neighbour ids), `R≈32–64`.

| resident structure          | bytes/vector | 1M      | 100M    | 1B       |
|-----------------------------|--------------|---------|---------|----------|
| PQ codes, `M=32`            | 32           | 32 MB   | 3.2 GB  | 32 GB    |
| PQ codes, `M=64`            | 64           | 64 MB   | 6.4 GB  | 64 GB    |
| graph adjacency, `R=32`     | 128          | 128 MB  | 12.8 GB | 128 GB   |
| graph adjacency, `R=64`     | 256          | 256 MB  | 25.6 GB | 256 GB   |
| **both, `M=32 R=32`**       | **160**      | 160 MB  | 16 GB   | 160 GB   |
| **both, `M=64 R=64`**       | **320**      | 320 MB  | 32 GB   | 320 GB   |

Reference: `docs/superpowers` / current IVF cold path is **near-zero** resident (only the
persisted quantizer object: centroids + coarse HNSW + summaries, KB–low-MB, N-*sublinear*
because it's per-cell not per-vector).

**Three operating points, as typed config (see §7):**

- **A. IVF (DEFAULT, unchanged).** Near-zero RAM. ~40% cell reads at recall≈1.0 on gist-960.
  This is the niche; it stays the default.
- **B. Global graph, codes-resident + edges/sectors paged.** Hold only the PQ codes
  resident (`M` bytes/vector); page graph adjacency from sectors on demand. RAM =
  `M`·N (32–64 MB/M). Navigation costs *some* GETs (sector reads for edges) but far fewer
  than IVF's 40% because the graph walks straight to the neighbourhood. This is the
  **RAM-bounded** variant and the interesting middle ground for BORSUK's niche.
- **C. Global graph, fully resident (classic DiskANN).** Codes + adjacency resident
  (`M`+4·`R` bytes/vector). Navigation is pure-RAM; the only GETs are the `k'` rerank
  reads. Recall≈1.0 at **≈1% reads** (the rerank fraction). RAM = 160–320 MB/M. This is
  the step-change, at the cost of the niche.

The prototype (§6) measures B and C against A on the **reads** axis to decide whether the
step-change is real at our scale and which variant to build first.

---

## 3. Build / compaction / MVCC / WAL / GC integration

The global graph is a **compaction-time artifact**, like the persisted coarse quantizer
(`quantizer_sidecar.rs`), but O(N) instead of O(cells):

- **Construction (at full compaction):** after cells/segments are built, run Vamana build
  over all live vectors — greedy-search insert + `α`-robust-prune to degree `R`. This is
  the expensive phase (Vamana build is ~`O(N log N)` distance calls); reuse the existing
  parallel k-means/graph SIMD kernels and the deterministic splitmix insertion order from
  `CentroidHnsw` (no `rand`, no `Date::now` — determinism rule). Fit the PQ codebook on a
  deterministic subsample (reuse `BuildConfig::pq_codebook_sample`, already carried in the
  manifest for exactly this).
- **Incremental / WAL:** new vectors from the WAL are appended to L0 as today. The global
  graph does NOT rebuild per WAL flush (too costly). Two options, deferred to
  implementation: (a) *stitch* — greedy-insert new nodes into the existing graph and add
  back-edges (bounded work, graph quality drifts until next full compaction); (b)
  *two-tier* — search the global graph for the compacted bulk, brute/IVF-scan the small
  un-compacted L0 tail, merge. (b) is simpler and matches the existing L0/segment split;
  recommend (b) first.
- **MVCC / tombstones:** the graph nodes are record ids; deleted (tombstoned) ids must be
  *skipped* during the walk (edges may still point at them until the next compaction
  re-prunes them out). The beam simply never emits a tombstoned id into the result set and
  does not read its sector — same tombstone set the search already consults. A tombstoned
  node can still be *traversed through* (its edges are navigational) but never *returned*;
  a fully-deleted region is pruned at the next compaction rebuild.
- **GC:** the global-graph object + PQ-code object are content-addressed (BLAKE3 of bytes),
  exactly like the quantizer object, so a superseded graph lands at a distinct path and the
  existing GC sweep reclaims the old one. Add its prefix (`global_graph/`) to the GC
  listing set (mirror `is_quantizer_path`).

---

## 4. Which quantizer for the resident codes — reuse TurboQuant? (the byte-budget subtlety)

**Yes for the rotation + asymmetric scoring; NO for the storage layout as-is.**

TurboQuant is the right *kernel*: SRHT/FWHT rotation (O(d log d)) spreads energy so scalar
quant is accurate, and `coarse_distance(rotated_query, code)` is the asymmetric
squared-Euclidean proxy we want for beam estimates — already the default quantizer, already
SIMD, already deterministic from a seed.

BUT its stored code is **1 byte per padded coordinate** (`padded_len()` — a 960-dim vector
→ 1024 bytes/vector regardless of bit-depth, because it stores one `u8`/coord, not a
bit-packed sub-byte code). At 1024 B/vector the "resident PQ codes" would be **1 GB per
million** — that is not DiskANN's tens-of-MB budget; it is worse than holding the raw f32s
paged. The RAM table in §2 assumes a **product code** of `M≈32–64` bytes/vector.

**Two ways to close the gap (both build ON TurboQuant, deferred to implementation):**

1. **Product quantization on the rotated coordinates.** Reuse TurboQuant's `shards`
   machinery: split the rotated space into `M` subspaces, train a 256-entry codebook per
   subspace (k-means, deterministic seed), store `M` bytes/vector (one codebook index per
   subspace). Scoring = `M` table lookups (asymmetric distance tables), the classic ADC.
   This is the smallest change that hits the byte budget and reuses the rotation + shard
   split already in `turboquant.rs`. **Recommended.**
2. **Bit-pack the scalar code.** At `bits=4`, pack two coords/byte → 512 B/vector for
   gist-960. Still ~0.5 GB/million — better but not enough. Rejected as the resident code;
   fine only for the small per-segment codes it's used for today.

The prototype (§6) uses a **4-bit scalar TurboQuant code paged from sectors** for variant
B (so it exercises the real rotation+scoring kernel and the real sector-read count) and, to
model variant C's resident budget honestly, reports what an `M`-byte product code *would*
cost — it does not fake a product codebook it hasn't trained. The go/no-go (§8) is explicit
that shipping variant C needs the PQ-on-rotated-coords codebook from option 1.

---

## 5. Cold-read path integration (1 read per beam frontier)

Reuse the persisted-object pattern verbatim:

- **`global_graph/<cs>.parquet`** — a content-addressed object (like the quantizer object)
  holding the graph header (entry point, `R`, `M`, codebook, sector directory) + the
  resident PQ codes. A cold query loads it with one read and caches it (mirror
  `PersistedQuantizerCache`: `Arc<Mutex<Option<(checksum, Arc<GlobalGraph>)>>>`).
- **Sectors** live either inline in that object (variant C, fully resident once loaded) or
  as a separate `global_graph_sectors/` prefix read by byte-range on demand (variant B).
  The sector directory maps node id → (sector id, offset), so a frontier expansion reads
  the *distinct sectors* its nodes fall in — the fragment-locality layout keeps that count
  small (the FRAG phase of the existing experiment already showed BFS-ordered fragments
  cut the touched-fragment count vs scattered cells; §6 extends that to sectors + resident
  codes).
- **Rerank** reads exact vectors from the existing Arrow sidecars by row id, batched per
  segment — the same call the IVF leaf already makes.

No new networking or caching primitives: it is the persisted-quantizer object, scaled to
O(N) and split into sectors.

---

## 6. Feasibility prototype (the crux measurement)

`centroid_hnsw::tests::global_vamana_reads_experiment`, `#[ignore]`d, seeded, reuses the
existing gist harness (`GIST_DIR`/`GIST_LIMIT`, same f32-matrix reader and k-means-cell
helper as `gist_cell_graph_experiment`). It also runs on a **synthetic** clustered set with
no dataset present, so it is runnable on a loaded laptop without downloads.

What it measures — **the make-or-break number: distinct SECTORS touched (≈ blob GETs) vs
recall@10**, for:

- **IVF baseline** — nprobe cells read by centroid rank (the ~40% number).
- **Global-graph beam, full f32 nav (upper bound on graph quality)** — the graph's best
  possible recall at a given beam width, sectors = BFS-fragment of touched nodes.
- **Global-graph beam, resident TurboQuant-code nav (variant B/C model)** — navigate on
  quantized estimates (the real `TurboQuantizer::coarse_distance`), rerank the top-k' by
  exact distance; count distinct sectors the walk + rerank touch. THIS is the number that
  decides whether resident-PQ + sector layout beats IVF's 40%.

The harness prints, per beam width `ef`: recall@10, mean distinct sectors read, and that as
a %-of-total (directly comparable to IVF's cell-read %). It seeds all randomness
(splitmix), so results are reproducible. See §8 for how it scales to the large-N harness
needed to *prove* the 1% claim (this prototype validates *direction* at 10⁴–10⁵, not the
10⁶⁺ absolute).

---

## 7. Typed, OFF-by-default config

Follow the `persist_coarse_quantizer` convention (typed bool on `BuildConfig`, `serde`
default preserving current behavior). Proposed (implementation, not in this design commit):

```rust
pub enum GlobalGraphMode {
    Off,               // DEFAULT — IVF path, near-zero RAM (unchanged)
    CodesResident,     // variant B: PQ codes resident, edges/sectors paged
    FullyResident,     // variant C: codes + adjacency resident (classic DiskANN)
}
// BuildConfig { global_graph: GlobalGraphMode (default Off), global_graph_degree: usize (R),
//               global_graph_pq_subspaces: usize (M), ... }
```

`Default` = `Off` so no existing behavior, test, or persisted byte changes. Building the
graph is gated behind a non-Off mode; the search path checks the mode and falls back to
IVF when Off or when the object is absent/corrupt (same fail-safe as the quantizer loader).

---

## 8. Honest risks

- **Build cost.** Vamana build is `O(N log N)` distance calls + `α`-prune; at 10⁶ this is
  minutes on a laptop, at 10⁹ it needs the sharded/merged DiskANN build (build per-shard
  graphs, merge). Not free; it is a full-compaction-only artifact.
- **Resident RAM departs from the niche.** §2 is the honest cost: 160–320 MB/million for
  variant C. This is *the* reason it is OFF by default. Variant B (codes only, 32–64
  MB/million) is the compromise that keeps most of the niche.
- **The blob-scatter problem is real** and is the whole reason a naive graph LOSES to IVF
  on reads (prior finding: 2–3× more cells). Sector layout is the mitigation, and its
  effectiveness is exactly what the prototype must prove — if BFS-fragment sectors don't
  cut the touched-sector count below IVF's cell count, the design does not pay off and we
  do NOT build it. This is a genuine make-or-break, not a foregone conclusion.
- **The 1% claim needs a large-N harness.** At 10⁴–10⁵ (laptop scale) both IVF's %-reads
  and the graph's %-reads are inflated by boundary effects; the ~1% figure is a 10⁶⁺
  asymptotic. The prototype validates *direction* (does resident-PQ + sectors beat IVF's
  reads at all, and does the gap widen with N?); proving the absolute 1% needs the S3
  full-corpus harness (`scripts/bench_s3_full.sh` pattern, `$/query` reads real GET counts)
  at 10⁶–10⁷. The design commit does not claim 1% proven — it claims the *direction* is
  what the prototype tests.
- **Edge-storage overhead + code staleness under churn.** `R`·4 bytes/vector of edges is
  significant (§2); under heavy churn the graph drifts until the next full compaction
  (variant (b) two-tier keeps correctness by scanning the L0 tail exactly).
- **Determinism.** Vamana build must be seeded (splitmix, like `CentroidHnsw`), no `rand`/
  `Date::now` in any lib path — the graph must build bit-identically twice.

---

## 9. Go/no-go — filled from the prototype (real gist-960)

The prototype ran on real gist-960 (`GIST_DIR` set), n=20k and n=40k, k=10, seeded.
Recall≈1.0 rows and reads:

| method                             | recall@10 | reads (n=20k) | reads (n=40k) |
|------------------------------------|-----------|---------------|---------------|
| **IVF** (nprobe for recall≈1.0)    | 1.000     | 41% (128/313) | 20% (128/625) |
| **Variant B** — codes resident, edges/sectors PAGED (nav sectors) | 0.98–0.995 | 47–61% | 37–53% |
| **Variant C** — codes+edges RESIDENT, only rerank reads GETs | 0.98–0.995 | **8.3%** (26 sectors) | **4.5%** (28 sectors) |

**Findings:**

1. **Variant C is the step-change and it is real.** With graph adjacency AND PQ codes fully
   resident, navigation costs zero GETs; the only blob reads are the `k'=40` rerank
   vectors, which fall in ~26–28 distinct sectors *regardless of N* (`k'`-bounded). So as a
   fraction of the index it **halves as N doubles**: 8.3% at 20k → 4.5% at 40k, trending
   toward the ~1% claim at 10⁶⁺. IVF, to hold recall≈1.0, must read a *fixed large fraction*
   (20–41%). Variant C's read advantage over IVF is ~5× and **widens with scale**. This is
   the DiskANN result, reproduced.

2. **Variant B is an honest NO.** Paging graph edges/codes from sectors during navigation
   touches 37–61% of sectors — MORE than IVF — because the beam frontier scatters across
   sectors faster than sector-locality co-locates it. This reproduces the prior "naive graph
   touches 2–3× more cells than IVF" finding. The RAM-bounded compromise (codes only) does
   NOT beat IVF on reads. Only *fully resident* edges (variant C) avoids the blob-scatter
   penalty. Sector layout helps the rerank reads, not the navigation paging.

3. **The RAM cost is the price of variant C** and it is not small (§2): 160–320 MB/million
   for codes+edges. It buys the 5×-and-widening read reduction. This is exactly why it must
   be OFF by default.

**GO/NO-GO:** **GO for variant C (fully-resident global graph), OFF by default, behind a
typed config**, for operators who can spend 160–320 MB/million RAM to cut blob reads ~5×
(widening with scale) at recall≈1.0 — the classic DiskANN tradeoff, now with BORSUK's
object-store rerank. **NO-GO for variant B (codes-resident, edges-paged):** it loses to IVF
on reads, so it is not worth building. Keep the near-zero-RAM IVF path as the default.

**Caveat for a real variant-C build (§4):** the prototype navigated on TurboQuant's *scalar*
code (1024 B/vector on gist-960) to exercise the real rotation+scoring kernel honestly. That
is 1 GB/million — far above the §2 budget. A shippable variant C needs the **product code**
(PQ on rotated coordinates, `M=32–64` bytes/vector) from §4 option 1; the recall/reads above
would shift modestly with the coarser code, which the S3 large-N harness (§8) must confirm
before shipping. The *direction* — resident graph + sector rerank ≈ 5× fewer reads than IVF,
widening with N — is proven here.

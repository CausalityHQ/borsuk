# Cold-path architecture verdict at dfd34208 (read-only)

**Verdict:** the current design cannot reach 444 ms at 1M through decoder or timing work, and its cold start is O(N). Keep the leaf tiers and nomination code, and replace the resident router with a paged centroid tree under a fixed wave budget.

Nothing in the repo was edited, built, launched or sent. I added one note to my own memory directory, outside the repo. I did not re-fetch the turbopuffer/SPFresh/SPANN pages or read `two_bit_mutations.rs`, `two_bit_gc.rs`, `sq8_s3_range.rs` or `native_ann_build.rs` bodies, so the lifecycle section below rests on the closeout docs and function outlines, not on a code read.

## 1. Causal ledger (candidate arm, ReLAION p50; CoHere within 2%)

Recomputed from `decode/cold/a0004/screen/block0..3-records.jsonl.gz`, 256 closed records.

| Phase | p50 ms | Status | Cause |
|---|---:|---|---|
| Head read (incl. first TLS) | 92.9 | measured | `two_bit_http.rs:150` |
| Staging | 447.0 | measured | serial per-object loop, `object_native_generation.rs:255` |
| – 9 serial HEADs | ~83 | measured (9 × ~9.2) | one HEAD per object before its GET |
| – 6 serial small GETs | 139.2 | measured (`get_header`) | ~23 ms each, sequential |
| – stream and write | 218.9 | measured | centroids 119 + graphs 45.5 × 2, sequential |
| Source HEAD | 9.5 | measured | `two_bit_generation.rs:421` |
| Decode | 268.4 | measured | eager f16→f32 of all 24M values + norms + SHA (`unit_centroid_pages.rs:165`), two nested-Vec graph decodes (`unit_centroid_graph.rs:438`) |
| Query (incoming HTTP) | 355.5 | measured | 128 source GETs at 16 parallel, then 32 SQ8 GETs |
| Spawn/bind/connect residual | 14.7 | measured | — |
| **Cold total** | **1203.9** | measured | p90 1242.8 |

- **Bytes per query:** source 22.9 MB p50 (max 43.5 MB) plus SQ8 16.77 MB, so 39.6 MB and 160 GETs. The needed source payload is only 16.28 MB (2544 units × 32 rows × 200 B, `source-paging-replay.md`); the rest is gap bridging.
- **Cold-staged metadata:** 57.69 MB, which is 57.7 B/row: centroids 48, two graphs 8.5, unit digests 1, page digests 0.125.
- **Memory:** RSS max 174 MB at 1M (measured). Decoded centroids are 96 B/row resident (arithmetic).
- **Unknown:** the split inside the 355 ms query (source wave, SQ8 wave, CPU) has no timers. The 128 GETs at 16 parallel imply 8 transport rounds; that is inference.
- **Unknown:** current offered QPS. At 39.6 MB/query, 8 QPS needs about 2.5 Gbit/s sustained, so bytes per query may be the throughput limiter; this is unmeasured.

**What decoder or timing tweaks cannot do:**
- With decode at 0 and the centroid/graph stream at 0, the remainder is 93 + 83 + 139 + 355 + 15 ≈ 685 ms p50, still above 444.
- With a root bundle, parallel staging and lazy decode added, I estimate about 540–640 ms (projection).
- Staging plus decode is O(N) at roughly 0.49 µs/row. Projected linearly, that is about 5 s at 10M and 49 s and 5.8 GB at 100M before the first query. The pending bulk-half conversion only shaves the 268 ms term.

## 2. Recommended architecture: paged centroid tree, four bounded waves

**Persistent layout (new generation format, old rejected):**
- **Head:** unchanged CAS pointer, but it embeds the root bundle's length and SHA.
- **Root bundle:** one object of at most 4 MiB holding the manifest, mean, top-level node centroids and node directory. One GET, no HEADs.
- **`router.bin`:** k-means tree over the existing 32-row unit centroids, fanout 256. Each leaf node is 256 entries of SQ8 centroid plus unit id (about 199 KB), with a per-node SHA in its parent.
- **Router closure:** a unit centroid entry may be listed in up to 2 nodes. Only the 24 B/row router entry is copied; rows stay single-copy.
- **Codes and exact tier:** `plane/records.bin` (200 B/row) and SQ8 (780 B/row) keep their formats, but physical row order becomes tree order, so a node's units are contiguous in both.
- **Deleted:** `centroids.bin` as a staged object, `graph.bin`, `diverse_graph.bin`, and the resident unit and page digest tables (digests move into parent nodes).

**Query waves:**
| Wave | Fetch | Cap |
|---|---|---|
| W0 | head, then root bundle | 2 GETs, ≤ 4 MiB |
| W1 | beam of router leaf nodes; exact-score their unit centroids | ≤ 16 GETs, ≤ 3.2 MB |
| W2 | two-bit codes for top units, via existing `cover_pages` | ≤ 32 GETs, ≤ 32 MiB |
| W3 | SQ8 exact rerank, unchanged | ≤ 32 GETs, 16.77 MB |

- **Reused as is:** `rank_walked_source`, `plan_walks`, `choose_budgeted_pages_sparse`, `fetch_verified_ranges_inner`, the SQ8 ranker, and the mutation delta.
- **Replaced:** only `discover_walks` (`two_bit_generation.rs`, the two HNSW walks with the 1272-evaluation cap) and `stage_generation_metadata`.
- **Mutation and compaction:** inserts stay in the existing sealed delta. In-process compaction assigns new rows to the nearest node, rewrites only touched nodes and their row ranges, and splits a node past 2× fill (SPFresh's local split/reassign idea). Publication stays immutable objects plus head CAS, with existing pins and delayed-delete GC.
- **Recovery:** the root SHA in the head authenticates everything reachable.

## 3. Capacity formulas (projections, not measurements)

With D = 768, U = N/32 units, fanout 256, closure ρ between 1 and 2:

| | 1M | 10M | 100M |
|---|---:|---:|---:|
| Router leaf nodes (ρ·N/8192) | 122–244 | 1.2k–2.4k | 12k–24k |
| Root entries at 776 B | 95–190 KB | 0.9–1.9 MB | 9.5–19 MB, over the cap, so one extra level |
| `router.bin` on S3 (24.25·ρ B/row) | 24–48 MB | 0.24–0.49 GB | 2.4–4.9 GB |
| Cold-staged bytes | ≤ 4 MiB | ≤ 4 MiB | ≤ 4 MiB |
| Sequential waves | 4 | 4 | 5 |
| GETs per query | ≤ 82 | ≤ 82 | ≤ 98 |

- **Resident memory:** root plus a bounded LRU of router nodes. I would declare 3 GiB at 100M; that is a target, not RSS.
- **1M cold target:** p90 under 444 ms. Wave arithmetic from measured primitives (93 head+TLS, ~25 per small GET, ~120 per 48 MB streamed) gives roughly 93 + 25 + 40 + 90 + 80 + 30 CPU ≈ 360 ms p50.
- **10M:** same wave count, so p90 under 1214 ms at 8 QPS is the declared context target.
- **100M:** one extra wave. Set the target from the measured 1M→10M curve; do not transplant 444 ms.
- **Cost:** GET charge per query halves (160 → ≤ 82). Reaching 8 QPS on a modest NIC needs roughly ≤ 16 MB/query, versus 39.6 MB today.
- **Build:** hierarchical Lloyd over unit centroids. `scale-envelope.md` says the existing fitter is unqualified at 100M, and that stays open.

## 4. Falsifier ladder

**Step A — replay only, existing artifacts, GT-free, no quality claim.**
- Inputs: the three closed traces behind `source-paging-replay.json` (walked, completion and nominated units per query) plus the published `centroids.bin`.
- Build the fanout-256 k-means tree offline. For each query, count the minimum nodes that cover every unit in the nominated pages, at ρ = 1 and ρ = 2, and the resulting W2 GETs and bytes under `cover_pages`.
- Implementation: one script modelled on `scripts/check_native_source_paging_replay.py`.
- GO: p95 node cover ≤ 16 and W2 ≤ 32 GETs / 32 MiB on ReLAION 1M.
- KILL: more than 32 nodes at ρ = 2.
- This predicts cold latency directly because GETs and bytes are what the wave budget is made of. It needs fine-enough nodes at 100k (about 49 nodes of 64 units) to discriminate.

**Step B — new measurement, ReLAION first-100k, paired ABBA against the current native arm, fresh dev split, k10 with R100 reported separately.**
- Minimal code: a router build in `two_bit_build.rs`, a new `discover` behind the existing seam in `two_bit_generation.rs`, and the root bundle in `object_native_generation.rs`.
- Decompose the loss four ways: routing (top-units overlap against the V146 flat oracle), coverage (GT rows inside fetched units), scoring (two-bit cut), and quantization (SQ8 against exact).
- GO: mean R10 ≥ 95% and no more than 0.5 pp below control, cold-staged bytes ≤ 4 MiB, ≤ 82 GETs, and lower cold p90 than control.
- KILL: R10 below 95%, or a routing-only loss above 1 pp at beam 16.

**Step C — fresh 1M on CoHere and ReLAION.**
- Current actuals are 99.375% and 96.875% on consumed panels, so CoHere has only 1.9 pp of headroom to the 95% floor.
- Gate: R10 ≥ 95% on both, cold p90 < 444 ms, then offered 8 QPS.

## 5. Counterargument, prior-failure distinction, effort

- **Strongest counterargument:** discovery is already the lossy layer (`next-route-research-20260928/fable-result.md`: exhaustive unit ranking 99.875% against the graph's 99.5625% at 100k). A beam over coarse nodes can miss three-way boundaries, and CoHere has little margin. Every earlier coarse router (V139, V149, V150) lost pages.
- **Why this is not V149 or V150:**
  - V149 summarised consecutive physical pages and was judged on page-plan capture and resident CPU at D96.
  - V150's own post-terminal note calls page-plan capture "an overly blunt screen" (73% capture, yet 99.52% truth coverage).
  - Here nodes are semantic k-means cells and the beam scores exact unit centroids, so within fetched nodes it matches the flat oracle. Closure copies only router entries, not rows. The gate is returned IDs plus wave bytes.
- **Why this is not the spill/closure arm:** row copy factor stays 1. The rejected arm copied 780 B rows at ρ ≥ 2.
- **Two-hour blocker:** only Step A fits in two hours (script plus replay). The format change, physical re-ordering and node-local compaction are multi-day, and the 100M build fitter remains unqualified.

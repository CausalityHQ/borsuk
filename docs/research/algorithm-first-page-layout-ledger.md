# Algorithm-first page-layout ledger

This ledger records the screens that decide whether BORSUK's blob-native
serving path can reach the quality bar on a hard 1M corpus. Every row here is
**evidence class 3** (scale/workload study) on the frozen ReLAION2B screen
registered in [methods](methods.md); none of it is a release claim, and none of
it is comparable to the vendor-reported numbers in
[market benchmark matrix](market-benchmark-matrix.md).

All screens use the same immutable inputs:

| Input | Object | SHA-256 |
|---|---|---|
| corpus | `v36-prefix-screen/.../attempt-0000/source.parquet` | `2796b579…0560cf86` |
| queries | `…/development-query.parquet` | `310bb54f…dadb2db54` |
| ground truth | `…/development-gt100.parquet` | `fed7524f…8b6c696e11` |

1,000,000 rows x 768 dimensions, 1,000 development queries, exact squared-L2
GT@100.

## V60 — can a proximity graph return the actual top-100 at all?

`scripts/v60_algorithm_first_diskann_returned_recall.py`, result
`research/v60-algorithm-first/diskann-returned-recall-6f193347e7ce5f86/a0001/`.

A standard DiskANN build (degree 64, build complexity 200) was measured on
**returned** Recall@100, not page containment.

| Setting | Aggregate Recall@100 | Worst query | p50 | p99 |
|---|---:|---:|---:|---:|
| complexity 128, beam 2, 1,000 queries | **99.595%** | **89.0%** | 44.648 ms | 46.942 ms |

Latency is local baseline-gp3 EBS on one Spot node; it is **not** an S3 number
and is never reported as one. S3 GETs and payload were not measured.

**Verdict: the graph family can meet the quality bar.** The problem is cost:
the serialized index is 11,061,547,192 bytes, or 11,061 bytes per row — 3.6x
the raw f32 payload. That is not a blob-native index.

## V61 — can static graph-order page packing carry that quality?

`scripts/v61_algorithm_first_graph_page_ceiling.py`, result
`research/v61-algorithm-first/graph-page-ceiling-a613f66b86eb2952/a0001/`.

The V60 topology was preserved and screened under three query-independent row
orders with an optimistic exact-score traversal — an upper bound on what any
real implementation of that layout could reach.

| Packing | rows/page | Aggregate | Worst query | pages p50/p95 | bytes/query |
|---|---:|---:|---:|---:|---:|
| bfs | 128 | 96.876% | 72.0% | 64 / 64 | 27.40 MiB |
| bfs | 64 | 94.580% | 69.0% | 64 / 64 | 13.70 MiB |
| bfs | 32 | 90.836% | 61.0% | 64 / 64 | 6.85 MiB |
| original | 128 | 60.328% | 49.0% | 64 / 64 | 27.40 MiB |
| random | 128 | 60.392% | 49.0% | 64 / 64 | 27.40 MiB |

The screen required 99% aggregate, 70% worst query, p95 <= 32 pages and
<= 16 MiB. **Every cell failed**, and every cell saturated the 64-page cap.

Two facts matter more than the pass/fail:

1. Graph BFS order is worth roughly 36 recall points over source or random
   order, so page locality is real — it is just not sufficient.
2. Because p50 = p95 = max = 64 pages everywhere, V61 measured a
   **routing-limited** traversal, not a containment ceiling. It could not
   distinguish "the layout cannot hold the answer" from "the traversal cannot
   find it".

Separating those two is what V63 exists to do.

## V63 — containment or discovery?

`scripts/v63_algorithm_first_layout_oracle.py`, result
`research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/`,
result SHA-256 `608ceff4…4598de9a`, 6:40 wall, 10.5 GiB peak RSS.

V63 screens the two halves of V61's failure separately. Layouts are built from
the corpus alone — no query, no ground truth — and then evaluated three ways:

- **linear** — one contiguous row permutation cut into fixed pages. Each row
  appears once, so the best-K-pages oracle is **exact**.
- **replicated** — SPANN-style postings, each row written into its `r` nearest
  centroids' lists, pages never straddling a list. Its oracle is greedy
  max-coverage, a **lower bound** on the true ceiling (measured shortfall
  <= 1.25 pp, `scripts/verify_v63_greedy_gap.py`), so a pass is conclusive and
  a failure is not.
- **routed** — the same postings read by a fixed untrained rule: lists in
  ascending centroid distance. Every fetched row is exactly rescored, so this
  containment **is** Recall@100 for an exact-rerank serving path.

The vectorised router is checked against a naive reference implementation on
every replication/page-size cell (`scripts/verify_v63_router.py`).

### Geometric order beats graph order, decisively

At 256 rows/page and a 32-page budget, aggregate oracle containment:

| Linear layout | Aggregate | Worst query |
|---|---:|---:|
| k-means 8192, centroid-chain order | **99.479%** | 72.0% |
| k-means 1024, centroid-chain order | 97.869% | 57.0% |
| DiskANN BFS order (from V61) | 82.762% | 48.0% |
| source order | 33.306% | 32.0% |
| random order | 33.247% | 32.0% |

This settles V61: its BFS packing was not merely under-routed, it was the
wrong layout. A corpus-only k-means order is worth ~17 points over it.

### Containment is solved at 1.00x storage

Linear k-means 8192, no replication, storage multiplier exactly 1.00:

| rows/page | 32 pages | 48 pages | 64 pages | 96 pages |
|---|---:|---:|---:|---:|
| 128 | 98.47% / 64% | 99.83% / 80% | 99.99% / 96% | **100.00% / 100%** |
| 256 | 99.48% / 72% | 99.96% / 88% | **100.00% / 100%** | 100.00% / 100% |

At 256 rows/page, 64 pages is 16,384 rows and 12.25 MiB under the sq8 cost
model, and it contains **every** ground-truth neighbour of **every** query.

Replication is not worth buying. It moves full containment from 64 pages to 48
and costs 3-9x storage; and because inflating posting lists means a fixed page
budget reaches fewer distinct lists, it barely helps the actual router.

### Discovery is the entire remaining gap

The same layout, read by the untrained nearest-centroid posting router:

| Budget | Oracle (linear, x1.00) | Router (r=1, 256 rows) | Router worst query |
|---|---:|---:|---:|
| 32 pages | 99.48% | 93.13% | 26% |
| 64 pages | 100.00% | 96.71% | 48% |
| 128 pages | 100.00% | 98.57% | 69% |

The router never reaches the ceiling, and the shortfall is far worse on the
worst query than in aggregate: where the oracle holds 100% of every query's
neighbours in 64 pages, the router finds 48% of the hardest query's.

**Verdict: the layout question is closed and the routing question is open.**
Static page layout can hold a 100%-Recall@100 answer set inside 64 pages and
12.25 MiB at 1.00x storage. What does not yet exist is a page selector good
enough to find those pages. Two structural reasons the current one cannot:
it routes at posting-list granularity while the oracle selects pages, and with
8,192 clusters over 1M rows the median list is 118 rows, so a list-granular
router spends its budget in the wrong units.

Next: a page-granular router over the preserved
`kmeans_8192-order.npy` layout, scored against this exact 100%/64-page ceiling.

## V64 — can a page-granular router close V63's gap?

`scripts/v64_algorithm_first_page_router.py`, result
`research/v64-algorithm-first/page-router-0850cde9d3be9a51/a0001/`,
result SHA-256 `6b7c87b7…0bf9398e4f`, 102.5 s.

Same layout artifact as V63 (`kmeans_8192-order.npy`, no re-clustering). Each
page carries resident summaries built from the corpus alone — the mean and
squared radius of the page, or of 4 or 8 contiguous sub-blocks inside it — and
the oracle is recomputed in-run rather than quoted from V63.

### Radius lower-bound pruning fails outright

| Scorer (256 rows/page) | 32 pages | 64 pages | 128 pages |
|---|---:|---:|---:|
| mean distance, 8 sub-blocks | 94.74% | 97.66% | 99.09% |
| **admissible lower bound**, 8 sub-blocks | **14.31%** | **28.02%** | **47.24%** |

The bound is correct — the self-test requires it never to exceed the true
distance to any member of the block it summarises — and it is still useless.
In 768 dimensions a block's radius dwarfs the spread between block centres, so
subtracting it collapses almost every page to a score near zero and the ranking
that survives is essentially "largest radius first". Branch-and-bound sphere
pruning does not transfer to this regime. Score by plain centre distance.

### Page-granular routing beats list-granular routing, and still plateaus

256 rows/page, against the same layout's exact ceiling:

| Router | resident B/row | 32 pages | 64 pages | 128 pages | p95 pages for 100% |
|---|---:|---:|---:|---:|---:|
| oracle (ceiling) | 0 | 99.48% / 72% | **100.00% / 100%** | 100.00% / 100% | **35** |
| page mean | 12 | 93.17% / 31% | 96.68% / 58% | 98.44% / 65% | 719 |
| 4 sub-block means | 48 | 94.53% / 32% | 97.53% / 63% | 98.99% / 73% | 502 |
| 8 sub-block means | 96 | 94.74% / 36% | 97.66% / 64% | 99.09% / 76% | 454 |

V63's list-granular router reached 96.71% / 48% at 64 pages; page granularity
lifts that to 97.66% / 64% for 96 resident bytes per row. Real, and not enough.

Three things this pins down:

1. **Finer summaries buy less and less.** Going from one mean per page to eight
   costs 8x the resident RAM and buys 0.98 points at 64 pages. Extrapolating
   toward one summary per row converges on brute force, because the resident
   cost is `4 * dimensions / block_rows` bytes per row by construction.
2. **The tail is the binding constraint, not the average.** At 64 pages the
   best router holds 97.66% in aggregate but 64% of the hardest query's
   neighbours, and needs 454 pages where the oracle needs 35 to cover 95% of
   queries completely. That is a 13x ranking gap, not a containment gap.
3. **RAM is what actually limits this design.** 96 B/row is 96 MB at 1M rows
   but 9.6 GB at 100M; even one mean per page is 1.2 GB at 100M. A single
   resident router over a static layout cannot be both sharp and bounded.

### Where this leaves the quality bar

Read against the market rather than the internal bar, one round trip of 64
GETs and 12.25 MiB already returns 97.66% Recall@100, and 128 GETs returns
99.09% — above turbopuffer's documented ~90% recall@10 example and AWS's
">90% average" for S3 Vectors (see
[market benchmark matrix](market-benchmark-matrix.md)). The 99.5% aggregate
with 80% worst-query bar used here is stricter than anything either competitor
publishes.

Closing the remaining gap with a *single* resident router is the thing the
evidence says not to attempt. The next falsifier is a two-stage read: one
round trip for compact per-row codes over a wide candidate region, then a
second for exact rows — which is also how turbopuffer spends its 3-4 cold
roundtrips.

## V65 — the two-stage read clears the quality bar

`scripts/v65_algorithm_first_two_stage.py`, result
`research/v65-algorithm-first/two-stage-85e79e65e32c3ea8/a0001/`,
result SHA-256 `a210ffa5…6b6adc`, 360.5 s, 16.2 GiB peak RSS.

Same V63 layout artifact, same V64 router (8 sub-block means, 256-row pages).
Stage one reads compact per-row codes for the rows in the router's best M
pages; stage two reads exact vectors for the best N of those rows. Codebooks
are corpus-only. Shortlist containment is Recall@100 after exact rescoring.

### A first correction to how V63 and V64 priced their bytes

V63 and V64 quote page bytes under an `sq8_plain` model of 784 bytes per row.
That is the right price for a *code* page, but both measure a path that ends in
exact rescoring, which needs the exact vector — 3,088 bytes per row. Their byte
columns are therefore code-layer prices attached to an exact-data path, and
they understate it by about 3.9x. The recall figures are unaffected. Everything
below prices each stage at what that stage actually reads.

### The codec is nearly free; the router's page budget is the whole constraint

Recall at 512-row shortlist, by router page budget M:

| Codec | bytes/row | M=64 | M=128 | M=256 | M=512 |
|---|---:|---:|---:|---:|---:|
| exact f32 (control) | 3,088 | 97.66/64 | 99.09/76 | 99.71/86 | 99.94/94 |
| SQ8 | 784 | 97.66/64 | 99.09/76 | 99.71/86 | 99.94/94 |
| **PQ 192x8** | **208** | 97.66/64 | 99.09/76 | **99.71/86** | 99.94/94 |
| 768-bit signs | 112 | 97.43/64 | 98.72/75 | 99.24/81 | 99.38/84 |
| PQ 64x8 | 80 | 95.54/55 | 96.47/59 | 96.77/62 | 96.85/61 |

SQ8 and PQ192 reproduce the exact-f32 control *digit for digit* at every
budget. A 208-byte code — 15x smaller than the exact row — costs nothing in
recall here. Below that it starts to bite: 64-subspace PQ saturates near 96.8%
however many pages it is given, because its own ranking noise, not the router,
becomes the limit.

Ranking all 1M rows with each codec confirms where each one's ceiling is: SQ8
reaches 100.00%/100% inside a 256-row shortlist, PQ192 100.00%/99% inside 512,
768-bit signs 99.90%/91% inside 1,024, PQ64 99.14%/75%.

### The operating point

PQ192 codes, 256-row pages, 512-row shortlist, priced per stage:

| M | Recall@100 / worst | stage 1 (codes) | stage 2 (exact) | total | single-stage equivalent |
|---:|---:|---:|---:|---:|---:|
| 64 | 97.66% / 64% | 3.25 MiB | 1.51 MiB | 4.76 MiB | 48.25 MiB |
| 128 | 99.09% / 76% | 6.50 MiB | 1.51 MiB | 8.01 MiB | 96.50 MiB |
| **256** | **99.71% / 86%** | **13.00 MiB** | **1.51 MiB** | **14.51 MiB** | 193.00 MiB |
| 512 | 99.94% / 94% | 26.00 MiB | 1.51 MiB | 27.51 MiB | 386.00 MiB |

**M=256 clears the bar: 99.71% aggregate Recall@100 with 86% on the worst
query, inside 14.51 MiB and two round trips.** The same recall through a
single-stage exact read costs 193 MiB, so splitting the read is worth 13.3x in
bytes — the first stage buys 15x more candidate rows per byte, and the second
touches only the 512 rows that survive.

The shortlist is not binding: for every codec at or above PQ192, N=512 and
N=2048 give identical recall, because exact rescoring puts true neighbours at
the very top of whatever the code layer shortlists. Only PQ64 needs a deeper
shortlist, and it still does not reach the bar.

### What this is not

- **Simulated I/O, not measured latency.** No GET was issued. Request counts,
  coalescing of adjacent selected pages, and S3 tail latency are unmeasured.
- **Stage two assumes row-addressable exact storage.** 512 rows x 3,088 bytes
  is a lower bound; fetching whole exact pages instead would cost the p95 99
  distinct pages those rows fall in, roughly 74 MiB. Row addressability is
  therefore a load-bearing design requirement, not an optimisation.
- **1M only.** Scale transfer to 100M is unproven and is the next real risk:
  the router's resident summaries are 96 bytes per row, which is 9.6 GB at
  100M.
- **Development split only.** The sealed holdout is not opened.

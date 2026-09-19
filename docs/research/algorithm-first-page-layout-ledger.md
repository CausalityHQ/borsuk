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

## V66 — the router's resident footprint collapses for free

`scripts/v66_algorithm_first_router_ram.py`, result
`research/v66-algorithm-first/router-ram-14361edac84d3007/a0001/`,
result SHA-256 `282c67f0…8d76d1bb30`, 122.7 s.

V65's remaining blocker was the router's 96 bytes per row — 9.6 GB at 100M
rows. This sweeps both axes that shrink it: summaries per page, and lossy
encodings of each summary. Recall is page containment, which V65 showed a
PQ192 code layer plus exact rescoring recovers intact.

Selected cells at M=256 pages, the V65 operating point:

| Encoding | blocks/page | resident B/row | GiB at 100M | Recall@100 / worst |
|---|---:|---:|---:|---:|
| f32 (control) | 8 | 96.00 | 8.94 | 99.71% / 86% |
| SQ8 | 8 | 24.00 | 2.24 | 99.71% / 86% |
| **PQ 192x8** | **8** | **6.00** | **0.56** | **99.71% / 84%** |
| PQ 192x8 | 4 | 3.00 | 0.28 | 99.68% / 81% |
| **PQ 192x8** | **2** | **1.50** | **0.14** | **99.61% / 82%** |
| PQ 192x8 | 1 | 0.75 | 0.07 | 99.43% / 76% |
| PCA-128 SQ8 | 8 | 4.00 | 0.37 | 99.58% / 82% |
| 768-bit signs | 1 | 0.38 | 0.03 | 99.04% / 69% |

**PQ192 summaries match the f32 control at one sixteenth of its footprint**,
and the gate still holds two further halvings down: two summaries per page at
1.50 bytes per row — **143 MiB resident at 100M rows** — returns 99.61%
aggregate with 82% on the worst query. The blocker is gone.

PCA to rank 128 is consistently *worse* than PQ at comparable size: 4.00 B/row
of PCA-SQ8 buys 99.58%/82% where 3.00 B/row of PQ buys 99.68%/81%. Sign codes
are cheapest of all but need M=512 to clear the bar.

### The unifying finding

Across V65 and V66 the same thing is true at both levels: **quantisation is
free and candidate count is everything.** A 208-byte row code reproduces an
exact f32 control digit for digit; a 192-byte page summary reproduces a
3,072-byte one. What moves recall is how many pages the router is allowed to
consider, and the entire purpose of quantisation here is to make considering
more of them affordable — 16x more resident summaries, 15x more candidate rows
per byte of stage one.

### Projected full stack, 1M measured

| Component | Cost |
|---|---|
| Layout | contiguous k-means order, **1.00x** storage |
| Resident router | PQ192 summaries, 2 per 256-row page: **1.5 B/row** (143 MiB at 100M) |
| Stage 1 per query | 256 pages x 256 rows x 208 B = **13.0 MiB** |
| Stage 2 per query | 512 rows x 3,088 B = **1.5 MiB** |
| Round trips | **2** |
| Recall@100 | **99.61% aggregate, 82% worst query** |

Unchanged caveats: I/O is simulated and no GET was issued; stage two assumes
row-addressable exact storage; every figure is 1M on the development split,
and 100M scale transfer is projected arithmetic, not measurement.

## V67 — what the design actually costs in object-store requests

`scripts/v67_algorithm_first_request_profile.py`, result
`research/v67-algorithm-first/request-profile-0c5394ddd0a2beb6/a0001/`,
result SHA-256 `dff6aec4…5594ab83`, 228.2 s.

Every result through V66 counted bytes, pages and rows. An object store charges
per *request*, and a request is a contiguous range, so none of those numbers
say what a query costs to serve. Pages are contiguous in the k-means layout, so
adjacent selections merge into one GET at the price of reading the gap between
them.

Stage one, M=256 pages of PQ192 codes:

| gap (pages) | requests p50 | requests p95 | bytes read | MiB p95 |
|---:|---:|---:|---:|---:|
| 0 | 103 | 139 | 1.00x | 13.00 |
| 2 | 79 | 110 | 1.13x | 15.39 |
| 4 | 67 | 95 | 1.28x | 18.23 |
| **8** | **54** | **77** | **1.59x** | **24.58** |
| 16 | 41 | 57 | 2.22x | 38.09 |

256 scattered pages collapse to 54 requests for a 1.59x byte premium. That is
the shape of the trade: requests fall roughly as the gap grows, bytes rise
faster, and there is no setting that gives both.

Stage two, the 512-row shortlist, is far more scattered — 345 requests if only
touching rows merge, but 28 at a one-page gap and 18 at eight.

**A realistic query is therefore roughly 54 + 28 = 82 requests, not 2.** That
is the number a serving design has to absorb, and it is why the next result
had to be measured against real S3 rather than modelled.

### Stage two may not be needed at all

Taking the top-100 straight from code scores, with no exact rescoring:

| Codec | bytes/row | returned Recall@100 | worst query |
|---|---:|---:|---:|
| PQ 192x8 | 208 | 88.280% | 65% |
| **SQ8** | **784** | **99.182%** | **81%** |

SQ8 returns 99.182% with **no second round trip and no exact data read at
all**. PQ192 cannot — at 208 bytes it ranks a shortlist perfectly but cannot
resolve the final 100, which is the first place in this whole series where the
cheaper code actually loses something.

That makes two candidate designs rather than one: two round trips with PQ192
codes plus exact rescoring at 99.603% containment, or one round trip of SQ8
codes at 99.182% returned. The second trades 3.8x the stage-one bytes for
halving the round trips and deleting stage two's scatter entirely.

## V68 — measured against real S3

`scripts/v68_algorithm_first_real_s3.py`, result
`research/v68-algorithm-first/real-s3-78125af7894dccc4/a0001/`,
result SHA-256 `56ba161b…4da7e19017`, 582.8 s.

The first result in this series that issued a GET. The index is built as real
S3 objects — `codes.bin` (PQ192, row-major in layout order) and `exact.bin`
(id + f32) — and 200 development queries are served against them with real
ranged GETs, no local cache, in-region on one c7i.8xlarge.

### Recall is real

**99.700% returned Recall@100, 94% on the worst query**, measured against the
exact ground truth after a real fetch and a real rescore. The simulated
99.71%/86% from V65 held; the worst query came out better than modelled.

### Latency and cost

| | p50 | p95 | p99 |
|---|---:|---:|---:|
| total | 336.33 ms | 471.71 ms | 513.28 ms |
| stage-one I/O | 138.08 ms | 197.24 ms | 232.14 ms |
| stage-two I/O | 80.25 ms | 158.28 ms | 220.89 ms |
| NumPy ADC compute | 115.36 ms | 139.54 ms | 145.72 ms |

81 requests and 36.03 MiB at p50. Repeated-pass latency is within 2% of
first-pass, which is expected: nothing is cached, so there is no warm path to
speak of yet.

V67 predicted 54 + 28 = 82 requests. The measurement came in at 81. The request
model is sound.

### Build throughput

1M vectors trained, encoded and uploaded in 249.6 s — **4,007 vectors/second
end to end**, of which 127.8 s is PQ training and 101.3 s encoding, both pure
NumPy. Upload of 3.27 GB took 4.9 s at 633.8 MiB/s.

### What this says, plainly

Recall is settled and better than either competitor publishes. Latency is not
yet competitive: 336 ms p50 against turbopuffer's documented 14 ms warm, though
it is well inside their 874 ms cold. Three separable causes, in order of size:

1. **36 MiB per query**, against the 14.51 MiB the V65 model assumed. Gap
   merging is the difference — stage one pays 1.59x for coalescing, and stage
   two fetches roughly 5,200 rows to deliver a 512-row shortlist, a 10x
   overshoot from a 256-row gap that is far too loose.
2. **115 ms of NumPy ADC.** The crate has SIMD PQ4 kernels; this is a harness
   artifact, not a property of the design, and should be read as an upper
   bound.
3. **4.85 QPS.** Also a harness artifact: one shared 64-thread pool serves
   every concurrent query, and Python's GIL serialises the ADC scoring. QPS
   flattens from 8 workers onward, which is the pool saturating, not S3.

Only the first is a real design problem. It is what V69 sweeps.

## V69 — requests, not bytes, set the latency

`scripts/v69_algorithm_first_serving_sweep.py`, result
`research/v69-algorithm-first/serving-sweep-dafe02e403d6bde2/a0001/`,
result SHA-256 `7b93f347…9fec2c50`, 382.2 s, 60 queries per cell.

The four serving knobs swept against the index V68 already published, so nothing
was rebuilt; codebooks regenerate from the same seed and reproduce the published
codes exactly.

| M | gap1 | gap2 | N | Recall@100 | worst | requests | MiB | I/O p50 | total p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 8 | 32 | 512 | 99.550% | 95% | 137 | 27.26 | 291.3 ms | 400.5 ms |
| 256 | 2 | 8 | 512 | 99.417% | 93% | 243 | 16.98 | **448.6 ms** | 522.8 ms |
| 256 | 8 | 32 | 256 | 99.400% | 92% | 102 | 24.15 | 238.3 ms | 338.9 ms |
| 128 | 8 | 32 | 256 | 98.400% | 81% | 83 | 13.57 | 182.6 ms | 233.2 ms |
| 64 | 8 | 32 | 256 | 96.433% | 65% | 64 | 8.59 | 142.5 ms | 164.7 ms |

243 requests carrying 17 MiB take 448.6 ms of I/O; 137 requests carrying 27 MiB
take 291.3 ms. Within this harness, reading 60% more bytes in 44% fewer requests
is 35% faster.

> **Withdrawn by V71.** This cell originally concluded "latency tracks the
> request count, not bytes". That conclusion was a harness artifact and it is
> wrong. Regressing I/O p50 on requests and bytes across all twelve cells gives
> a linear **1.64 ms per request** term at R²=0.997 — and a linear term is the
> signature of a serialised resource, not of a concurrent wave against S3,
> which would grow like the tail of a max-of-N. That 1.64 ms is botocore CPU
> for signing, event dispatch and response parsing, held under the GIL. It also
> independently predicts the QPS plateau: 115 ms of ADC plus 81 requests times
> 1.64 ms is 245 ms of GIL time per query, or 4.1 QPS against the 4.85
> measured. The native reader in V71 reverses the ordering outright.

## V70 — one round trip of SQ8

`scripts/v70_algorithm_first_single_stage_sq8.py`, result
`research/v70-algorithm-first/single-stage-a4a695d66f508edf/a0001/`,
result SHA-256 `8ec7273f…b14484ed`.

If requests are what cost, then deleting stage two — its extra round trip, its
scattered shortlist and about a third of the requests — should win. SQ8 rows at
780 bytes (id, precomputed squared norm, one byte per dimension) are returned
with no rescoring at all.

| M | gap | Recall@100 | worst | requests | MiB | I/O p50 | total p50 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 128 | 8 | 98.183% | 82% | **39** | 39.32 | **123.8 ms** | **140.2 ms** |
| 256 | 8 | 99.133% | 95% | 58 | 82.79 | 194.2 ms | 230.2 ms |
| 512 | 8 | 99.450% | 98% | 75 | 164.94 | 304.9 ms | 386.0 ms |

Build is 6,681 vectors/s — faster than PQ's 4,007 because there is no codebook
to train.

### The two designs, at matched recall

| | Recall@100 | requests | bytes | total p50 |
|---|---:|---:|---:|---:|
| single-stage SQ8, M=128 | 98.183% / 82% | 39 | 39.32 MiB | 140.2 ms |
| two-stage PQ192, M=128 | 98.400% / 81% | 83 | 13.57 MiB | 233.2 ms |

SQ8 is 1.7x faster for 2.9x the bytes.

**This measurement cannot be trusted to decide between them.** The Python and
boto3 per-request overhead — TLS, HTTP parsing, and the GIL — is far larger
than a native client's, which inflates exactly the quantity the trade turns on
and biases the result toward the fewer-request design. The measured 4.85 QPS,
flat from eight workers, is the same artifact: one shared thread pool and a GIL
serialising the scan, not an S3 limit.

The crate already has an object-store client and SIMD PQ4 kernels. The decision
belongs to a native reader over the same published objects, not to another
Python sweep.

## V71 — a native reader, and the reversal it forced

`crates/borsuk-v71/`, results
`research/v71-algorithm-first/native-reader/a0001/`. 200 development queries
per cell against V70's published `sq8.bin`, in-region on c7i.8xlarge, no local
cache, `object_store` 0.14 — the same client the crate already uses.

| M | gap | Recall@100 | worst | requests | MiB | I/O p50 | scan p50 | total p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 64 | 8 | 97.190% | 65% | 22 | 17.9 | 47.4 | 3.6 | 54.3 | 98.1 | 126.1 |
| **128** | **2** | **98.440%** | **79%** | **48** | **27.2** | **46.7** | **3.5** | **54.1** | **87.9** | **110.2** |
| 128 | 8 | 98.615% | 82% | 37 | 37.9 | 67.4 | 5.3 | 77.1 | 150.7 | 254.4 |
| 256 | 4 | 99.185% | 94% | 69 | 63.0 | 109.6 | 6.3 | 120.4 | 265.2 | 274.4 |
| 512 | 8 | 99.415% | 97% | 74 | 162.9 | 259.9 | 14.9 | 280.0 | 354.8 | 442.9 |

### The ordering reverses

In Python, a wider gap merge always won: fewer requests, more bytes, faster. In
the native client the opposite holds. At M=128, gap=2 costs **48 requests and
27.2 MiB at 54.1 ms**, while gap=8 costs **37 requests and 37.9 MiB at
77.1 ms**. More requests and fewer bytes is now 30% faster. Every conclusion
drawn from the Python request-versus-byte trade is withdrawn.

The physics that survives is per *wave*, not per request. A wave of N parallel
GETs pays roughly the p(1−1/N) quantile of first-byte latency, so going from 37
to 48 requests moves which tail quantile you land on by very little, while 10
MiB of gap waste is paid in full.

### The scan was the other half

The first native run spent **81 ms** in the scan against 69 ms of I/O — waiting
on nothing and still dominating, because widening a byte to a float one element
at a time does not vectorise. Eight-lane accumulation across cores took it to
**3.5 ms**, a 23x cut, and only then did the I/O ordering above become visible.

### Against the crate's own prior cold path

`docs/research/cold-read-latency-design.md` records the existing two-wave Rust
path at **58.6 GETs, 27.16 MiB, 205–229 ms p50** over three repetitions. V71
reads **48 GETs and 27.2 MiB — the same I/O — in 54.1 ms**, because it is one
wave instead of two. The gain is structural, not tuning.

### Corrections this run forces on earlier entries

- **The 1.5 bytes/row resident router is a representation claim, not a serving
  measurement.** Every reader measured here decodes PQ summaries to float32 and
  holds them: two 768-dimensional float32 summaries per 256-row page is
  **24 B/row, about 2.24 GiB at 100M rows**. V66 showed the compressed form
  loses no recall; no run has yet served from it.
- **V70's build rate is not comparable to V68's as published.** V70 excludes
  upload where V68 includes it, and V70 *does* train PQ — for the router — so
  "no codebook to train" was wrong. On V68's boundary V70 is about
  **6,622 vectors/s**. Both exclude building the global k-means order.
- **60-query cells cannot carry p99 or worst-query claims.** V69 and V70 use the
  first 60 development queries, where nearest-rank p99 is just the maximum. V71
  uses 200.
- The uploaded objects are **not yet an independently reopenable index**: no run
  persists its codebooks, `low`/`span`, or router alongside the rows they
  interpret. V69 regenerates them from the corpus and a seed. That is a release
  gap, not a recall gap.

## V72 — the router was the byte problem all along

`scripts/v72_resident_row_router.py`, result
`research/v72-algorithm-first/resident-router-ca1c136d54e8fc41/a0001/`,
result SHA-256 `7e39e483…5f40dfa14e`, 173.3 s.

V63 showed the layout holds every query's top-100 inside 64 pages, p95 of 35.
V71's page-summary router needed 256 pages for 99.2%. That four-to-sevenfold
inefficiency, not the page size or the gap merge, is why a query read tens of
MiB. V64 and V66 only ever summarised *pages*; this summarises *rows*.

Page containment for a resident per-row product-quantised code, selecting rows
and reading whatever pages they land in:

| code | bytes/row | GiB at 100M | shortlist | Recall@100 | worst | requests p50 |
|---|---:|---:|---:|---:|---:|---:|
| PQ16 | 16 | 1.49 | 2,048 | 99.248% | 80% | 36 |
| PQ32 | 32 | 2.98 | 512 | 98.623% | 83% | 18 |
| **PQ64** | **64** | **5.96** | **512** | **99.676%** | **90%** | **21** |
| PQ64 | 64 | 5.96 | 2,048 | 99.987% | 98% | 51 |

Against the page-summary router at its best comparable point — 99.185% for 69
requests and 63 MiB — a 64-byte row code reaches **higher recall in a third of
the requests and under half the bytes**. Routing, not layout and not coalescing,
was the binding constraint from V64 onward.

A first run of this probe returned 1.3% recall. That was an inverted
permutation, not a finding: 32,768 rows touching 889 pages covers 22.7% of the
corpus and returned 24%, which is chance. The identifier round trip through the
layout is now asserted rather than assumed.

## V73 — the serving result

`crates/borsuk-v71/` with the row-code router, results
`research/v73-algorithm-first/row-router/a0001/`. 200 development queries per
cell, real S3 ranged GETs, no local cache, in-region c7i.8xlarge, one round
trip.

| shortlist | gap | Recall@100 | worst | requests | MiB | route | I/O | scan | **total p50** | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 2 | 98.350% | 85% | 13 | 6.3 | 4.3 | 34.2 | 1.8 | **40.6 ms** | 62.2 | 74.3 |
| **512** | **2** | **99.155%** | **90%** | **18** | **10.3** | 4.4 | 39.2 | 2.2 | **46.1 ms** | 80.9 | 245.4 |
| 2,048 | 0 | 99.445% | 97% | 60 | 25.3 | 4.4 | 81.9 | 3.1 | 90.0 ms | 269.0 | 286.8 |

Against V71's page-summary router at matched recall — 99.185% in 69 requests,
63 MiB and 120.4 ms — the row-code router delivers **99.155% in 18 requests,
10.3 MiB and 46.1 ms**: 2.6x faster, 3.8x fewer requests, 6.1x fewer bytes.

### Where this stands against the blob-native competitors

| | Recall | latency | cache |
|---|---|---|---|
| **BORSUK V73, measured** | **99.155% Recall@100** | **46.1 ms p50, 80.9 p95** | **none — every query reads S3** |
| turbopuffer, vendor-reported | aims for >90--95% Recall@10 across live queries | 874 ms cold p50, 14 ms warm on its 10M x 1,024 website workload | NVMe + RAM on query nodes |
| S3 Vectors, vendor-reported | ">90% average" claimed | ~100 ms warm | opaque managed |

These are context rows, not a paired benchmark: the datasets, recall cutoffs,
hardware, service caches, and request semantics differ. BORSUK's measured
uncached tuple is promising, but the vendor rows cannot support a speedup or
quality-superiority ratio. BORSUK has no cache tier, so turbopuffer's published
warm number remains a separate operating point rather than a target already
met.

### Honest limits

- **The router is 64 bytes per row resident: 64 MiB at 1M, 6.4 GiB at 100M.**
  V72's PQ16 row gives 99.248% at 16 B/row and 36 requests, so the frontier
  trades RAM against requests; 100M scale is still projected arithmetic.
- **The p99 tail is not solved.** 245 ms at the 512-row point against 80.9 ms
  p95. A wave of N parallel GETs pays about the p(1−1/N) quantile, so the tail
  is S3's, not the design's — hedging and S3 Express One Zone are the untested
  levers.
- Single object per index. S3 request-rate scaling is per prefix, so one key
  caps near 5,500 GET/s: about **300 QPS per index** at 18 requests per query.
  Sharding along the k-means order is required and untested.
- Development split only; the sealed holdout is not opened. Write path is bulk
  build only, and incremental ingest with visibility is unmeasured.

## V74 — encode and object-publication screen, measured

`crates/borsuk-v71/src/bin/v74_ingest.rs`, results
`research/v74-algorithm-first/ingest/a0001/`. In-region c7i.8xlarge, real S3.

Every earlier write number divided a whole-corpus build by its wall time. V74
measures two narrower stages: encoding a deterministic synthetic batch into an
844-byte experimental row, and PUT acknowledgement for one immutable object.
The production reader has no delta reader or generation manifest, so an
acknowledged object is **not query-visible** and this is not sustained ingest.
Its combined SQ8-plus-router-code row also differs from the reader's split S3
row and resident-manifest code layout.

| batch | concurrency | encode v/s | publish v/s | MiB/s | PUT ack p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 32 | 133,335 | 654,000 | 526.4 | **33.7 ms** | 101.4 | 108.2 |
| **10,000** | **16** | **224,910** | **772,282** | **621.6** | 121.8 ms | 254.3 | 261.2 |
| 50,000 | 16 | 230,078 | 733,628 | 590.5 | 694.1 ms | 859.6 | 932.0 |
| 100,000 | 8 | 230,530 | 487,020 | 392.0 | 1,012.2 ms | 1,234.6 | 1,234.6 |

**Neither stage is query-visible ingest.** The payloads are encoded before the
publish clock starts, so 772,282 v/s is only what S3 accepted for these object
sizes. The slower-stage arithmetic below is a pipeline projection, not an
end-to-end measurement:

- Pipelined stage projection: **224,910 vectors/s**, encode-bound on 32 vCPU.
- Serialised (pessimistic, encode a whole batch before sending any): 174,183 v/s.

| | write throughput | visibility |
|---|---:|---:|
| BORSUK V74 stages | 224,910 vectors/s projected from measured stages | 33.7 ms PUT acknowledgement at 1k batches |
| turbopuffer, vendor-reported | ~10,000+ vectors/s | 165 ms p50 for a 500 kB commit; 1 WAL entry/s/namespace |
| S3 Vectors, vendor-reported | 2,500 vectors/s/index | not published |

The vendor rows are context only. V74 does not include reader visibility,
durability metadata, deduplication, or compaction and therefore cannot support
a throughput or visibility comparison with either managed service.

### What this does not yet cover

- **There is no delta read path.** The benchmark's objects are not referenced
  by the reader. A generation manifest, query merge, deduplication, tombstones,
  crash recovery, and compaction must be built and measured before any
  query-visible ingest claim exists.
- Vectors are generated deterministically. Encode cost does not depend on their
  contents at fixed dimension — quantisation is a clip and a round, code
  assignment a fixed-size argmin — but the codebooks here are synthetic too, so
  this measures rate, not the recall of freshly ingested rows.
- Encode is scalar per dimension; the SIMD treatment that took the read scan
  from 81 ms to 3.5 ms has not been applied to the write path.

## V75 — the validation split accepts the configuration

`scripts/v75_run_remote.sh`, results
`research/v75-algorithm-first/validation/a0001/`. Same binary, same index
objects, same corpus-only codebooks. Per [methods](methods.md), validation may
reject a configuration but never retune it, so exactly the two points
development selected were run — and over the **full 1,000 validation queries**,
not the 200 used while iterating.

| shortlist | split | queries | Recall@100 | worst | requests | MiB | p50 | p95 | p99 |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 512 | development | 200 | 99.155% | 90% | 18 | 10.3 | 46.1 | 80.9 | 245.4 |
| **512** | **validation** | **1,000** | **99.272%** | **88%** | **21** | **11.0** | **49.0** | **137.9** | **250.1** |
| 256 | development | 200 | 98.350% | 85% | 13 | 6.3 | 40.6 | 62.2 | 74.3 |
| **256** | **validation** | **1,000** | **98.659%** | **81%** | **13** | **6.5** | **40.5** | **57.1** | **84.2** |

**Validation recall is slightly higher than development at both points**, so the
configuration is not overfitted to the split that chose it. Worst-query recall
is a little lower, which is what five times as many queries should do: more
chances to draw a hard one.

That the split genuinely differed is checked rather than assumed — the exported
manifest hashes to `3fef64ff…` against development's `1147d005…`, and the two
differ only in their query and ground-truth blocks.

The **sealed holdout remains unopened.**

### Where the work stands against the goal

| criterion | status | evidence |
|---|---|---|
| high recall | **met on this validation split** | 99.272% Recall@100 on 1,000 held-out ReLAION queries; vendor metrics use different data and cutoffs |
| low read latency | **met on this workload** | 40.5–49.0 ms p50 with real uncached S3 GETs; vendor rows are different operating points, not paired baselines |
| write throughput | **not measured end to end** | V74 measured synthetic encode and PUT stages only; no query-visible delta path exists |
| scale | **not met** | every figure above is 1M x 768; 100M is projected arithmetic |

Open, in the order they bind: the query-visible delta path, generation commit,
and compaction do not exist; one object per index
caps near 300 QPS until it is sharded; the p99 tail is the wave's p(1−1/N)
quantile and neither hedging nor S3 Express One Zone has been tried; there is
no cache tier, so turbopuffer's 14 ms warm is unreachable; and 100M is
unmeasured.

## V76 — the query-rate ceiling is the router, not S3

`crates/borsuk-v71/` throughput pass, results
`research/v76-algorithm-first/throughput/a0001/`. Queries driven concurrently
against the same index object on c7i.12xlarge, 48 vCPU, in-region.

| shortlist | workers | QPS | GET/s | errors | p50 | p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 8 | 107.0 | 1,390 | 0 | 60.5 ms | 125.3 ms |
| 256 | 32 | 103.7 | 1,348 | 0 | 208.8 ms | 902.1 ms |
| 256 | 128 | 110.9 | 1,442 | 0 | 850.2 ms | 2,852.7 ms |
| 256 | 384 | 108.6 | 1,412 | 0 | 2,730.2 ms | 7,727.1 ms |
| 512 | 32 | 84.9 | 1,783 | 0 | 266.7 ms | 1,131.8 ms |
| 512 | 384 | 80.0 | 1,680 | 0 | 3,676.1 ms | 8,444.6 ms |

**The ledger's "one object per index caps near 300 QPS" claim is withdrawn.**
It was arithmetic from S3's documented per-prefix request rate, and the
measurement does not support it: throughput plateaus at ~107 QPS with **zero
errors**, at ~1,400 GET/s — a quarter of the 5,500/s the prefix limit would
allow. Nothing is being throttled.

QPS flat while latency grows in proportion to workers is a closed system
sitting at its service rate. The resource is CPU, and it is the router:

- The router scores **every row on every query** — 1,000,000 rows x 64
  subspaces = **64M table lookups**, measured at 4.3 ms wall on 48 cores, so
  about **206 core-ms per query**.
- 48 cores divided by 0.206 core-seconds gives ~233 QPS as a ceiling, and 107
  measured once the SQ8 scan and I/O threads take their share. The arithmetic
  and the measurement agree.

### This is the scale wall, and it is not RAM

Earlier entries treated the router's resident footprint as the scaling risk. It
is not. **The router is O(N) per query.** At 100M rows the same scan is 100x the
work — roughly 430 ms of wall time on 48 cores before a single byte is fetched,
or about 1 QPS per node. Resident RAM at 6.4 GiB would have been affordable;
the scan is not.

The fix is hierarchy, and the layout already supports it: rows are in k-means
chain order, so contiguous regions are geometrically coherent. A first level
over page summaries — which V66 showed survive PQ compression intact — selects
candidate regions, and the row codes are then scanned only inside them.
Restricting the row scan to 512 pages cuts the per-query work from 64M lookups
to roughly 5.7M, and a third level would remove the remaining O(N) term. This
is the next change, and until it is made the design serves 1M well and does not
scale.

## V77 — hierarchical routing

`crates/borsuk-v71/` with a coarse level, results
`research/v77-algorithm-first/hierarchical/a0001/`. 200 development queries,
shortlist 512, gap 2, real S3.

| coarse regions | Recall@100 | worst | requests | MiB | route | I/O | scan | total p50 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 98.865% | 90% | 18 | 10.1 | 1.28 ms | 36.4 | 2.5 | 40.2 ms | 93.9 |
| **1,024** | **99.155%** | **90%** | 18 | 10.3 | **2.19 ms** | 37.0 | 2.5 | **41.8 ms** | 76.2 |
| 3,907 (all) | 99.155% | 90% | 18 | 10.3 | 5.97 ms | 37.2 | 2.5 | 45.9 ms | 84.0 |
| *flat router (V73)* | *99.155%* | *90%* | *18* | *10.3* | *4.40 ms* | *—* | *—* | *46.1 ms* | *245.4* |

**Recall is identical to the flat router at 1,024 regions**, for half the router
CPU. Scanning all 3,907 regions reproduces the flat result exactly, which is
the correctness check: the coarse level is a filter, not a different algorithm.

The scaling property is the point. The row-code scan is now **O(regions), not
O(N)** — 1,024 regions is 262k rows scanned whether the corpus holds 1M rows or
100M. The coarse level remains O(N) but over summaries rather than rows, at one
dense dot product per 128 rows.

### Throughput improved, but less than the CPU saving predicts

| router | core-ms/query | ceiling | measured QPS | efficiency |
|---|---:|---:|---:|---:|
| flat | 331 | 145 | 107.0 | 74% |
| regions=1,024 | 225 | 213 | 105.5 | 49% |
| regions=256 | 181 | 265 | 124.6 | 47% |

Halving the router did not double QPS, so the router was not the whole cost.
Attributing what is left: the SQ8 scan moves 10.3 MiB to do 20 MFLOP, which at
2.5 ms across 48 cores is **0.16 GFLOP/s per core**. It is nowhere near compute
bound. The eight-lane accumulation fixed the scalar loop but the u8-to-f32
widening is still built one element at a time, and the remaining cost is that
conversion and the memory traffic behind it. A widening load is the next fix,
and it is a bigger lever on throughput than any further routing work.

**Per-node QPS is ~125 and readers are stateless**, since the index is immutable
in object storage and nothing is cached between queries. Horizontal scaling is
therefore available by construction — but that is an argument, not a
measurement, and no multi-node run has been made.

## V78 — the widening load

`crates/borsuk-v71/`, results
`research/v78-algorithm-first/widening/a0001/`. Same index, same queries.

| | scan p50 | total p50 | QPS (regions=256) | Recall@100 |
|---|---:|---:|---:|---:|
| V77 | 2.50 ms | 41.8 ms | 124.6 | 99.155% |
| **V78** | **1.59 ms** | **40.4 ms** | **137.3** | 99.160% |

Taking fixed-size arrays out of the slices removed the per-element bounds
checks and cut the scan 36%. Recall is unchanged, as it must be — the self-test
requires the vectorised inner product to match the scalar form at every width.

### What still limits throughput

Per-query CPU is now 1.25 ms route plus 1.61 ms scan, about 137 core-ms across
48 cores, which puts the ceiling near 350 QPS against **137 measured — 39%
efficiency**. The gap is not the kernel any more. Each query spreads its own
scan across every core with rayon, so under concurrent load the queries contend
for the same pool instead of running beside one another.

The fix is to stop parallelising inside a query once load is high and let
query-level concurrency do the work: the same core-ms then buys close to the
full ceiling. That trades single-query latency for throughput and should be a
runtime choice, not a build-time one. It is the next throughput change, and it
is worth more than further kernel tuning.

## Where the work stands

Measured on real S3, in-region, no cache, 1M x 768 ReLAION:

| | BORSUK, measured | turbopuffer, vendor | S3 Vectors, vendor |
|---|---|---|---|
| recall | **99.272% Recall@100** (held-out validation, 1,000 queries) | aims for >90--95% Recall@10 across live queries | ">90% average" claimed |
| read latency | **39.4–49.0 ms p50**, uncached | 874 ms cold p50, 14 ms warm | ~100 ms warm |
| write | V74 stage screen only: 224,910 vectors/s projected pipeline, 33.7 ms PUT ack | ~10,000+ vectors/s, 165 ms commit p50 | 2,500 vectors/s/index |
| per-node QPS | **137**, stateless readers | not published | ~hundreds/index claimed |

Open, in the order they bind:

1. **Throughput leaves 2.5x on the table** to in-query rayon contention.
2. **100M is unmeasured.** The row scan is now O(regions); the coarse level is
   still O(N) over summaries and needs SIMD or a third level.
3. **Query-visible deltas and compaction do not exist yet** — V74 is an encode
   and object-PUT stage screen, not a write-throughput result.
4. **No cache tier**, so turbopuffer's 14 ms warm is unreachable by design.
5. **Multi-node scaling follows by construction** from stateless readers over an
   immutable index, but no multi-node run has been made.
6. The **sealed holdout remains unopened.**

## V79 — a throughput hypothesis that failed

`crates/borsuk-v71/`, results
`research/v79-algorithm-first/concurrency/a0001/`.

V78 left per-query CPU at ~137 core-ms with a 350 QPS ceiling against 137
measured, and the stated explanation was that every query spreads its own scan
across all cores, so queries contend for one rayon pool. The predicted fix was
to run each query's CPU on one thread and let query-level concurrency use the
machine. **It made throughput worse.**

| regions | V78 (in-query parallel) | V79 (in-query sequential) |
|---:|---:|---:|
| 1,024 | 117.0 QPS | **46.9 QPS** |
| 256 | 137.3 QPS | **73.7 QPS** |

Halved, not doubled. The diagnosis was wrong in an instructive way: the CPU
work runs inline on a tokio worker thread, so making it sequential does not
free a core — it **blocks that worker for the whole scan**, and the I/O futures
sharing the same runtime are starved behind it. Spreading the work across rayon
at least got it off the worker quickly.

So contention was real but misattributed. The fix is not to serialise the CPU;
it is to keep it off the I/O runtime altogether — `spawn_blocking` or a
dedicated CPU pool, with tokio doing only I/O. That is the next change.

The sequential path stays in the code, unused, because it is the thing a future
attempt will want to compare against.

**The configuration measured best remains V78's**: 99.16% Recall@100 at 40.4 ms
p50 and 137 QPS per node.

## V79–V81 — three throughput hypotheses, all wrong

V78 reached 137.3 QPS per node against a 350 QPS ceiling implied by its
137 core-ms of per-query CPU. Three attempts to close that gap each made
throughput worse, and the record is kept because the negative result is the
useful part.

| attempt | change | QPS (regions=256) |
|---|---|---:|
| **V78** | in-query rayon, CPU inline on tokio workers | **137.3, stable to 384 workers** |
| V79 | sequential CPU per query | 73.7 |
| V80 | CPU moved to the blocking pool | 139.9 at 32 workers, **59.2 at 128** |
| V81 | blocking pool bounded to the core count | 39.7 |

Each step was a reasonable inference from the previous measurement and each was
refuted by the next:

- **V79** assumed queries were contending for one rayon pool. Serialising them
  did not free cores — it held a tokio worker for the whole scan and starved the
  I/O futures behind it.
- **V80** moved the CPU off that runtime, which fixed the starvation and lifted
  the peak slightly, but the blocking pool defaults to 512 threads so a few
  hundred runnable threads then fought over 48 cores past 32 concurrent queries.
- **V81** bounded that pool to the core count, and throughput fell furthest of
  all: long CPU tasks now queue behind a hard limit while their I/O has already
  completed.

**The configuration that measures best is the simplest one**, and it is
restored: rayon inside the query, CPU inline, 137.3 QPS holding flat from 8
concurrent queries to 384. The 2.5x headroom the core-ms arithmetic suggests is
real, but nothing tried here reaches it, and the honest position is that the
remaining gap is not yet understood. A profile — not another hypothesis — is
what it needs.

## V82 — 10M rows, measured

`scripts/v82_scale_build.py` with the unchanged native reader, results
`research/v82-algorithm-first/scale-10m/a0001/`. **deep-image-96-angular:
9,990,000 real vectors with shipped ground truth**, already part of this
repository's standard-dataset matrix. Angular, so vectors and queries are
unit-normalised, which makes squared-L2 rank identically to cosine and leaves
the shipped truth correct for what the serving path computes.

| | 1M x 768 (ReLAION) | **10M x 96 (deep-image)** |
|---|---:|---:|
| rows | 1,000,000 | **9,990,000** |
| pages | 3,907 | 39,024 |
| Recall@100 | 99.155% | **98.430%** |
| worst query | 90% | **95%** |
| requests | 18 | **45** |
| bytes/query | 10.3 MiB | **4.2 MiB** |
| **total p50** | **40.4 ms** | **41.6 ms** |
| p95 | 63.1 ms | 69.5 ms |
| QPS/node | 137 | 84 |
| resident router | 64 B/row | 48 B/row |

**Latency is flat across a tenfold increase in rows** — 40.4 ms to 41.6 ms —
and requests grew 2.5x for 10x the corpus, which is the sublinear behaviour the
layout was supposed to give. Recall holds at 98.4% with a *better* worst query
than at 1M.

The coarse level behaves exactly as V77 predicted it would. It is the remaining
O(N) term, and at 10M it shows: routing costs 6.62 ms at 4,096 regions and
**31.40 ms at 16,384**, where at 1M it cost 1.28–2.19 ms. Region budget is now
a real tuning knob rather than a free parameter, and at 4,096 regions — a tenth
of the pages — recall is unchanged from scanning four times as many.

The 4,096-region throughput sweep completed without errors and peaked at
84.10 QPS (128 workers). The 16,384-region control did **not** remain healthy:
its 384-worker cell recorded 177 failed queries, 18.17 successful QPS,
10.47 s p50, and 62.53 s p99. It is a failed saturation cell, not evidence for
the selected 4,096-region operating point, and must not be omitted when
describing the campaign.

### What this establishes, and what it does not

It establishes that the selected 4,096-region design **works on a real 10M
corpus against real ground truth**, at flat unloaded latency, sublinear request
growth, and with the build, memory and serving path unchanged from 1M. The
high-concurrency 16,384-region control failed, so the campaign does not prove
that every routing budget remains reliable under saturation.

It is **not** a clean row-count-only comparison: the dimension differs, 96
against 768, which is why bytes per query fell rather than rose. And it is
still not 100M — the coarse level's O(N) term is the thing that would bind
there, and V77's third-level remedy remains unbuilt.

Build at 10M took 1,274.8 s, or **7,836 vectors/s** end to end including
k-means over 16,384 clusters. That is a bulk-build figure and is not comparable
to V74's 224,910 vectors/s synthetic encode/PUT pipeline projection, which
neither reclusters nor makes the objects query-visible.

## V83 — corrected query admission and native profile

`crates/borsuk-v71/` and `scripts/v83_run_remote.sh`, immutable evidence under
`research/v83-algorithm-first/reader-profile/a0002/`. The earlier throughput
harness placed ordinary query futures in `buffer_unordered`; synchronous
routing and scan work before each await therefore ran in the parent task. V83
spawns each admitted query as its own Tokio task and counts only successful
queries in QPS. This is a measurement-harness correction, not a serving
algorithm speedup, so it makes the V79--V81 causal conclusions stale.

On the same ReLAION 1M x 768 development artifacts and selected
regions=256/shortlist=512 point, the corrected sweep reported:

| workers | successful QPS | errors | p50 | p99 |
|---:|---:|---:|---:|---:|
| 8 | 101.32 | 0 | 52.45 ms | 256.47 ms |
| 32 | 137.07 | 0 | 159.73 ms | 734.81 ms |
| 128 | 154.86 | 0 | 567.57 ms | 2,206.52 ms |
| 384 | **176.98** | 0 | 1,618.97 ms | 7,266.49 ms |

The historical V78 harness reported 137.33 QPS at 128 workers and 136.09 at
384. Those rows are useful only as evidence of the harness defect; they are not
a paired production speed comparison. V83's 16-query quality pass is also too
small for a recall claim. The 384-worker point maximises completed work while
having unusable tail latency, so **176.98 QPS is a saturation ceiling, not a
release operating point**.

The native `perf` pass ran for 31.19 s and used only 12.262 of 48 CPUs on
average, with 2,301,634 context switches and 353,231 CPU migrations. Flat
cycles were dominated by Rayon/crossbeam scheduling and epoch work (including
19.55% in epoch pin/steal and 7.43% in epoch advancement), while actual coarse
routing, fused scan, and candidate materialisation were smaller individual
terms. The next falsifier therefore keeps independent query tasks but removes
nested Rayon from each query, comparing both modes in one same-host ABBA run.

## V84 — removing nested Rayon does not improve the operating point

`scripts/v84_run_remote.sh`, successful evidence under
`research/v84-algorithm-first/query-level-cpu/a0003/`. Attempts a0001 and
a0002 stopped before compilation or science at immutable-source and manifest
magic checks; their terminal receipts are retained. The successful attempt ran
Rayon A, sequential A, sequential B, Rayon B in one c7i.12xlarge Spot process
environment. Query admission remained independent Tokio tasks in both arms;
only CPU work within each query changed.

| admitted workers | Rayon mean QPS | sequential mean QPS | QPS delta | Rayon p50 | sequential p50 | sequential p99 delta |
|---:|---:|---:|---:|---:|---:|---:|
| 8 | 91.25 | 93.13 | +2.06% | 51.51 ms | 54.13 ms | -0.73% |
| 32 | 149.39 | 144.83 | -3.05% | 162.79 ms | 113.09 ms | -10.15% |
| 128 | 179.15 | 182.64 | +1.95% | 550.00 ms | 552.88 ms | -2.12% |
| 384 | 178.99 | 177.01 | -1.11% | 1,578.48 ms | 1,601.72 ms | +2.06% |

Both arms returned exactly 98.000% Recall@100 on the 16-query diagnostic,
30 median requests, and 12,979,200 median bytes. Those 16 queries establish
functional equality only; V75 remains the quality evidence. In the unloaded
diagnostic, Rayon averaged 44.03 ms p50 and sequential averaged 50.80 ms, a
15.36% regression, while p99 was effectively equal (79.46 versus 78.90 ms).

The sequential perf-stat pass used 4.831 CPUs on average, 470,056 context
switches, and 4,813 migrations, versus V83 Rayon's 12.262 CPUs, 2,301,634
switches, and 353,231 migrations. Removing work stealing therefore saves
scheduler activity, but the saving does not become useful throughput and costs
unloaded latency. The detailed sequential symbol report was not produced
because this host's `perf report` rejected the requested `tid` sort key; the
runner nevertheless exited zero, so only the authenticated perf-stat counters
are usable from that profiling sub-step.

The preregistered promotion required at least 10% paired QPS gain, identical
results, and no more than 5% p99 regression. Sequential CPU fails the QPS gate
at every load. It is rejected, Rayon remains the selected path, and no longer
open-loop qualification is justified for this change. The next binding work is
the query-visible delta/generation/compaction path, not another CPU scheduler
hypothesis.

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

## V85 — exact row evidence isolates the remaining quality loss

The query-visible-delta work also supplied a fixed 900,000-base +
100,000-resident-delta quality screen on ReLAION 1M x 768. All cells used the
same query-independent K-means-8192 physical page order, 256-row pages, and
hard limits of 81 padded pages, 32 ranges, and 16 MiB per query. These are
development diagnostics and remain claim-ineligible.

The best PQ64 reciprocal-rank exact planner used the top 1,024 encoded rows
and reached 98.5938% page-SQ8 Recall@100 and 98.6562% after exact reranking.
At a matched top-2,048 evidence width it reached 98.4375% page-SQ8. A
base-only conditional page-posterior fitted on 256 hashed self-queries retained
a 100% candidate-set physical oracle, but reached only 98.3750% page-SQ8 and
98.4062% exact. Its controlled loss against the matched top-2,048 arm was two
of 3,200 hits; the larger comparison against top-1,024 is a performance
comparison, not an isolated calibration ablation.

Before testing another page representation, attempt `exact-row-control-a0015`
replaced PQ ADC row scores with exact float32 row distances and left the
top-2,048 reciprocal-rank aggregation and exact physical planner unchanged.
It ran once from source
`486335f46829bfee313182fe00e8176be4d69ad1` on c7i.8xlarge Spot instance
`i-00948ce1f8a168460`, then the instance terminated.

| metric | exact-row control |
|---|---:|
| page-SQ8 Recall@100 | **99.5625%** (3,186 / 3,200) |
| exact rerank Recall@100 | **99.7188%** (3,191 / 3,200) |
| query 15 page-SQ8 hits | **93 / 100** |
| query 28 page-SQ8 hits | **98 / 100** |
| physical oracle | **100.0000%** |
| maximum requests | 32 |
| maximum bytes | 16,676,928 |
| scientific wall | 33.57 s |
| peak RSS | 9,972,240 KiB |
| swaps | 0 |

The immutable result is under
`s3://borsuk-bench-453182569524-euc1/research/v85-shared-overlay/486335f46829bfee313182fe00e8176be4d69ad1/exact-row-control-a0015/attempt/`.
The result SHA-256 is
`10526c02beaa0df3a160b8e58e9e6b47d526a1378127acb62b7d76dbabc3c8ef` and
the terminal SHA-256 is
`78308fdcd9629f93b5e8a4c52fc4526def8adf0bb2a15b45b04f149e78160136`.

This is a causal ceiling, not a serving result: its 9.51 GiB peak RSS and
exhaustive float32 scoring violate the 100M memory and throughput target. It
does establish that PQ64 row-code resolution is the dominant source of the
quality gap. Page aggregation and page SQ8 still account for the remaining 14
page-SQ8 misses, so the next fixed 1M falsifier must recover most of the exact
row ordering with a compact hierarchical router rather than tune another
single page centroid.

## V86 — PQ192 clears the untouched slice but the bounded wave-one planner fails

Source `cfccee9bf19467173451bbf1019f482b0ad940e3`, one c7i.8xlarge Spot
instance `i-0e0892ff65676b0ca`, and immutable evidence under
`research/v86-coarse-to-fine/cfccee9bf19467173451bbf1019f482b0ad940e3/a0001/attempt/`.
The instance terminated immediately after its successful terminal marker.
The result SHA-256 is
`761bc6715e604b24028127b0886079c1a1cde4b3272dff0090db6fa92695a231`;
the terminal SHA-256 is
`fc65a94cdbbf54af5ed570c3bae0a2cc5120e5ba16784239208ca53fae71e9e4`.

V86 trained one base-only PQ192x8 codebook set, encoded all 900,000 base rows
and two half-page means per physical page with those books, fetched at most
340 header-bearing code pages in 32 ranges, ranked only the fetched codes,
then fetched at most 81 page-SQ8 pages in 32 ranges after a 512-row shortlist.
The 100,000-row resident delta was included in every final rerank: the
page-SQ8 arm used its production SQ8 codes, while the exact diagnostic used
float32 vectors. The burned queries 0--31 remained the development gate;
queries 200--327 were an untouched confirmation range. The result is
claim-ineligible and qualifies neither 100M CPU nor serving latency.

| split | page-SQ8 Recall@100 | exact Recall@100 | base-only recall | worst |
|---|---:|---:|---:|---:|
| development 0--31 | **98.6875%** (3,158/3,200) | **98.7500%** (3,160/3,200) | **98.5412%** | **88** |
| confirmation 200--327 | **99.4219%** (12,726/12,800) | **99.6094%** (12,750/12,800) | **99.4097%** | **92** |
| combined diagnostic | **99.2750%** | **99.4375%** | — | **88** |

The preregistered development gate required at least 99.25%, query 15 at
least 90 hits, and base-only recall at least 99.1%. It failed: query 15
returned 88 hits and development base-only recall was 98.5412%. The untouched
confirmation gate passed, but cannot override the development failure.

The failure is sharply attributed. Across development base truth, the stages
retain `2,879 -> 2,843 -> 2,843 -> 2,839 -> 2,839 -> 2,837` hits for wave-one
page selection, the PQ192 top-512 shortlist, the wave-two physical pages,
exact rerank, and page-SQ8 respectively. Across confirmation the corresponding
sequence is `11,519 -> 11,470 -> 11,470 -> 11,469 -> 11,469 -> 11,451`.
PQ192 adds no shortlist loss after wave one. The primary loss is therefore the
two-summary, 32-range wave-one physical selection, not row-code resolution.

Wave one used at most 16,733,440 bytes and 32 requests; wave two used at most
16,676,928 bytes and 32 requests. Scientific wall time was 6:00.31, peak RSS
10,289,948 KiB, and swaps zero. The 100M representation arithmetic is 150 MB
resident page-summary codes, 786,432 bytes of codebooks, and 19.181 GB of S3
row codes, but a flat 100M scan would perform 150,000,000 ADC table lookups per
query and is explicitly unqualified. No 100M scale or latency claim follows.

The next experiment must use only the already burned development queries to
repair wave-one selection, then consume a new untouched confirmation range.
It must not retune against queries 200--327 or weaken the failed gate.

## V87 — more fixed-block page summaries do not repair wave one

Source `c22de65341f8e39ac8df121be95956e8e284baa2`, immutable evidence under
`research/v87-summary-capacity/c22de65341f8e39ac8df121be95956e8e284baa2/`.
Attempt `a0001` stopped before science because its Spot instance had no AWS
credentials; instance `i-052d3f1f41f395d90` was terminated after the
cloud-init failure was classified. Attempt `a0002` attached the existing
benchmark instance profile and changed no scientific code or configuration.
It ran once on c7i.8xlarge Spot instance `i-0c2ba11d52af0ab75` in
`eu-central-1a`; the runner shut the instance down after its successful
terminal marker, and the instance is verified terminated.

V87 compared the registered two-summary V86 control with eight fixed
contiguous block means per physical page. Both arms reused exactly one PQ192
book and row-code artifact, the same burned development queries 0--31, the
same production-SQ8 resident delta, and the same wave-one/wave-two limits:
32 ranges and 16,733,440 bytes for wave one, then 32 ranges and 16,676,928
bytes for wave two. No S3 query requests were issued by this offline screen;
64 requests per query is a planned serving maximum, not a latency
measurement.

| arm | summaries/page | page-SQ8 Recall@100 | exact Recall@100 | base-only recall | query 15 | worst | wave-one base truth |
|---|---:|---:|---:|---:|---:|---:|---:|
| registered control | 2 | **98.6875%** (3,158/3,200) | **98.7500%** | **98.5412%** | 88 | 88 | 2,843/2,879 |
| fixed-block challenger | 8 | **98.6875%** (3,158/3,200) | **98.7500%** | **98.5412%** | 90 | 90 | 2,844/2,879 |

The control reproduced V86 exactly, including artifact digest
`649c739a81fcb930afdbd15b3692357a45a3106cdb7385aa37e8ed48badd1c56`.
The challenger improved the burned worst query but did not improve aggregate
or base-only recall, so it failed the unchanged 3,176-hit, query-15=90, and
991,000-ppm base gate. Attribution also stayed planner-bound: the control had
all 2,879 base truth hits inside its top-1,024 page ranks and lost 36 to the
physical range planner; the challenger had 2,878 rank-visible hits and lost
34 to the planner. Neither arm selected truth only through intervening gap
pages.

The eight-summary 100M projection is 600,000,000 resident bytes and
600,000,000 summary ADC lookups per query before hierarchical qualification.
It is not a serving candidate. This result rejects only fixed contiguous
block means at the registered request/byte limits; it does not reject richer
page representations. More fixed-block summaries and a wider flat rank scan
are closed. The next burned-development falsifier must change the scoring or
physical-selection objective while keeping the request and byte ceiling, not
add nested query-level parallelism.

The canonical result SHA-256 is
`5ad0e56180b7fe9627fb9b058817b48807bba5d45e5052a59743434351669293`;
the terminal SHA-256 is
`6fca57de91ef6df004d84fe268695cbb2d3853ce5436392d72c16506d1e35255`.
Scientific wall time was 4:34.43, peak RSS was 10,305,976 KiB, and swaps were
zero. The result remains claim-ineligible and does not consume the reserved
confirmation queries 328--455.

## V88 — pure coverage-first planning is worse than reciprocal rank

Source `4b7bf08b8b0a3aa2c58e1d7b2a45fb2b859cbf48`, immutable evidence under
`research/v88-planner-objective/4b7bf08b8b0a3aa2c58e1d7b2a45fb2b859cbf48/`.
Attempt `a0001` at source `3dca126c95a3730da836c2e1604ac2993104f225`
completed the computation but failed before result serialization because the
remote Python 3.9 runtime does not support `zip(..., strict=True)`. Its terminal
SHA-256 is `a4945a01d0c3d1a49a75aba4093cbe22ad65a6f0c8b1e7f0032b07c2bcca1167`.
The minimal runtime repair was verified locally before the sole clean rerun.

Attempt `a0002` ran once on c7i.8xlarge Spot instance
`i-0591e1c9ca5bf4595` in `eu-central-1a` with the benchmark instance profile.
It compared the registered reciprocal-rank control against a coverage-first
objective on the same two-summary artifact and burned development queries
0--31. Coverage-first assigned every ranked page a dominance weight greater
than the total reciprocal-rank mass, then used reciprocal rank only as the
tie-break. All codebooks, row codes, page summaries, query inputs, SQ8 delta,
and physical limits were identical between arms.

| arm | objective | page-SQ8 Recall@100 | base-only recall | query 15 | worst | wave-one base truth |
|---|---|---:|---:|---:|---:|---:|
| registered control | reciprocal rank | **98.6875%** (3,158/3,200) | **98.5412%** | 88 | 88 | 2,843/2,879 |
| challenger | coverage first | **96.5000%** (3,088/3,200) | **96.1098%** | 89 | 73 | 2,771/2,879 |

Both arms had the same rank evidence: 2,793 base-truth hits inside rank 100,
2,855 inside 256, 2,864 inside 340, 2,875 inside 512, and all 2,879 inside
1,024. The control lost 36 rank-visible hits to physical planning; the
challenger lost 108. Pure coverage therefore overvalues low-ranked pages and
discards substantially more high-value pages. It failed the unchanged
3,176-hit, query-15=90, and 991,000-ppm base gate. The independently computed
physical oracle was 2,879 aggregate base hits with an 84-hit worst query for
both arms, so physical contiguity is not the limiting ceiling.

Each arm stayed within 340 wave-one pages, 32 wave-one ranges and 16,733,440
wave-one bytes, followed by 81 wave-two pages, 32 wave-two ranges and
16,676,928 wave-two bytes. These are planned serving budgets: the screen made
zero S3 query requests and provides no latency measurement. The current dense
100M traceback projects to 8,791,406,250 bytes per query and is explicitly not
serving-qualified; a successful objective would still require a sparse bounded
planner before promotion.

The canonical result SHA-256 is
`bb8b3f530e51ed29219898bdb15e0e3d41a094a1c24fc5f0f91b7b2b78afa8fb`;
the successful terminal SHA-256 is
`c5657a0b23678bced45acca3ab961aaab75c9a42694825b89501a7df729e1314`.
Scientific wall time was 4:31.71, peak RSS was 10,287,276 KiB, and swaps were
zero. The instance is verified terminated. The result is claim-ineligible and
does not consume reserved confirmation queries 328--455. Pure coverage-first
planning is closed; the next bounded falsifier must interpolate between count
and harmonic rank using a fixed monotone hit-probability calibration derived
without touching the reserved confirmation split.

## V89 — corpus-calibrated rank utility is only marginally better

Source `0710b2db3556faa52b22dcc68cf986eff95ee6e2` ran once on c7i.8xlarge
Spot instance `i-0b974f632e2c3793e` in `eu-central-1a`. The instance published
a successful terminal and shut down immediately; it is verified terminated.
Immutable evidence is under
`research/v89-calibrated-planner/0710b2db3556faa52b22dcc68cf986eff95ee6e2/runs/v89-calibrated-planner-20260919T205953Z-0710b2db/a0001/`.
The canonical result SHA-256 is
`132940bdf1b20f0277a41e602eaca173cd5d4df77119e7cd6f4831f3de0de9fb`;
the terminal SHA-256 is
`997996b5cc25ac9951e147dfe00f62bd43a0b7c57833a37d6fccb7245cc9d6aa`.

V89 kept the registered two-summary PQ192 artifact, row codes, burned
development queries 0--31, resident SQ8 delta, and both physical budgets
unchanged. It changed only wave-one page utility. The challenger fitted a
fixed monotone expected-truth-rows curve from 128 SHA-selected resident-delta
vectors, using exact float32 L2 against the 900,000-row base. These corpus
vectors are not development, confirmation, or truth inputs. The selected
calibration pool, base vectors and IDs, full artifact, rank limit, and
neighbour count are all digest-bound. Ranks 0--31 are singleton bins, ranks
32--255 use width-eight bins, ranks 256--1023 use width-32 bins, and all later
ranks share a fixed overflow bin. The calibration SHA-256 is
`73fcbc873a987a0fbf46901922fa91e45abb67b85ee8b1750c53db433ef9a041`.

| arm | objective | page-SQ8 Recall@100 | base-only recall | query 15 | worst | wave-one base truth |
|---|---|---:|---:|---:|---:|---:|
| registered control | reciprocal rank | **98.6875%** (3,158/3,200) | **98.5412%** | 88 | 88 | 2,843/2,879 |
| challenger | calibrated rank utility | **98.7500%** (3,160/3,200) | **98.6106%** | 87 | 87 | 2,846/2,879 |

Both arms had identical rank evidence: 2,793 base-truth hits inside rank 100,
2,855 inside 256, 2,864 inside 340, 2,875 inside 512, and all 2,879 inside
1,024. The calibrated objective reduced planner misses from 36 to 33 but
recovered only two final hits and regressed the worst query. It therefore
failed the unchanged 3,176-hit, query-15=90, and 991,000-ppm base gate. The
reserved confirmation queries 328--455 remain unused.

Each arm stayed within 340 wave-one pages, 32 wave-one ranges and 16,733,440
wave-one bytes, followed by 81 wave-two pages, 32 wave-two ranges and
16,676,928 wave-two bytes. This offline screen made zero S3 query requests;
64 requests per query remains a planned maximum, not measured latency.
Scientific wall time was 6:44.84, peak RSS was 10,287,080 KiB, CPU utilization
reported by `time` was 806%, and swaps were zero. The dense 100M traceback
remains 8,791,406,250 bytes per query and is explicitly unqualified.

This result closes global rank-only utility calibration at the registered
physical budget. It does not close query-dependent page evidence. The next
smallest 1M falsifier should change the page representation rather than tune
another rank curve: deterministic diverse page witnesses or an equivalent
query-dependent page sketch must first demonstrate quality under the same
340-page/32-range ceiling. Any passing representation still needs a sparse
hierarchical router before a 100M serving claim.

## V90 — residual per-row page evidence improves routing but misses the gate

Source `f72e5413d51e76f484101836c31a56a79df398e7` ran once on c7i.8xlarge
Spot instance `i-0f4c43443c0cfc53f` in `eu-central-1a`. The instance published
a successful terminal and shut down immediately; it is verified terminated.
Immutable evidence is under
`research/v90-residual-row-sketch/f72e5413d51e76f484101836c31a56a79df398e7/runs/v90-residual-row-sketch-20260919T214945Z-f72e5413/a0001/`.
The canonical result SHA-256 is
`71054c0d79a5ccaa371972caa0790469a668e5b76ec3ed62b1181ce594d15430`;
the terminal SHA-256 is
`08dd444bd1eccec4beb75d556bbdcc8e9ddb89e8dc8c18544abb0267284b727d`.
That authenticated canonical result records residual-sketch SHA-256
`21aae14867749ac8dfed26043f8cb225024d4928bb81db32d488dd0c23a5a3bd`.

V90 kept the registered two-summary PQ192 control, its top-1,024 candidate
fence, the reciprocal-rank physical planner, resident SQ8 delta, exact rerank,
and both physical budgets unchanged. The challenger added a query- and
truth-blind 16-byte residual PQ code per base row. A page residual was defined
relative to the decoded mean of its two registered control summaries. For each
query the challenger decoded means for only the 1,024 control candidates and
ranked them by minimum residual-row ADC, second minimum ADC, then page ID. The
sketch, complete base, control artifact, fixed training configuration, and
injected page evidence were digest-bound. Reserved confirmation queries
328--455 were not read because the burned-development gate failed.

| arm | page evidence | page-SQ8 Recall@100 | base-only recall | query 15 | worst | planner misses |
|---|---|---:|---:|---:|---:|---:|
| registered control | two PQ192 summaries/page | **98.6875%** (3,158/3,200) | **98.5412%** | 88 | 88 | 36 |
| residual challenger | PQ16 residual code/row | **98.9688%** (3,167/3,200) | **98.8538%** | 90 | 90 | 26 |

The challenger recovered nine final hits and ten wave-one base-truth hits, but
failed the unchanged 3,176-hit and 991,000-ppm base-only gates by nine hits and
2,462 ppm respectively. Its exact-rerank recall was 99.0312%. Both arms had all
2,879 base-truth hits inside rank 1,024. The candidate-fenced physical oracle
was 3,200/3,200 overall and 2,879/2,879 base, with query 15 and the worst query
both at 100 hits. The candidate fence is therefore not the causal limit; the
fixed residual-minimum evidence still orders some relevant pages too late.

Each arm stayed within 340 wave-one pages, 32 wave-one ranges and 16,733,440
wave-one bytes, followed by 81 wave-two pages, 32 wave-two ranges and
16,676,928 wave-two bytes. The offline screen made zero S3 query requests and
provides no serving-latency measurement. Scientific wall time was 4:33.48,
peak RSS was 10,317,988 KiB, CPU utilization was 1,122%, and swaps were zero.

At 100M rows the representation plus resident float32 delta projects to
2,057,023,104 bytes (1.916 GiB), including 1,598,400,000 bytes of residual row
codes. Candidate mean scratch is 3,145,728 bytes and no complete decoded
page-mean table is resident. This is not a serving-memory qualification: the
current dense physical-planner traceback still projects to 8,791,406,250 bytes
per query, and serving CPU and latency remain unqualified. The exact V90
residual-minimum configuration is rejected without a sweep; it does not close
other query-dependent row-to-page reductions.

## V91 — soft occupancy mass is selection-equivalent to residual row minimum

The first attempt used source `3d28c97775fcbbf65e5f79c0096eec49acf3f136`
on c7i.8xlarge Spot instance `i-03e7d7909448cf58b` in `eu-central-1a`.
All input hashes passed and the scientific calculation reached its evidence
validator, but the registered Amazon Linux Python 3.9 runtime rejected two
Python-3.10-only `zip(..., strict=True)` calls. It published terminal SHA-256
`6a4928534bf63d490885f2cd8405df07e8b2eb7cac74713d604fed3b6a43914f`
with exit 96 and no result. A focused regression reproduced the boundary
before the two calls were replaced with indexing over already-validated equal
shapes. The repair passed 15 focused tests, Ruff, pycompile, and diff-check.

The sole clean attempt used source
`be09210505e1030e38cabe8ea9341bb40bbdc2df` on c7i.8xlarge Spot instance
`i-0f6292eadca977649` in `eu-central-1a`. The instance published a successful
terminal and terminated immediately. Immutable evidence is under
`research/v91-soft-occupancy/be09210505e1030e38cabe8ea9341bb40bbdc2df/runs/v91-soft-occupancy-20260919T225043Z-be092105/a0001/`.
The canonical result SHA-256 is
`472fb74da59808bdc346354e124bf6ddf233604650f23485148fca22acde12d3`;
the terminal SHA-256 is
`9e00cbda6b658c60036d81c712c76f94188b6236a64061a12f93d9630efe302a`.

V91 kept V90's authenticated PQ16 residual row codes, top-1,024 candidate
fence, exact reciprocal-rank fallback, resident SQ8 delta, reranker, and both
physical budgets unchanged. It changed only wave-one page utility. For each
query it converted the 100 closest residual-row estimates into fixed-point
soft occupancy mass using a temperature fixed by distances 100 and 200. One
mass quantum dominated the maximum feasible reciprocal-rank sum, so rank was
only a deterministic lower-order fallback. The row-min score matrix, raw mass,
compound weights, candidate fence, and their dependency graph are digest-bound.

| arm | page utility | page-SQ8 Recall@100 | base-only recall | query 15 | worst | planner misses |
|---|---|---:|---:|---:|---:|---:|
| V90 control | residual-row minimum rank | **98.9688%** (3,167/3,200) | **98.8538%** (2,846/2,879) | 90 | 90 | 26 |
| V91 challenger | soft top-100 occupancy, reciprocal-rank fallback | **98.9688%** (3,167/3,200) | **98.8538%** (2,846/2,879) | 90 | 90 | 26 |

The arms selected identical physical pages for every one of the 32 burned
development queries. V91 therefore failed the unchanged 3,176-hit,
2,854-base-hit, and query-15=90 gate by nine total and eight base hits. The
candidate-fence oracle remained perfect at 3,200/3,200 total and 2,879/2,879
base, with a 100-hit worst query. This equality rejects this fixed soft
occupancy reduction as a planner repair: multiplicity among its closest 100
rows adds no effective choice under the registered range/page constraint. It
does not reject other page assignments, representations, or query-dependent
reducers. Reserved confirmation queries 328--455 remain unread.

Wave one remained capped at 340 pages, 32 ranges, and 16,733,440 bytes; wave
two remained capped at 81 pages, 32 ranges, and 16,676,928 bytes. The offline
screen made zero S3 query requests. Scientific wall time was 4:50.54, peak RSS
was 10,319,428 KiB, CPU utilization was 1,169%, and swaps were zero. The 100M
representation projection remains 2,057,023,104 bytes (1.916 GiB), while the
current dense traceback is still 8,791,406,250 bytes per query and is neither
memory-, CPU-, nor latency-qualified for serving. No larger-scale or
confirmation run is authorized by this result.

## V92 — independent wave-two row nominations displace stronger evidence

V92 tested the cheapest causal follow-up to V91 without changing wave one,
the authenticated PQ16 residual codes, the PQ192 shortlist, the resident SQ8
delta, or either physical page budget. For each query it ranked residual-row
estimates only on pages omitted by the frozen wave-one plan. The closest 512
rows contributed an independent reciprocal-rank page objective to the existing
PQ192 wave-two objective. Query and truth labels were not inputs to nomination.
After nominations were frozen, a truth-only L0 diagnostic required at least 8
of V90's 26 wave-one planner misses to occur among the 512 nominated rows; a
failure would terminate before either scientific arm.

The source slice passed seven focused tests in 0.164 seconds, scoped Ruff,
pycompile, shell syntax validation, and diff-check before it was committed as
`54e16b5d06585050c586abaef7e9906ab1cf1094`. The first Spot instance,
`i-00caf6da03e774ced`, was terminated by the controller before usable science:
the generated bootstrap emitted a literal `\\n` in its checksum line and did
not enable shell fail-fast behavior. The controller published a bounded
bootstrap-failure receipt. The generated bootstrap was then syntax-checked
with `set -euo pipefail` and a real newline before the sole clean replacement.

The clean attempt ran on c7i.8xlarge Spot instance
`i-07da2f1387e1da1f4` in `eu-central-1a`. Every source and dataset hash passed,
the instance published a successful terminal, and it terminated immediately.
Immutable evidence is under
`research/v92-wave2-nominated-rows/54e16b5d06585050c586abaef7e9906ab1cf1094/runs/v92-wave2-nominated-20260919T232608Z-54e16b5d/a0001/`.
The canonical result SHA-256 is
`85eca542754d43f3029b0812770dce4fa3776aa4c1d7de9604e2fa1252e1a115`;
the terminal SHA-256 is
`b299ee44aa40869550d1751b55be7279ece299a81068121dfea3097b05d074b6`.
The row-min score SHA-256 remained exactly V91's
`09dc6af46edbf7adfe3edc091996e38602327e05737206aa46c4869211b3345e`;
the frozen nomination authority SHA-256 is
`4667b6265fdbd7cc4ea87a992a3777006eacbb7ce840c12565b649ebbbb48309`.

L0 passed: 14 of the 26 missed truth rows occurred in the label-blind top-512
nominations. The two-arm result nevertheless rejected the merge policy:

| arm | wave-two evidence | page-SQ8 Recall@100 | base-only recall | query 15 | worst |
|---|---|---:|---:|---:|---:|
| V90 control | PQ192 top-512 rows | **98.9688%** (3,167/3,200) | **98.8538%** (2,846/2,879) | 90 | 90 |
| V92 challenger | PQ192 plus independent PQ16 nominations | **97.1875%** (3,110/3,200) | **96.8739%** (2,789/2,879) | 83 | 83 |

Only one query improved, 13 were unchanged, and 18 regressed. The challenger
selected about 39 nominated pages per query on average and displaced 30--50
control pages on many queries. Page-level truth coverage fell by 58 and exact
reranking fell by 58 before SQ8 lost one further hit. The failure is therefore
not an SQ8-only artifact: assigning the two rank lists equal independent mass
made low-resolution PQ16 evidence replace substantially stronger PQ192 pages.
It rejects this replacement merge, not the residual representation or a
protected additive rescue path.

Both arms retained the same maxima: 340 wave-one pages, 32 wave-one ranges,
16,733,440 wave-one bytes, 81 wave-two pages, 32 wave-two ranges, and
16,676,928 wave-two bytes. The offline screen made zero S3 query requests.
Scientific wall time was 4:35.12, peak RSS was 10,456,180 KiB, CPU utilization
was 1,079%, and swaps were zero. The 100M representation projection remains
2,057,023,104 bytes and remains explicitly unqualified for serving latency,
CPU, and memory because the dense planner traceback is still projected at
8,791,406,250 bytes per query.

A read-only counterfactual over the immutable result established the next
bounded hypothesis without another dataset pass. Keeping all 81 control pages
and adding, rather than substituting, rescue pages is monotonic for page-level
and exact candidate coverage. Aggregating reciprocal-rank support over the
top 256 nominations and adding at most the best 24 rescue pages would raise
the observed candidate truth ceiling from 3,169 to 3,183 hits while adding at
most 24 pages (4,941,312 bytes) and 24 uncoalesced requests. This is only a
preregistered feasibility calculation: final SQ8 Recall@100 remains unmeasured,
and the old 81-page/32-range boundary would be deliberately relaxed rather
than silently reinterpreted. Reserved confirmation queries and larger-scale
data remain unread.

## V93 — protected PQ16 rescue passes the 1M development gate

V93 tested the preregistered additive follow-up to V92. It preserved every
V90 wave-two page and compared three bounded additions: ordinary extra PQ192
pages, the best 24 pages supported by the first 256 PQ16 nominations, and the
same PQ16 nominations filtered through their PQ192 code pages. Query and truth
labels were not selection inputs. The original 340-page/32-range wave-one plan,
resident SQ8 delta, exact reranker, and frozen 32-query development split were
unchanged. The rescue arrays and inherited row-min evidence were digest-bound.

The first attempt used source `1d2618a62ab8f0c80f8cd6bcf5b232f873819a2c`
on c7i.8xlarge Spot instance `i-0efd67c7be611cb52`. All source and dataset
hashes passed, but the screen rejected a legitimate query with zero new rescue
pages because its authority check still required at least one. It published
terminal SHA-256
`d1f6c8bc9dbf7d1197cc1f6ecb32403ba62ea3c9beeec9e65fcc4af05d3c3256`
with exit 96 and no scientific result, then terminated. A focused repair made
the registered "at most 24" rule literal and preserved an empty-rescue test.

The sole clean attempt used source
`798bbf4125c1281f55c269a46547066d2d78fa0d` on c7i.8xlarge Spot instance
`i-098afa3cfb6dd845f` in `eu-central-1a`. Every source and dataset hash passed,
the instance published a successful terminal, and it terminated immediately.
Immutable evidence is under
`research/v93-protected-rescue/798bbf4125c1281f55c269a46547066d2d78fa0d/runs/v93-protected-rescue-20260920T001102Z-798bbf41/a0001/`.
The canonical result SHA-256 is
`2b202891fcbefc11c3ecfb9a96e94b0810320f1ab7cff90f07b0f9cbb2444c3f`;
the terminal SHA-256 is
`3fea85acbeec313fc51f2d5e3984294e364c93856d23dfba947a47a08e8f8ce0`.
The row-min score SHA-256 remained exactly V91/V92's
`09dc6af46edbf7adfe3edc091996e38602327e05737206aa46c4869211b3345e`.

| arm | added evidence | page-SQ8 Recall@100 | exact recall | base-only recall | query 15 | worst | max requests | max bytes |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| V90 control | none | **98.9688%** (3,167/3,200) | 99.0312% (3,169/3,200) | 98.8538% (2,846/2,879) | 90 | 90 | 64 | 33,410,368 |
| budget-only | protected PQ192 pages | **99.1250%** (3,172/3,200) | 99.1875% (3,174/3,200) | 99.0274% (2,851/2,879) | 95 | 93 | 86 | 38,351,680 |
| direct rescue | protected PQ16 pages | **99.3750%** (3,180/3,200) | 99.4688% (3,183/3,200) | 99.3053% (2,859/2,879) | 93 | 93 | 88 | 38,351,680 |
| code-filtered rescue | PQ16 then PQ192 filter | **99.3750%** (3,180/3,200) | 99.4688% (3,183/3,200) | 99.3053% (2,859/2,879) | 93 | 93 | 98 | 36,856,320 |

Budget-only improved one query and left 31 unchanged. Both PQ16 rescue arms
improved nine queries, left 23 unchanged, and regressed none. Direct rescue
beat the budget-only attribution control by eight final SQ8 hits, exceeding the
preregistered four-hit attribution margin, and passed the 3,176-total,
2,854-base, query-15=90, worst=90, and no-query-worse-than-control-minus-one
gates. It is therefore the accepted V93 arm. Code filtering retained identical
quality while reducing mean data pages from 81.41 to 59.69, but its additional
code-page wave increased the maximum request envelope from 88 to 98; direct
rescue remains the simpler accepted design.

This was an offline 1M algorithm screen and made zero S3 query requests. Its
request and byte values are reconstructed physical maxima, not latency
measurements. Scientific wall time was 5:42.66, peak RSS was 10,432,568 KiB,
CPU utilization was 894%, and swaps were zero. The representation projection
at 100M remains 2,057,023,104 resident bytes, but the current dense planner
traceback remains 8,791,406,250 bytes per query and is explicitly unqualified
for 100M serving memory, CPU, or latency.

V93 establishes that protected query-dependent PQ16 rescue contains useful
signal beyond merely spending 24 more PQ192 pages. It does not yet establish
real-S3 latency. The next gate is a native real-S3 1M measurement of the fixed
direct-rescue arm, followed by a sparse hierarchical replacement for the dense
traceback before any 100M qualification or reserved confirmation run.

### V94 preregistration — confirm the frozen arm before serving engineering

The sequencing above is superseded before any reserved query was read. The
operator prioritized proving the 1M algorithm step by step before investing in
native-S3 integration or a sparse serving implementation. V94 therefore opens
only the untouched development-confirmation ordinals 328 through 455 against
the immutable 1M corpus. It makes zero S3 query requests and does not establish
latency. Ordinals 456 through 999, the separately registered validation role,
sealed holdout, larger corpora, and performance queries remain unread.

The direct-rescue policy is frozen exactly as V93 selected it: 256 PQ16 row
nominations, at most 24 protected additional pages, the same PQ192 control,
SQ8 delta, exact reranker, training seeds, physical layout, and per-query
budgets. The sole paired arms are the V90 control and V93 direct rescue. This is
a configuration confirmation, not a new attribution experiment; V93 remains
the authority that PQ16 beat the equal-budget PQ192 control.

The direct-rescue arm must reach at least 12,685 of 12,800 Recall@100 hits,
991,000-ppm base-only recall, an 85-hit per-query floor, no query more than one
hit below its paired control, and at least four more total hits than control.
All values are fixed before opening the split. The last condition prevents an
equal-quality arm from accepting the extra page budget. One c7i.8xlarge Spot
cell runs fixed 16-thread BLAS with one query at a time and no Rayon or nested
query work stealing. It has a 1,800-second scientific cap and publishes the
original terminal and complete evidence before shutting down. A pass advances
to native real-S3 1M latency measurement; a failure rejects direct rescue and
returns to algorithm diagnosis without serving or larger-scale engineering.

### V94 result — frozen direct rescue confirms on 128 untouched queries

The sole attempt used source
`f110610fd731c47a1cab403d307590cddcc019dc` on c7i.8xlarge Spot instance
`i-0b23eb9fcb07a82d1` in `eu-central-1a`. All source and dataset hashes
passed, the instance published a successful terminal, and it terminated
immediately. Immutable evidence is under
`research/v94-protected-rescue-confirmation/f110610fd731c47a1cab403d307590cddcc019dc/runs/v94-confirmation-20260920T003759Z-f110610f/a0001/`.
The canonical result SHA-256 is
`71a1ce7b31b189e8eadd6ecddf192f00d9451ce42c0f1a71d378f2bfc09e8f7e`;
the terminal SHA-256 is
`3520829138671c878a2463291e54904eedb5ef61ee00985bb078cbebcc631b93`.
The frozen direct-rescue evidence SHA-256 is
`95a29d766d5cd45c42ca747ef3b4c7e2ec5fe020de3c6f744bf1ae2657d681ba`,
and the newly recomputed row-min score SHA-256 is
`304110613c8f2e6ae5e0bd87f32cd2b967e52022ea75aab253747b8927877d5e`.

| arm | page-SQ8 Recall@100 | exact recall | base-only recall | worst | mean / max planned GETs | mean / max planned bytes |
|---|---:|---:|---:|---:|---:|---:|
| V90 control | **99.0859%** (12,683/12,800) | 99.2578% (12,705/12,800) | 99.0426% | 77 | 55.11 / 64 | 27,465,352 / 33,410,368 |
| frozen direct rescue | **99.4531%** (12,730/12,800) | 99.6406% (12,754/12,800) | 99.4517% | 86 | 77.16 / 88 | 32,406,664 / 38,351,680 |

Direct rescue improved 19 of 128 paired queries, tied 109, and regressed none.
It added 47 final SQ8 hits, exceeding the preregistered four-hit benefit gate,
and passed the 12,685-total, 991,000-ppm-base, 85-worst-query, and paired
non-regression gates. V94 therefore accepts the frozen direct-rescue
configuration on this untouched development-confirmation split. Ordinals 456
through 999, the validation role, and the sealed holdout remain unread.

This was still an offline algorithm confirmation and made zero S3 query
requests. The GET and byte counts above reconstruct the physical request
envelope; they are not latency measurements. Scientific wall time was 7:44.02,
peak RSS was 10,568,872 KiB, CPU utilization was 767%, and swaps were zero.
The fixed 16-thread BLAS configuration processed one query at a time without
Rayon or nested query work stealing.

The next gate is a native real-S3 1M measurement of this exact frozen arm. It
must measure cold object GET latency, request concurrency, bytes, CPU, and
end-to-end query latency without downloading the corpus locally. Only after
that fail-fast serving gate passes do we replace the dense per-query traceback
with a sparse hierarchical index and consider a larger corpus. V94 establishes
quality, not 100M scalability or production latency.

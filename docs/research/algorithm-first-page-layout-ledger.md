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
of 3,200 hits, or 0.0625 percentage points; the larger seven-hit comparison
against top-1,024 is a performance comparison, not an isolated calibration
ablation. Thirty-two queries cannot resolve an effect this small. No causal
promotion follows until the frozen existing arms are paired-rescored across
all 1,000 available development queries with per-query evidence and an
interval for their paired difference.

The V85 runner configured 16 BLAS/OpenMP threads for this work. Its recall
evidence remains usable, but its timings are not single-thread query-cost
evidence. The frozen PQ64 row representation also projects to approximately
6.39 GB at 99.9 million base rows before planner workspace; it cannot satisfy
the eventual sub-3-GiB resident target unchanged. These limitations pause
further selector or calibration tuning and prevent treating the 1M instrument
as a qualified 100M serving architecture.

### Later paired-rescore closure

The registered 1,000-query paired rescore closed the 32-query ambiguity. The
first attempt, `paired-rescore-1000-a0016`, ran source
`900f39c50c527bfb48123975fa1da3ed7294a9e3` on Spot instance
`i-0cccea9231b1292aa` and terminated with exit 96 before emitting a result:
the paired-only path disabled shortlist cells, but the shared recall
denominator was initialized inside the shortlist loop. Its authenticated
terminal SHA-256 is
`68ff2b40f2d279fa39294dcc7f51a2a912da16522064f6148686a8451b80b821`.
The defect was reproduced by a rank-only unit fixture, fixed test-first, and
delivered as `ad7561997e0ba38baf92541e90318042b2da0a38`.

The one clean retry, `paired-rescore-1000-a0017`, used the same immutable
ReLAION 1M x 768 development source, query, truth, layout, arm parameters, and
100,000-repetition paired bootstrap. It ran on c7i.8xlarge Spot instance
`i-05542a10787290ee8` and terminated normally. The registered 32-query prefix
SHA-256
`354ddcb810a13ee8fe981b6904bdfeb21084fe423c79deca8be75f32d21b0aa1`
recomputed exactly before the 1,000-query evidence was accepted.

| frozen arm | page-SQ8 Recall@100 | exact Recall@100 | worst page / exact hits |
|---|---:|---:|---:|
| exact rank, top 1,024 | **99.2850%** (99,285/100,000) | **99.4870%** (99,487/100,000) | 76 / 76 |
| exact rank, top 2,048 | **99.2490%** (99,249/100,000) | **99.4430%** (99,443/100,000) | 76 / 76 |
| page posterior, top 2,048 | **99.2210%** (99,221/100,000) | **99.4100%** (99,410/100,000) | 75 / 75 |

Against its matched top-2,048 rank control, the posterior lost 28 page-SQ8
hits, or 0.0280 percentage points, with a deterministic paired-bootstrap 95%
interval of **[-0.0520, -0.0060] percentage points**. It lost 33 exact hits,
or 0.0330 points, with interval **[-0.0580, -0.0100] points**. Per query, the
posterior page result won/tied/lost 12/954/34 and the exact result 12/948/40.
The top-1,024 rank arm also remained better than the posterior by 0.0640 page
points and 0.0770 exact points. Thus the conditional page posterior is
rejected; no further calibration or selector tuning is justified by V85.

The canonical result is 476,810 bytes with SHA-256
`63f1f094b14f09b48798b26a1a482809c6b405ec084b07585378f3597d8ab586`
under
`s3://borsuk-bench-453182569524-euc1/research/v85-shared-overlay/ad7561997e0ba38baf92541e90318042b2da0a38/paired-rescore-1000-a0017/attempt/`.
The terminal SHA-256 is
`b47f1cd9bb2db9ec872852152e3de9e807306c3b6aff354df378c4a5f1b43ea3`.
Wall time was 38:23.12, peak RSS 16,523,152 KiB, aggregate CPU utilization
241%, and swap zero. These are whole-screen construction/evaluation resources,
not serving latency. The instance terminated and local inspection scratch was
removed. This evidence remains claim-ineligible. Work now returns to the
registered V85 delta/mutation/compaction qualification; no 10M or 100M scale
promotion follows from this rescore.

### Task-5 fail-fast closure — resident delta fixes S3 work, centroid pages fail quality

Commit `856a5988e185f9f26be96354353510468c583ae7` changed the
qualification reader so that an authenticated delta run is fetched once at
startup, held resident under a 128-MiB cap, and merged with every query under
snapshot semantics. Delta pages no longer consume per-query S3 requests or
bytes. The focused resident-delta test, the 8 binary tests, the 10 delta library
tests, the 21 package tests, the 15 qualification/build-delta tests, strict
package Clippy, Ruff 0.15.20, Python compilation, shell syntax, formatting, and
diff checks all passed before the commit was pushed.

The current-format 10,000-row fail-fast replay `preflight-a0019` ran on
c7i.8xlarge Spot instance `i-06047d8adffe2484d` and terminated normally. At
the registered 32-page arm it recovered every ground-truth neighbour on all
100 development queries: aggregate and worst-query Recall@100 were both
**100.0000%**, with 32 GETs/query, 1,154,656 bytes/query, and peak RSS
25,255,936 bytes. The canonical result SHA-256 is
`acd1da99700a36492a7a58f1e32ea60a49e9423b1feef40f4db5756c7560bfcd`
under
`s3://borsuk-bench-453182569524-euc1/research/v85-delta-qualification/856a5988e185f9f26be96354353510468c583ae7/preflight-a0019/attempt/`.
The instance terminated and local packaging scratch was removed.

Attempt `a0020` deliberately tried the current reader against the immutable
historical 100,000-row artifacts from source
`021fb90a80270f42cc255317c693223427892d09`. It stopped before
science with exit 99 because those artifacts predate the current SQ8 page
schema. This is an expected pre-release format boundary, not evidence about
quality. No compatibility reader or migration path will be added.

The replacement current-format 100,000-row screen `100k-current-a0021` built a
90,000-row base plus a 10,000-row resident delta and evaluated 32 development
queries using real in-region S3 range GETs. It ran on c7i.8xlarge Spot instance
`i-0a47061ce4c6ab276`, terminated normally, and was terminated immediately
after its terminal marker. Build wall time was 4.20 seconds, peak RSS was
1,830,196 KiB, and aggregate CPU utilization was 685%.

| arm | Recall@100 | worst query | GETs/query | max bytes/query | query latency |
|---|---:|---:|---:|---:|---:|
| centroid pages, 16 | **83.9062%** (2,685/3,200) | 37% | 16 | 2,695,208 | p50 51.045 ms, max 68.330 ms |
| centroid pages, 32 | **92.8750%** (2,972/3,200) | 74% | 32 | 5,209,112 | p50 89.200 ms, max 101.855 ms |

The p16 and p32 result SHA-256 values are respectively
`c9d2476b87edc35ff7cdadb1daa717ccc7c7a0efe5730d35e6c5df799493eae8`
and `8c69aa9372c7f7f43105f6a6a51bb854d547b9017241873006c7363c57311be9`.
The evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v85-delta-qualification/856a5988e185f9f26be96354353510468c583ae7/100k-current-a0021/attempt/`.

**Ruling:** resident authenticated deltas solve the duplicated-request defect,
but centroid-to-contiguous-page routing is rejected at 100,000 rows under the
frozen 99% Recall@100, 32-GET, 16-MiB gate. A 1M Task-5 run would only make an
already decisive failure slower, so it remains fenced. The sole next
representation hypothesis is a bounded PQ16 per-row-code page nominator,
screened first on this 100,000-row shape against the centroid p32 control. No
request gate will be loosened implicitly, and no 10M/100M promotion occurs
unless the validator has recomputed per-query recall, mutation, and compaction
evidence and a complete sub-3-GiB 100M memory worksheet is feasible.

### PQ16 100k page-nomination closure

The one permitted follow-up representation hypothesis reused a 16-byte
query-independent PQ code per base row, ranked a fixed 2,048 rows by ADC, and
treated the authenticated 10,000-row delta as resident. The source, query,
truth, generation, base-run, and delta-run bytes were authenticated before
science. Results are offline page-containment evidence, not page-SQ8 serving
quality or latency.

Attempt `100k-a0022` stopped before science with exit 95 because its submitted
shell transcribed the delta digest incorrectly. The generation manifest's
registered digest was re-read directly, the failed Spot instance
`i-05b6fb39384b726a8` was terminated, and no result was emitted. No scientific
claim uses this attempt.

Attempt `100k-a0023` used source
`24383d853474a19702d18d2de700bee3618167f5` and measured the diagnostic policy
that reads every physical page touched by the top-2,048 rows. On 32 development
queries it reached **99.7500%** page containment (3,192/3,200), with 96% worst
query. This required as many as **53 GETs and 45,651,248 bytes/query**; the
median query touched 98 pages and used 39 GETs. The arm therefore fails both
registered S3-work gates despite clearing aggregate quality. Its canonical
result SHA-256 is
`5c4be5718f4ac2bb80a7b8abea49bf15f308c1439c4c6064318d7e8fc8f8dc3d`
under
`s3://borsuk-bench-453182569524-euc1/research/v85-pq16-page-nomination/24383d853474a19702d18d2de700bee3618167f5/100k-a0023/attempt/`.
Scientific wall time was 11.28 seconds, peak RSS was 2,197,948 KiB, and swap
was zero. Spot instance `i-0c093338b4fef14b9` terminated.

Attempt `100k-a0024` corrected the probe to the already-established
deterministic page policy: reciprocal-rank page weights from the same top-2,048
PQ16 rows, followed by exact maximum-weight dense selection under 32 ranges and
an 83-page conservative span cap. Source
`2fa9f00f96e562641e5bbc8b3a8eead408dc0689` had four focused tests plus Ruff
0.15.20, Python compilation, and diff checks green before execution.

| PQ16 policy | aggregate containment | worst query | max GETs | max bytes |
|---|---:|---:|---:|---:|
| read every touched page | **99.7500%** | 96% | 53 | 45,651,248 |
| exact bounded reciprocal-rank planner | **98.7500%** (3,160/3,200) | 93% | **32** | **13,201,752** |

The bounded arm met both S3-work gates but missed the 99% aggregate and
worst-query gates. Nine queries fell below 99 hits; queries 1 and 23 each
returned 93/100. Its canonical result SHA-256 is
`a54b384a3f097302469830cf2ea3a7156ee86225ff491669df8e8e98fb446e05`
under
`s3://borsuk-bench-453182569524-euc1/research/v85-pq16-page-nomination/2fa9f00f96e562641e5bbc8b3a8eead408dc0689/100k-a0024/attempt/`.
Scientific wall time was 11.96 seconds, peak RSS was 2,198,268 KiB, and swap
was zero. Spot instance `i-069f7909f44ca3189` terminated.

**Ruling:** direct PQ16 row nomination on the current physical layout is
rejected under the joint quality and S3-work contract. Reading enough nominated
pages clears quality but exceeds work by a wide margin; the exact bounded
planner clears work but loses 40 of 3,200 neighbours. This does not reject all
PQ representations or all possible layouts, but it does reject this sole
registered follow-up hypothesis. No 1M, 10M, or 100M promotion follows, and no
request or quality boundary is loosened from this development evidence.

### Competitive qualification contract v2 — frozen before the next 1M split

The original 99% average plus 99% absolute-worst Recall@100 rule was a research
aspiration, not a market-parity requirement. It is stricter than the published
first-party targets available for comparable object-backed services: Amazon S3
Vectors documents greater than 90% average recall for most datasets, while
turbopuffer documents a 90--95% Recall@10 target across live queries. These are
not paired measurements: their datasets, cutoffs, filters, and latency methods
differ from BORSUK's. They define an external competitive floor, not evidence
that a BORSUK arm has matched either product.

Before opening another 1M held-out split, qualification matrix v2 therefore
freezes three independently recomputed distributional gates: at least 96%
average Recall@10, 97.5% average Recall@100, and 90% p05 Recall@100. Absolute
worst-query Recall@100 remains reported but is no longer a veto. The 32-GET,
16-MiB, and 3-GiB ceilings are unchanged. Receipt v3 derives every quality
value from ordered raw result and truth IDs; the remote runner no longer trusts
its own aggregate fields. This contract is externally anchored and is not a
retroactive pass for the 32-query development arms. In particular, their
Recall@10 was not measured.

The executable 100M worksheet also closes the representation-only accounting
gap. At 100,000,000 live rows with at most 8,000,000 uncompacted mutations it
budgets 1,600,000,000 bytes of page-ordered PQ16 codes, 256,000,000 bytes for a
sorted 32-byte-per-mutation directory, 3,125,016 bytes of page offsets, 786,432
bytes of codebooks, 268,435,456 bytes for sixteen bounded sparse-planner
workspaces, 33,554,432 bytes of metadata reserve, and 536,870,912 bytes of
runtime reserve. Total projected residency is **2,698,772,248 bytes**, leaving
522,453,224 bytes below 3 GiB. This is feasibility arithmetic, not a serving
measurement. It requires exact delta vectors and page bodies to remain in S3,
page-ordered codes so no ID-to-page table is resident, and a sparse touched-page
planner. The current dense traceback would project **4,595,961,792 bytes** and
is explicitly disallowed; the sparse replacement is not yet implemented.

### V85 competitive rescore — 1,000-query development evidence

Source `fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d` ran one c7i.8xlarge
Spot cell, instance `i-07da75d2499d6e449`, against all 1,000 development
queries and a freshly recomputed exact truth for the authenticated 100,000-row
current-format corpus. Query execution was serial with fixed 16-thread BLAS;
there was no Rayon or query-level work stealing. The instance terminated after
publishing both raw result files. Evidence is under
`research/v85-competitive-rescore/fb976932ecd4076e2f76a7cb7e7aa7efe01e9a2d/runs/v85-100k-dev1000-20260920T094401Z-fb976932/a0001/`.

| fixed arm | average Recall@10 | average Recall@100 | p05 Recall@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| centroid p32, real S3 | 97.4500% | 94.6600% | not promotion-tested | 32 | 5,480,704 |
| bounded PQ16 page containment | **99.8200%** | **99.1890%** | **95.0000%** | 32 | 14,126,480 |

The PQ16-minus-centroid paired bootstrap 95% interval is **+2.0000 to
+2.7500 percentage points** at Recall@10 and **+4.1479 to +4.9340 points** at
Recall@100. The intervals use the same 1,000 queries and 10,000 deterministic
paired resamples. PQ16 passes qualification contract v2 on this fully burned
development split. It does not promote by itself: this was page containment,
not a real-S3 PQ16 latency measurement, and no held-out query was opened.

The centroid arm made 32,000 actual range GETs. Its p50/p95/p99 query latency
was 86.273/634.093/655.755 ms. Truth construction took 1:03.39 at 1,685,640
KiB peak RSS; centroid replay took 4:23.90 at 86,080 KiB; PQ16 construction and
scoring took 32.53 seconds at 2,212,064 KiB. Every phase reported zero swaps.

The original terminal is retained with exit 104 because a post-science reducer
incorrectly expected duplicated truth IDs inside the centroid result. Both raw
scientific outputs were already complete. The corrected reducer at
`698e80d5ac46c9969b280be1429798c5686a12b3` uses the PQ sample's authenticated
truth authority, independently recomputes both arms, and produced canonical
summary SHA-256
`c7bf42254618670457b911ab003f803cc8aa4e9e479e5f2771af543d27aa2979`.
The raw centroid and PQ16 SHA-256 values are respectively
`3572f999d23ebfbf69c308b3fd603588203ea615ec34888056c802a73c4a6ed1`
and `3b01a1bd33294707d0bc706efa3e136291c3ddffbb3c8dc5d129a4f6a9964951`.
No scientific rerun was performed.

**Ruling:** 99% average Recall@100 is achievable at 100,000 rows, but it is no
longer the product gate. The vendor-aligned distributional contract is the
authority. The fixed PQ16 arm advances exactly once to an untouched 1M
held-out screen; its training seed, 2,048-row shortlist, reciprocal-rank page
weights, 32-range/16-MiB work limits, and quality gates are frozen. Failure
returns to page-locality design; success advances to a native real-S3 replay
and sparse-planner implementation, not directly to 10M or 100M.

### Original exact-row control

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

### V95 preregistration — fail-fast real-S3 frozen-plan replay

V95 first isolates the S3/layout risk before porting the dense planner. It
reuses confirmation ordinals 328 through 343 and their already-frozen V94
direct-rescue page plans; it opens no new query or truth role and permits no
retuning. The 900,000 base rows are encoded once into authenticated fixed-width
PQ192 and per-page SQ8 objects in Standard S3. Before measurement, the builder
unlinks the local base corpus and exits. A fresh process retains only the
100,000-row SQ8 delta, queries, truth, and canonical plan, then replays the
registered wave-one and wave-two ranges through actual in-region S3 GETs.

Queries execute serially. Each request wave has at most 16 I/O workers; there
is no query-level pool, Rayon, or nested work stealing. The result must exactly
reproduce every frozen query's page-SQ8 hits, reconcile physical GETs and bytes,
and authenticate both S3 objects by length and SHA-256 checksum before the
first range read. An untimed, separately counted 32-GET preflight establishes
all 16 HTTP connections before measurement. The bounded storage-I/O feasibility
gate is p50 at most 250 ms and p95 at most 500 ms over 16 queries; nearest-rank
p95 is therefore also the cohort maximum. Decode, rerank, and total latency are
reported separately but do not decide this storage-floor gate. These loose
limits decide only whether this physical request envelope deserves full reader
engineering; they are not a production SLA or competitor claim.

This probe deliberately excludes planner latency and is claim-ineligible. A
pass advances to the native full-planner/reader measurement, where routing,
I/O, decode, rerank, and end-to-end latency are all measured. A failure sends
the design directly to page packing/request coalescing rather than spending on
128-query or larger-corpus evaluation.

### V95 result — quality reproduces, but the real-S3 tail gate fails

The sole attempt used source
`2da2ab1f480a0019942209f1093806f1117ca18a` on c7i.8xlarge Spot instance
`i-094269d7a4169d002` in `eu-central-1a`. Every source and dataset hash passed,
the instance published a complete terminal, and it terminated immediately.
Immutable evidence is under
`research/v95-real-s3-plan-replay/2da2ab1f480a0019942209f1093806f1117ca18a/runs/v95-real-s3-20260920T011819Z-2da2ab1f/a0001/`.
The canonical result SHA-256 is
`5ed8db6c68c8fdbf99ea83933aa2e514794e7872025417644e0325c78b94e948`;
the terminal SHA-256 is
`51eb3eb4a4f508d03e9240be7755df21576463caea5a5871b96ce19a7c2131d0`.

The replay authenticated a 173,043,456-byte PQ192 code object with SHA-256
`b8e984b83e4140e97b308683e9c2c2c40cd1c90a137e711251d2a4fa9d91375c`
and a 723,902,208-byte page-SQ8 object with SHA-256
`b05154d3d8e707acde5b4984ff1f5fc05b4830f7d7d5ceb815038cbbfbc59f56`.
The fresh replay process held no local base corpus. Its 32 untimed preflight
GETs transferred 4,081,664 bytes; the 16 measured queries issued 1,205 real
range GETs, for 1,237 actual S3 requests including preflight.

| metric | real-S3 frozen-plan replay |
|---|---:|
| Recall@100 | **98.8750%** (1,582 / 1,600), exact frozen-plan match |
| worst query | **86 / 100** |
| GETs/query, mean / max | **75.31 / 88** |
| bytes/query, mean / max | **31,145,600 / 38,351,680** |
| code I/O p50 / max | **89.927 / 205.356 ms** |
| data I/O p50 / max | **116.686 / 436.832 ms** |
| storage I/O p50 / max-of-16 | **205.070 / 642.188 ms** |
| decode p50 / max | **104.994 / 134.711 ms** |
| rerank p50 / max | **132.826 / 145.606 ms** |
| end-to-end p50 / max-of-16 | **493.714 / 971.243 ms** |

The storage-only gate required p50 at most 250 ms and max-of-16 at most
500 ms. The median passed, but the tail failed at 642.188 ms, so V95 rejects
this measurement boundary and does not advance to full-reader engineering.
Quality is not the failure: every query reproduced its frozen offline hit
count exactly.

The failed tail is concentrated in the first measured query. Ordinal 328 used
84 GETs and 38,351,680 bytes and took 642.188 ms of storage I/O; later
ordinals 340 and 343 transferred the same maximum bytes with 85 and 88 GETs
but took only 214.051 and 213.775 ms. That is evidence of a position-dependent
transport effect after the small-page preflight, not proof of its mechanism.
The next bounded falsifier must therefore reverse the first-query order and
repeat the same immutable ranges from fresh clients before changing page
packing. It can reuse these two S3 objects and needs no corpus download,
training, reranking, or larger query split.

### V96 preregistration — reverse-order transport diagnosis

V96 changes only the temporal order of the immutable V95 range plan. A fresh
S3 client authenticates the frozen V95 plan plus the exact PQ192 and page-SQ8
object identities, performs the same separately counted 32-GET connection
preflight, and then executes ordinals 343 through 328 serially. It downloads no
corpus, query, truth, delta, or vector artifact, decodes no page, and performs
no planner or reranker work. Each query reads exactly its registered code and
data ranges with at most 16 concurrent range requests. The probe therefore
tests transport ordering only and cannot change or newly substantiate quality.

The prior 500 ms storage-I/O tail threshold is retained without tuning. If the
first reversed query (ordinal 343) exceeds it while ordinal 328 does not, the
position-dependent-tail hypothesis is supported. If ordinal 328 exceeds it in
the final position while ordinal 343 does not, a query-specific range-plan
effect is supported. If both exceed it, the cause remains unresolved; if
neither does, the original tail did not reproduce. Every outcome remains
claim-ineligible. No packing, client, retry, concurrency, or object-layout
change is permitted until this classification is terminal.

### V96 result — the S3 tail follows first measured position

The sole reverse-order attempt used source
`76112a31cae8efa828a19a2957b6b968062d30c7` on c7i.8xlarge Spot instance
`i-05018540f05b6f008` in `eu-central-1a`. The terminal completed with exit zero
and the instance terminated. Immutable evidence is under
`research/v96-s3-transport-order/76112a31cae8efa828a19a2957b6b968062d30c7/runs/v96-reverse-order-a0001/`.
The result and terminal SHA-256 values are
`6450878a3d99327a7fb9a370c3fdf746e0b4173597888f8f456e906bdaf5d12e`
and `1f60759f7e7244a1814f3a1674ff205cec398089c00bfe0a19a7c60cc6cf0113`.

The probe issued the same 32 untimed preflight GETs and 1,205 measured range
GETs as V95. Measured work remained 498,329,600 bytes: 75.3125 GETs and
31,145,600 bytes per query on average, with maxima of 88 GETs and 38,351,680
bytes. Reversed ordinal 343 became position zero and took **577.646 ms** of
storage I/O. Ordinal 328 moved to position 15 and fell to **206.272 ms** even
though both transferred the same maximum bytes. Cohort storage I/O was
200.796 ms p50, 577.646 ms max-of-16, and 232.386 ms mean. The registered
classification is `position-dependent-tail-supported`.

This reproduces the tail on a different maximum-work query and removes page
choice or ordinal 328 as its cause. It does not prove the lower-level mechanism,
but it rejects page repacking as the next response: transport/client warm-state
handling must be isolated before changing immutable layout. The diagnostic
wall was 4.23 seconds, peak RSS was 173,428 KiB, and swaps were zero. It
downloaded no corpus, query, truth, delta, or vector artifact and remains
claim-ineligible.

Preparation took 4:20.19 at 10,398,504 KiB peak RSS and zero swaps. The fresh
replay took 10.05 seconds at 2,106,012 KiB peak RSS and zero swaps. This result
is claim-ineligible, measures plan replay rather than planner latency, and
transfers wave-one code pages without decoding them.

## V85 competitive gate — fixed 1M validation rejects the current PQ16 layout

After the full 1,000-query 100k development rescore, V85 froze PQ16 seed 7216,
a 2,048-row shortlist, the exact reciprocal-rank page planner, 32 GETs, and 16
MiB per query. It then opened the registered 1M validation role once. The
900,000-row base plus 100,000-row delta used 768 dimensions and 256-row pages;
the 1,000 validation queries and exact Recall@100 truth were authenticated from
the immutable V36 dataset freeze. This was an offline page-containment test,
not a serving-latency measurement, and it read no sealed holdout.

The first attempt used source
`ed61ab22571467f98f026a7057f94ecf2e79d846` on c7i.8xlarge Spot instance
`i-06be3537c5c3f2e76`. Its 24 GiB virtual-address cap rejected a PyArrow 512
MiB allocation during build even though peak RSS was 9,135,912 KiB and swaps
were zero. It published exit 100 in phase `build`, terminal SHA-256
`a7666c347e9a9c8502214011a540dc6fafd5e51b9fa84ce6725013710740068e`,
and terminated. This was a harness failure with no scientific result. A
test-first repair raised only the scientific build address-space cap to 48
GiB; it did not change the frozen algorithm, quality gates, or serving-memory
contract.

The sole scientific attempt used source
`da42b3da7a78a7a44ab259bb06f12c42527e8800` on c7i.8xlarge Spot instance
`i-0872a019ab6814b59` in `eu-central-1a`. Every source and dataset hash passed,
the instance published a successful terminal, and it terminated immediately.
Immutable evidence is under
`research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/`.
The canonical result SHA-256 is
`62f629f7706adc0972ce209dffc5ba4e02a8f2567aa3cad154fefda3a68aa0f2`;
the terminal SHA-256 is
`6cb7b8fc499954212c2fe2aa316be09c064cb23361b8f6a7e65b00b06e23e581`.

| metric | fixed 1M validation result | gate |
|---|---:|---:|
| average Recall@10 | **97.5200%** | at least 96% — pass |
| average Recall@100 | **93.5050%** | at least 97.5% — fail |
| p05 Recall@100 | **70.0000%** | at least 90% — fail |
| median / p75 Recall@100 | **99% / 100%** | reported |
| worst Recall@100 | **32%** | reported, no absolute veto |
| GETs/query | 20 median / **32 max** | at most 32 — pass |
| bytes/query | 16,361,264 median / **16,734,128 max** | at most 16,777,216 — pass |

The current layout therefore passes the vendor-aligned average Recall@10 and
physical request/byte bounds, but fails its own distributional Recall@100
contract. The failure is a hard-query tail rather than uniform degradation:
the 25th percentile was 92%, the median was 99%, and at least 25% of queries
were perfect, while the p10/p05/min were 79%/70%/32%. This rejects the present
page-locality design at 1M; it does not establish that 99% average recall is a
necessary universal product threshold, nor does it justify increasing S3
downloads.

Build wall time was 29.59 seconds at 10,905,200 KiB peak RSS. PQ evaluation
wall time was 3:05.04 at 10,973,932 KiB peak RSS. Both phases reported zero
swaps and the monitor observed zero memory PSI. The result remains
claim-ineligible because page containment was computed offline. The validation
role is now burned and must not be retuned against. The next research step is
bounded development-only page-locality diagnosis and one replacement
hypothesis aimed at the lower tail; 10M/100M promotion and native-S3 replay of
this rejected arm remain fenced.

### V85 query-independent PQ co-occurrence page ordering is marginal

The one permitted post-rejection representation probe changed only the
physical order of existing immutable pages. It sampled 256 base rows by a
fixed query-independent SplitMix64 order, ranked 512 PQ16 rows for each, built
a weighted graph from each pseudoquery's first 32 unique pages, and emitted a
deterministic maximum-adjacency page order. Page membership, PQ16 codebooks and
codes, the 2,048-row evaluation shortlist, reciprocal-rank planner, 32-range
cap, 16-MiB cap, and all 1,000 already-burned 100k development queries were
unchanged. Validation and holdout roles were not opened.

The sole run used source `7ffc2eb7aedc875f2756ddb05a3fb0f006c8473e` on
c7i.8xlarge Spot instance `i-004cafd6cfd32ef10` in `eu-central-1a`. It
published a successful terminal and terminated immediately. Immutable evidence
is under
`research/v85-pq16-cooccurrence-layout/7ffc2eb7aedc875f2756ddb05a3fb0f006c8473e/runs/v85-cooccurrence-100k-20260920T102300Z-7ffc2eb/a0001/`.
The canonical result SHA-256 is
`93e4a153259aafc1e0beba952559f673664b8b7b2a6fe1701c2360ad58720cc8`;
the terminal SHA-256 is
`eeece60b0f44f35060b0649ba738135586b99508726e38ecd325c19e605e4c2f`.

| fixed arm | Recall@10 | Recall@100 | p05 / worst Recall@100 | median / max bytes | max GETs |
|---|---:|---:|---:|---:|---:|
| existing order | **99.8200%** | 99.1890% | 95% / 80% | 12,544,712 / 14,126,480 | 32 |
| PQ co-occurrence order | 99.8100% | **99.2850%** | **96% / 82%** | 12,948,240 / 14,545,648 | 32 |

The challenger recovered 96 Recall@100 hits per 100,000 while losing one
Recall@10 hit, and increased both median and maximum bytes. It therefore failed
the preregistered non-regression rule even though its absolute competitive
quality gates passed. Scientific wall time was 48.37 seconds, peak RSS was
2,210,672 KiB, and swaps were zero. The result shows that query-independent
physical adjacency contains some useful tail signal, but the effect is too
small and the byte direction is wrong. This exact layout family is rejected
without a parameter sweep or 1M rerun. Work returns to mutation/compaction
qualification and no additional representation hypothesis is opened from this
burned evidence.

### V85 100k fragmentation and compaction qualification passes

The registered correctness screen used ReLAION 100k x 768: the first 90,000
rows formed the base, 10,000 rows formed the query-visible delta, and 32 fixed
development queries used exact subset GT@100. It compared one, ten, and one
hundred immutable delta runs and the post-compaction generation. Every arm
returned identical ordered identifiers and identical truth. Aggregate, worst,
and p05 Recall@100 were respectively **99.5312%**, **98%**, and **99%**.

The first scientifically classifiable attempt, source
`c40e49432b64fcd45246e945765088708b6691b1`, preserved receipt SHA-256
`005394a3d32528cf8854a17054b91d3e87eff76d7d7a0d922f22ce5873221368`.
It failed only the physical-amplification gate at 5.768043x. The compactor was
authenticating each immutable run and then reading it again page by page, and
it reread the completed output to hash it. This was physical duplicate work,
not a result-ordering or recall defect.

Source `f96cfd4513df17d138dd6fee3c8d70a94ccb588c` changed compaction to read
each authenticated input once and hash output incrementally. Its sole final
Spot run on `i-0e6cb6b74de103953` completed GREEN and terminated. Immutable
evidence is under
`research/v85-delta-compaction-100k/f96cfd4513df17d138dd6fee3c8d70a94ccb588c/runs/v85-delta-compaction-100k-20260920T110919Z-f96cfd4/a0001/`.
Receipt, summary, screen-time, and terminal SHA-256 values are respectively
`87b7ca3bc15cf6e103b702cb52cae60697b5c99abd6a4cd0c5b82838730c0f37`,
`c81b0a115c42b937936bbb2a95622bcadd5a1f57d59b356cb5817bab17166620`,
`b25b1b6507d1b71de305905d39d07a4240d668f1ea0706a63794e6bfa339b3ca`,
and `7ae3d8d60febd4df12ba848be871f2c534b562230574ee74046077df4190f914`.

| metric | final result | gate |
|---|---:|---:|
| logical live bytes | 7,850,000 | reported |
| physical read bytes | 14,573,739 | reported |
| physical write bytes | 8,476,287 | reported |
| physical amplification | **2.936309x** | at most 5.0x — pass |
| scientific wall | 46.22 s | reported |
| maximum RSS | 1,876,496 KiB | reported |
| swaps | 0 | required — pass |

This closes fragmentation invariance and local compaction amplification at
100k. It is a semantic local-artifact screen, not S3 request, latency, or
throughput evidence. The next fail-fast gate is the registered 1,000-operation
replacement/tombstone trace; larger serving qualification remains fenced until
the validator recomputes that evidence.

### V85 100k replacement and tombstone trace passes

The earlier full-receipt validator derived latest-write ordering only from a
runner-supplied write list. That could detect an internally stale claim, but it
could not prove that the Arrow mutation directory, stale and new physical
rows, and compacted survivors agreed. Source
`55e961a3970aab061056dcce0265c1a19c103e11` added a bounded artifact screen
that requires those concrete witnesses for exactly 500 base-row replacements
and 500 base-row tombstones. The receipt is claim-ineligible and adds no bytes
to serving artifacts.

The sole ReLAION 100k x 768 Spot run used instance
`i-0a62172f265d7ce91`, completed GREEN, and terminated. Immutable evidence is
under
`research/v85-mutation-100k/55e961a3970aab061056dcce0265c1a19c103e11/runs/v85-mutation-100k-20260920T112659Z-55e961a/a0001/`.
Receipt, summary, screen-time, and terminal SHA-256 values are respectively
`b5be0925b06af802d505b557f3a3e272a1bf1b2612aaa972ff5fcdecbb4e9baf`,
`2dbd25af7860dfef9e5e75d79000427dcf9b1fa2131b1de2ea4fdb9c8673fb49`,
`842676532fb5b73d753f51e8721e8efc8c87618a3ba29a297c49a0f59ea679e3`,
and `e7aa77fcb695e3f6cbe85da5226bab8a83102a2f30626532add37385e2958e07`.
The before, mutated, and compacted generation SHA-256 values are
`4db5bce8eda58c4f2098c05fbcaca701b8558effcd1407c9a73f47ca9deb0118`,
`53c9a0239ea497794b6609bf2aa7c29a881acafc560843a36ca76b28be2e0b2b`,
and `149a27cd11b08b5cf3292d424a6cadaf28499f2b6056e3fce4981af0ca345892`.

Independent current-source validation recomputed all 1,000 cases. Every
replacement retained a stale sequence-1 base witness, had a distinct
sequence-2 physical code at the exact directory location, and survived
compaction with that code. Every tombstone retained its stale base witness,
had a null directory location, and was absent after compaction. Scientific
wall time was 5.65 seconds, peak RSS was 1,898,540 KiB, CPU utilization was
506%, and swaps were zero. This closes mutation semantics at 100k; it is not
query latency, S3 request, throughput, or 1M serving evidence.

### V85 fixed SRHT preconditioner loses to identity PQ16

The sole remaining representation hypothesis applied one query-independent
seeded sign plus orthonormal Walsh-Hadamard rotation, zero-padding 768 to 1,024
dimensions, before the same 16-byte PQ encoder. It kept seed 7216, the
2,048-row shortlist, the exact reciprocal-rank planner, all physical work
limits, and all 1,000 already-burned ReLAION 100k development queries fixed.
The frozen identity result was reused byte-for-byte; it was not rerun.

Source `f1bcc1095099c9401df371f36f017f31564e4121` ran once on Spot instance
`i-09be192a172b56e8d`, which terminated. Evidence is under
`research/v85-pq16-srht/f1bcc1095099c9401df371f36f017f31564e4121/runs/v85-pq16-srht-100k-20260920T114220Z-f1bcc10/a0001/`.

| fixed arm | average Recall@10 | average Recall@100 | p05 Recall@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| identity PQ16 | **99.8200%** | **99.1890%** | **95%** | 32 | 14,126,480 |
| sign-Hadamard PQ16 | 99.6600% | 98.9380% | 94% | 32 | **13,646,848** |

The paired 10,000-resample 95% intervals for SRHT minus identity are
**-0.3000 to -0.0400 percentage points** at Recall@10 and **-0.3260 to
-0.1790 points** at Recall@100. SRHT saves at most 479,632 bytes/query but
causes a statistically resolved quality regression, so it is rejected and
does not advance to 1M.

The scientific result SHA-256 is
`3d31eda1f4f192e992dfa55f92881faf1c20fecd75116dba81fd84c2ec3154c7`.
Scientific wall was 49.84 seconds, peak RSS was 2,078,072 KiB, CPU utilization
was 531%, and swaps were zero. The original terminal SHA-256 is
`8dbd9a79921d2b2afb2766dec2b3f2b976b51198ee27dcbda76203c84fc9a549`;
it records exit 99 after science because the reducer used Python 3.10's
`zip(strict=...)` on the Python 3.9 remote runtime. No science was rerun.
Reducer source `842fec13a0c4f9a461b78a1437d97123c31b206c` removed that runtime-only
incompatibility, revalidated every immutable per-query sample, and produced
canonical repaired summary SHA-256
`b298e9d0ba47e63dc46f8ad25f57b211e05b47eb5dea1c3150c046dc97bc9ac6`.

**Ruling:** a random orthogonal preconditioner does not repair the PQ16 row
ranking gap. The 99% aspiration remains useful as a stretch diagnostic, but
the frozen product gate remains the vendor-anchored distributional contract:
96% average Recall@10, 97.5% average Recall@100, and 90% p05 Recall@100 under
32 GETs and 16 MiB. This development result passes those absolute gates but
loses causally to the simpler identity baseline; therefore identity remains
the current 100k reference and no parameter sweep or 1M promotion follows.

### V85 sparse residual PQ improves the 100k fixed arm

The next and only open representation hypothesis retained the 16-byte PQ16
code for every row and selected the query-independent quartile with the largest
base-PQ reconstruction error. Those selected rows received an eight-byte PQ8
residual code and a four-byte combined-reconstruction norm. Queries and truth
did not participate in selection or training. The identity and residual arms
used the same seed-7216 base PQ16, 2,048-row shortlist, reciprocal-rank planner,
32-GET cap, 16-MiB cap, and all 1,000 already-burned ReLAION 100k development
queries.

Source `cc5847b52c8652e1dce75b5225be8958eaab8c58` ran once on Spot
instance `i-0766ab62baf1130d9`, which terminated. Evidence is under
`research/v85-sparse-residual/cc5847b52c8652e1dce75b5225be8958eaab8c58/runs/v85-sparse-residual-100k-20260920T115910Z-cc5847b/a0001/`.

| fixed arm | average Recall@10 | average Recall@100 | p05 / worst Recall@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| identity PQ16 | 99.8200% | 99.1890% | 95% / 80% | 32 | 14,126,480 |
| sparse residual PQ | **99.8600%** | **99.3260%** | **96% / 82%** | 32 | 14,268,568 |

The paired 10,000-resample 95% intervals for sparse residual minus identity
were **+0.0100 to +0.0800 percentage points** at Recall@10 and **+0.0950 to
+0.1820 points** at Recall@100. The challenger therefore passed the fixed
100k promotion rule. The scientific result, summary, timing, and terminal
SHA-256 values are respectively
`ad33152c6bea6318b5b979b70855cc5392df3c5ec102b047f7d359f7e50e8fcf`,
`9b5f953e9ad2e91baf0b8e7b016f166b0481a732307651a164df2a38d1fbf39f`,
`e71b31838924e87f6a70d7e6e9a034cf4b3c16e005cfb074eb5e3856d1a2603d`,
and `c94ef5ba3474837f5f665338b00241c660036c30e1a4dff1b15dcbd711c35b86`.
Scientific wall time was 40.32 seconds, peak RSS was 2,210,904 KiB, CPU
utilization was 906%, and swaps were zero.

The executable 100M worksheet projects 3,012,839,936 resident bytes after
adding the bitmap rank directory, leaving 208,385,536 bytes below 3 GiB. Its
512-MiB runtime reserve is now explicitly partitioned into 256 MiB of sixteen
bounded range-response buffers, 128 MiB of decode scratch, and 128 MiB of
allocator, stack, and runtime reserve. This remains a feasibility projection;
the sparse planner and measured serving residency do not yet exist.

### V85 preregistration — 1M development locality ceiling before validation

The 100k screen is not byte-constrained, whereas the rejected 1M validation
arm used 16,361,264 median bytes against the 16-MiB cap. A second look at the
already-burned validation role is therefore fenced until one fail-fast
development screen establishes causality on the same frozen 256-cell layout.
The screen opens only previously unread development ordinals 456 through 583
and reads neither validation nor holdout.

One c7i.8xlarge Spot process trains the base PQ16 once and evaluates exactly
three paired 2,048-row-ranking arms through the identical 32-GET/16-MiB
reciprocal-rank planner: identity PQ16, the fixed 25% sparse residual arm, and
exact-f32 row scoring. It records SHA-256 identities for the base books, base
codes, and both immutable page runs. An independent reducer recomputes every
arm's per-query hits, aggregates, physical maxima, paired truth, and causal
classification.

The decision order is fixed. If exact-f32 fails 96% average Recall@10, 97.5%
average Recall@100, or 90% p05 Recall@100, the layout/locality policy is
rejected and no quantizer promotion follows. If exact-f32 passes but sparse
residual fails those gates or regresses identity average or p05 Recall@100,
the residual arm is rejected. Only if both checks pass may the already-frozen
claim-ineligible validation rerun proceed without retuning. No native-S3,
10M, 100M, or sealed-holdout work follows directly from this screen.

### V85 1M development ceiling rejects sparse residual scoring

The first attempt used source
`7a6dca3847b6dd5c051de1da91c560abf9ed3a4d` on Spot instance
`i-0e0bb0a096e8e5ca9`. It authenticated all downloaded bytes but stopped in
six seconds before emitting science because the development query and truth
roles use the V36 `embedding` and long-form ranked schemas, while the new
runner had selected the validation-role `vector` and nested-neighbor readers.
Its terminal SHA-256 is
`d87e6be246ff7e5c56d97a88aeb0c23350103c71ab4029187f458f26650ee6de`.
This was a harness failure, not quality evidence. Source
`03a64eb79cb4a25797425d6615214cbcff4c297f` added strict explicit readers for
the two authenticated role formats and preserved stderr.

The one scientific attempt used source
`03a64eb79cb4a25797425d6615214cbcff4c297f` on c7i.8xlarge Spot instance
`i-042c2d37f20804056`. It opened only development ordinals 456 through 583,
published the complete canonical three-arm result, and terminated. Evidence is
under
`research/v85-sparse-residual-development/03a64eb79cb4a25797425d6615214cbcff4c297f/runs/v85-sparse-residual-development-20260920T122501Z-03a64eb/a0001/`.

| fixed arm | average Recall@10 | average Recall@100 | p05 / worst Recall@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| identity PQ16 | 97.3437% | 94.0312% | 70% / 52% | 32 | 16,727,848 |
| sparse residual PQ | 97.8125% | 94.3750% | 70% / 52% | 32 | 16,727,848 |
| exact-f32 row ceiling | **100.0000%** | **99.0781%** | **94% / 79%** | 32 | 16,734,128 |

Exact-f32 passed every absolute gate on the identical frozen layout and
planner. Page locality is therefore sufficient for this split. Sparse
residual improved average Recall@10 by 0.4688 points and Recall@100 by 0.3438
points but left the p05 and worst-query tail unchanged and failed the 97.5%
average Recall@100 and 90% p05 gates. The preregistered classification is
`sparse-residual-rejected`; the burned validation rerun remains fenced. The
next representation work must improve compressed row-score fidelity rather
than spend more page bytes or change the accepted locality policy.

The scientific result SHA-256 is
`bfdc4b71e2d855e80e0e4c538053ea15ecbe9f728f4bb99b3dd44fb41edac981`.
Science completed in 1:42.70 at 10,865,224 KiB peak RSS, 704% aggregate CPU,
and zero swaps. The attempt then exited 99 in validation because the remote
Python 3.9 runtime rejected `zip(strict=...)`; its terminal, timing, and
validator-log SHA-256 values are respectively
`164480d25b0513db127e0ce25982e511a1c45254a78ac911b5c2b3085459bbc8`,
`0ce56e365bde07348da6e83517e64f09e314f3c26c886dfed0557ee76e70eacc`,
and `6aedfd1b510b118d958490c96f46552bf5688dc790443d557506de87066b833b`.
No science was rerun. Validator source
`eb7d1c47e168ddb0a4345b5e3b8996d438fe9fd0` added a Python-3.9 regression
test and independently recomputed the immutable result. The repaired canonical
summary SHA-256 is
`1908850070120f7e7a1dd5267abd779678a8b6719197d662411a061aa12a6f54`.

### V85 all-development rescore confirms the compressed-score bottleneck

The 128-query slice was too small to resolve a few-hit representation effect.
Source `edf92ccffe7d669533d396848ff1652883a6dd3d` therefore repeated the exact
same frozen three-arm comparison on all 1,000 already-burned ReLAION 1M
development queries. It changed no representation, training seed, shortlist,
layout, page planner, or physical budget. The independent reducer authenticated
all per-query truth and hit identities and recomputed deterministic paired
10,000-resample confidence intervals.

The original Spot process ran on `i-00f89b2db75d1a31a` in `eu-central-1a`,
completed with exit zero, and terminated. Evidence is under
`research/v85-sparse-residual-development/edf92ccffe7d669533d396848ff1652883a6dd3d/v85-development-all1000-20260920T123800Z-edf92cc/a0001/`.

| fixed arm | average Recall@10 | average Recall@100 | p05 Recall@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| identity PQ16 | 97.8800% | 94.4680% | 75% | 32 | 16,734,128 |
| sparse residual PQ | 98.0900% | 94.7750% | 76% | 32 | 16,734,128 |
| exact-f32 row ceiling | **100.0000%** | **99.3950%** | **96%** | 32 | 16,734,128 |

The paired 95% intervals for sparse residual minus identity are **+0.0800 to
+0.3400 percentage points** at Recall@10 and **+0.2280 to +0.3940 points** at
Recall@100. Sparse residual is therefore a real improvement, but it remains
2.725 points below the 97.5% Recall@100 gate and 14 points below the 90% p05
gate. Exact-f32 exceeds both gates under the identical physical work limit;
its paired intervals over identity are **+1.7500 to +2.5200 points** at
Recall@10 and **+4.4690 to +5.3900 points** at Recall@100. The registered
classification remains `sparse-residual-rejected`. This closes the sampling
ambiguity without reopening validation or introducing another representation.

The canonical result, summary, timing, and terminal SHA-256 values are
respectively
`1a4674b0f8749fc9ea23c2d64c9075c589007cc0ec937e94ac332a912555b569`,
`4ec59cf9a25570804165f53ac776990d3374b917119dd3f01f40dcbc10b9bf7c`,
`4b8f9da31e6da9144c98b1784431ea237be7104e7a6945bc2663f52c5a35ecf3`,
and `e8914bdc0008e7c2fdee2e2bee58d9f586f5ae34edd76a91ef8b61e738fa3279`.
Scientific wall time was 8:15.42, peak RSS was 11,132,400 KiB, aggregate CPU
was 477%, and swaps were zero. All local evidence and source scratch was
explicitly removed after independent current-source validation.

## V97 — the full-development width screen rejects the common summary fence

V97 compared the five registered resident representations on all 1,000
ReLAION-1M development queries under one query-independent two-summary,
128-page candidate fence and the fixed 32-GET/16-MiB serving budget. The raw
producer retained every per-query truth, hit, page and byte identity. A
separate reducer authenticated the 32,449,663-byte result and independently
recomputed every sample, aggregate, 10,000-draw paired interval, gate and
100M memory worksheet.

The successful source was
`6d413d367e38a6ddb7106537bd26233b526c1ee7` on c7i.8xlarge Spot instance
`i-0a0ff2dc21aee92d4` in `eu-central-1c`; the instance terminated. Evidence is
under
`research/v97-row-width-screen/6d413d367e38a6ddb7106537bd26233b526c1ee7/runs/v97-g1-20260920T162741Z-6d413d3/attempt-0001/`.

| representation | avg R@10 | avg R@100 | p05 / worst R@100 | max GETs | max bytes | projected 100M resident bytes |
|---|---:|---:|---:|---:|---:|---:|
| exact f32 behind the same fence | **92.270%** | **82.193%** | **51% / 7%** | 32 | 6,366,152 | n/a |
| PQ16x8 | 82.720% | 71.888% | 33% / 4% | 32 | 6,401,472 | 2,861,603,104 (pass) |
| PQ24x8 | 86.490% | 74.077% | 38% / 6% | 32 | 6,274,304 | 3,661,603,104 (fail) |
| PQ32x8 | **88.580%** | **75.515%** | **38% / 6%** | 32 | 6,272,736 | 4,461,603,104 (fail) |
| PQ32x4 | 81.940% | 69.395% | 31% / 5% | 32 | 6,451,712 | 2,860,865,824 (pass) |
| summary-only PQ16x8 | 65.860% | 59.037% | 13% / 0% | 32 | **5,059,152** | **1,260,816,672 (pass)** |

Against PQ16, the paired 95% average-R@100 intervals in percentage points
were PQ24 **+1.885 to +2.497**, PQ32 **+3.305 to +3.957**, PQ32x4
**-2.972 to -2.036**, and summary-only **-13.685 to -12.005**. The width
effects are therefore resolved, but none is eligible: every representation
missed the absolute 96% average R@10, 97.5% average R@100 and 90% p05 R@100
contract. Exact-f32 also failed far below the gate, so quantizer width is not
causal at this boundary. The common summary fence is rejected and `winner` is
null. No arm advances to G2, and no 10M/100M spend follows.

The canonical result, independent rescore, resources and terminal SHA-256
values are respectively
`e5cfd9bdcf523989f8217f626626690a26f934e1b5d63c08129c176d8c12ee10`,
`6e72211b5efebe2b567f5f8de4ea6c3820a06a2c59b799de871cf5144e4218af`,
`be6adac9152a793ed5048d10ad207b94a273cf87539a974da13bb5bf25b394e9`,
and `a3b85df8f5d2a8676e4e67d9d7b37a371b626a27537839dadbef2ca8f2bf060c`.
Screen and rescore wall times were 2:33.36 and 8.53 seconds. Whole-cell wall
was 195 seconds, peak process RSS was 11,195,404 KiB, PSI full remained zero,
and swaps were zero. At the observed eu-central-1c Spot rate of $0.692/hour,
the successful-cell compute estimate is $0.0375. Two separately preserved
pre-science/harness failures and one one-second receipt failure bring the
estimated total G1 compute exposure to about $0.096; none produced quality
evidence.

**Ruling:** retain the fixed physical budget and the 100M `<3 GiB` worksheet,
but pause row-width selection. The next and only representation hypothesis is
a bounded hierarchical router that must restore the exact-f32 ceiling on the
same 1,000-query development evidence before compressed widths are rescored.
This is internal research evidence only; no matched S3 Vector or TurboPuffer
measurement exists at G1, so no competitive parity claim is made.

## V98 preregistration — bounded hierarchy before row-width selection

V98 is one fail-fast ReLAION-1M development attempt. It uses all 1,000 frozen
development queries and exact top-100 truth; it reads no validation or sealed
holdout query. Source `1e9ea0935a4f90dd1b3ac9d2f52a80e5e96f0fd8` is archived
at
`s3://borsuk-bench-453182569524-euc1/research/v98-hierarchical-row-router/1e9ea0935a4f90dd1b3ac9d2f52a80e5e96f0fd8/source/source.tar.gz`
with SHA-256 `b4b4774ade03086f5a7e83f759186fd49000be77107f9e9ec476687b07fc8849`
and 10,674,746 bytes. The sole attempt prefix is
`research/v98-hierarchical-row-router/1e9ea0935a4f90dd1b3ac9d2f52a80e5e96f0fd8/runs/v98-g1-20260920T175131Z-1e9ea09/a0001/`.
The preregistered dual-critique result is
`edcde2149f98bc38c5121b387b165fe9baf0ca6df79658ac624abf86866bcb87`.
An earlier local direct-script preflight of source
`c73d7d6b9c7e900cded890055b5875808957e33c` failed at import resolution before
any AWS API call, reservation, instance, or spend. Its uploaded source archive
is unused; source `8ec42e96373f84664c6725592a9b2869e622cbcc` adds the exact
subprocess regression and the established direct-execution import boundary.
That source then atomically reserved its prefix, but EC2 rejected
`RunInstances` before creating an instance because decoded user data exceeded
the 16,384-byte service limit. It produced no science or spend and is not
reused. Source `7b6baa80d8e7670d66a4e1b1cb7743c98416c17c` adds a tested
size-bounded user-data bootstrap; the full reviewed runner remains
authenticated inside its exact source archive. That source reserved its prefix
and launched Spot instance `i-0fa45996f014d5479` in `eu-central-1c`, but the
post-launch create-only receipt failed before science because the reservation's
temporary signing hook was not unregistered and duplicated the next PUT's
signed headers. The safety boundary immediately terminated the instance; no
scientific artifact was produced and its prefix is not reused. Source
`1e9ea0935a4f90dd1b3ac9d2f52a80e5e96f0fd8` fixes the botocore unregister
call and adds a two-PUT regression proving each conditional-signing hook is
removed before the next receipt.

The six immutable input identities are:

| role | bytes | SHA-256 | URI |
|---|---:|---|---|
| source | 1,458,450,077 | `2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86` | `s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet` |
| queries | 1,558,506 | `310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54` | `s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet` |
| truth | 2,046,505 | `fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11` | `s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet` |
| generation | 416,563 | `45fa4e708ab660151a7b1ea79e35eada6090faced1bfb147f7e20cac7055e754` | `s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/generation.json` |
| base | 708,888,104 | `c2e86f6199777c21dde048ff4a46aae65ce96a4b20f03c11905851c80312b862` | `s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/base-000.arrow` |
| delta | 80,900,000 | `a0498ff17acbe8cc81a8b2501e7ffcd0d6998c5379efaae5895c64d3bcc7e869` | `s3://borsuk-bench-453182569524-euc1/research/v85-pq16-1m-validation/da42b3da7a78a7a44ab259bb06f12c42527e8800/runs/v85-1m-validation-20260920T100638Z-da42b3d/a0001/artifacts/delta-000.arrow` |

The query-independent hierarchy fixes eight same-role pages per root, at most
65,536 root groups, 4,096 exposed pages, 1,024 retained pages, 262,144 scored
rows, and a 2,048-row shortlist. Every query remains capped at 32 physical
GETs and 16,777,216 encoded bytes. Containment and then exact-f32 must each
pass 96.0000% average Recall@10, 97.5000% average Recall@100, and 90% p05
Recall@100 before PQ16, PQ24, PQ32, PQ32x4, and the summary-only control run.
Paired decisions use seed 7216, exactly 10,000 shared bootstrap rows, and the
independently reproduced matrix identity. Terminal classes are
`hierarchy-containment-rejected`, `hierarchy-exact-ceiling-rejected`, or
`widths-evaluated`; a width also requires a complete 100M projection below
3 GiB and a paired average-Recall@100 interval not inferior to PQ16.

The worker is one c7i.8xlarge Spot instance using available x86_64 AMI
`ami-06121aa3085b6f918`, tried serially in eu-central-1c, 1b, then 1a. The
2026-09-20 price snapshots were $0.6911, $0.7781, and $0.7224 per hour;
$0.7781/hour is the conservative registered estimate. Science has a 7,200
second wall cap and a 48-GiB process virtual-memory cap. The worker stops after
three consecutive five-second memory-PSI `full avg10 > 0.50` observations or
more than 1,048,576 KiB of swap growth. The launcher allows 900 seconds for a
terminal after the science cap, then terminates the exact instance in every
terminal or error path. The maximum registered compute plus 100-GiB gp3
exposure is $1.80. The worker uploads immutable result, rescore, resources,
logs, and then terminal; incomplete result bytes are never inspected.
Named local scratch is removed after independent authentication. Duplicates,
automatic restarts, 10M/100M work, production changes, and competitive parity
claims are fenced until this attempt is terminal and independently verified.

### V98 result — exact hierarchy ceiling rejected

The sole registered attempt completed on Spot instance
`i-06dae1df91b93028f` in `eu-central-1c`. It evaluated all 1,000 frozen
ReLAION-1M development queries against exact top-100 truth. The canonical
terminal is 1,516 bytes with SHA-256
`f173a3b1f726f23340bbbf510b3222cb8fec01525a0991ac127741827d10173d`;
it records exit code 0, status `complete`, and `claim_eligible=true`. The
terminal-bound evidence is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 52,466,885 | `f8d6382fdfe84b5708cbfeaeae526e2b651c9a5f9d408e74b14c196b632de6fe` |
| rescore | 790 | `740d6f475e21850e768398c2d7a661d3030815f1b2c2999fe8b49d4e7066e56c` |
| resources | 457 | `b313a9c89f3150eba9a021b4a4217ff981fcd545f7c2ad7ec078815bc78ce87c` |
| worker log | 11,376 | `b7df5611795f650422fd11cde99a11205a3de894f14e511e7612a09adb496d7b` |

Current-source hostile recomputation authenticated the result and produced
rescore bytes exactly equal to the terminal-bound rescore object. The shared
10,000-row bootstrap matrix has SHA-256
`8146b2c781e73f07293b293e11b31e6d923c3d456491a417d0ec847ac4ec8d28`.
The hierarchy contains 7,278 pages and 910 eight-page roots; its 574,328-byte
Arrow evidence has SHA-256
`5e47350fcb36d9696500429e454fc35dce0e11909ca5ad8de81e9bd2243769e8`.

| stage | avg R@10 | avg R@100 | p05 R@100 | max GETs | max bytes | quality | resource |
|---|---:|---:|---:|---:|---:|---|---|
| hierarchy containment | 98.8600% | 98.5180% | 92.0000% | n/a | n/a | pass | pass |
| exact-f32 ceiling | 98.8600% | 83.3020% | 54.0000% | 32 | 6,427,384 | **fail** | pass |

Both stages evaluated at most 910 roots, 4,096 pages, and 142,841 rows per
query. The exact ceiling misses the 97.5000% average and 90% p05 Recall@100
gates by 14.1980 and 36.0000 percentage points. Fail-fast therefore stopped
before PQ16, PQ24, PQ32, PQ32x4, or summary-only evaluation; there is no width
winner, paired width interval, or applicable 100M width worksheet. The
classification is `hierarchy-exact-ceiling-rejected`. This localizes the
failure after hierarchy containment: the registered 2,048-row shortlist and
eight-page selection cannot preserve broad top-100 coverage even though the
exposed hierarchy contains it. G2, 10M, 100M, and parity claims remain fenced.

The worker ran for 271 seconds, reported a maximum process RSS of 613,620 KiB,
zero swap at start and finish, memory PSI `full avg10=0.00` at start and
finish, and an upper-bound Spot charge of 58,574 microdollars at the registered
$0.7781/hour price. A worker-log audit found the continuous PSI comparison was
not operational: awk parsed the unparenthesized ternary after `print` as output
redirection. This does not alter the deterministic negative quality decision;
RSS, endpoint PSI, and swap were far below their stops, and no latency or
throughput claim is made. Commit `da5f5410c8018ec6c118ee5687d50c3ed40ba931`
adds a failing-then-passing executable awk regression and the parenthesized
comparison. The instance is terminated, all named authentication scratch was
removed, and no retry is authorized or needed for this rejected architecture.

An authenticated post-terminal oracle explains the failure more precisely.
With each 256-row page charged as one GET, even a truth-aware selector can
recover at most 85.9710% average Recall@100 and 61% p05 within 32 pages: the
top 100 neighbors occupy 43.337 distinct pages on average. The current exact
planner is therefore only 2.6690 percentage points below that impossible
single-page-GET ceiling. This rules out shortlist or scoring tweaks on the same
physical planning unit.

The same immutable result and authenticated generation manifest support one
different physical oracle. Consecutive pages in each object are contiguous byte
ranges, so up to 32 adjacent intervals can remain 32 S3 range GETs. Greedily
merging the cheapest same-object gaps covers every retained truth page within
both caps for 983/1,000 queries; required bytes are 6,876,464 at p50,
13,226,760 at p95, and 17,869,232 at p99. For the 17 over-budget queries, a
feasible truth-aware interval drop by reward-per-byte yields 98.7500% average
Recall@10, 98.3720% average Recall@100, and 91% p05 Recall@100 with at most 32
GETs and 16,740,984 bytes. This is an oracle, not a serving algorithm: it uses
truth labels only to prove that the current clustered physical order can meet
the gates when adjacent pages share a GET.

The next and only G1 representation hypothesis is therefore an authenticated,
query-blind adjacent-range planner. Exact row ranks choose deterministic
same-object page ranges under the unchanged 32-GET/16-MiB caps; only if its
exact ceiling passes on all 1,000 queries may PQ width selection and the full
100M `<3 GiB` worksheet resume. S3 Vector and TurboPuffer are not measured in
this cell, so no matched or published-number parity claim is made.

## V99 result — prototype timed out before scientific evidence

V99 source `6082926d035580f3833c363dcf846c9a2e1273c2` is archived at
`s3://borsuk-bench-453182569524-euc1/research/v99-ranked-gap-range-router/6082926d035580f3833c363dcf846c9a2e1273c2/source/source.tar.gz`
with SHA-256
`cdd9a919e51e4b89ce0aa41c12d87b200bee616aa3d4b3ec1356dcf63374803d`
and 10,696,645 bytes. The sole attempt prefix is
`research/v99-ranked-gap-range-router/6082926d035580f3833c363dcf846c9a2e1273c2/runs/v99-g1-20260920T183715Z-6082926/a0001/`.
It reused the six exact V98 input identities and the registered critique
SHA-256, but increased the deterministic shortlist from 2,048 to 8,192 rows
while preserving 1,000 ReLAION-1M development queries, exact top-100 truth,
32 GETs, 16,777,216 bytes, and all hierarchy caps.

The one-time `c7i.8xlarge` Spot instance `i-06baa9b6fffdd0ce3` ran in
`eu-central-1c` and is terminated. Its 350-byte terminal has SHA-256
`60e6b1180c5598e63e8493e3a18ac945eaf158143288090d7e9a99dafa745b0d`,
status `failed`, exit code 97, `claim_eligible=false`, and no evidence map.
The producer was stopped by its exact 7,200-second timeout before writing a
result. `/usr/bin/time` independently records exit status 124, 7,207.41 user
seconds, 27.88 system seconds, 100% CPU, 2:00:00 wall, maximum RSS 11,136,608
KiB, one major fault, and zero swaps; that receipt is 810 bytes with SHA-256
`b91d278adc470d4783fe160a3f153e972e54fbd1c5debf9b8f22eb6fdb437d19`.
All seven source/input hashes passed before science. The 481-byte worker log
has SHA-256
`081375013048e166e879691a565b382002737a94e56a7d693aa8b1a51ea8b4c6`.
Cleanup resource serialization also failed after the producer timeout, so no
canonical resources receipt or exact spend was emitted; the compute exposure
is bounded by the registered Spot price and wall envelope, and no result,
recall, width, latency, throughput, or parity claim exists.

**Ruling:** this is a prototype-performance failure, not evidence against the
ranked-gap representation. The scalar Python scorer maintains an 8,192-entry
heap one row at a time and repeats that work across exact and width arms, using
only one of 32 vCPUs. Before any new full cell, replace per-row heap traffic
with bounded deterministic NumPy block selection, prove byte-for-byte
`(distance,row_id)` equality on ties and adversarial inputs, and require a
small fixed-query performance preflight. A new immutable revision may run one
fresh 1,000-query attempt only after that preflight demonstrates sufficient
headroom under the same 7,200-second cap. No same-revision retry, 10M/100M
work, G2 promotion, or competitor comparison is permitted from V99.

## V99 repaired producer result — exact ceiling passes; compact arms fail G1

The performance-repaired source is
`b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0`. Its 10,698,435-byte source
archive is at
`s3://borsuk-bench-453182569524-euc1/research/v99-ranked-gap-range-router/b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0/source/source.tar.gz`
with SHA-256
`8cea900dd3bbb617a4e36e538aac59108c40509ed559bc266f76077135e26ffd`.
The sole attempt prefix is
`research/v99-ranked-gap-range-router/b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0/runs/v99-g1-20260920T205800Z-b3db2b2/a0001/`.
It reused the six registered ReLAION-1M development identities, all of which
authenticated before science, and evaluated all 1,000 development queries
against exact top-100 truth under 32 GETs, 16,777,216 bytes, 1,024 retained
pages, 262,144 scanned rows, and an 8,192-row shortlist.

The fixed synthetic preflight passed before the full evaluation: 262,144 rows
and an 8,192-row shortlist took 0.076031 seconds for exact scoring, 0.060969
seconds for PQ16 scoring, and 0.286084 seconds for range planning, each below
the registered 5-second stop. The producer then completed in 1:58:23 with
exit status 0, 7,574.16 user seconds, 1,134.68 system seconds, 122% CPU,
maximum RSS 11,107,352 KiB, one major fault, and zero swaps. The canonical
129,153,048-byte result has SHA-256
`a122e673dd005c8ec40e311aa3fe32391d8506db2ac732401bd090fcb863bad6`.
The instance `i-07ab13e2dbbf903fd` was a `c7i.8xlarge` Spot instance in
`eu-central-1c` and is terminated. Memory PSI remained zero, swap remained
zero, and the registered resource receipt estimates $0.952267 of Spot spend.

The source reducer rejected the completed result because it incorrectly
required every interior page read by a merged adjacent range to be one of the
hierarchy-retained pages. Ranked-gap serving deliberately reads authenticated
contiguous gap pages between retained endpoints. The terminal therefore
remains immutable `failed`, exit 98, and `claim_eligible=false`; its 350 bytes
have SHA-256
`4bea9d8098f0cd8bef096252df5589dd25d9c13011bac66a53f66c00bc9f9d54`.
The same cleanup also compared decimal RSS fields lexicographically and wrote
the reducer RSS rather than the producer maximum into `resources.json`.
Reducer/receipt repair `ac200754164f07bb61976ba4f9d91bc570e028bb`
requires retained range endpoints while permitting authenticated interior
gaps and compares RSS numerically. It does not modify the producer result.

The repaired independent recomputation is stored separately at
`s3://borsuk-bench-453182569524-euc1/research/v99-ranked-gap-range-router/b3db2b2d060eeb56a3890ab9607cdaea85bfa7e0/runs/v99-g1-20260920T205800Z-b3db2b2/a0001/posthoc/ac200754164f07bb61976ba4f9d91bc570e028bb/rescore.json`.
It is 3,164 bytes with SHA-256
`76d9f55378e790e4cc7bca8c2583da5ed81641dd17e7d3999bb4d68b95d846a9`,
binds the original result SHA-256 in object metadata, recomputes all 1,000
samples with the registered 10,000-resample matrix, and used 1,395,480 KiB
maximum local RSS with zero swaps.

The hierarchy containment ceiling passes at 98.8600% average Recall@10,
98.5180% average Recall@100, and 92% p05 Recall@100. The exact-vector
ranked-gap ceiling also passes at 98.8700%, 98.4240%, and 91%, respectively,
with 32 maximum GETs and 16,777,200 maximum bytes. This proves that the
hierarchy and adjacent-range physical representation can satisfy the G1
quality/resource gates.

No compact arm passes all quality gates:

| Arm | avg R@10 | avg R@100 | p05 R@100 | max GETs | max bytes | 100M resident projection |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| PQ16x8 | 96.3100% | 91.9180% | 66% | 32 | 16,777,216 | 2,869,504,702 B |
| PQ24x8 | 97.3900% | 93.2660% | 71% | 32 | 16,777,216 | 3,669,504,702 B |
| PQ32x8 | 97.9500% | 94.2420% | 75% | 32 | 16,777,216 | 4,469,504,702 B |
| PQ32x4 | 94.7600% | 88.9840% | 59% | 32 | 16,777,208 | 2,868,767,422 B |
| summary-only PQ16x8 | 93.4000% | 89.4150% | 57% | 32 | 16,777,216 | 1,268,718,270 B |

PQ24x8 and PQ32x8 improve significantly over PQ16x8 under paired bootstrap,
but exceed the 100M `<3 GiB` resident limit and still miss average and tail
Recall@100. PQ32x4 and summary-only are significantly worse than PQ16x8. The
classification is `widths-evaluated`, `winner=null`, and every arm is
ineligible. These are paired ReLAION-1M development measurements, not S3
Vector or TurboPuffer results; no competitor parity claim is made.

**Ruling:** G1 rejects plain PQ16/PQ24/PQ32/PQ32x4 and summary-only ranking
for this hierarchy. The physical hierarchy/range layout is retained because
its exact ceiling passes. Do not scale to 10M/100M or weaken the registered
quality gates. The next representation hypothesis must improve compact
ranking at a resident cost compatible with `<3 GiB`; it must first beat PQ16
on the same immutable 1,000-query paired harness and retain the exact
32-GET/16-MiB physical contract.

## V100 result — page-relative residual PQ improves PQ16 but fails G1

V100 tested the single registered follow-up representation: one query-blind
16-byte residual PQ code per row, trained around page means decoded from the
authenticated V98 hierarchy summaries, followed by V99's unchanged adjacent-
range planner. Source commit
`b7630390f343320036f04fa013a48d0f4e5294f0` is archived at
`s3://borsuk-bench-453182569524-euc1/research/v100-page-residual-range/b7630390f343320036f04fa013a48d0f4e5294f0/source/source.tar.gz`;
the archive is 10,729,710 bytes with SHA-256
`795a768a88936f982c2ec28ea6208c103dea646f839bbf4a23c4034e2ff573e6`.
The sole attempt used all 1,000 frozen ReLAION-1M development queries, exact
top-100 truth, 32 GETs, 16,777,216 bytes, the same hierarchy caps, and a
matched V99 PQ16 control. No validation or holdout queries were consumed.

The `c7i.8xlarge` Spot instance `i-0b6ffb451324fa4c3` ran in
`eu-central-1c` and is terminated. Its 1,507-byte canonical terminal has
SHA-256
`ff339a048b22ce04feee3d9876d6750361d0ee161e198f1216d4055c24baaae9`,
status `complete`, exit code 0, and `claim_eligible=true`. Terminal-bound
evidence is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 27,795,092 | `bb00194d057ddce193341fd6e754b0b1551834eaea672c8e41a074126ac97fe0` |
| rescore | 819 | `6fd83b4c4bb597a8e0fd1650524fdd5c3fe535ead1c9e8adc2b43b03d0a4362e` |
| resources | 461 | `7c273869c99459c6214243f4a4be645c24662a3855bd0673ad1d84d46cb3a9f4` |
| worker log | 352 | `e45363bdf19e957326291503284a46011142a90829094bb44f7903b5f743f241` |

The independent reducer authenticated the result and recomputed every query's
truth-page membership, selected-range union, recall, aggregates, shared
10,000-resample paired intervals, memory projection, and final
classification. Its decision is:

| arm | avg R@10 | avg R@100 | p05 R@100 | max GETs | max bytes | quality | resource |
|---|---:|---:|---:|---:|---:|---|---|
| matched V99 PQ16 | 96.3100% | 91.9180% | 66% | 32 | 16,777,216 | fail | pass |
| V100 page-relative residual PQ16 | 97.1400% | 92.8780% | 69% | 32 | 16,777,216 | **fail** | pass |

The paired 95% interval for V100 minus PQ16 is +0.5000 to +1.1700 percentage
points at Recall@10, +0.7220 to +1.2020 points at Recall@100, and 0 to +5
points at p05 Recall@100. The improvement is real but far below the absolute
97.5000% average and 90% p05 Recall@100 gates. Its conservative 100M resident
projection is 2,872,650,430 bytes, including bounded decoded-mean scratch and
zero resident dense page means, so memory passes. Classification is
`page-residual-rejected`.

The fixed preflight passed at 262,144 rows and an 8,192-row shortlist: exact
scoring took 0.074349 seconds, PQ16 scoring 0.061057 seconds, and range planning
0.286555 seconds. The full producer took 26:31.51 wall, 2,194.02 user seconds,
1,960.77 system seconds, 261% CPU, and 13,073,008 KiB maximum RSS. Independent
reduction took 1.37 seconds and 342,468 KiB maximum RSS. Swap and memory PSI
were zero throughout; the registered resource receipt estimates $0.216800 of
Spot spend. All named local scratch was removed.

**Ruling:** reject this V100 representation without retry or parameter tuning.
It proves that page-relative residual evidence improves compact ranking, but
not nearly enough under the fixed 32-GET/16-MiB physical budget. G1 still has
no eligible compact representation; 10M/100M, G2 promotion, and competitor
parity claims remain fenced. Do not reinterpret this as failure of the exact
hierarchy/range layout, whose V99 exact ceiling remains above every quality
gate.

## V101 result — two-stage residual PQ improves flat PQ16 but fails fast

V101 tested one same-width alternative to flat PQ16: two query-blind PQ8
stages over each row, where the second stage quantizes the first stage's
residual. Serving evaluates the exact reconstructed squared distance with
two 8-byte code planes and resident `2*c1·c2` cross-term tables. This keeps the
row representation at 16 bytes and changes neither V98 hierarchy exposure nor
V99 adjacent-range selection.

The immutable source revision was
`9ea8cec9299e5f2d67e832d6245c4b28cb7534e4`. Its source archive was
`s3://borsuk-bench-453182569524-euc1/research/v101-two-stage-residual-pq/9ea8cec9299e5f2d67e832d6245c4b28cb7534e4/source/source.tar.gz`, SHA-256
`cf760cee4484eee6767ba67fab72781054bd115b0eb25adce03bf3ec4069f51d`,
10,733,490 bytes. The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v101-two-stage-residual-pq/9ea8cec9299e5f2d67e832d6245c4b28cb7534e4/runs/v101-g1-screen-20260921T001446Z-9ea8cec/a0001`.
It used c7i.8xlarge Spot instance `i-0b8ac9facd94241db` in eu-central-1c,
terminated after its claim-eligible terminal. The terminal is 1,542 bytes,
SHA-256 `645441283cd1a08972734c02e47ab7026210d6e682baef2fb99ee1ccb0592570`.

The fail-fast cohort was the fixed first 128 ordinals of the already-burned
ReLAION-1M development query/truth objects; all six complete frozen input
objects were authenticated before slicing. No validation or holdout query was
opened. Exact evidence identities are:

- result: 3,535,908 bytes, SHA-256
  `5862c940910e6dffc015702824a72738fd81f2ed47da1b62b2007cfcf077e3d1`;
- independent rescore: 841 bytes, SHA-256
  `5f10869ce49f73faeeb970a0adabdcf8f7ca3b6f441f232a4c5c91a91bbedb2e`;
- resources: 459 bytes, SHA-256
  `eedcfc70a44cbe423df313c86fea5a9020eb12775870ada5b4ffccfcf64763b0`;
- worker log: 352 bytes, SHA-256
  `661ee4c40c226f1b7de2fd28807865d19df968c533f829cddb1b0c4c1460e3be`;
- query-independent two-stage artifact: SHA-256
  `cb3976b6e95dc34d1e55eb73d5de0c4561a340b52b6e1d9f876c534d2b464815`.

| 128-query arm | avg Recall@10 | avg Recall@100 | p05 Recall@100 | max GETs | max bytes | quality | resources |
|---|---:|---:|---:|---:|---:|---|---|
| matched V99 flat PQ16 | 96.4843% | 91.3203% | 65% | 32 | 16,777,216 | fail | pass |
| V101 two-stage residual PQ8+PQ8 | **97.3437%** | **92.2109%** | **70%** | 32 | 16,777,208 | **fail** | pass |

The paired 10,000-resample 95% interval for V101 minus flat PQ16 is -0.1562
to +2.1094 percentage points at Recall@10, +0.2031 to +1.6016 at
Recall@100, and -3 to +10 at p05 Recall@100. The independent reducer exactly
recomputed every sample, aggregate, interval, projection, and the
`two-stage-residual-pq-rejected` classification.

The complete 100M resident projection is 2,872,388,286 bytes, including the
786,432-byte second codebooks and 2,097,152-byte cross-term tables, so the
memory gate passes. The attempt took 363 seconds, reached 11,161,764 KiB peak
RSS, had zero swap and zero memory PSI, and cost an estimated $0.048400.

**Ruling:** reject V101 without a 1,000-query continuation. The positive paired
Recall@100 delta confirms that flat PQ16's wide subspaces lose ranking
information, but two-stage additive quantization recovers less than one point
on this screen and remains more than five points below the 97.5% gate. It is
not close enough for parameter tuning. G1 still has no eligible compact row
representation; G2, real-S3 promotion, 10M, and 100M remain fenced.

## V102 result — bounded S3 PQ48 refinement is feasible but misses quality

V102 tested whether moving a wider row-ranking representation out of resident
RAM and into one bounded object-storage refinement wave could close V99's
compressed-ranking gap. It preserved the immutable V99 hierarchy and final
ranked-gap page selection. For each query it required every one of the
hierarchy-retained PQ48 code pages to fit a first wave of at most 32 range GETs
and 16 MiB, scored those codes, and then retained the existing final page-data
wave of at most 32 range GETs and 16 MiB. The two waves are reported separately;
V102 is not represented as satisfying the original one-wave G1 budget.

The immutable source revision was
`2c560a8e70c4c5cbc55d1e6b6eebdad7a070a2da`. Its 10,753,340-byte source
archive is at
`s3://borsuk-bench-453182569524-euc1/research/v102-two-wave-pq48/2c560a8e70c4c5cbc55d1e6b6eebdad7a070a2da/source/source.tar.gz`, SHA-256
`c0ef1a85e1ef108c144807bd1b3588884d9fbe60ad5adc0945d8b3b1b1097bdc`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v102-two-wave-pq48/2c560a8e70c4c5cbc55d1e6b6eebdad7a070a2da/runs/v102-g1-screen-20260921T003800Z-2c560a8/a0001`.
It used c7i.8xlarge Spot instance `i-0895e3e26faec590a` in eu-central-1c,
which terminated after its claim-eligible terminal. The 1,511-byte terminal has
SHA-256 `b4b624512e9faa88bb9b5032272ab1e280e15931c847ba721959ef634e7498a7`.

The fixed fail-fast cohort was again the first 128 already-burned ReLAION-1M
development queries, with all complete frozen inputs authenticated before
slicing. The exact evidence identities are:

- result: 25,168,460 bytes, SHA-256
  `9dc6059476bceea713aa83a373d158558e93f56091cd15e42c3d26a897d410e8`;
- independent rescore: 883 bytes, SHA-256
  `55d05030703e7f66760e1d7e4abc98635dd317319bf4e4d88486ee03f521e636`;
- resources: 459 bytes, SHA-256
  `005ec18b71581744ce3bd610a2f338f29d3a3fbe49b804f65ab20be736172dc5`;
- worker log: 352 bytes, SHA-256
  `0faa827d1e7507fae9d1411c57823e73c9fef5d71c6459782cdbb60b10ed3ccf`;
- PQ48 books: 786,432 bytes, SHA-256
  `c24b36f9808e2ce4d6ba6e6bf1c2f89e26b96ceed09eec01bd809ecb7d2b1c3a`;
- one-million-row PQ48 code plane: 48,000,000 bytes, SHA-256
  `9c5de9f957917b07acca7c0083d2cef614e0539f4c2b772cbdb2c42f322fc6f1`.

| 128-query arm | avg Recall@10 | avg Recall@100 | p05 Recall@100 | final max GETs | final max bytes |
|---|---:|---:|---:|---:|---:|
| matched V99 flat PQ16 | 96.4843% | 91.3203% | 65% | 32 | 16,777,216 |
| V102 S3-resident PQ48 | **98.5937%** | **95.1328%** | **80%** | 32 | 16,777,200 |

The separate refinement-code wave reached at most 32 GETs and 12,742,320
bytes. The paired 10,000-resample 95% interval for V102 minus flat PQ16 was
+0.9375 to +3.6719 percentage points at Recall@10, +2.7969 to +4.8828 at
Recall@100, and +7 to +20 at p05 Recall@100. The independent reducer
recomputed the retained hierarchy pages, both waves, every sample, aggregate,
interval, projection, and the `two-wave-pq48-rejected` classification.

The complete 100M resident projection is 1,269,504,702 bytes; row codes are
zero resident bytes, while the immutable S3 PQ48 plane is 4,800,000,000 bytes.
The cell took 654 seconds, reached 11,072,196 KiB peak RSS, had zero swap and
zero memory PSI, and cost an estimated $0.087200.

**Ruling:** reject V102 without a 1,000-query continuation. The bounded
object-storage refinement wave is physically feasible and recovers 3.8125
Recall@100 points over matched PQ16, but it remains 2.3672 points below the
97.5% average gate and 10 points below the p05 gate. The monotone but
diminishing PQ16/PQ24/PQ32/PQ48 evidence does not justify a PQ64 tuning ladder,
which would also consume the full 16-MiB refinement budget before any gap
bytes. G1 still has no eligible representation; further work must change the
ranking objective or representation family rather than widen conventional PQ.

## V103 result — exact source norms make PQ48 ranking worse

V103 tested one causal change to V102's row-ranking score while preserving the
same V99 hierarchy, retained rows, PQ48 books and codes, page layout, and final
ranked-gap selector. The matched control used reconstructed-vector squared L2.
The challenger stored each row's exact source squared norm and used
`||x||² - 2 q·PQ(x)`, eliminating the reconstructed-centroid norm term. This
added four immutable S3 bytes per row but no resident row-code bytes.

The immutable source revision was
`48de8a004e098d26749da6a259f9cfd0921a6194`. Its 10,695,431-byte source
archive is at
`s3://borsuk-bench-453182569524-euc1/research/v103-metric-aware-pq48/48de8a004e098d26749da6a259f9cfd0921a6194/source/source.tar.gz`, SHA-256
`60a107decb37a202c30d2a46f8911e15cc9dc045104a82d6fea334d41d32d469`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v103-metric-aware-pq48/48de8a004e098d26749da6a259f9cfd0921a6194/runs/v103-g1-screen-20260921T094600Z-48de8a0/a0001`.
It used c7i.8xlarge Spot instance `i-054001a7a41a07bd5` in eu-central-1c,
which terminated after its claim-eligible terminal. The terminal SHA-256 is
`fa3447ac4ca8f47382782c34b101fc32617ee8b93eb8ac211ada580ad37fcbd4`.

The fixed fail-fast cohort was the first 128 already-burned ReLAION-1M
development queries. Complete frozen inputs were authenticated before slicing;
no validation or holdout query was consumed. Terminal-bound evidence is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 25,147,316 | `e2e69b312c50c715c31b4a3dce50992fb5af3973cc3235da34cd8bae7fb3790c` |
| independent rescore | 900 | `8469adf97803a019478b59211b13bbfefd4cb28eae860396baa56af705b3dcc6` |
| resources | 459 | `3b590b85a09d3f7b59dcadbd38fc78af0303a152efd662554e1d10385dda9f3a` |
| worker log | 352 | `fe1e2560c77a63ae9933c1560f2ff1acd21c9e81d747176b0f060f87b75847ea` |

| 128-query arm | avg Recall@10 | avg Recall@100 | p05 Recall@100 | final max GETs | final max bytes |
|---|---:|---:|---:|---:|---:|
| matched V102 PQ48 L2 | **98.5937%** | **95.1328%** | **80%** | 32 | 16,777,200 |
| V103 exact-source-norm score | 96.3281% | 92.2890% | 69% | 32 | 16,777,144 |

The paired 10,000-resample 95% intervals for V103 minus the control were
-3.6719 to -1.0156 percentage points at Recall@10, -3.7344 to -1.9922 at
Recall@100, and -16 to -4 at p05 Recall@100. Every interval is strictly
negative. The independent reducer authenticated and recomputed the samples,
aggregates, intervals, physical projections, and
`metric-aware-pq48-rejected` classification.

The exact source vectors are already nearly unit normalized: one million row
squared norms ranged from 0.9987742901 to 1.0011970997, while the 128 query
squared norms ranged from 0.9991053343 to 1.0008934736. Removing the PQ
reconstruction norm therefore collapses much of the ranking signal toward a
dot-product ordering and loses substantially to ordinary L2 ADC. This result
does not support tuning a mixture weight on the burned cohort.

The first refinement wave used at most 32 GETs and 13,804,180 bytes. The final
page wave used at most 32 GETs and 16,777,144 bytes. The complete 100M resident
projection remains 1,269,504,702 bytes, and the immutable S3 code-plus-norm
plane is 5,200,000,000 bytes. The attempt took 708 seconds, reached
11,037,796 KiB peak RSS, had zero swap and zero memory PSI, and cost an
estimated $0.0944.

**Ruling:** reject V103 without retry, parameter tuning, or a 1,000-query
continuation. Exact source norms do not correct conventional PQ's ranking
distortion; they make it decisively worse. The V99 exact ceiling still proves
that the hierarchy and page layout can pass, while V102 and V103 localize the
remaining G1 blocker to compact retained-row ranking. The next experiment must
change that representation family or reduce the retained shortlist enough to
afford materially stronger row evidence. G2, 10M, 100M, and competitor parity
claims remain fenced.

## V104 result — 768 retained pages are the first exact fail-fast survivor

V104 measured how far the unchanged V99 hierarchy could shrink its retained
row envelope before exact-f32 row ranking lost the registered quality gates.
It changed no representation, hierarchy summary, exposed-page ordering, page
layout, final ranked-gap planner, truth, or resource cap. One hierarchy route
was computed per query, and the registered 128/256/512/768/1,024-page arms
used deterministic prefixes of that same page-score order.

The immutable source revision was
`472bd3ffc0dcd1a1409fc338bd42eef60ddf0e8b`. Its 10,766,022-byte archive is at
`s3://borsuk-bench-453182569524-euc1/research/v104-exact-retention-ladder/472bd3ffc0dcd1a1409fc338bd42eef60ddf0e8b/source/source.tar.gz`, SHA-256
`900cb2d7272082e6cca750da47d3fc2888cc5a4b967fb4ec9f4b63be44aaffd0`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v104-exact-retention-ladder/472bd3ffc0dcd1a1409fc338bd42eef60ddf0e8b/runs/v104-g1-screen-20260921T013150Z-472bd3f/a0001`.
It ran on c7i.8xlarge Spot instance `i-0d63e18af0a850744` in eu-central-1c,
which is terminated. The canonical terminal SHA-256 is
`32db3242b277f79ea599ff151d428a59c5d24968cb709ec52240aea1bf4f831a`.

The fixed cohort was the first 128 already-burned ReLAION-1M development
queries. No validation or holdout query was consumed. Terminal-bound evidence
is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 8,808,332 | `b54c1a3daef19dd0cc256b8d2f90744e527d6bc6a09a7ac374ccdef5e331c8b3` |
| independent rescore | 1,720 | `a1488f162126e36de589a4102deb4141decfa64f8954cc646e0f7fc887f1d419` |
| resources | 459 | `bd17f2e1de884b3a41d01aa01265b36ed9d2116dc0ff6de925465f128ac27110` |
| worker log | 352 | `9774a9c9a0d955a5013a1fe0330d15becd9d2c5c9f7dce8c0bd20118b33abae0` |

| retained pages | max observed rows | avg R@10 | avg R@100 | p05 R@100 | max GETs | max bytes | gate |
|---:|---:|---:|---:|---:|---:|---:|---|
| 128 | 19,024 | 91.7968% | 87.1796% | 55% | 32 | 15,018,040 | fail |
| 256 | 36,528 | 95.8593% | 93.1640% | 74% | 32 | 16,777,216 | fail |
| 512 | 72,432 | 98.4375% | 96.8125% | 85% | 32 | 16,777,200 | fail |
| 768 | 107,933 | **99.1406%** | **97.8750%** | **90%** | 32 | 16,777,200 | **pass** |
| 1,024 | 142,799 | **99.1406%** | **98.3515%** | **91%** | 32 | 16,777,200 | **pass** |

The independent reducer rebuilt the hierarchy, recomputed each arm's retained
fence, authenticated every range endpoint and interior gap, recomputed every
hit, aggregate, resource maximum, and the `exact-retention-survivor`
classification. A separately committed posthoc reducer at
`44a40d16d8a516a50bc3cb83f73855e668d25614` computed shared-seed paired
10,000-resample intervals from the immutable result. Its 891-byte receipt has
SHA-256 `9ddf3a59cf98b4777b73aa0988fc52214246acb03db17daff54d41593c6cf544`
and is stored below the attempt at
`posthoc/44a40d16d8a516a50bc3cb83f73855e668d25614/ci.json`.

Against the 1,024-page control, the 768-page arm's 95% intervals were exactly
0 to 0 percentage points at average Recall@10, -0.7500 to -0.2500 points at
average Recall@100, and -8 to 0 points at p05 Recall@100. The smaller arms were
strictly worse on average Recall@10 and Recall@100; the 512-page arm, for
example, lost 2.0703 to 1.0547 Recall@100 points and 10 to 2 p05 points.

The attempt took 738 seconds, reached 11,096,908 KiB maximum RSS, had zero
swap and zero memory PSI, and cost an estimated $0.0984.

**Ruling:** kill 128, 256, and 512 retained pages. The 768-page arm is the
smallest provisional survivor and reduces the worst observed exact-scoring
set from 142,799 to 107,933 rows, but its first-128 p05 is exactly on the gate
and its average Recall@100 is significantly below the 1,024-page control. Run
only the 768-page arm over all 1,000 frozen development queries before using
its capacity to choose a stronger compact representation. Do not rerun the
ladder, consume validation/holdout, or scale beyond 1M.

## V105 result — full development confirms the point gate but rejects 768-page non-inferiority

V105 evaluated only V104's preregistered 768-page arm on all 1,000 frozen
ReLAION-1M development queries. It changed no hierarchy, exposed-page order,
exact scorer, final ranked-gap planner, truth, or 32-GET/16-MiB resource cap.
No validation or sealed-holdout query was read.

The immutable source revision was
`e43219e8ba2da1c71c11660513bec0bc1fa782e2`. Its 10,772,785-byte source
archive is at
`s3://borsuk-bench-453182569524-euc1/research/v105-exact-retention-confirmation/e43219e8ba2da1c71c11660513bec0bc1fa782e2/source/source.tar.gz`,
SHA-256
`89b88c3a94bd533276bd698a07e37fed3a4ed326a992b2805c34cc2933e4a1f5`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v105-exact-retention-confirmation/e43219e8ba2da1c71c11660513bec0bc1fa782e2/runs/v105-g1-confirm-20260921T015504Z-e43219e/a0001`.
It ran on c7i.8xlarge Spot instance `i-056cc69cc2a4e9ada` in eu-central-1c,
which is terminated. The canonical 1,575-byte terminal is `complete`, exit 0,
and claim-eligible, with SHA-256
`0458f8bdd5ff1988d034773f334164304ad6ed5b60efcba6395906af804232a3`.

Terminal-bound evidence is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 13,749,782 | `89b2990c5f0fa9dff25e76e4f09bf9a88b156cfca5e74ac4b9a0d9136f420e12` |
| independent rescore | 522 | `5c57d338c5340df934204a710dfe5e7cfba64d35e843aebafb28e394fcadea88` |
| resources | 461 | `3bb335e4df1f54b50638f008c314bb01b0344345425176eabb75bfd99c6fcdda` |
| worker log | 352 | `2063161c4eae6adffa5c5f5e9f4a24b7985ad06a0b8b70b6d8fa80d49deef394` |

The independent reducer authenticated the complete result and recomputed all
1,000 query fences, selected ranges, truth hits, resource maxima, aggregates,
and the `exact-retention-confirmed` point-gate classification. The 768-page
arm reached 98.6200% average Recall@10, 97.9960% average Recall@100, and
90% nearest-rank p05 Recall@100, with at most 32 GETs, 16,777,216 bytes, and
107,933 observed scored rows. It therefore passes the three absolute quality
gates and both resource gates.

Paired comparison source `44253e35538c517e6df3e9c74a4e7ff8a13e2c80`
then authenticated this result and V99's immutable 129,153,048-byte exact
1,024-page control result, SHA-256
`a122e673dd005c8ec40e311aa3fe32391d8506db2ac732401bd090fcb863bad6`.
It paired all 1,000 query ordinals, truth IDs and truth pages and reused the
registered seed-7216 10,000-resample matrix, SHA-256
`8146b2c781e73f07293b293e11b31e6d923c3d456491a417d0ec847ac4ec8d28`.
The canonical 701-byte comparison receipt is stored at
`s3://borsuk-bench-453182569524-euc1/research/v105-exact-retention-confirmation/e43219e8ba2da1c71c11660513bec0bc1fa782e2/runs/v105-g1-confirm-20260921T015504Z-e43219e/a0001/posthoc/44253e35538c517e6df3e9c74a4e7ff8a13e2c80/comparison.json`
with SHA-256
`b5d297454e8fe34c8dce39f953c843679dd2d663eee9997450d1ea5853c7409e`.
Against the 1,024-page control, the paired 95% intervals were -0.4000 to
-0.1300 percentage points for average Recall@10, -0.5190 to -0.3460 points
for average Recall@100, and -3 to -1 points for p05 Recall@100. Every interval
is strictly negative.

The Spot cell took 1,532 seconds, reached 11,031,512 KiB maximum process RSS,
had zero swap and zero memory PSI, and cost an estimated $0.204267. The local
paired reducer took 3.61 seconds, reached 1,366,524 KiB maximum RSS, had zero
swap, and left memory PSI at zero. All downloaded scratch objects were
authenticated before interpretation.

**Ruling:** the 768-page arm is an absolute-gate survivor but is killed for
promotion by the standing paired-CI non-inferiority rule. Its apparent capacity
reduction may not size the next representation. Retain V99's 1,024-page,
142,799-observed-row exact envelope as the qualified capacity boundary. Do not
rerun V104/V105, consume validation/holdout, or scale beyond 1M. The one next
representation hypothesis must change the ranking family within that envelope;
conventional PQ widening and exact-norm correction are already rejected.

## V106 result — fixed anisotropic PQ48 is decisively worse than ordinary PQ48

V106 tested the one remaining representation hypothesis on the first 128
burned ReLAION-1M development queries. It kept V102's frozen hierarchy,
1,024-page retained envelope, 48-byte code width, page planner, 32-GET cap,
16-MiB cap, truth, and ordinary-PQ48 control. The only challenger change was a
single coordinate pass using the theory-derived anisotropic vector-quantization
loss with fixed threshold 0.2, followed by inner-product ranking. No threshold,
pass count, or query-dependent parameter was tuned. Validation and sealed
holdout remained unread.

The immutable source revision is
`0a20bfea9b51174834728e0da5f2d5b44cdb9f28`. Its 10,801,942-byte source
archive is at
`s3://borsuk-bench-453182569524-euc1/research/v106-anisotropic-pq48/0a20bfea9b51174834728e0da5f2d5b44cdb9f28/source/source.tar.gz`,
SHA-256
`50870445b0f8f37313e3acf72c651ba8fd23d8aa20883bb7d547bbb19495d2b5`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v106-anisotropic-pq48/0a20bfea9b51174834728e0da5f2d5b44cdb9f28/runs/v106-g1-screen-20260921T023700Z-0a20bfe/a0001`.
It ran on c7i.8xlarge Spot instance `i-0f1447401c2f5c577` in eu-central-1c;
the instance is terminated. The terminal is complete, exit 0, and
claim-eligible.

Terminal-bound evidence is:

| role | bytes | SHA-256 |
|---|---:|---|
| result | 25,152,054 | `e46c465a9b8cbc15dd80ef7d92f901797b27b22e7db2708f6c8599811eea7d79` |
| independent rescore | 883 | `2b049cace5030f2334823669173b1957bab1edaffa78884d5fe65a078a2ece4b` |
| resources | 460 | `91d2cc1b8a4fab493ec1c5ea7b913ae26aab561b2ff15f55987acd639961b87d` |
| worker log | 352 | `e5938828224b1bc67ebf9f344d94925511a0d406777ddde7e751803c137ecefc` |

The independent reducer authenticated the complete result, rebuilt the
hierarchy and fetch plan, regenerated both code planes, recomputed every sample
and aggregate, reproduced the seed-7216 10,000-resample paired intervals, and
verified the rejection classification.

| arm | avg R@10 | avg R@100 | p05 R@100 | max GETs | max bytes |
|---|---:|---:|---:|---:|---:|
| ordinary PQ48 control | 98.5937% | 95.1328% | 80% | 32 | 16,777,200 |
| anisotropic PQ48 | 97.2656% | 91.8281% | 70% | 32 | 16,777,160 |

The challenger-minus-control paired 95% intervals were -2.4219 to -0.4688
percentage points for average Recall@10, -4.2578 to -2.3984 points for average
Recall@100, and -16 to -3 points for p05 Recall@100. Thus every quality
interval is strictly negative, and the challenger also fails all three absolute
quality gates. Both arms pass the fetch gates. The shared serving projection is
1,269,504,702 resident bytes at 100M rows, below the 3-GiB cap; the immutable
48-byte code plane remains on object storage rather than resident RAM.

The Spot cell took 867 seconds, reached 11,159,856 KiB maximum process RSS,
had zero swap and zero memory PSI, and cost an estimated $0.1156.

**Ruling:** kill fixed-threshold anisotropic PQ48 immediately. Do not tune its
threshold or pass count, do not run it on all 1,000 development queries, and do
not consume validation/holdout. This falsifier rejects this specific
score-aware noise-shaping construction; it does not reject every learned or
non-centroid representation. G1 remains open at the qualified V99 1,024-page
exact envelope, and the next work returns to the registered native
snapshot/delta/mutation/compaction qualification rather than launching another
representation arm.

## Production-native 100k gate — exact page scoring exposes a routing failure

The first production-native qualification cell built the current exact-f32
page format, loaded the resulting snapshot through the production reader, and
ran all 1,000 registered development queries against the frozen 100,000-row,
768-dimensional ReLAION subset and its exact top-100 truth. This is a local
object-store measurement on the Spot instance, not an S3 cold-latency result
and not a competitor comparison.

The immutable source revision is
`69be0e20f74b892dce20ecfa33a865c11b55a0a3`. Its 10,801,921-byte source
archive has SHA-256
`092b9dd850e17cb2d09f3db4be023f2ec6ec820c9de730da399b52079944009b`.
The sole attempt prefix is
`s3://borsuk-bench-453182569524-euc1/research/native-ann-100k/69be0e20f74b892dce20ecfa33a865c11b55a0a3/runs/native-100k-dev1000-20260921T043822Z-69be0e20/a0001/`.
It ran on c7i.8xlarge Spot instance `i-01143597ce44de7cd` in
eu-central-1, which is terminated. The 393-byte terminal is `complete`, exit
0, and has SHA-256
`8d320c17f76cb24a12c2fc9e1d303e97ebaf81fd4165643a6c400b60aa837f55`.
The release binary was 39,655,848 bytes with SHA-256
`17d80b0cc68144ada6a4429586f5c28057b55fc8dff7eff9202b7cc8a8cbe893`.

The authenticated inputs were:

| role | bytes | SHA-256 |
|---|---:|---|
| source | 145,121,661 | `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d` |
| queries | 1,544,342 | `4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac` |
| exact truth | 512,093 | `ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7` |

Terminal-bound output evidence was:

| role | bytes | SHA-256 |
|---|---:|---|
| canonical result | 2,920 | `d29690543c637d1d50f1cdc370e68d30bcad00b766ba32b5ef484c262d5426e6` |
| per-query samples | 399,980 | `1ac51be90f59e6319eec33be5a8a6c2afd178b50c3d32e060915005fecae55d0` |
| resource log | 158 | `4d77cae7da16e884507fb019f1dd60d8be3d15a1deb740fd541553c5bfce3109` |
| process timing | 2,003 | `b3cba337efa83dc2ad17dd4c845fff3a3280004fd2f63faf4b14d6c1f5626743` |

| measured quantity | result |
|---|---:|
| average Recall@10 | **51.7200%** |
| average Recall@100 | **33.9530%** |
| nearest-rank p05 Recall@100 | **16.0%** |
| warm-local query latency p50 / p95 / p99 | 19.984 / 20.765 / 21.932 ms |
| logical GETs / pages | 16,778 / 20,000 |
| logical bytes | 16,347,716,032 |
| exact rows scored | 5,117,984 |
| build / 1,000-query wall | 18.589 / 20.107 s |
| whole science wall | 39.54 s |
| peak process RSS | 2,315,800,576 bytes |

Every query stayed within 20 logical GETs/pages and 16,354,720 bytes. The
production index contained 19 segments and reported 320,543,803 collection
resident bytes. The process used zero swap; the resource monitor observed zero
memory PSI. A separate bounded recomputation authenticated all 1,000 sample
ordinals, 100 unique returned IDs per query, every hit count, aggregate,
latency quantile, and I/O total against the canonical result.

**Ruling:** the cell is intentionally `claim_eligible=false` and fails all
three frozen quality gates by a wide margin. Exact scoring is not the failure:
the route exposes only 5,117.984 of 100,000 rows per query on average, so true
pages are excluded before exact scoring. Kill this production-native routing
configuration and do not scale it to 1M. G1 must first qualify a stronger
bounded shortlist under the fixed 32-GET/16-MiB serving budget; its latency
must then be measured on real S3 rather than inferred from these warm-local
numbers.

## V107 result — 1,024-page capacity exposes a stale 512-row nomination gate

V107 reran the registered five-width G1 screen on all 1,000 ReLAION-1M
development queries after replacing V97's already-rejected 128-page capacity
with 1,024 pages. The page count came from V104/V105's qualified capacity
boundary, but the distinct two-summary ranker remained subject to a fresh
exact-f32 causal gate. No validation or holdout query was consumed.

The immutable source revision was
`b404f1018aab344a72d272a14cf7f700071f6ad0`. Its 10,803,881-byte source
archive has SHA-256
`fe26c9721b9415205f447248d856b7f8ae83314cc7f5188f967e56049b6aa03c`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v97-row-width-screen/b404f1018aab344a72d272a14cf7f700071f6ad0/runs/v97-g1-20260921T044920Z-b404f101/a0001/`.
It ran on c7i.8xlarge Spot instance `i-058127a6e5eae76a7` in
eu-central-1b, which is terminated. The canonical 415-byte terminal is
`complete`, exit 0, with SHA-256
`c2ebb5bf02c9ec22b5d6813d31035a3a1b0ea9cfbf9562015b076fe18c6b8175`.

Terminal-bound evidence was:

| role | bytes | SHA-256 |
|---|---:|---|
| producer result | 32,480,230 | `deae6837f4b5bcf5acbf62cc66469e704b9b409f7353305ce9cbf046f5c48636` |
| independent rescore | 1,588 | `71be530ea3d5c51acdbb4dc2fc3a4f8fa988ed91fc065fea08f382bf89bd6c0d` |
| resources | 386 | `7493d5c0839a97a59823f10a513e6d51db7c5cf3640f483335a009a8965fc02e` |
| screen timing | 2,895 | `93b7b46743c58cd3e7db77f0c5dd6713c0e65649d015cd8bca55f5f784715798` |
| rescore timing | 3,002 | `ab301dca279e8889f795d3f77ced812b0c5c469388040502921918d8f0ec8013` |

| representation | avg R@10 | avg R@100 | p05 / worst R@100 | max GETs | max bytes | 100M resident |
|---|---:|---:|---:|---:|---:|---:|
| exact f32 | **99.7100%** | **83.7470%** | **54% / 39%** | 32 | 6,427,384 | n/a |
| PQ16x8 | 84.0200% | 71.6900% | 34% / 4% | 32 | 6,451,712 | 2,861,603,104 B |
| PQ24x8 | 89.2100% | 74.4480% | 39% / 12% | 32 | 6,445,432 | 3,661,603,104 B |
| PQ32x8 | **92.3500%** | **76.2620%** | **42% / 15%** | 32 | 6,277,448 | 4,461,603,104 B |
| PQ32x4 | 81.3400% | 67.1660% | 27% / 5% | 32 | 6,451,712 | 2,860,865,824 B |
| summary-only PQ16x8 | 65.8600% | 59.0370% | 13% / 0% | 32 | 5,059,152 | 1,260,816,672 B |

The independent reducer authenticated the producer result, all 1,000 query
ordinals and samples, every aggregate and budget maximum, the fixed-seed
10,000-resample paired intervals, and every 100M memory term. PQ24 and PQ32
were strictly better than PQ16 on both average recall metrics, but both exceed
the 3-GiB resident projection and every compressed arm failed all three
absolute quality gates. The exact-f32 arm passed Recall@10 but failed average
and p05 Recall@100, so no width is causally interpretable and `winner` is null.

The screen took 618.43 seconds; the independent rescore took 8.69 seconds.
Whole-cell elapsed time was 660 seconds, peak process RSS was 11,502,932 KiB,
and both swap and memory PSI remained zero. At the observed eu-central-1b
c7i.8xlarge Spot price of $0.7763/hour, estimated compute cost was $0.1423.

**Ruling:** kill this 512-row shortlist configuration and do not interpret the
width ordering as a G1 decision. Expanding the page fence recovered Recall@10
but not the long Recall@100 tail. The remaining gate difference from the
qualified V104/V105 exact path is the row nomination bound: V107 retained only
512 rows, while the qualified exact path retained 8,192 before the identical
32-GET/16-MiB planner. The next gate-correctness cell changes only that bound
to 8,192 and must first restore the exact-f32 gate; if it does not, G1 stops
without another representation or scale run.

## V108 result — 8,192 nominations do not repair two-summary page ordering

V108 was the final gate-correctness attempt for the two-summary G1 router. It
changed only V107's row shortlist from 512 to the V104/V105-qualified 8,192;
the 1,024-page summary fence, five registered widths, 32-GET/16-MiB planner,
dataset, all 1,000 development queries, exact truth, seeds, and independent
reducer were unchanged. No validation or holdout query was consumed.

The immutable source revision was
`25acd3be21524ee2b0ab4040121ccf8d7ff60902`. Its 10,805,447-byte source
archive has SHA-256
`e4b608216164c9ef864cc92ca7b552b1616b205bcdf1f4a79de97180e65b1e05`.
The sole attempt prefix was
`s3://borsuk-bench-453182569524-euc1/research/v108-row-width-shortlist/25acd3be21524ee2b0ab4040121ccf8d7ff60902/runs/v108-g1-20260921T050437Z-25acd3be/a0001/`.
It ran on c7i.8xlarge Spot instance `i-0c02bc7a8d4376a10` in
eu-central-1c, which is terminated. The canonical 415-byte terminal is
`complete`, exit 0, with SHA-256
`16b981c377dc098082963dd9cb2fc84fef786d957a5d5a5b79983bfd5f0c9a75`.

Terminal-bound evidence was:

| role | bytes | SHA-256 |
|---|---:|---|
| producer result | 32,622,142 | `5c2c8902bea2dd9e1344793942465204b32664ee1bc0743254bd648ffece8c68` |
| independent rescore | 1,588 | `704cd14f371e7f64128f0ebc1c765f4bb4fd420b45de9ece4310aed7a4de6616` |
| resources | 386 | `3642f39b6ae5a8ca62963e005587ef03f286f3cf3df435b3be4f28d7ef72b607` |
| screen timing | 2,895 | `4d5cbe062a56383698d6379fcd6176611f6638bc3ab8e94d28baee137ab47bd8` |
| rescore timing | 3,002 | `a46ef06bbd76db528dd79c7ffb313ae6d6121455f2dfe0a9688d4ced40e29f7f` |

| representation | avg R@10 | avg R@100 | p05 / worst R@100 | max GETs | max bytes | 100M resident |
|---|---:|---:|---:|---:|---:|---:|
| exact f32 | **99.7100%** | **83.7470%** | **54% / 39%** | 32 | 6,427,384 | n/a |
| PQ16x8 | 84.1600% | 71.7690% | 34% / 4% | 32 | 6,451,712 | 2,861,603,104 B |
| PQ24x8 | 89.3500% | 74.4990% | 39% / 12% | 32 | 6,445,432 | 3,661,603,104 B |
| PQ32x8 | **92.4200%** | **76.2860%** | **42% / 15%** | 32 | 6,277,448 | 4,461,603,104 B |
| PQ32x4 | 81.3500% | 67.1690% | 27% / 5% | 32 | 6,451,712 | 2,860,865,824 B |
| summary-only PQ16x8 | 65.8600% | 59.0370% | 13% / 0% | 32 | 5,059,152 | 1,260,816,672 B |

The independent reducer authenticated all 1,000 samples, aggregates, budget
maxima, paired intervals, and memory terms. The exact-f32 aggregate and every
exact-f32 resource maximum are identical to V107. Widening nominations by 16x
therefore cannot change the first 32 distinct pages emitted by the ranked
evidence. Compressed changes were negligible; every arm still failed all
three quality gates, and `winner` remained null.

The screen took 644.08 seconds; the independent rescore took 8.97 seconds.
Whole-cell elapsed time was 687 seconds, peak process RSS was 11,506,036 KiB,
and both swap and memory PSI remained zero. At the observed eu-central-1c
c7i.8xlarge Spot price of $0.688/hour, estimated compute cost was $0.1313.

**Ruling:** close the two-summary router and its row-width line. The exact
causal gate failed twice, and V108 falsifies the shortlist-size hypothesis:
the summary-derived page ordering, not the nomination count or compressed row
width, misses the Recall@100 tail before exact page scoring. Do not run V109,
real-S3 qualification, validation, 10M, or 100M for this line. Preserve V107
and V108 as negative evidence and proceed to release/closeout rather than
opening another architecture or critique.

## Architecture closeout — matched-service disposition

The post-V108 closeout attempted one matched Amazon S3 Vectors cell because
that comparison is competitor evidence, not a qualification of the rejected
BORSUK router. The registered workload was the same frozen ReLAION-1M
development workload: 1,000,000 source rows × 768 float32 dimensions, all
1,000 development queries, exact GT100, Euclidean distance, and `topK=100`.
The three Parquet inputs retained their V108 authorities:

| role | bytes | SHA-256 |
|---|---:|---|
| source | 1,458,450,077 | `2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86` |
| development queries | 1,558,506 | `310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54` |
| development GT100 | 2,046,505 | `fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11` |

The immutable executable revision was
`01320ab960c6d13c69080dad4ca434ec19bcf79d`. Its 10,823,530-byte source
archive has SHA-256
`684d062c492373e4a687af3b91e373270485346e7f3c93eb4924e9cd65c8441a`.
The sole resource-creating attempt was
`s3://borsuk-bench-453182569524-euc1/research/matched-s3-vectors-1m/01320ab960c6d13c69080dad4ca434ec19bcf79d/runs/matched-s3v-20260921T054216Z-01320ab9/a0001/`.
It used c7i.8xlarge Spot instance `i-0566ef1d52e7604cc` in eu-central-1c;
the instance is terminated.

The 268-byte canonical terminal is `failed`, exit 94, claim-ineligible, with
SHA-256
`e1179b60b91def3173f86f4f74a73d84d7ef89e3ff8f0fe6547ca6b4b6f34035`.
The authenticated input downloads and hashes completed, but the producer
failed before its first S3 Vectors control-plane call: PyArrow attempted a
NumPy conversion while NumPy was absent from the remote environment. The
preserved worker log is 1,481 bytes with SHA-256
`2b8bace1e37a1b91f88cd554a74c537fab0dcd2c84bedcc206f3a2360f9e16af`;
the 1,857-byte process timing has SHA-256
`a14eefee41703485c34d4e4139ed887c67effbf2a809dae05a0a7d468165e3fb`.
The 455-byte resource receipt has SHA-256
`9aa76055ab12d5da83946542ab076e0fe7c07843000fec25b3aa0007dfebb704`:
32 seconds elapsed, 109,760 KiB peak process RSS, zero swap, zero memory PSI,
and estimated Spot compute spend of $0.006116 at $0.688/hour.

No vector bucket or index was created, and a post-terminal service listing
confirmed the registered temporary bucket name was absent. The instance
terminated at 05:43:03 UTC. The one-attempt protocol forbids replacing this
failed cell, so there is **no matched S3 Vectors recall, latency, throughput,
bytes, or service-cost measurement** from this campaign. Published S3 Vectors
numbers remain context-only and must not be presented as matched evidence.
The reproducibility defect is repaired in
`8b906dbc2cf324d57cfdd8bed8bea4b2c92fe326`, which pins NumPy, PyArrow,
and boto3 in one remote requirements file; that repair was not used to rerun
the closed cell.

Turbopuffer could not be executed. The environment contained no Turbopuffer
API credential, tenant, region, or namespace; AWS Secrets Manager returned no
matching secret name, and SSM Parameter Store returned no matching parameter
name. This is a concrete access blocker, not a performance result. Published
Turbopuffer figures in the market matrix remain explicitly vendor-reported,
non-matched context.

**Closeout ruling:** no release candidate is justified. V108 closes the
two-summary router/row-width architecture on its exact-f32 causal failure, and
the sole matched-service attempt produced no claim-eligible competitor result.
Do not scale this router to validation, 10M, or 100M, and do not imply parity
with S3 Vectors or Turbopuffer. Preserve the evidence and tag this revision as
an architecture closeout.

## Bounded-reader next result — matched ReLAION-1M evidence

The next-result phase did not reopen V108 or tune its rejected router. It
packaged the strongest already-working one-wave native SQ8 reader and measured
it against a repaired matched Amazon S3 Vectors cell on the same frozen
ReLAION-1M development workload: 1,000,000 source rows × 768 float32
dimensions, all 1,000 development queries, exact Euclidean GT100, and
`topK=100`. The source, query, and truth identities are the three authorities
listed in the preceding closeout section.

### Native bounded BORSUK reader

The immutable scientific source was
`26716f9eae90688ea0b047a83211d99855062858`. Its 10,840,876-byte source
archive has SHA-256
`b204ee2d5693b8f4de472595b80fd259ee8b3512c00d92fee665a1d30b54c398`.
The sole scientific attempt was
`s3://borsuk-bench-453182569524-euc1/research/bounded-reader-next-result/26716f9eae90688ea0b047a83211d99855062858/runs/bounded-reader-20260921T074858Z-26716f9/a0001/`.
It ran on c7i.12xlarge Spot instance `i-018f299784382235e` in
eu-central-1c; the instance is terminated. A preceding harness-only attempt
under source `80ef77ba4f9b9c76381bacdbf79a5c888bca4286` failed before science
because Amazon Linux's default Python could not install the pinned NumPy and
PyArrow versions. It cost at most $0.0062 and its prefix was not reused.

The reader authenticated the historical 780,000,000-byte SQ8 object with full
SHA-256
`2284f24745f964ff2b125eedb883d5cd8ff6afab0738f49f9e16e593167a318b`
before science. It used the unchanged fixed operating point: 256 coarse
regions, 512 shortlisted rows, gap 2, and 128 concurrent ranged GETs. It ran
two fixed sequential passes over all queries, then a throughput ladder. The
1,195,625-byte per-query Parquet evidence has SHA-256
`b3455f813822dc39376c53f4dc427157f3aaa8fd9fe65854aa8d3668a574b3c0`.
An independent reducer recomputed every feature-ID hit, aggregate, latency,
GET, and byte statistic; its SHA-256 is
`21186a7a1b692dc4ade80eb936919e3b2cc2fff7be7553318b1b3473af6bc8f1`.
The producer result SHA-256 is
`532080d202538dc252640257fb5f30eea0de3b4453b897f45eb000962989133d`;
the terminal SHA-256 is
`283f560c9a86e0e5a4980519ea216c20105711bf84b156ff706f6fdb79c4c76d`.

| metric | first-connection pass | connection-reuse pass |
|---|---:|---:|
| average Recall@10 | **99.2800%** | **99.2800%** |
| average Recall@100 | **99.0260%** | **99.0260%** |
| p05 / worst Recall@100 | **97% / 82%** | **97% / 82%** |
| latency p50 / p95 / p99 | 41.458 / 70.074 / 161.862 ms | **41.765 / 65.509 / 89.746 ms** |
| GETs p50 / p95 / p99 / max | 20 / 52 / 63 / 84 | 20 / 52 / 63 / 84 |
| bytes p50 / p95 / p99 / max | 10.474 / 22.280 / 26.089 / 33.706 MiB | same |

The throughput ladder had zero errors at every point: 81.5 QPS at 8 workers,
144.1 QPS at 32, **182.4 QPS at 128**, and 177.2 QPS at 384. The native
process ran for 118.96 seconds and reached 11,334,412 KiB peak RSS at the
aggressive concurrency ladder; it swapped zero pages and memory PSI remained
zero. The full wrapper, including manifest construction, took 421 seconds and
cost at most $0.0842 in Spot compute. S3 request charges were not preserved as
a separate receipt, so this is not a complete serving-cost claim. The high
throughput-ladder RSS is also not a 100M resident-memory proof; query
concurrency and response buffers must be bounded before a scalability claim.

### Matched Amazon S3 Vectors

A live two-vector preflight first proved the exact boto3 1.42.97 / botocore
1.42.97 / NumPy 2.4.2 / PyArrow 24.0.0 dependency and service lifecycle against
API `2025-07-15`; its canonical receipt SHA-256 is
`181876e6594b21e586d109c1e52f682f9b53bd917fcfdd23840494e25cee6549`.
Both preflight resources were deleted before the matched launch.

The matched scientific revision was
`db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2`. Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/matched-s3-vectors-next-result/db47333f3ace06d0e6cb0dbd6fcf41150b5e87c2/runs/matched-20260921T080153Z-db47333/a0001/`.
It ran once on c7i.8xlarge Spot instance `i-0017a626222c604c0` in
eu-central-1c. The terminal SHA-256 is
`f502f9443b954a674a296896d138d7e039d58f37850c9f1ee46cbabd6f226488`;
the producer result and per-query Parquet SHA-256 values are respectively
`b6d392170152a535596bd706349e44acd64b38ed1108197e71745c317b51d009`
and `870b24d95178f6087c762d248fec10ee7d885ac990346dfe91134e4f777aa346`.
Independent local recomputation from the terminal-bound samples and frozen
truth matched every aggregate below.

The two products used the same 1,000 query identities and exact truth, but not
the same query schedule: BORSUK used ascending query ordinal while the S3
Vectors harness used one frozen deterministic permutation. Recall aggregates
are order-independent. The latency distributions are matched-workload
measurements, not paired per-query-order evidence, and may include different
temporal service effects.

| metric | fresh-index first pass | immediate repeated pass |
|---|---:|---:|
| average Recall@10 | 97.6800% | 97.7400% |
| average Recall@100 | 90.9380% | 91.0450% |
| p05 / worst Recall@100 | 67% / 39% | 68% / 39% |
| latency p50 / p95 / p99 | 73.350 / 239.556 / 328.051 ms | 61.372 / 93.516 / 120.567 ms |

S3 Vectors ingested 1,000,000 vectors in 848.09 seconds, or **1,179.1
vectors/s**, through 2,000 `PutVectors` calls. The full cell took 1,113
seconds, peaked at 551,780 KiB RSS, used no swap, and cost at most $0.2127 in
Spot compute. This excludes S3 Vectors service charges. No concurrent query
throughput cell was registered, so no S3 Vectors QPS comparison is claimed.
The temporary index and vector bucket were deleted and independently confirmed
absent; the Spot instance is terminated.

### Decision

On this exact matched workload, the bounded BORSUK reader is materially better
than S3 Vectors in average and tail recall and in both observable sequential
latency distributions. This is matched-workload product evidence, not a paired
query-order experiment or a universal vendor claim.
It does not qualify a release candidate: the measured reader still lacks the
generation/delta/mutation/compaction release surface, its maximum per-query
bytes exceeded 16 MiB, and the throughput ladder's 10.81-GiB peak RSS does not
support the 100M sub-3-GiB target. Turbopuffer remains access-blocked, so its
published figures remain non-matched context only.

**Next-result ruling:** preserve this reader as the strongest measured 1M
baseline and stop architecture tuning. The immediate production work is to
bound concurrent response memory and attach this exact read path to the native
snapshot/generation/delta/mutation/compaction API. Do not spend on 9.99M or
100M until those semantics pass at 100k and the 100M resident worksheet is
below 3 GiB. Publish a next-result decision tag, not a release-candidate tag.

## Native geometric page-layout development screen

The pre-release architecture reset tested whether the production-quality loss
was caused by routing alone or by the physical page layout. The frozen workload
was ReLAION development-100k: 100,000 source rows, 768 float32 dimensions,
Euclidean distance, all 1,000 development queries, and exact GT100. The source
Parquet is 145,121,661 bytes with SHA-256
`a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`;
the truth Parquet is 512,093 bytes with SHA-256
`ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7`.
Construction was query- and truth-blind. Evaluation computed the exact best
GT100 coverage subject jointly to at most 32 pages and 16 MiB of encoded page
bodies. These values are therefore page-layout headroom, not a deployable
query-time router, ranking, latency, throughput, or release measurement.

The final scientific source was
`f7a09ea5df975fb6c2e36a53ed56f53492b334fd`. Its 10,944,487-byte source
archive has SHA-256
`a0b45a1fb68e77c1d95d90a26a35160fa10976fda087fdfd3275661b75824809`.
The sole cell for that revision ran on c7i.8xlarge Spot instance
`i-006452984d4d83798` in eu-central-1c. Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/native-geometric-layout/f7a09ea5df975fb6c2e36a53ed56f53492b334fd/runs/relaion-100k-dev1000-a0001/`.
The instance is terminated.

| query-blind physical layout | average R@10 | mean R@100 | p05 R@100 | worst R@100 | decision |
|---|---:|---:|---:|---:|---|
| decimal-ID order, 256 rows | 63.720% | 61.377% | 47% | 41% | control |
| balanced random projection, 256 rows | 50.340% | 49.548% | 42% | 35% | killed |
| balanced two-means, 256 rows | 99.060% | 98.460% | 89% | 75% | killed: p05 below 90% |
| balanced two-means, actual 480-KiB SQ8 page cap | **99.920%** | **99.844%** | **100%** | **88%** | advance to router falsification |

The canonical result is 4,379 bytes with SHA-256
`ee936fdba1dde54a1126cdd2cb09cc38a9d53a3f53acedfb9a7fa8c562d2497a`.
The four membership SHA-256 values, in table order, are
`633dc52514ac0c15d3deb881db965ad433433354bbf9ffba3c89ea2504f2e2e7`,
`9e07406a0ca7658caaa329d0194d56fa8400636805bbd6378b99c4aca9d76c5e`,
`dc3e6ee05a4f9f30a295b9a47ac348905a9e37cd8d4d60e0445a11b6b1deb518`,
and `f72b80f1341bd64599e51a69e627f0b9a2280be4f5235a6c5995fef8eacd866c`.
The corresponding per-query evidence SHA-256 values are
`66b15c2441cf3fd814310e2b97ec854c404abfabbdfe463c6891e4f2e4e217d1`,
`4f4c433ab4b140f4a3800a6f2447e4125bbd5d4f7f257ec74e459c347a65da6d`,
`056221506947b263e559e0f0128aecce7558762b94a8407d60f523afe9aa93ac`,
and `4e154efb50df6dda1079e0becebc9ad207c544d8b3d6e57f84e34a01bf92bd32`.

Arm wall times were respectively 2:20.45, 1:40.72, 3:20.67, and
1:24.65. Peak arm RSS ranged from 1,536,028 to 1,536,536 KiB. The terminal
recorded 629 seconds; launch through verified termination was approximately
647 seconds, costing at most $0.1233 at the recorded $0.6861/hour Spot rate,
excluding negligible request/storage charges. The canonical terminal is 264
bytes with SHA-256
`8f56dddf85b18661ca4619c8e23e8f847516b542d6ba2e72e149cda5cf759ad7`.

The remote terminal is explicitly `failed`, `claim_eligible=false`, at
`phase=validate`, exit 255. All scientific membership, evidence, result,
resource, and seal objects were already immutable, but the worker did not
upload `validation.json`; therefore this is not represented as a complete
remote receipt. No scientific rerun was made. A controller-side independent
replay re-authenticated the frozen source, truth, four memberships, four
per-query evidence files, and canonical result; it independently recomputed
all 1,000 samples and aggregates and reproduced `control`, `killed`, `killed`,
`advance` exactly. The replay exited zero in 17:25.71, used 230,984 KiB peak
RSS, and used zero swap; its PID and explicit scratch were cleared.

Three earlier immutable revisions exposed harness defects before this valid
decision: EC2 user data was double-base64 encoded, two embedded Python
programs contained a newline-escaping syntax error, and the evaluator assumed
a row-expanded truth schema instead of the authenticated fixed-list GT100
schema. A later diagnostic used numeric binary IDs instead of the native
builder's lexicographically sorted decimal record IDs. Each defect was covered
by a focused regression before the next clean revision; no failed prefix was
reused.

**Decision:** the ID-ordered production layout is rejected and the 480-KiB
balanced two-means layout has enough physical containment headroom to justify
one bounded query-independent router falsifier. This does not promote the
layout to production and does not authorize 1M, 9.99M, or 100M. The next gate
must prove that a resident router can select the same useful pages without
truth, then measure SQ8 ranking and the complete 32-GET/16-MiB read path at
100k. A failure closes or materially redesigns that router; it must not be
hidden by another truth-aware layout score.

### Native geometric router 100k decision preregistration

This is plan **Task 3**, not algorithm version V3. Historical experiment labels
such as V98 and the frozen Publication V3 schema use independent namespaces and
do not identify this architecture.

Exactly one immutable ReLAION development-100k Spot cell is authorized for the
query-independent balanced-two-means 480-KiB layout plus its geometric tree,
full-dimensional page-centroid refinement, and page-local SQ8 ranking. The cell
uses all 1,000 frozen development queries. Its source, query, and GT100 SHA-256
values are respectively
`a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`,
`4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`,
and `ab8bfae34f753512f352581218596fc0f043354f8168192c856278b3ab5a0ce7`.
Construction cannot access queries, truth, or the network. Membership, tree,
and page-representative artifacts are authenticated, made read-only, and
uploaded before the evaluator receives query or truth capability.

The fixed serving envelope is a 128-leaf frontier, at most 32 selected pages,
at most 16 MiB of encoded page bodies per query, a 3-GiB process-group RSS cap,
and a 7,200-second wall cap. Advancement requires average Recall@10 at least
96%, average Recall@100 at least 97.5%, p05 Recall@100 at least 90%, and a
complete two-generation 100M resident-memory worksheet below 3 GiB. The
producer result remains `claim_eligible=false`; an independent validator must
authenticate the exact source/query/truth and sealed artifact bytes and
recompute every route, ranked SQ8 hit, aggregate, and decision. Any failed
quality gate returns to a material 100k redesign. It does not authorize another
parameter-only attempt or any 1M/10M/100M run.

### Native geometric router 100k terminal — failed page selection

The sole preregistered cell ran from source commit
`67c88488fb17a9f02715c6d262a22225cf950de5` and source archive SHA-256
`81b03af7d175f2208be89421a15654b58210f0272db8107c9b6447b77641a0f7`
(10,964,373 bytes). The exact source/query/truth identities are listed in the
preregistration above. Its immutable prefix is
`s3://borsuk-bench-453182569524-euc1/research/native-geometric-router/67c88488fb17a9f02715c6d262a22225cf950de5/runs/relaion-100k-dev1000-a0001/`.
It ran on Causality Spot `c7i.8xlarge`, instance `i-07c28c2770548c28e`
in `eu-central-1c`; the instance is terminated. Terminal status is `complete`
with exit 0 and 160 seconds elapsed, and the scientific result is explicitly
`claim_eligible=false`. The canonical result SHA-256 is
`b5753cfba5a029fba3cca5cc122d4e51c7a02d5b69bc0de2219ee0d41a5b2607`;
per-query evidence SHA-256 is
`9a33d86a6ce297ca61fe08d22206a2938b2660710735433103546003b8b79452`.
The remote independent validator reauthenticated the frozen inputs and sealed
membership/tree/page artifacts, recomputed all 1,000 query routes and ranked
SQ8 results, and returned `killed`; its receipt SHA-256 is
`5de489c3a94db6e9f3669f89476938e81f0290dc4a1eb870494e5372946c3a5a`.

| Same frozen ReLAION development-100k split | avg R@10 | mean R@100 | p05 R@100 | worst R@100 | max pages / encoded bytes |
|---|---:|---:|---:|---:|---:|
| truth-aware two-means layout coverage oracle (earlier cell; not a serving route) | 99.920% | 99.844% | 100% | 88% | 32 / 16 MiB cap |
| query-blind geometric router, exact GT page containment | 97.030% | 94.417% | 77% | 50% | 32 / 15,664,840 |
| query-blind geometric router plus page-local SQ8 rank | 97.030% | 94.349% | 77% | 50% | 32 / 15,664,840 |

The oracle-to-route mean R@100 gap is 5.427 percentage points; subsequent
SQ8 ranking loses only 0.068 points. This decomposition identifies page
selection as the dominant failure on the fixed layout, not page-local SQ8.
The router misses the 97.5% mean and 90% p05 gates despite meeting the 96%
R@10, 32-page, and 16-MiB gates. Construct/evaluate/validate wall times were
25.22/47.93/48.50 seconds; peak process RSS was respectively
2,390,428/1,639,400/1,637,580 KiB, with zero swaps. These are screen
resources, not native cold-S3 serving latency or 100M memory measurements.

**Decision:** kill this fixed split-plane best-first plus single page-centroid
refinement router. Do not enlarge the frontier, reuse the scientific prefix,
or promote to 1M. A genuinely different query-independent page-selection
representation must first restore GT page containment under the same 32-page
and 16-MiB budget at 100k, with paired per-query evidence. Only then recheck
SQ8 ranking and a complete two-generation 100M resident worksheet; the
current screen does not qualify the production store or competitive parity.

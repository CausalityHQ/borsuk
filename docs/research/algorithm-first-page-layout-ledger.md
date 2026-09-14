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

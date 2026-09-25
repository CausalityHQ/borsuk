# V244 owner-partitioned graph commit, ReLAION-100k closeout

**Decision: pass the frozen 100k construction gate and promote the same
source-only batch policy to one 1M gate.** Frozen-snapshot searches
produce edge proposals; updates are grouped by owner row and applied
to disjoint row ranges in parallel. Each row receives proposals in
original insertion order. This removes V243's serial commit bottleneck
without changing graph bytes, serving output or format.

The sole `causality` c7i.4xlarge Spot cell was `a0001` on
`i-062baaed282000313` in `eu-central-1c`; it is **terminated**.
Source commit `ad367b68fa2fb47e96851fe29460a120535c64dc`, source
archive SHA-256
`7d706bd53cbb6b2504ab1cd8cf955dc8df06811e1d048e81fc46764d236b290b`,
terminal SHA-256
`71df3134b48f222542769f96ca1cad520676b4de166e68c7a05c466ed7559c44`.
The terminal exited zero. The original launcher replayed the size and
SHA-256 of all ten artifacts and confirmed termination. Sealed raw
SHA-256:
`68efcf6c10640fc5a2c3ab6b9b98c1cbcb30119a9413e98a8617e777737d2575`.
Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v244-owner-commit-100k/ad367b68fa2fb47e96851fe29460a120535c64dc/runs/a0001/`.

Frozen source: ReLAION-100k D768 cosine k=100, prior-used development
queries 0–255 and method-held-out queries 256–999, the same
authenticated physical-order source, FP16 plane, PQ books/codes and
requests as V243. M=32, M0=64, ef_construction=128, PQ ef/FP16
shortlist=2048/2048; eight build workers. The V244 graph is exactly
V243's 26,572,470-byte artifact (SHA-256
`498ab9f5a3da7c672ccf362d4ae5c96a853b7d4f9f2d6fe9d7708f0250045d03`).
All 100,000 rows are reachable, minimum in-degree four, zero below
four in-edges, maximum degree 98 and 6,463,421 directed base edges.
The narrow release graph thread-count test passed 1/1.

| Verified builder | Rust build time, s | Peak builder RSS, B | Graph identity |
| --- | ---: | ---: | --- |
| V242 selected serial | 138.640 | 531,288,064 | different topology |
| V243 same batch, serial commits | 79.243 | 533,233,664 | `498ab9f5…45d03` |
| **V244 owner-partitioned commits** | **20.725** | **537,739,264** | **`498ab9f5…45d03`** |

V244 saved **58.518 s (73.8%)** versus the same-topology V243
builder and **117.915 s (85.1%)** versus V242. These are descriptive
across Spot instances, though exact graph identity isolates the
construction change from topology. `/usr/bin/time` recorded 21.06 s
wall, 139.59 s user CPU, 666% CPU utilization and zero swaps. It met
the frozen at-most-55 s and 600,000,000 B gates.

Both V243 and V244 raw artifacts were authenticated against terminal
receipts, and V244's sealed raw bytes matched its uploaded raw. Replay
found **1,000/1,000 identical ordered PQ result lists** and
**1,000/1,000 identical exact-FP16 diagnostic lists**. V244 returned
25,536/25,600 development GT100 hits, 74,231/74,400 held-out hits,
**99,767/100,000** combined, p05=99, and p95 base visits 23,030.
Loaded eight-worker in-process p50/p90/p95/p99 was
5.730/6.655/6.915/7.368 ms, 1,363.1 QPS, peak serving RSS
211,501,056 B and zero vector-body GETs. These are not network
product latency or a vendor comparison.

The instance ran from 21:51:02 to terminal upload 21:58:50 UTC
(468 s). At the eu-central-1c Spot quote of $0.3676/hour, compute
was approximately **$0.0478**, excluding EBS, S3, termination tail
and billing rounding.

**Next gate:** one frozen ReLAION-1M D768 build, exact GT100 and
same-revision in-process serving comparison with V219, then an
end-to-end product comparison if quality and resources pass. Build
memory still includes the authenticated FP16 plane, one F32 source
plane, graph and eight 4N-byte visit arrays. Do not infer 10M/100M
RSS, recall or time from this 100k cell. Lean can establish conditional
operation and allocation bounds under explicit assumptions; measured
recall, latency and allocator/RSS remain empirical gates.

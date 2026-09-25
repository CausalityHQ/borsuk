# V243 batched graph build, ReLAION-100k closeout

**Decision: fail the frozen build-time gate and stop this batch design.**
The candidate was deterministic across one and eight workers, retained
paired quality and reachability, and reduced build time by 42.8% from
V242. Its eight-worker Rust build took **79.243 s**, above the
preregistered **70 s** ceiling. The candidate API and worker path are
removed after this closeout; the serial V242 builder remains selected.
No 1M promotion, 10M/100M inference or vendor performance claim follows.

The sole `causality` c7i.4xlarge Spot cell was `a0001` on
`i-0198d5bada812e8f1` in `eu-central-1c`; it is **terminated**.
Source commit `a0f313e55eb3614edde0b61d99f36ddaf57ad308`, archive
SHA-256
`ab589d39da4660cf6e72f8b782f91cd4d3422803a10dee0bbe0284f592de5e39`,
terminal SHA-256
`9e03704084abb3f39c73b6cd78e2df2f581877a6aaf678ecb151245dce2db90b`.
The terminal exited zero. The original launcher replayed all 13
artifact lengths and SHA-256 hashes and confirmed termination. Sealed
raw SHA-256:
`c6f4c234d9829db0da69c0fb3514cd1f4552dc4bdf7e4baf62fa5110acf6b6cf`.
Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v243-batched-graph-100k/a0f313e55eb3614edde0b61d99f36ddaf57ad308/runs/a0001/`.

Frozen source: ReLAION-100k D768 cosine k=100, prior-used development
queries 0–255 and method-held-out 256–999, identical authenticated
source/plane/PQ/request roster to V242. M=32, M0=64,
ef_construction=128, PQ ef/FP16 shortlist=2048/2048. The candidate
searched each deterministic batch against one frozen adjacency and
committed proposals in node order. The one- and eight-worker graphs
both have SHA-256
`498ab9f5a3da7c672ccf362d4ae5c96a853b7d4f9f2d6fe9d7708f0250045d03`
and 26,572,470 B. The narrow release thread-independence test
passed 1/1. Structure: 100,000/100,000 reachable, minimum in-degree
four, zero rows below four in-edges, maximum degree 98 and 6,463,421
directed base edges.

| Verified build | Rust build time, s | Peak builder RSS, B | Graph SHA-256 |
| --- | ---: | ---: | --- |
| V242 serial control | 138.640 | 531,288,064 | `d8b70919…af2f` |
| V243 batch schedule, one worker | 134.968 | 531,111,936 | `498ab9f5…45d03` |
| V243 same schedule, eight workers | **79.243** | **533,233,664** | `498ab9f5…45d03` |

The eight-worker arm was **59.397 s (42.8%)** faster than V242 and
missed the 70 s gate by **9.243 s**. Its `/usr/bin/time` wall build was
1m19.57s, with 132.73 s user CPU and **167%** CPU utilization; neither
build swapped. This is consistent with a substantial serial commit
fraction, but the exact time split was not instrumented and remains an
inference. Worker count changed neither graph bytes nor structure.

The eight-worker graph returned 25,536/25,600 development GT100 hits,
74,231/74,400 held-out hits, **99,767/100,000** combined and p05=99.
V242 had 99,771/100,000, so this candidate lost four GT100 hits.
Only seven of 1,000 queries changed their ordered top-100 PQ lists;
the result-set overlap was 99,993/100,000. Exact-FP16 navigation had
99,782/100,000 hits versus V242's 99,786. Loaded eight-worker
in-process p50/p90/p95/p99 was 5.750/6.697/6.917/7.352 ms,
1,367.1 QPS, p95 base visits 23,030, peak serving RSS
211,693,568 B and zero vector-body GETs. These are one-host serving
measurements, not network product latency or a vendor comparison.

The instance ran from 21:33:47 to terminal upload 21:44:57 UTC
(670 s). At the eu-central-1c Spot quote of $0.3676/hour, compute
was approximately **$0.0684**, excluding EBS, S3, termination tail
and billing rounding.

**Root-cause decision:** the frozen-snapshot search parallelized, but
edge commits still ran sequentially and achieved only 1.67 effective
CPUs during graph build. The remaining work needs a different
construction/update representation with owner-partitioned commits or
another source-only parallel graph algorithm, rather than a different
batch-size parameter on this used query panel. First count or profile
the serial connect share, then preregister one material 100k falsifier
with paired quality, deterministic output, time, RSS and cost gates.

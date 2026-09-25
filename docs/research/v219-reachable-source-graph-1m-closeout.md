# V219 reachable source graph ReLAION-1M closeout

**Decision: pass the frozen 1M gate; select ef=4,096 / FP16 shortlist=4,096
as the smallest passing arm.** This is an in-process candidate for one
same-revision end-to-end serving gate, not a measured network product win.

The sole valid `causality` c7i.4xlarge Spot measurement was `a0002` on
`i-03e3e09fd27664770` in `eu-central-1c`; it is terminated. Source
commit `008ab6fbc50e6293e0599a33993c619702109bd9`, archive SHA-256
`3ab806cd7b15ecdc50b53757c1d33c808cbe7c60365091e1156147ecdbdef1a4`,
terminal SHA-256
`782fe56ee77a7f16c39e77a6012da63d899e201a694984d919952e894cc90180`.
The terminal reports complete and exit zero; all 13 artifact byte counts
and SHA-256 hashes passed independent replay. The raw returned IDs were
sealed before the V198 GT100/V199/V155 witness opened. Sealed raw SHA-256
`ccc29dd912248c6bc86c49bdcd86bfc3cd28534d37e05690102425df2c3bcab9`
was separately replayed. Evidence is under
`s3://borsuk-bench-453182569524-euc1/research/v219-reachable-graph-1m/008ab6fbc50e6293e0599a33993c619702109bd9/runs/a0002/`.
The discarded `a0001` launch was terminated before a terminal or
measurement after EC2's transient instance-visibility error; its
launcher regression is fixed in the `a0002` source.

Dataset and split: **ReLAION-1M D768 validation ordinals 0–999,
already used**, k=100, exact GT100. Paired strongest BORSUK V199 has
99,605/100,000 hits and p05 98; V155 has 99,567/100,000. The source
graph uses M=32, M0=64, ef_construction=128 and V218's generic
physical-order cycle plus incoming-edge repair. The authenticated base
layer has **1,000,000/1,000,000 reachable** rows, minimum in-degree
four, zero rows below four in-edges, 64,927,667 directed edges and
maximum outgoing degree 256. The V217 old graph had 37,780 unreachable
rows and 70,473 below four in-edges. V219 graph SHA-256 is
`a2805a97c1955adf1cdc0b0da43b4ff205eb1d4646916c09d3c4d2b9f7c1ee0b`.

| Frozen ef / shortlist | GT100 hits / 100,000 | p05 hits/query | Loaded eight-worker p50/p90/p95/p99 ms | Completed QPS | p95 base visits | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 2,048 / 2,048 | 99,509 | 98 | 6.775 / 9.269 / 9.938 / 10.967 | 1,081.1 | 34,349 | quality fail |
| **4,096 / 4,096** | **99,664** | **98** | **13.365 / 17.697 / 18.911 / 20.737** | **555.6** | **57,688** | **pass, selected** |
| 8,192 / 8,192 | 99,767 | 99 | 26.651 / 33.951 / 36.155 / 38.842 | 280.3 | 95,709 | pass |
| 16,384 / 16,384 | 99,822 | 99 | 53.429 / 65.709 / 69.457 / 74.644 | 141.0 | 156,063 | pass |

The selected arm gains 59 GT100 hits over paired V199, with 119
query wins, 793 ties and 88 losses. Every arm used zero vector-body
GETs. Peak serving RSS was 2,023,636,992 bytes, including
312,621,756 bytes loaded graph heap, 1,544,000,000 bytes FP16 plane,
64,000,000 bytes PQ64 codes, 4,000,000 bytes PQ cosine norms,
8,000,000 bytes physical map and 32,000,000 bytes of eight workspaces.
Cold hydration took 3.223 s. These request latencies are whole
**in-process** timings; they exclude network admission, generation
changes and external object-store service. The V199 latency values
were internal advancement ceilings from a different serving scope,
not matched product latency comparators.

Graph build took 2,665.254 s by Rust timer (44m28.72s wall), peaked
at 8,394,833,920 bytes RSS and produced a 266,910,966-byte artifact.
Source preparation took 18.31s wall and peaked at 7,954,759,680
bytes RSS. The instance launched 2026-09-25 11:41:53 UTC and its
terminal landed at 12:32:44 UTC, 3,051s later. The launch-time Spot
quote was $0.3631/hour, implying **about $0.308 compute**; this is
an estimate excluding EBS, S3, termination tail and billing rounding.

The structural, paired quality, p05, loaded p95/p99, throughput, 3 GiB
RSS and zero-GET gates passed for the selected arm. The production
architecture decision is to retain the source-only reachable graph,
PQ64 cosine navigation and authenticated FP16 rerank at this frozen
4,096/4,096 operating point for the next gate. Memory is reported by
its actual graph, plane and workspace components; no vector-count
cutoff or 100M RAM ceiling is inferred from this 1M observation.

**Next gate after the authorized restart:** one same-revision
end-to-end service run on this validation-1000 panel, recording raw
per-query recall@100, p50/p90/p95/p99, concurrency, throughput,
cache state, bytes/GETs, peak RSS, hardware/region and full cost. Run
a direct S3 Vectors cell on the same corpus, split and k, disabling
client response caches and recording both products' actual cold and
reuse semantics. BORSUK's FP16 plane is resident after hydration;
opaque S3 Vectors server caching cannot be assumed equivalent. Label
any unequal cache condition rather than claim a matched latency win.
The older authenticated S3 Vectors development-split
first-pass p90 193.867 ms is historical context only. Turbopuffer has
no authenticated matched tenant result; its published cold p90 is
context, not a win. Only after that product gate qualifies should a
frozen 10M scale cell be launched. Lean can prove construction and
memory/operation bounds under explicit assumptions, but observed
recall and wall-clock latency still require measured data.

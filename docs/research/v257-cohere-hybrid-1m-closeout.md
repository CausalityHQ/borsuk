# V257 CoHere first 1M: hybrid fails exact quality

The frozen V256 hybrid transferred to CoHere-large-10M canonical first
1,000,000 rows, D768 cosine, k100, on the authenticated prior-used test
panel: development ordinals 0–255 and validation 256–999. The production
decision is **no promotion**. Its loaded query resource gates passed, but
both split quality gates, both p05 gates and the combined exact-hit gate
failed. These are in-process measurements, not client HTTP or a matched
S3 Vectors or Turbopuffer comparison.

The immutable completed attempt is source commit
`98861a488218510f6362d589c99879b1ecc2a97a`, archive SHA-256
`e914047838e67b8e047e8e70921eb17b01e5a63629c55524346401210f4d3ba8`,
Spot `i-0ffad142dc93a5249` in `eu-central-1c`, now terminated. Its
[terminal](s3://borsuk-bench-453182569524-euc1/research/v257-cohere-hybrid-1m/98861a488218510f6362d589c99879b1ecc2a97a/runs/a0002/terminal.json)
has SHA-256
`1dcbc9aed01ad6ccda95d0b8e4a83db7e626a78cba9c6ede2c12c8c3277d3027`,
status `complete`, exit 0. The original launcher replayed every artifact
size/hash and confirmed EC2 termination; independent local replay used
sealed IDs SHA-256
`66f2799fc0a57dd60089a02e8b9a4fc400d18cca3d3a99a9b024334edf2c192b`,
truth SHA-256
`62e14eba043fafb8d8ec7c833d7d320c5d823c549683d15e5eacdff365a87f39`
and loaded raw timing SHA-256
`4e9510930e01aabb286fe0ff8317b43d2be7e462207e52266fadfdb758a9acd6`.
The frozen remote regression test for the 1M hybrid serving schema passed
before measurement (one test, zero failures).

| Method, same source and query panel | Development GT100 hits / 25,600; p05 | Validation GT100 hits / 74,400; p05 | Combined / 100,000 | Loaded p50/p90/p95/p99, ms | Loaded QPS | p95 PQ scores |
|---|---:|---:|---:|---:|---:|---:|
| V255 graph alone | 25,213; 93 | 73,504; 94 | 98,717 | 21.888 / 24.977 / 25.952 / 27.456 | 356.1 | 72,353 |
| **V257 graph + coarse PQ hybrid** | **25,404; 96** | **73,971; 97** | **99,375** | **44.320 / 47.924 / 49.140 / 50.871** | **178.98** | **90,311** |

V257 development mean R@100 was 0.99234375 and validation was
0.99423387, versus the frozen minimum 0.995 in **each** split, p05≥98
and combined≥99,500. It gained 658 net exact hits over V255 but still
missed 625. The same 1,000 sealed loaded raw samples independently
reproduced all four latency percentiles. Sequential p50/p90/p95/p99
was 46.903/50.937/52.364/54.690 ms; work p50/p90/p95/p99 was
69,857/86,294/90,311/97,313 PQ scores. Peak serving RSS was
2,056,728,576 B; cold hydration took 3.325 s; the resident FP16 plane
was 1,544,000,000 B. Query vector-body GETs were zero. Loaded p95≤60
ms, p99≤70 ms, QPS≥160, p95 work≤150,000 and RSS≤3 GiB therefore
passed. V257 did not proceed to an HTTP product gate.

The closed first attempt, source commit `80f9d35c1283e0161983e578b3d779c58aebd62e`,
Spot `i-07bd3db116577a500`, terminated before measurement at the
serving loader. Its [terminal](s3://borsuk-bench-453182569524-euc1/research/v257-cohere-hybrid-1m/80f9d35c1283e0161983e578b3d779c58aebd62e/runs/a0001/terminal.json)
has SHA-256
`e1cef23d26f9d3fb96276541084de06ec6ba09bd6b75206267518fc958530f06`,
status `failed`, phase `serve`, exit 1. The root cause was the loader's
wrong 100k graph-schema guard; commit `98861a48` repaired it. Attempt
`a0002` verified and reused the closed `a0001` source/graph/coarse
artifacts, avoiding a duplicate build. `a0001` preparation took 93.31 s
at 7,413,228 KiB peak RSS; diverse graph build took 1,950.95 s at
5,195,380 KiB; coarse build took 492.94 s at 9,040,664 KiB. Each
reported zero swap. These are reused `a0001` costs, not new `a0002`
build measurements. Exact truth scoring in `a0002` took 19.65 s at
15,125,200 KiB peak RSS.

`a0001` ran from 11:22:36 to its 12:12:57 UTC terminal; `a0002` ran
from 12:18:39 to its 12:28:58 UTC terminal. At the recorded
$0.3663/hour Spot quote, approximate compute through those terminals
was $0.3074 and $0.0630 respectively ($0.3704 total), excluding EBS,
S3 and cleanup tail. Both instances are terminated.

## Post-terminal candidate ceiling diagnostic

This is descriptive analysis of the **closed** artifacts, not a frozen
measurement arm. For each of the same 1,000 queries, a read-only replay
recomputed the top32 source-trained centroid route, formed the union of
its postings and V255's graph final k100, then counted exact GT100 IDs
anywhere in that candidate set. It used only the authenticated closed
centroids, offsets, postings, V255 graph raw IDs, requests and exact
truth. The route alone contained 89,304/100,000 GT100 IDs; the full
route-plus-graph union contained **99,411/100,000**, development
25,412/25,600 (p05 96), validation 73,999/74,400 (p05 97). V257
returned 99,375, just 36 below this candidate ceiling. Routed unique
rows were 16,706 at median and 21,748 at p95. Thus exact FP16 rescoring
of the **same** candidates cannot meet the combined 99,500 or split/p05
gates. The primary failure is candidate coverage at 1M, not merely PQ
ordering of candidates. A separate 33-query global-PQ sample contained
3,281/3,300 GT IDs in its top8192, but that sample does not establish
a full-panel global-PQ quality or latency result.

**Next gate:** make a material candidate-generation/index change,
preregister one cheap 100k falsifier, and admit another 1M campaign only
if that design clears quality and bounded-work gates. Do not tune the
same fixed top32 route or treat this in-process latency as product
latency. The 10M and competitor comparison gates stay closed.

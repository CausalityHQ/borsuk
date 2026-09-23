# PQ192 returned-ranking G0c closeout

Status: complete, **stop PQ192 as the sole final scorer on the fixed 100k
code-wave roster**. This is an offline rank-fidelity test, not a query-path
S3 performance result.

## Authority and method

- ReLAION-100k **development**, the same 1,000 GT100 queries and exact same
  fetched candidate rows as G0/G0b. Source commit
  `d0c9b801c2c107008419a64dd31c54465fff9220` on `origin/main`;
  source archive SHA-256
  `bceab606173b3e2745d046081bb90d4318bd85a13623f2a577608aba5d3904b3`.
- Immutable attempt:
  `s3://borsuk-bench-453182569524-euc1/research/native-pq192-returned-g0c/d0c9b801c2c107008419a64dd31c54465fff9220/runs/relaion-100k-dev1000-a0001`.
  Complete terminal SHA-256
  `897084a09abafee8ddcbab51d48729e97e054940dfd857e0df8daa35483e2df3`.
  The original controller exited 0, authenticated all nine terminal-listed
  artifacts and their cross-file result/code/evidence/validation bindings.
  A separate S3 readback verified all nine SHA-256 digests and byte counts.
  Remote independent validation reconstructed codebooks/codes from the
  authenticated source, checked each physical row's centroid assignment,
  recomputed all 1,000 rankings, and reported `valid: true`.
  Spot instance `i-0b94a844e597f45e0` (`c7i.8xlarge`, eu-central-1c)
  is terminated.
- Corpus-only PQ192x8 training used the predeclared V65 Lloyd rule:
  192 four-coordinate subspaces, 256 centroids, 10 iterations, seed 6501
  plus subspace ordinal. The 192-byte codes and 786,432-byte raw codebooks
  are preserved under authenticated artifacts; the code artifact is ordered
  by the sealed physical source permutation. Source ordinal ties were
  deterministic. The exact-score control reproduced grouped truth
  containment on every query.

## Measured result

| ReLAION-100k development, 1,000 GT100 queries | PQ192 score | Exact float32 on same fetched rows |
| --- | ---: | ---: |
| Returned Recall@100 | **88.433%** | **98.590%** |
| p05 returned GT100 hits/query | **79** | **93** |
| Queries below 90 hits | **500** | **31** |
| Paired lost GT100 hits/100,000 | **10,157** | 0 |
| p95 paired per-query loss, hits | **18** | 0 |

The preregistered gate allowed at most 250 paired lost hits (0.25 point)
and a PQ p05 within one hit of exact. Both fail widely: paired mean loss is
**10.157 percentage points**. The same-row exact ceiling of 98.590% also
remains below the 99% product target. Historical G0/G0b on these rows
returned 90.295% with the reconstructed-norm two-bit score and 88.464% with
its stored-norm variant; neither passed. V65's earlier 1M PQ192 result was
**512-row shortlist containment after exact rescoring**, not PQ-only returned
Recall@100, so G0c does not contradict it.

The fixed 32-group plan would transfer at most **16,040,144 B/query** at
208 B/row including a conservative 16-B stable ID, under the 16,777,216-B
cap. This is authenticated arithmetic over a prior plan, **not** a new S3
GET measurement. The remote evaluation took 8:11.71 with 2,143,740 KiB
maximum RSS; validation took 12:06.23 with 2,146,068 KiB maximum RSS.
Both had zero swaps. Sampled worker-tree peaks were 2,200,567,808 and
2,203,643,904 bytes, below the 3-GiB cap. Terminal elapsed time was 1,260
seconds. These are offline worker resources, not serving latency or QPS.

## Decision

Stop PQ192 final ranking on this fixed wide roster. Three tested ~200-B
score/record choices now lose 8.295, 10.126 and 10.157 paired points to the
same exact control. This does **not** prove every possible 200-B representation
fails on every distribution, and it does not rank PQ192 on a better candidate
layout. It does establish that promoting these code-only final scorers to a
1M serving gate would repeat a known local failure.

The next product design needs both a precise final-score path and better
physical routing. The strongest relevant historical route uses 1M
development/validation PQ64 resident codes and SQ8 pages, with V75 validation
99.272% returned Recall@100 at 21 GETs, 11.0 MiB and p95 137.9 ms, but its
64-B/row full resident router violates the 100M memory envelope and its
full-row scan violates scale throughput. A bounded hierarchical route plus
SQ8 or exact reranking must be tested under a frozen layout, including
generation-rollover memory and measured one- or two-wave latency. A proposed
dual-precision one-wave path remains a hypothesis and needs an offline
containment/rank-margin gate before any new object build. Lean's proven
byte/margin implications require authenticated per-query and service-time
premises; the three measured ranking failures cannot be proved away.

For the 3-GiB resident target, the Lean two-generation arithmetic is sharper:
if two full 100M-row resident generations coexist and 64 MiB is reserved for
the process, **each generation's row representation must be at most 15 B/row
before all other metadata**. This conditional bound rules out simply
carrying V75's 64-B/row router into that rollover model. Shared generations,
off-memory code access, a narrower route, or a revised memory target require
their own explicit correctness and performance evidence.

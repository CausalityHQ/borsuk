# Stored-norm G0b closeout

Status: complete, **stop both tested 200-byte two-bit final-score formulas**.
This is a paired offline returned-ranking result, not S3 serving latency or
throughput. The ReLAION-100k development cohort remains burned for design.

## Authority

- Source commit `915e5a2eea297ccb07f7765c4d4614f2d51f99de` on `origin/main`;
  archive SHA-256
  `d58eb6e17dddd0ee981cedde3c743caae7c0606a1a944acfa73ec7be15b135f0`.
- One immutable Causality Spot attempt:
  `s3://borsuk-bench-453182569524-euc1/research/native-two-bit-norm-g0b/915e5a2eea297ccb07f7765c4d4614f2d51f99de/runs/relaion-100k-dev1000-a0001`.
  Complete terminal SHA-256
  `f25263ca73340aab9286c781a4c815ff4e4d55137e153b2f5bb52a87319535be`.
  Original controller exited 0 and read back all seven authenticated
  artifacts. A separate readback verified their SHA-256 and lengths. The
  remote independent score/rank validator reported `valid: true`. Instance
  `i-0ca8727fb8a18f16a` (`c7i.8xlarge`, eu-central-1c) is terminated.
- ReLAION-100k **development**, 1,000 fixed GT100 queries. All three arms
  returned 100 distinct stable IDs from exactly the same closed code-wave
  group ranges and source rows. The old terminal, source, membership, codes,
  queries, truth and evidence were authenticated. Each stored four-byte norm
  matched the centered source-vector norm after float32 encoding for every
  one of the 100,000 source records. Exact-arm hits matched old fetched-group
  containment on all 1,000 queries (zero mismatches).

## Measured paired result

| ReLAION-100k development, 1,000 GT100 queries | Historical reconstructed norm | Stored exact norm, same two-bit dot | Exact float32 on same rows |
| --- | ---: | ---: | ---: |
| Returned Recall@100 | 90.295% | **88.464%** | 98.590% |
| p05 returned GT100 hits/query | 82 | **79** | 93 |
| Queries below 90 hits | 335 | **519** | 31 |
| Paired net loss versus exact, hits/100,000 | 8,295 | **10,126** | 0 |
| p95 paired per-query loss, hits | 14 | **17** | 0 |

The historical primary reproduces G0 exactly, confirming a matched replay.
Using the stored norm increases the mean loss to **10.126 percentage points**
and lowers mean returned Recall@100 by **1.831 points** relative to the old
score. The preregistered G0b gate allowed at most 250 paired lost hits
(0.25 point) and p05 no more than one below exact. It fails widely. The
same-row exact ceiling, 98.590%, is itself below the 99% product target;
routing/layout must also change. The old planned code-wave maximum remains
32 GETs and 15,423,240 bytes/query; G0b did not issue query-path S3 reads.

Remote evaluation took 7:55.24 wall time with 1,539,024 KiB maximum RSS;
independent validation took 6:00.27 with 1,539,388 KiB maximum RSS. Both
reported zero swaps. Sampled worker-tree peaks were 1,450,647,552 and
1,449,000,960 bytes, below the 3-GiB cap. Terminal elapsed time was 876
seconds. These are offline worker resources, not serving QPS or p95 latency.

## Decision

The exact norm field was authentic, but substituting it did not repair the
two-bit dot estimate; the old coded norm evidently compensated some of that
error on this cohort. This is an inference from the paired measurements, not
an identified per-row error decomposition. Stop both tested code-only final
scores. Do not promote either to the suspended 1M route gate.

The next cheap decisive gate is a frozen-roster **rank-error tolerance and
physical-byte worksheet** before building another object: quantify the
actual score-error/margin distribution and exact same-row hit ceiling at
100k, then compare candidate representation widths on a newly frozen 1M
physical plan. A simulation alone cannot certify a new code; it can reject
widths and route plans whose conditional best case misses the target. Any
survivor needs measured returned ranking, physical containment, GET/byte
accounting, and a separate 100M rollover-memory proof/measurement. The
existing Lean bounds remain conditional and cannot supply unseen data or
network premises.

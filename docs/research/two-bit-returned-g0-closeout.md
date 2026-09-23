# Two-bit returned-recall G0 closeout

Status: complete, **stop the 200-byte rotated two-bit code as the sole final
scorer**. This is an offline representation test, not a serving latency or
throughput measurement and not a new product baseline.

## Authority and method

- Dataset/split: ReLAION-100k **development**, the fixed 1,000-query GT100
  cohort. No validation or new holdout was opened.
- Source commit: `9cf22cad5eba477b688806327f14fa248c8afb2b` on
  `origin/main`; source archive SHA-256
  `a394bca734a17b72a26a430d5197a60f483c7719cf9d2cb3cc9f93ecc38aefb8`.
- Immutable Spot attempt:
  `s3://borsuk-bench-453182569524-euc1/research/native-two-bit-returned/9cf22cad5eba477b688806327f14fa248c8afb2b/runs/relaion-100k-dev1000-a0001`.
  Its complete terminal SHA-256 is
  `80bc90f0f45f9511c67c5ed4914184bcc4c83e4e0c91951fc22f297e7d864f51`.
  The original controller exited 0, read back and authenticated all seven
  terminal-listed artifacts, and terminated Spot instance
  `i-05c23952055b258df` (`c7i.8xlarge`, eu-central-1c). An independent
  controller-side S3 readback verified the same seven SHA-256 digests and
  byte counts. The remote second implementation validated every returned
  per-query count and the summary (`valid: true`).
- Candidate rows and GET/byte plans came from the *closed prior* 100k
  code-wave terminal, SHA-256
  `82875585b8854d0a5f20ff4ebafe629e50ff64808ae8d0f577a1e1935e9caf19`.
  Both arms score exactly these fetched rows and return 100 distinct stable IDs,
  breaking float32 ties by source ordinal. The exact arm uses source vectors;
  the primary arm uses the historical 200-byte rotated two-bit records.
  Source, query, truth, membership, code and old evidence identities were
  authenticated before use. All 1,000 exact-arm hit counts equaled the closed
  fetched-group containment counts, so the old ID mapping was reproduced.

## Measured result

| ReLAION-100k development, 1,000 GT100 queries | 200-byte two-bit score | Exact float32 on same fetched rows |
| --- | ---: | ---: |
| Returned Recall@100 | **90.295%** | **98.590%** |
| p05 returned GT100 hits/query | **82** | **93** |
| Queries below 90 returned GT100 hits | **335** | **31** |

The paired net loss was **8,295/100,000 GT100 hits**, or **8.295 percentage
points**; median per-query loss was 8 hits and p95 paired loss was 14 hits.
The primary was worse on 987 queries, equal on 13, and better on none. The
preregistered G0 limits were at most 0.25 percentage point paired mean loss
and primary marginal p05 no more than one hit below exact. Both fail widely.

The fixed physical plan averaged 76,708.93 candidate rows, 32 GETs and
15,342,423.352 code bytes/query (median 77,107 rows and 15,422,040 bytes;
maximum 77,113 rows and 15,423,240 bytes). These are historical planned
code-wave quantities carried into an offline replay, **not** new S3 serving
measurements. The prior actual-read page-containment comparison, 98.418%
primary versus 98.515% exact, was a different metric; it did not predict
code-only returned ranking. The exact arm here is a same-row returned-ranking
ceiling, not the prior exact page-nomination arm.

The remote replay took 7:12.96 wall time with 1,539,272 KiB maximum resident
set; independent validation took 6:24.41 with 1,539,320 KiB maximum resident
set. Both reported zero swaps. Sampled process-tree peaks were 1,421,828,096
and 1,399,631,872 bytes, below the 3-GiB worker limit. Terminal elapsed
time was 856 seconds, including setup and both phases. These are **offline
worker resource measurements**, not query p95 or QPS.

## Decision and next gate

The representation loses true neighbors during final top-100 ranking even
though they are present among fetched rows. The next design must change
final-score information, not merely page routing or GET scheduling. The
current single-wave code-only serving contract is stopped at G0; its planned
1M routing promotion is withheld. A new 100k paired representation screen
must hold the exact fetched-row roster and truth fixed, record code width,
reconstruction/rank fidelity and a byte/memory worksheet, and compare the
strongest same-row exact control. Only a representation that clears a
preregistered returned-ranking gate may enter a new physical layout and 1M
route test. Lean can prove conditional bounds under explicit score-error,
margin, byte and service-time premises; this measured miss cannot be turned
into a proven recall or latency guarantee by assuming the failed premise.

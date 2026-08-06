# Realistic durable-write attempt ledger

This architecture qualification measures durable inserts on the official
Cohere Medium 1M corpus (1,000,000 vectors, 768 dimensions, cosine distance).
Scalar acknowledgement latency and batched throughput are separate workload
points. While an attempt or cell is incomplete, inspect only terminal markers,
process/service health, resource availability, and non-measurement progress;
never inspect its measurement CSV files.

| Attempt | Revision | Dataset descriptor SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|---|
| preparation | `3040883` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | not applicable | `s3://borsuk-bench-453182569524-euc1/research/datasets/cohere-medium-1M/preparation/3040883/` | not applicable | terminal dataset validation passed: 1,000,000 train rows, 768 float32 dimensions, 1,000 aligned test/ground-truth rows, 1,000 neighbours per query, aggregate source SHA-256 `c0c572f0265181a182ae904383f97d0e3137521eb52bd3c05d1a3935bab0273b`; no measurement campaign launched |
| v1 | `6f5ba5d` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v1/index/` | terminal preflight failure before output creation or measurement: unprivileged service could read the frozen dataset but the launch-time validator attempted to rewrite root-owned `meta.json`; service exited 1, no result/index object or measurement artifact was created, and host memory/disk remained healthy. Replaced by a read-only frozen dataset check. Source archive SHA-256 `6455f4a40c53debbeec9a8c9f10fb1dd3a1d9280c41222b6f53d285246e99eef`. |
| v2 | `08905a5` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v2/index/` | terminal pre-build failure before any measurement: the service account could not create files in the root-owned shared Cargo target directory `/data/target`; service exited 101 and the root failure marker was written. The fail-closed validator rejected the incomplete campaign before measurement inspection. Replaced by a private run-owned Cargo target. Source archive SHA-256 `0ee4d953795845ac40d01de9f64bd6c1ab68ae0ff15a29436ef9211d3d979ed1`. |
| v3 | `08905a5` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v3/index/` | terminal base-build failure before any write cell or measurement: the 1M-row/768D global-PQ build rejected its inferred Arrow identity-buffer layout as not range-addressable. The root failure marker was written, no cell marker exists, and the fail-closed validator rejected the campaign before measurement inspection. Host memory and disk remained healthy. Source archive SHA-256 `0ee4d953795845ac40d01de9f64bd6c1ab68ae0ff15a29436ef9211d3d979ed1`. |
| v4 | `7165204` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v4/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v4/index/` | terminal production-gate failure at the first `cohere-medium-1M/r01/b1` cell. The format-v22 1M-row/768D base build completed in 533,764.076 ms and compaction in 175,700.864 ms, proving the preceding Arrow identity-layout blocker fixed. The 100 scalar durable calls then took about 21m16s wall time and emitted `CELL_COMPLETE` and `INSERT_VISIBILITY_COMPLETE`, followed by the exact write-p95 failure marker and root failure marker. The service exited; memory and disk remained healthy. The fail-closed root validator rejected the missing success marker and the terminal-cell validator rejected the root failure marker, so no measurement CSV was opened and no numeric p95 beyond at least 200 ms is defensible. Source tracing found that insert-only duplicate validation rebuilt the growing vector WAL tail on every call; format v23 replaces that quadratic path with hash-partitioned compact ID-directory lookup. |
| v5 | `dcff032` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v5/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v5/index/` | terminal diagnostic stop at the first `cohere-medium-1M/r01/b1` cell. Format v23 removed vector-WAL-tail decoding, but after 18 minutes the 100-write process had consumed 16m40s CPU and about 286 GB of process reads while remaining non-terminal. The host retained 58 GiB available memory and 92 GiB free disk. The process was terminated to avoid wasting paid time; the runner preserved exact cell/root failure markers and exited. Both fail-closed validators returned 2 before measurement parsing (`campaign is incomplete`; terminal-cell scope `campaign has a failure marker`), so no CSV was opened. A deterministic production-shape reproduction found the 1,024-bit segment ID bloom admitted 10,000/10,000 absent IDs at 5,461 records, forcing duplicate validation to decode many immutable base segments. Format v24 expands segment/routing blooms while retaining compact tombstone blooms. |
| v6 | `02cde87` | `54c733e39adfcaa9ee10f3ed8bd8e66ada9f8f9a1a73e9753f5c5c2044b79254` | `2c9b9df7ef5c232f92f6bc9d135774070df9998c74fa31307bef0a43977ffdfe` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v6/results/` | `s3://borsuk-bench-453182569524-euc1/research/realistic-durable-write/20260805-v6/index/` | terminal production-gate failure at the first `cohere-medium-1M/r01/b1` cell. Format v24 completed the 1M-row/768D base ingest in 547,853.308 ms and compaction in 176,468.804 ms, then completed 100 scalar durable inserts in 72,118.095 ms before the exact p95 gate wrote `CELL_FAILED` and root `REALISTIC_DURABLE_WRITE_FAILED`. The structurally complete cell validated and therefore permits its frozen artifacts to be inspected: 1.387 inserts/s, p50 549.329 ms, p95 1,028.593 ms, p99 1,667.283 ms, maximum 21,735.853 ms, 2,188 GETs, 893 PUTs, 64 DELETEs, 155 HEADs, and 4 LISTs. Ninety-nine non-maximum samples still averaged 508.833 ms. The ordinary steady-state patterns were 3--9 GETs and 8 PUTs; operation 64 synchronously performed 1,717 GETs, 101 PUTs, and 64 DELETEs during automatic materialization. The root fail-closed validator rejected the incomplete campaign and the terminal-cell validator rejected its failure marker, while the structural benchmark validator passed. Source archive SHA-256 `270b37f95f0b4c3c03e7a0e83e3555df3dc2145348695b28c8f2b9d0d18d7eb1`. This disproves the strict scalar path as the production ingest architecture; qualification moves to bounded group commit and off-acknowledgement materialization rather than launching an unchanged v7. |

The preregistered architecture qualification uses batches 1, 32, 128, and
1,024 over three repetitions with 100 raw durable batches per cell. It is
claim-ineligible until a committed source revision is archived, fresh prefixes
are recorded above, the root completion marker exists without a failure marker,
and the repository validator reconciles every frozen cell. Publication requires
a separate five-repetition campaign from the selected frozen revision.

## Local causal read-path checkpoints

These terminal local runs are architecture diagnostics, not AWS or publication
claims. They use the pinned Cohere Medium 1M source descriptor but a 16,000-row
mutation workload over a freshly cloned 1M-row/768D base, 32 writers, four
in-flight operations per writer, 16 records per operation, eight lanes, 2,000
logical cells, and 20 read queries per phase.

- Revision `771e29b`, format v26/global descriptor layout v2, terminal r16:
  recall@10 was 1.0; scalar durable-write p95 was 57.101 ms; acknowledgement
  throughput was 51,502 records/s; drain took 5.720 s; drain-inclusive
  throughput was 2,714 records/s. Post-drain read p95 was 165.893 ms and passed
  the 200 ms gate; active-tail read p95 was 202.953 ms and narrowly failed it.
  Compared with the preceding current-format diagnostic, post-drain requests
  fell from 114 to 78 and HEADs from 54 to 36; active HEADs fell from 36 to 18.
  The result proves one redundant routing walk was removed, but it does not
  qualify ingest because drain-inclusive throughput remains below 10,000
  records/s and it does not qualify reads because active-tail p95 remains above
  200 ms.
- Revision `d4b5ee8`, terminal r17, removed both remaining certified routing
  walks. The structurally reconciled local arm preserved recall@10 1.0 and
  measured 68.808 ms write p95, 48,494 acknowledged records/s, 97.701 ms
  active-tail read p95, and 3.137 ms post-drain read p95. Drain remained
  5.572 s, limiting end-to-end throughput to 2,772 records/s. A subsequent
  env-gated phase profile attributed 1.919 s to rebuilding the persisted
  fallback segment quantizer and 1.847 s to the separate global delta build;
  segment build/write was 1.001 s and the initial manifest publication was
  0.692 s.
- The next one-factor r19 invalidated the stale fallback quantizer
  in the segment publication and deferred its optional rebuild to maintenance.
  It preserved recall@10 1.0, 57.560 ms write p95, 99.021 ms active-tail read
  p95, and 3.952 ms post-drain read p95. Drain fell to 3.857 s and end-to-end
  throughput rose to 3,893 records/s. This is material but remains below the
  10,000 records/s gate; segment publication and global-delta construction
  remain synchronous duplicate passes. The full gate then exposed and fixed a
  latent metadata-only publication defect: root coverage versions advanced
  without advancing the nested delta certificate, which could hide delta hits
  after the certified routing-walk optimization. All base/delta append,
  promotion, and stale-generation regressions now preserve the nested coverage
  invariant.
- The following single-publication r20 built the delta from the active summaries
  already held by lane drain and published segments plus complete base/delta
  coverage atomically. It preserved recall@10 1.0, 59.467 ms write p95,
  101.041 ms active-tail p95, and 3.204 ms post-drain p95. Drain fell from
  3.857 to 3.398 s and end-to-end throughput rose from 3,893 to 4,371
  records/s. Direct in-memory delta encoding in r21 then produced the same
  descriptor checksum as the persisted-segment path and removed the
  object-store reread of every just-written segment. On the warm local
  filesystem it was only a small improvement: 3.353 s drain and 4,436
  end-to-end records/s, with recall 1.0 and 62.484/97.532/3.122 ms
  write/active-read/post-read p95. The remaining gap is duplicate segment and
  global-bundle encoding/writes, not routing discovery or local rereads.

## Post-r26 implementation checkpoint

The next local implementation factor adds caller-owned scratch buffers to the
SRHT/product-quantizer encoder and reuses the exact fixed-width row buffer in
the global-PQ spool. Focused byte-equivalence tests cover both reusable paths,
and the global-PQ test suite passes. A subsequent attempted r27 local rerun
was terminally rejected at post-reopen exact recall because it used a
previously materialized diagnostic index rather than a pristine base; its
partial measurement files were not inspected and it provides no numeric
performance evidence. A fresh-base terminal run is required before attributing
any throughput change to this factor.

The first fresh-base attempt then exposed a real scratch-buffer correctness
defect: padded SRHT coordinates were not cleared between vectors, so 768D
inputs (padded to 1,024) could contaminate later codes. The attempt failed the
post-reopen exact-recall gate; its partial measurement files were not
inspected. A regression now covers non-power-of-two dimensions and clears the
entire caller-owned rotation buffer before each transform. The fix requires a
new pristine-base terminal run.

The corrected terminal r33 used a freshly built 1M-row Cohere 768D base,
32 writers, 32 operations per writer, 16 records per operation, pipeline
depth four, and eight worker lanes. All five local phase markers were present,
the process exited successfully, and the source archive identity matched
`d382a64`. It preserved inserted-ID recall@10 1.0, write p95 59.045 ms,
active-tail read p95 81.341 ms, post-drain read p95 28.218 ms, and
drain-inclusive throughput 5,716.636 records/s. This is a terminal local
architecture result, not a 10,000-record/s pass and not 100M-scale or AWS
evidence; the paired five-repetition production campaign remains unlaunched.

The next unbenchmarked factor replaces hierarchical coarse assignment's
full-parent allocation and sort with a fixed four-entry distance/index
selection. Its ordering regression and all 18 global-PQ tests, plus the full
library, group-commit, and fault-injection gates, pass. It changes no persisted
layout or routing semantics; a fresh terminal throughput run is still needed
before assigning it a numeric gain.

Terminal r34 then measured the fixed parent selector from the source-identified
`5aa2c35` revision on a fresh 1M-row Cohere 768D base. All five local markers
were present with recall@10 1.0, write p95 61.875 ms, active-tail read p95
82.675 ms, post-drain read p95 12.770 ms, and drain-inclusive throughput
5,659.450 records/s. This is neutral to r33's 5,716.636 records/s within the
single-cell noise; the factor is retained for bounded allocation behavior but
is not credited with a throughput gain.

The next implementation slice overlaps lane segment persistence with global
delta construction. It assigns provisional checksums only to the concurrent
builder, then substitutes the checksums returned by the actual segment writes
before manifest validation/publication; the fallback remains sequential when
the prior coverage certificate is stale. Full library (498 passed),
group-commit (29 passed), fault-injection (12 passed), strict Clippy, and
formatting gates pass. No performance claim is made until a fresh terminal
arm verifies recall and manifest coverage.

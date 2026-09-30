# Source paging development contract

Authenticated closed-trace replay in 721d4b7c was independently reproduced by
parent: self-check passed, all 192 consumed development rows replayed, and JSON
was byte-identical. ReLAION FIRST1M D768 external development0–63 needs up to
51,712,000 source bytes under 128 GETs for lossless one-phase page closure.
Its 64-GET worst case is 83,814,400 bytes, exceeding 64 MiB. Both FIRST100k
ReLAION and CoHere development0–63 fit 32 GETs/32 MiB. Fresh CoHere 1M and
10M/100M geometry remain unknown. This is geometry replay, not measured recall,
latency, QPS, RSS or cost.

## Next development intervention

- Preserve graph discovery, sorted initial unit scores, adaptive completion,
  2544-unit ceiling, score ties, nominated pages and ordered SQ8 results exactly.
- Fetch the full 256-row page closure of initially walked units. Fetching only
  final SQ8 pages is forbidden: it changes nomination. Bridge smallest gaps
  using the existing lossless contiguous-cover implementation; drop no unit.
- Source allowance: at most 128 logical range GETs and 67,108,864 bytes/query;
  at most 16 concurrent source GETs. One score-independent fetch phase has
  bounded concurrency, so it can contain multiple transport rounds.
- SQ8 allowance remains separately at most 32 GETs and 16,773,120 bytes/query.
  Combined admission is at most 160 GETs and 83,881,984 payload bytes/query,
  excluding separately charged planning/codec/runtime/SDK overhead. Query slots,
  source buffers, source authority and retired generation pins must be charged.
- Reject source overflow before any query payload I/O; report source and SQ8
  counters separately, preserving source charges on later SQ8 failures.
- Local full-source reference and paged mode must return identical traces,
  physical SQ8 plans and ordered IDs on qualifying fixtures/panels. A digest,
  ETag, missing coverage or identity failure terminates the query; no retries.

These are preregistered development caps, not frozen production defaults. The
next native gate first verifies parity and failures, then measures cold HTTP
under the declared protocol. A quality/performance failure remains FAIL.
Current centroid/graph decode and resident scale remain independent bottlenecks;
source paging alone is not evidence of achieving the competitor product goal.

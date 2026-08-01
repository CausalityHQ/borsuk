# Logical-cell write-routing attempt ledger

The production matrix is eligible only when its immutable result tree contains
`LOGICAL_CELL_ROUTING_COMPLETE`, contains no
`LOGICAL_CELL_ROUTING_FAILED`, and the fail-closed validator accepts every
paired summary, raw append sample, resource file, and correctness gate. Partial
attempts never authorize performance claims.

| Attempt | Source SHA-256 | Manifest SHA-256 | Result prefix | Index prefix | Status |
|---|---|---|---|---|---|
| v1 | `ea7322911393bec64f3153328bc412806546047a593f02ea8498dd3ba2564de8` | `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v1/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v1/indexes/` | failed before index construction or measurement; after the release build, the runner treated `BORSUK_ROUTING_SMOKE=0` as smoke mode and rejected the production shape; `LOGICAL_CELL_ROUTING_FAILED` is terminal and the attempt is ineligible |
| v2 | `ff62ebb0641e9c115c0600f10eb1428e22d93fdadb37ee10b6b1f003b06bf8ef` | `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v2/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v2/indexes/` | failed before index construction or measurement; the first S3 index request used the SDK's `us-east-1` default and rejected the bucket's regional redirect; `LOGICAL_CELL_ROUTING_FAILED` is terminal and the attempt is ineligible |
| v3 | `ff62ebb0641e9c115c0600f10eb1428e22d93fdadb37ee10b6b1f003b06bf8ef` | `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v3/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260731-v3/indexes/` | failed during `c2000/r01/w32/flat` with `ConcurrentModification { path: "collection/wal-frontier/57/HEAD/CAPACITY" }`; `LOGICAL_CELL_ROUTING_FAILED` is present, `LOGICAL_CELL_ROUTING_COMPLETE` is absent, and the runner exited with status 1; the attempt is terminal, incomplete, and ineligible for performance claims |
| v4 | `74903033cb1bd9b5be61adc1c4d46e72449e9ab6822f1099efde49d98a0386eb` | `b07a617061245b3f60fe0f40948746fa0c2790c3e042b012a5c0c902e22644d1` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260801-v4/results/` | `s3://borsuk-bench-453182569524-euc1/research/logical-cell-routing/20260801-v4/indexes/` | running from clean revision `78a2f0a`, including the verified `2f50aa2` saturated-frontier admission fix; fresh result and index prefixes were empty, no competing benchmark process was present, and the identical preregistered five-repetition matrix launched on `c7g.8xlarge` in `eu-central-1` |

Each attempt's source, result, and index prefixes were verified empty before
upload or execution. Source archives and manifest objects use SSE-S3. Until a
running attempt reaches a terminal marker, inspect only marker and
infrastructure health; do not read partial CSV files.

The v3 failure was established from its terminal marker and non-measurement
campaign log only. No partial measurement CSV was inspected. It is production
correctness evidence that the 32-writer flat route can exhaust a frontier
shard's bounded mutable head; it is not latency or throughput evidence. A
future attempt must first resolve and regression-test admission at the
`CAPACITY` sentinel, then use fresh immutable prefixes and a new attempt id.

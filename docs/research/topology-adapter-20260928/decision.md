# Immutable graph adapter and paired replay: verified preparation

Base72e803a2. Added an offline,100k/D768-capped graph-variant builder and extended the existing offline plan demo with alternating paired dev0–63 replay. Production graph construction/compaction defaults, persistent formats and query algorithms are unchanged. No corpus quality query ran in this slice.

## Implemented and tested

- The adapter authenticates a current-v3 source root before output allocation, reads capped components, reconstructs the nearest graph byte-identically, and builds the optional diverse graph from the same centroid blob. Graphs must differ while their80-byte header and every node's level count remain identical.
- It copies fixed metadata into independent staging files and changes only graph_sha256 and graph_resident_bytes in the manifest. It validates the completed root through the normal loader and publishes the new directory with the existing Linux rustix NOREPLACE pattern. It never overwrites the source or a destination, or copies the caller-owned canonical source/SQ8 payload.
- Source-only reports include both graph byte counts, structural resident declarations, node/layer degrees, and build wall/process CPU nanoseconds. The hidden degree accessor returns caller-owned diagnostic payload; it does not alter graph construction/search.
- Paired mode loads both authenticated roots in one process, conservatively charges two1GiB generation allowances, alternates arm order by ordinal, and emits existing plans/traces plus arm/root SHA and route wall/process CPU nanoseconds. Validation pairing is rejected before root/query access. Ordinary output retains exactly its three original fields.
- Synthetic tests verify a distinct257-unit graph/root, unchanged original/component bytes, only two changed semantic fields, unchanged node levels and degree caps, normal candidate loading, destination/trust/corruption rejection;64 same-root queries produce128 alternating records with semantic trace/plan parity. Ordinary64 output is also checked. This proves mechanics, not distinct-root corpus quality or timing.

## Closed Causality AWS assurance

| Cell | Verified result | Instance and closure |
| --- | --- | --- |
| red/a0001 | Both named fixtures failed for the intended missing adapter stub and unsupported paired flag; Cargo101, expected-red worker0 | i-00ea83d0bb5ec799b, terminated177s |
| green/a0001 | Focused2 passes; full workspace/all-targets2684 passes,0 failures,26 existing ignored across147 targets | i-0d670a0f46778dae1, terminated921s |

Both cells used bounded c7i.2xlarge Spot in eu-central-1a,4 Cargo jobs,1500-second test/1800-second worker caps and80GiB encrypted disposable EBS. Unique source/reservation/tag guards and the shared lock prevented overlapping/repeated workers. Only terminal markers and EC2 health were monitored while live; closed artifacts were collected and reconciled afterwards. No local Rust builds/tests/queries or Spark work ran.

Red archive1f6687109c852fad0f8fb170a65e4c2dd2ad3883680250a7df130c6d6f01a704 and terminalf9727fc63015cb7a594bd011205c426ca6c5ef1147b84b2f1a9435ad5ba1b165 were independently checked before implementation. Green archive9e626bc95702276eabbe2f86f6431af5ebd9b889d8e4ed92a25380b11f2fe360 and terminal84c52b24209e6297b01076faeef700a60e8375a441f87d2bdac869413b4693bd were checked after completion. All391 Rust/Cargo files matched their respective frozen checkpoints, and each cell's four artifacts matched terminal SHA/length records. Compute estimates are$0.0105 red and$0.0545 green from observed Spot quote and elapsed wall, excludingEBS/S3; not invoices or product cost.

Narrow Opus ecbf51f941cd40f2 found no blocker for the correctness run. review-reconciliation.md records scope, required root-field/arm-hash controller checks, timing sensitivity and offline durability/platform limits. Authentication alone does not prove a topology-only comparison. No paid job or consultation remains active for this slice; default architecture review cadence remains intact.

## Next gate and product gaps

Execute controller-next.md: implement and AWS-test the current-v3 metadata/control/decomposition wrapper, preserve frozen scorer files, prove exact historical native-control component/plan/score identity, add root-field/arm-hash checks before paired queries, classify candidate geometry failures separately, and freeze full input/compiler/resource/spend identities before one ReLAION-first mechanism run. A scientific failure skips CoHere, validation and scale; no cap/seed sweep or automatic retry.

Existing ReLAION744 method-validation remains KILL at98.8508064516% returned recall@100 versus99.5685483871% exhaustive SQ8, a0.717742pp deficit above0.5pp. These are historical offline results, unchanged here. No new cold HTTP percentiles, QPS, physicalS3 request/RSS-swap or lifecycle-dollar result exists. Fresh confirmation, scalable source layout, post-maintenance quality/rewrite cost,1M/10M/100M and matched wins against BOTH S3 Vectors and Turbopuffer remain open. The full native product goal stays active; no operator decision is needed for the next preparation.

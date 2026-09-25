# V236 ReLAION-1M persisted graph collection closeout

**Decision: publication and reload gate passed.** The authenticated
single-root graph was published to a fresh S3 prefix, followed by a
collection revision 1 and one CAS mutation batch to revision 2. A
decoded revision-2 reader reloaded the graph from its authenticated
local cache and returned all 1,000 complete V230 ID lists exactly.
The held revision-1 reader remained unchanged. This establishes the
persisted-state path; it does not measure end-to-end HTTP latency.

Source commit `4fde270ac6a27833626701aa777c646fb2cff95a`, source
archive SHA-256
`a5c20a3bcfc3103042ac643f598a8b2645e06f9b466eb39ef8ff634fd4ee7a33`.
The sole `causality` c7i.4xlarge Spot host
`i-0da548b2b19417bd1` in `eu-central-1c` was independently
confirmed **terminated**. Original terminal SHA-256
`5f32aca133f8544d23f13e30e2f9c469ca87a2b3f626fdbc8cf4eb029989c7d8`;
closeout SHA-256
`bb6d7cf60d62ee06e486951fc193dc921d4f13c69a16654344312d9ce772da6c`.
The terminal exited zero; all seven artifact lengths and hashes and
both sealed raw streams passed independent replay. Evidence prefix:
`s3://borsuk-bench-453182569524-euc1/research/v236-graph-collection-1m/4fde270ac6a27833626701aa777c646fb2cff95a/runs/a0001/`.

Dataset: ReLAION-1M D768, validation queries 0–999 previously used in
method development, cosine k=100, ef/shortlist 4096/4096. The batch
upserted every 100th physical row under its same source ID and
authenticated FP16 vector (10,000 rows); the logical corpus and truth
were unchanged. Revision-1 IDs matched the sealed V219 base witness
1,000/1,000, and revision-2 IDs matched the sealed V230 mutation witness
1,000/1,000. Independent GT100 replay of revision 2 yielded 99,668
hits of 100,000: 25,509/25,600 in queries 0–255 and 74,159/74,400
in queries 256–999, with p05 99 hits/query. Revision-2 raw SHA-256 is
`4b794285552b8513e948c2d2cd63d182bec1edc495539e34400008c4603768fe`.

| Verified V236 observation | Value |
| --- | ---: |
| Graph root SHA-256 | `c59650ec920d031ff236f5ab47331db88b71fdac0462cd07a568d51c549b7caf` |
| Authenticated revision-2 mutation snapshot | 15,450,080 bytes |
| New decoded overlay owned bytes | 31,005,000 bytes |
| Cold / warm graph blob GETs | 5 / 0 |
| Cold / warm graph blob response bytes | 1,879,697,462 / 0 |
| Graph publish / cold hydrate wall time | 28.961 / 34.433 s |
| Mutation CAS publish / warm hydrate wall time | 0.372 / 4.804 s |
| Peak process RSS with both readers pinned | 4,346,863,616 bytes |
| Spot compute estimate to terminal | $0.03768 |

The loader's GET and response-byte counters cover graph blobs only.
Collection-head, root, and mutation-snapshot metadata requests were
not instrumented; the exact total S3 request count and transfer cost
are therefore unknown. The compute estimate excludes EBS, S3 and
billing adjustments. Both pinned readers currently own separate 1M
graph allocations, a likely cause of the 4.35 GB RSS peak; shared-base reuse
is a production memory opportunity, subject to root-identity checks.

**Next gate:** serve revision 2 loaded from this persisted state over
VPC-peer HTTP on a frozen 1,000-query panel. Compare complete IDs and
GT100 with V230/V233 and p50/p90/p95/p99, throughput, RSS, bytes/GETs
and cost with V233 at the same concurrency and cache state. Do not add
V236 in-process time to a separate network result. After that result,
decide whether shared-base reuse and mutation compaction meet explicit
quality, latency and memory budgets; no fixed vector-count knee.

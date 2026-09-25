# V195 used 1M rerank diagnostic: precision works, sidecar GETs fail

## Decision and authority

V195 is a **development-only** diagnostic on V194's now-used
ReLAION-1M D768 source pseudoquery SHA ranks 2945–3456. It did not
change the 511 feasible V194 optional-risk physical plans. The
predeclared separate FP16 sidecar layout **fails** its joint planned
byte/GET envelope. This result does not rescue V194 or advance a
production default.

One Causality Spot `c7i.12xlarge` cell `a0001` completed from source
commit `9b7bcc581887688247b4fab244a5dab8d37dd3c9`, source archive
SHA-256 `0f672316d84ef2e2ae9dd5a699f19112ca43cbb245351744b6ad5d7b96e23398`,
and instance `i-0dc2f66d74a5c0ba3`, now terminated. Its complete
terminal is at
`s3://borsuk-bench-453182569524-euc1/research/v195-used-1m-rerank-diagnostic/9b7bcc581887688247b4fab244a5dab8d37dd3c9/runs/a0001/terminal.json`,
SHA-256 `62a2a7b190983b5e422f679edf8a5120164f9ab886a3e5db5bd32eefc2abdc02`.
The diagnostic seal, raw per-query IDs and summary have SHA-256
`77267a71e9c9f40cc086af4e44029943b8a09559b59a7a7a998d652ee9026ca7`,
`998cc015f670e470bbb435d47fc49d0aac5b22d0315a7d51eae326e4e48e18fa`,
and `e4ac85f650a5c8f34f81a77e03c36208d6b952ae7aa732450d272ec493069d20`.
The worker re-read its immutable S3 seal before opening source GT100;
the controller rehashed the terminal artifacts and confirmed the seal
preceded raw publication. Checked-in
`scripts/check_v195_used_1m_rerank_diagnostic.py` independently
recomputed all 512 GT100/returned-ID intersections, sidecar interval
charges, aggregate summaries and the frozen decision. The V195 worker
also reconstructed V194's original SQ8 returned IDs and hits exactly
on all 511 feasible queries using the authenticated source and SQ8.

## Measured used-panel diagnostic

V194 optional-risk on 511 feasible queries returned 50,692 GT100
IDs, with 50,941 physically fetched GT100 IDs. Its one infeasible
query had a 766-unit mandatory floor at 32 GETs. Giving that query
`max(672, floor)=766` units with the same frozen optional utility and
price produces a 766-unit, 32-GET plan: 19,119,360 planned bytes and
**81/100** fetched and SQ8 returned GT100 hits. That exceeds V194's
672-unit per-query gate, so it remains a diagnostic, not a V194 pass.
Across all 512 queries, the dynamic-base SQ8 result is 50,773 hits,
3,883,676,160 planned bytes and 5,179 GETs.

For each fixed SQ8 shortlist K, V195 simulated a full-generation
FP16 payload in V164 physical order, reranked the shortlisted rows by
cosine, and priced whole 32-row FP16 sidecar ranges. One sidecar unit
contains 49,152 bytes. Sidecar storage is 1,536,000,000 bytes per
1M-row generation, before metadata or replicas. These are offline
measurements and **optimistic planned** sidecar costs, not live S3
latency or billed storage.

| K | GT100 in SQ8 shortlist | FP16 returned GT100 / 51,200 | Float32 control returned | Combined planned bytes | Combined planned GETs | Queries over 32 combined GETs |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 51,022 | 51,022 | 51,022 | 5,219,725,824 | 16,035 | 204 |
| 160 | 51,022 | 51,022 | 51,022 | 5,519,307,264 | 17,075 | 235 |
| 200 | 51,022 | 51,022 | 51,022 | 5,869,564,416 | 17,994 | 254 |
| 256 | 51,022 | 51,022 | 51,022 | 6,318,076,416 | 18,793 | 279 |

The SQ8 top-128 shortlist contains **all 51,022 physically fetched
GT100 IDs** under the dynamic-base plans. FP16 and source float32
reranking both return all of them, recovering **all 249** of V194's
feasible-query SQ8 return losses. This isolates the correctable
return-ranking error to SQ8 precision on this used panel. The 178
remaining misses from 51,200 are candidate omissions, feasible-plan
allocation and the dynamic query's 81/100 physical coverage. This
does not establish FP16 quality on new queries.

The K=128 separate-sidecar plan uses **1,336,049,664 extra planned
bytes and 10,856 extra GETs**. Combined GETs 16,035 exceed the frozen
11,328 aggregate envelope by 4,707, although combined bytes are under
the 5,700,611,604-byte envelope. With the **same fixed K=128
shortlists and sidecar order**, bridging the 4,707 cheapest remaining
gaps across all queries is a posthoc optimistic minimum of
**633,176,064 additional bytes**. This yields at least
5,852,901,888 combined bytes, **152,290,284 above** the byte cap.
Thus even the cheapest globally selected interval bridges cannot meet
both aggregate limits for this layout and K. The corresponding lower
bounds for K=160/200/256 are still higher. The dynamic-floor query
uses 32 SQ8 GETs and 32 extra sidecar GETs at K=128, reaching the
diagnostic 64-GET per-query cap; a 32-GET serving cap cannot admit this
two-object path for that query.

V195 offline processing used 0:58.42 wall, 280.42 user CPU seconds and
11,017,344 KiB peak RSS on Spot. It did not measure serving latency,
charged resident RAM, throughput or S3 request cost. The strongest
historical BORSUK 1M returned comparator is V155 **used validation-1000
real queries**, 99,567/100,000 hits at 11,134,007,040 planned bytes
and 22,126 GETs; it is unpaired and its scaled envelope is a screening
target, not evidence of a matched product comparison.

## Next architecture decision

Do not optimize the optional price on this panel: precision, not its
small two-hit gap versus full-rank, dominates the failure. A revised
generic architecture must bring rerank precision into fewer object
requests, for example through co-located precision tiers, differently
grouped rerank pages, or a bounded cache whose memory scales with the
requested recall and corpus. The 766-unit mandatory tail also needs a
generic floor-aware request budget and an explicit latency/cost tradeoff.
Any such format should be screened on **used** V194 identities, then
frozen and tested on new identity-disjoint source queries, real-query
transfer, and live S3 under matched resource and latency conditions.
Lean can prove conditional byte, GET, trace and recall bounds given a
layout and validated error premises; it cannot establish those
premises or measured S3 latency by arithmetic alone.

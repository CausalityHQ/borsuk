# V158 D768 PQ-primary returned-quality closeout

## Decision

**Reject PQ-first-100 as a substitute for exact-SQ8 primary selection.**
On the same used ReLAION-100k D768 development queries, it loses 14,908
GT100 hits out of 100,000 against the V114 exact-primary control. It loses
on every one of the 1,000 paired queries and misses every preregistered
quality floor. Neither arm approaches production quality on this 100k
corpus, so the exact-primary route is a control, not a qualified product
point here. Do not promote PQ-primary to 1M or wire it into V156 serving.

The returned hit count equals fetched physical GT coverage for every query
in both arms. Thus the measured failure is page selection/coverage in this
cell; the SQ8 returned scorer did not lose additional GT IDs that had been
fetched. The next diagnostic must separate source-only 512-nominee coverage
from the primary and interval planner's losses. A representation or routing
change is required if the 512-nominee ceiling is low; a page-admission or
graph-expansion change is required if the nominated GT is present but the
physical plan drops it. Do not tune a dataset-specific threshold.

## Verified paired measurement

Both arms used the same frozen V114 512 PQ64 nominees, authenticated SQ8
object, 513/1 weighted physical interval algorithm, 32 GET cap,
16,777,216-byte cap, V85 exact GT100 and **used** development-1000 split.
Only the 100 primary rows changed. Plans were sealed before GT was opened.

| ReLAION-100k D768 development-1000 | Exact-SQ8 primary | PQ-first-100 primary | Unit |
| --- | ---: | ---: | --- |
| Returned Recall@100 | 77.302 | 62.394 | % |
| Returned GT hits | 77,302 | 62,394 | /100,000 |
| p05 returned hits | 71 | 51 | hits/query |
| Physical GT coverage | 77,302 | 62,394 | /100,000 |
| Planned GETs | 32,000 | 32,000 | GETs/1,000 queries |
| Planned bytes | 16,766,231,040 | 16,765,407,360 | bytes/1,000 queries |

PQ-primary lost on 1,000 queries, tied on zero and won on zero. Its primary
row overlap with the exact-SQ8 set had median 70/100, p05 53/100 and range
37–92. Both arms had 1,000 queries below 90 GT hits. These are offline
replays, not live S3 latency measurements. V114's ReLAION-1M development
quality and V85's 100k results use different corpora, index architectures
or physical schedules; they are context and are not paired comparators for
this gate.

## Provenance

The complete Causality `c7i.8xlarge` Spot attempt used source commit
`9933ae6a5445e0eaf4b408d63e6cc0f135817447`, instance
`i-0cdd67a45ca5c1f96`, terminal SHA-256
`09306fa1aca94748635eca32ac9259e934249ee1fb620c3ec333977517a2dd35`.
Its immutable prefix is
`s3://borsuk-bench-453182569524-euc1/research/v158-pq-primary/9933ae6a5445e0eaf4b408d63e6cc0f135817447/runs/a0001/`.
The instance is confirmed terminated. A postterminal read checked the
length and SHA-256 of all six artifacts. The independent checker recounted
all 1,000 query results, stable-ID mappings, GT hits, physical coverage,
byte/GET caps, summary and failed gate verdict; it reports `pass`.

| Artifact | SHA-256 |
| --- | --- |
| GT-blind plans | `5d89b1b5f4bcaf019bd4091e46291412fe56ce0e5bdb9cca0178e4162a6c383a` |
| Plan seal | `2bfd5ec30abe399cd9a8f2fec14edf18bf03f6b57332ea975a9dd5a6cb309c40` |
| Raw returned IDs and per-query outcomes | `12a77f91b6b0898ae2b9ed17f0450556eec7bf0a2bdc9d2f4130bd71193e00ba` |
| Summary | `1a839810793743ec9b43149f7a195580229299da379a0b976395146e5cf737c0` |
| Independent check | `101464ccb08b91b7cf88592325f102be9765e4635849616f43ae7b780f029d53` |

An earlier attempt at source `d01aa1b77fde0c45874692b7cd90ef9c0a15c14f`
stopped in reduction before any quality result because the new reducer
incorrectly assumed stable source IDs were contiguous physical ordinals.
The terminal captured the failure, the instance was terminated, and the
new source revision explicitly mapped sparse int64 IDs to physical rows.
No output from the failed attempt contributes to the measurement.

## Next gate

Use the same frozen 100k requests, SQ8 ID map, GT and terminal-bound V158
plans to report GT coverage at four points for each query: all 512 PQ64
nominees, the exact-SQ8 primary 100, the PQ-primary 100 and each final
physical plan. Register the diagnostic before opening the GT and publish
raw per-query values with an independent recount. Then change the layer
responsible for the largest loss and run one paired 100k returned-quality
gate before any 1M promotion. The V156 graph method remains a candidate,
not an assumed repair.

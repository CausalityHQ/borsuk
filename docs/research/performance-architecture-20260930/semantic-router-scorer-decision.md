# Semantic scorer development decision

ReLAION FIRST100k, D768 cosine, consumed development ordinals 0–63:
**PASS; proceed to CoHere with the same frozen policy.** All 64 pairs
succeeded, fresh unchanged source-completion control matched authenticated
historical discovery, plans and ordered IDs, and every identity/budget gate passed.

| Verified local result | Control | Semantic candidate |
|---|---:|---:|
| Mean returned recall@10 | 99.6875% | 98.125% |
| Mean returned recall@100 | 99.515625% | 97.140625% |
| Recall@10 p05 | 100% | 90% |
| Mean modeled source + SQ8 bytes | 31,356,970 | 21,857,100 |
| Mean modeled query bytes including candidate router | 31,356,970 | 23,686,537.9375 |
| Mean modeled query GETs including candidate router | 67.53125 | 49.828125 |
| Offline total CPU p50 | 51.695162 ms | 31.175876 ms |

Candidate recall@10 delta is −1.5625 percentage points, with 628/640 hits
against the preregistered minimum 608. Recall@100 delta is −2.375 points.
Discovery-unit recall@10 is 97.65625%; page closure/fetched coverage is
98.4375%; returned recall is 98.125%. Nomination accounts for the main loss.
Page selection and the SQ8 byte cap added no further coverage loss on this
panel. Exhaustive native SQ8 equals control recall@10/@100.

Candidate root is 752,232 bytes; selected leaf bytes maximum 1,359,820.
Maximum source/SQ8 combined bytes 24,638,720 and GETs 56; including router,
maximum 26,750,772 bytes and 73 modeled GETs. Whole evaluator completed in
17.47 seconds, maximum RSS 161,836 KiB, cgroup peak 177,057,792 bytes,
zero swap/OOM under 512 MiB and two CPUs. These include offline authentication
and separate exhaustive evaluation; they are not production query RSS.

Offline total timing includes synchronous evidence/parity overhead. Modeled
ranges are not physical S3 requests. No cold HTTP, QPS, cost, fresh split,
1M/100M scale, or matched vendor win follows. The stricter historical R100
diagnostic is missed; the preregistered practical R10 development gate passes.
The next decisive test is CoHere actual returned quality with unchanged
nomination/scorer/budgets; only both survivors earn object-native cold HTTP.

Exact source and binary qualification: semantic-router-scorer-assurance.json.
Authenticated terminal/result/resources: scorer/relaion-a0001/verification.json
and six immutable gzip bodies. No retry or cap expansion was used.

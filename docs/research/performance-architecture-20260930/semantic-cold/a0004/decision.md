# a0004: GO to bounded offered-load qualification

2026-10-01. Verified closed terminal complete/exit 0; original controller session 46594 collected. Owned Spot i-0ac878f23221e3a87 terminated and waited, elapsed receipt 501 seconds. All 53 artifact bodies authenticated; all 512 records independently revalidated against authenticated panels, qualified binary/source and exact saved reducer. Four native publication receipts revalidated. See independent-verification.json.

## Measured convergence row

FIRST100k D768 cosine development ordinals 0–63, k10; 128 observations per pooled arm/dataset from two repeated blocks. Same qualified binary and scoring, graph control versus semantic candidate. Fresh native process/store/TLS each call; application cache off, S3 service cache uncontrolled. Publication validation preceded measurement. Whole cold boundary includes process launch through first HTTP response.

| Dataset | Graph recall@10 | Semantic recall@10 | Delta pp | Graph cold p50/p90/p95/p99 ms | Semantic cold p50/p90/p95/p99 ms |
|---|---:|---:|---:|---|---|
| ReLAION | 99.6875% | 98.125% | -1.5625 | 642.654 / 681.962 / 691.768 / 710.007 | 555.592 / 591.231 / 603.049 / 665.764 |
| CoHere | 98.59375% | 95.78125% | -2.8125 | 631.287 / 675.040 / 687.253 / 769.336 | 547.311 / 579.886 / 588.723 / 604.230 |

Both candidate blocks per dataset meet frozen >=608/640 hits. All calls successful; zero failed/dropped/aborted. Pooled candidate p90 and p95 improve for BOTH datasets. Native 512 MiB admission/RSS gate passes, all process/directory cleanup confirmed. R100, offered-load QPS, saturation, total lifecycle cost and generation-swap memory remain UNMEASURED. The hourly quote and storage allowance are estimates, not measured total cost.

## Bottleneck and next decisive test

Candidate median metadata staging remains 277.952 ms ReLAION / 273.216 ms CoHere; metadata GET-header interval ~188/187 ms and HEAD interval ~74/73 ms dominate startup. Candidate median native query is 171.411/161.387 ms; source ~38.980/34.955 ms, discovery ~38.895/38.167 ms, SQ8 ~79.811/79.885 ms. Stage percentiles are not additive tail decompositions. Semantic startup decode falls to ~3.329/3.185 ms; savings do not establish 1M scalability.

Decision: retain this valid candidate and proceed to a preregistered bounded offered-load test up to 8 QPS, reusing existing scheduler/lifecycle. Preserve exact identity/scorer/accounting/quality/resource gates, report every offer and success-conditioned versus offered-population latency, drops/errors/aborts and full-span throughput. Root freezes protocol and owns launch; no architecture/reviewer restart. Future startup changes should address serial metadata network admission based on this measured evidence, one intervention per arm.

Remaining BOTH-vendor gap: no matched vendor measurement, no fresh 1M/10M/100M, no saturation/cost curve. Turbopuffer published 1M D768 444 ms cold p90 is a different scale/protocol; this 100k result is NOT a vendor win. Existing earlier a0001–a0003 failures stay immutable FAIL. No operator decision needed.

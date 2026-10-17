# Directory-bound original-unit capsule ranges and flat download qualification

## Verified increment

Unreleased production generation v11 uses BORSDR02/BORSCP02, exact router-bound page rosters, deterministic lowest-leaf duplicate ownership and endpoint-only range trimming. Every fetched original 32-row unit is authenticated, including losing duplicates and internal bridges. Whole publication validation remains separate. Flat records remain available; production rejects old incompatible markers. Incremental publication/store/compaction/GC callers are migrated and tested. No new production dependency or controller.

Exact native revision: 966b06a7b920ce993335484cbb8b4e3cf95b79bd. All affected tests, release, workspace Clippy correctness/suspicious and real shim-unset workspace test-build passed. A later duplicated page with all physical hashes refreshed was rejected by original-unit authentication. Native 1M preparation took125.40s wall,259616KiB peak RSS,swap0; this is preparation, not query performance.

## 1M/D1024/cosine/k10, all1,000 frozen requests

| Point | Recall | p90 ms | p95 ms | Sequential QPS | Query CPU s | RSS MiB | SOURCE GETs | SOURCE bytes |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Retained one-pass capsule | 97.83% | 253.267 | 281.477 | 4.776 | 158.619 | 114.50 | 24710 | 41963275448 |
| Directory-bound v2 capsule | 97.83% | 244.576 | 275.342 | 5.060 | 145.634 | 101.70 | 24651 | 40208885880 |
| Current flat flat64 | 97.83% | 203.267 | 221.574 | 6.157 | 115.803 | 87.32 | 109872 | 28967583744 |

All10,000 ordered results/score bits/physical ordinals/plans/traces and SQ8 charges are identical. SQ8 total is30,041 logical GETs/10,722,616,576 verified bytes. Historical full-cohort timing is not counterbalanced. Separate32-query ABBA runs provide paired diagnostic timing, never population percentiles. Client sourcecache is off; S3 backendcache is uncontrolled. QPS is sequential query_count/sum(query_wall), not concurrent service throughput.

## Flat download decision

The existing selector accepts64/128. Both use the same flat root and ELF; SOURCE GET/byte charges and quality are identical, while SQ8 has at most32 requests. Verification remains inline on a current-thread runtime; no CPU worker increase is claimed. The selector was prospectively chosen by the smaller sum of two32-query query-wall totals after exact parity. Both points remain evidence.

- B1/A1: CPU ratio 0.991335, wall ratio 0.924777, sequential-QPS ratio 1.081342.
- B2/A2: CPU ratio 1.011069, wall ratio 1.252949, sequential-QPS ratio 0.798117.

## Competitors and remaining work

Retained S3 Vectors result:95.71% recall,p90 199.660ms,p95 211.891ms,7.402 sequentialQPS. Its independent producer/input authority still needs reconciliation for a strong matched comparison. No matched Turbopuffer result exists. No vendor win, billing, total lifecycle cost, concurrent service or100M result is claimed.

Next: retain the measured quality/latency/GET/byte Pareto points. Profile SOURCE network/authentication versus scoring, then qualify independent global/per-query download, compute and resident-byte limits with one causal intervention. Keep actual-input admission, separate canary, frozen source and counterbalanced checks; do not reject useful gains solely for old efficiency targets.

All source/preparation/build/test/data execution occurred on Causality EC2/EBS; Devbox only orchestrates and integrates Git objects.

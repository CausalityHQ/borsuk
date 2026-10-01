# Metadata waves: offered development gate GO

FIRST100k D768 cosine k10, consumed development ordinals0–63. Fresh matched roles at .25/.5/1/2/4/8 offered QPS; 64 offers per cell, six workers. All1536 calls succeeded. All110 terminal bodies and gzip counterparts authenticate; independent offline replay exits0. Owned instance i-0dabc6c640e186e42 is terminated.

| Dataset / role | Recall@10 | Cold p50 / p90 / p95 / p99 ms | Full-span completion QPS at8 offered |
|---|---:|---:|---:|
| ReLAION / control | 98.12500% | 512.993 / 550.097 / 570.244 / 582.305 | 7.6256 |
| ReLAION / candidate | 98.12500% | 410.406 / 453.787 / 464.779 / 514.048 | 7.7007 |
| CoHere / control | 95.78125% | 508.769 / 544.664 / 552.445 / 624.850 | 7.6223 |
| CoHere / candidate | 95.78125% | 406.954 / 462.515 / 496.155 / 676.132 | 7.7462 |

Quality delta is0pp on both datasets. Candidate p90 improves17.51% ReLAION and15.08% CoHere against current matched controls. CoHere candidate p99 is676.132ms versus624.850ms control: worse, retained as a tail limitation. No aggregate percentile subtraction establishes stage causality.

Cold tails end at wire response completion, excluding validation/cleanup. Full-span QPS includes validation/drain/cleanup from first scheduled offer; finite64-offer cells do not establish sustainable8QPS or saturation. Frozen offered gate passes; do not replace measured completion QPS with the offered rate.

Shared cgroup peak240,787,456B; swap/OOM0. Candidate8QPS native peakRSS36,720,640B ReLAION/36,237,312B CoHere. Submitted HTTP-service attempts and consumed payloads are accounted; confirmed wire requests, unread/TLS/header bytes and total lifecycle dollar cost remain unmeasured.

This100k development result is not a matched S3 Vectors/Turbopuffer win and not1M/10M/100M evidence. Next decisive gate: qualify explicit Fresh1m binary-root implementation, seal a genuinely new64-query panel, then bounded ReLAION-first1M quality/build/memory falsifier; CoHere only after survival. All old sealed1M ordinals0–999 were consumed.

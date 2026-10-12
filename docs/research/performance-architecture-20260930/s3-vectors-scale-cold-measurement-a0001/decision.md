# S3 Vectors 1M first-pass result — 2026-10-12

Native source 5e448bfafa96ed42982884c64c0005fe0e4664af; qualified delivery 5a4503c1737a7e237b499b76616f9a93df4acb0d; service prerequisite 462edefe970d058b81c1553465434b9ee7dfa440. Original instance i-041b7d96cf0536754. Config SHA d8758b9c36dbd0089f6d85c3c2d077ffff6c2fc54c28bd932b1f6b4b9884d561.

| Arm | Recall@10 | p90 ms | p95 ms | Sequential reciprocal QPS |
|---|---:|---:|---:|---:|
| S3 Vectors | 96.10% | 197.550 | 215.023 | 7.358 |
| BORSUK width32 B1 | 97.80% | 243.195 | 280.108 | 4.844 |
| BORSUK width32 B2 | 97.80% | 257.823 | 291.690 | 4.761 |

S3 Vectors first query350.676ms, p50 129.669ms, p99 243.048ms, whole-pass QPS7.016, 9610/10000 hits, zero underfilled queries. BORSUK is higher quality but slower; neither is a matched-quality win. BORSUK evidence: e35d97c44071cf553c878ad9c1ae936564651d1f. No matched Turbopuffer reproduction or total-dollar win.

Same frozen1M corpus/query/reference-truth pins, D1024/cosine/k10, no filters, serial queries, eu-central-1. S3 Vectors ENHANCED index; full1M rows listed and bit-exact read back, then recorded3604384ms inactivity. Backend caches uncontrolled: this is an inactivity-conditioned first pass, not proven cache eviction. BORSUK client payload/local-drive cache off, resident router and connection reuse; differing service cache behavior and quality remain disclosed. Common truth was derived from original f32 vectors; canonical normalized-f32 ranking parity remains unmeasured.

All5 native stages0, wrapper original/final0, cgroup drained. Peak57802752B, swap/OOM0. Archive1606759B SHA72fd94b2437ef7eb250284810cab33ade40d51f3004dcca6d352de7ae50b263f authenticated. Config matches frozen template except exact host identity. Native reduction VALID/all1000 queries sealed/truth opened after seal. Service index/bucket deleted and absence verified. Original EC2 terminated, root volume absent, external timer inactive.

Observed15013 SDK transmits across publication/verification/query/cleanup; publication included2000 puts,2000 lists and10000 full readback gets. These are observed calls, not billed requests. Actual lifecycle bill remains null, so no throughput-per-dollar claim.

Next causal candidate: completion-driven shared SOURCE/SQ8 fetch scheduling at fixed width32, deterministic output/error order and full draining. Test-only falsifier exists privately, uncompiled/unrun. Apply fixed-anchor hill-climbing policy; do not reduce quality cumulatively or relabel unmatched results as a vendor win. Preserve this original campaign, with no repeat query pass on its deleted index.

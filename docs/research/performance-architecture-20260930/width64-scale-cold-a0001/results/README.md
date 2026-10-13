# Cold fetch width32/64 — verified result, 2026-10-13

Frozen configuration63874940; codef5a0c36b; one shared qualified baseline ELF3dfad916 and qualified comparison ELF955210af. Same1M Cohere D1024 cosine k10, all1000 frozen queries, CPU0/100%, library512MiB, outer1GiB, swap0, pids128. Payload/local-drive cache off; resident router and within-process connections allowed; provider cache uncontrolled.

| Cell | Width | Recall@10 | p50 ms | p90 ms | p95 ms | p99 ms | Sequential QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| A1 |32|97.80%|180.830|226.980|250.650|309.959|5.355|
| B1 |64|97.80%|142.404|194.550|219.463|324.954|6.526|
| B2 |64|97.80%|139.421|176.208|196.672|298.061|6.812|
| A2 |32|97.80%|175.554|201.843|221.339|255.738|5.660|

Decision: select width64 for this measured workload under the frozen promotion rule. Paired p95 gains12.442%/11.144%; p90 ratios0.857/0.873; sequentialQPS ratios1.219/1.204. Every arm returns9780/10000 hits and zero underfill, with exact ordered ID/score-bit, full trace, physical plan and logical GET/byte parity. All18 real admission/canary/measurement/reduction stages closed0. This is configuration selection, not proof of a general production default.

Tradeoff: p99 worsened4.838%/16.550%. Width64 process peaks74,300/76,556KiB; cgroup peaks113,434,624/121,499,648B; swap/OOM0. SOURCE sums fall108.240/104.482s to76.462/72.775s per1000 (29.36%/30.35% improvement). Query process CPU remains about92–95s per pass; phase wall time alone cannot identify the next CPU kernel to optimize.

Measured S3 Vectors reference on the retained panel:96.10% recall,215.023ms p95,7.358 sequentialQPS. BORSUK's new p95 range straddles that value and its throughput remains lower. Provider cache protocols differ and common-GT original/normalized-f32 ranking parity is unmeasured; no overall matched vendor win. Turbopuffer has no matched reproduction. Two repetitions per arm do not establish concurrent capacity, total lifecycle cost or100M scale.

Original commandbc0b9490 completedSuccess0 at03:19:03.879UTC. Instancei-0950b1f42a568acce terminated, volumevol-0d546e5eead516f60 deleted, external timer stopped. Immutable opaque archive lives at s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261013/source-width64-scale-cold-a0001/terminal/evidence.tar.gz; its exact SHA is retained here. Raw query rows were never extracted or processed locally; Rust-produced native aggregates and supervision metadata are mirrored.

Next: retain64 as the measured point, profile remaining CPU/network and p99 behavior before another causal change, and continue matched vendor/lifecycle/scaling qualification. SIMD requires a measured compute bottleneck; warm-cache work remains outside current scope.

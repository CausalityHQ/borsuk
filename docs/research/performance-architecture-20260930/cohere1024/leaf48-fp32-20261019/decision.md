# Leaf48 FP32 outcome — 2026-10-19

Retain semantic leaf limit 48 as a quality/cost research candidate. On the closed Cohere 1M, D1024, cosine, k=10 development cohort, hits rose from **9,803/10,000 (0.9803)** to **9,852/10,000 (0.9852)**: **49 additional hits across 43 queries, zero lost hits and zero regressed queries**. This improves quality while consuming more upstream I/O; it does not establish production qualification or resource dominance.

The only algorithm intervention was leaf limit 40→48. FP32 shortlist 16, coalescing false, 65,664 canonical bytes/16 GETs/16 parallel GETs per query, Q1, download parallelism 128, authentication offload2, source 64 MiB/128 GETs, SQ8 32 MiB/32 GETs and library memory 2 GiB stayed fixed. The library model already supported 48. Neither source nor defaults nor qualified binaries changed.

The preregistered failure-enriched screen covered all 133 baseline-loss queries as 128+5: 1,093→1,140/1,280 and 40→42/50 hits. Net gain 49 with no regression passed the net-gain>0 gate and triggered the actual full 1,000-query run. Admission and separate canary each completed 1 query, 9/10 hits, with zero native/reader exits. Screen/admission/canary are diagnostic subsets, not population estimates.

| Full-cohort recall (nearest rank) | Leaf40 baseline | Leaf48 candidate |
| --- | ---: | ---: |
| minimum | 0.3 | 0.3 |
| p01 | 0.7 | 0.8 |
| p05 | 0.9 | 0.9 |
| p10 | 0.9 | 0.9 |
| median | 1.0 | 1.0 |
| queries with all 10 hits | 867 | 890 |

The complete before/after histograms and every improved query's ordinal, hit counts and gained truth IDs are in [report.json](report.json); the regression list is empty. Root independently recounted the two closed cohorts and supplied the histogram and nearest-rank quantiles. The original controller separately authenticated canonical row SHA values, sequential-f64 cosine score bits and the original depth16 baseline oracle. This docs worker reused those closed verifications and did not open raw vectors, requests or truth.

| Logical verified full-run query cost | Leaf40 baseline | Leaf48 candidate | Increase |
| --- | ---: | ---: | ---: |
| source GETs | 97,797 | 109,872 | 12,075 |
| source bytes | 24,255,289,344 | 28,967,583,744 | 4,712,294,400 |
| SQ8 GETs | 30,014 | 30,041 | 27 |
| SQ8 bytes | 9,974,177,024 | 10,722,616,576 | 748,439,552 |
| canonical GETs | 16,000 | 16,000 | 0 |
| canonical bytes | 65,664,000 | 65,664,000 | 0 |
| total GETs | 143,811 | 155,913 | 12,102 |
| total bytes | 34,295,130,368 | 39,755,864,320 | 5,460,733,952 |

Router query payload cost was zero. Each cell additionally recorded binding cost of 3 GETs/28,594 bytes, outside the query totals. Candidate full-process wall/CPU were 212.592479120 s/124.925295420 s; query wall/CPU sums were 205.826055340 s/118.665706817 s. Observed native process peak was 140,640,256 bytes (not a complete library-residency or host-memory measurement). Candidate query wall p50/p90/p95/p99 were 185.507708/284.869145/378.560419/450.099070 ms. All queries sealed, none underfilled, with zero failed logical GETs. Stage runtimes, CPU, observed peaks and GET/byte costs are retained in report.json.

These are candidate timings and logical verified bytes/GETs. The baseline is a historical closed cohort with different execution/cache history. There is no paired-speed, provider-cache-eviction, holdout, vendor-win, billed-byte/request, wire-byte or production-readiness claim. The same frozen development cohort informed the failure-enriched selection; recall gain is evidence on this cohort only. Full-cohort qualification remains false and an external gate remains required. Optimize efficiency while preserving this quality result; root retains scientific decisions and any future run authority.

Original handles: EC2 i-0424fbcd99f9777de; unit borsuk-leaf48-fp32-a0001; invocation 19a125c8573a42b69c5f92a65993bbeb; launch SSM 1d1c9cda-f21f-4c7b-809e-032cb0126aeb. MainPID=0, Result=success and ExecMainStatus=0 were observed only after closure; C/pipeline.exit and C/terminal.json both give exit 0. The collected transient unit's current invocation field is empty; the original launch SSM binds the invocation. See [terminal.json](terminal.json).

Evidence base b327cf87df3406635c49537ea14cf5f958dde2e3; unchanged Rust source 70c1c1d094b2ab926ec38fd9220d78ea1ecfcbf1. Controller SHA256 b14c7701b4a8aa85c1909e93f22599183925e0ab91e643ec4a8210b8d60db6e5; frozen configuration SHA256 6ab61880418a37618a7d63c4d50da6a0eb8df872b132c1806ede60db04d21484. Original archive is s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261019/leaf48-fp32-a0001/evidence.tar.gz, 19,942,502 bytes, SHA256 1cb3bce1e42c40cfb2022b7405226d191fe35982deb9326334c597d5cf3b09f7. Full result SHA256 3116a078ff5abf0c15e38d65ea1dc1905a7993ec057de375acf84ea81137007b; historical baseline SHA256 2bda08997e598964b556ced9b5993271696045855bf9026b736d84c3ef260407. Generation, request, truth, binary and input hashes are preserved in [verification.json](verification.json) and report.json.

authority.json contains reused historical qualification/instance/shutdown metadata, not current-time authority. Current configurations, measurement-addendum.json, launch receipt and terminal markers govern. Root's additional closed recount has SHA256 649cc3418b1af19532ae87800c8950568fef39be38276f6ad47826f60efd18e9 and was produced after the original archive; its full metadata is copied into report.json.

Root also closed its residual-boundary metadata check: all 148 remaining missing truth hits were classified as not semantic-nominated or SOURCE-scored. This uses the shared authenticated physical-order unit32 mapping; it does not distinguish router geometry from capacity policy. The exact rows and scope are copied into report.json; root-boundary-verification.json SHA256 59ab2e7778e94c50170416e5bd86fc9767abbca10c43d3ffe138cfcf3990b8a9. No raw input was opened by this worker.

Delivery is documentation only: four new files plus one appended ledger row. Sparse checkout was configured before checking out the evidence base. The known absent inherited blobs are preserved as unowned Git tree entries; audited write-tree --missing-ok and commit-tree avoid materializing them. Post-commit exact-path, whole-crates/unowned equality, diff-check and bundle-verification receipts accompany the create-only S3 handoff. Root integrates and terminates; this worker does not merge, push, build, rerun native experiments or alter cloud lifecycle.

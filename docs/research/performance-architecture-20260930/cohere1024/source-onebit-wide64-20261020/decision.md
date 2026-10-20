# Wider one-bit SOURCE counterfactual: quality gate missed

Frozen estimator/seed/top256, first64 original query ordinals on authenticated Cohere1M D1024 cosine k10 nominations. This is an offline diagnostic, not a planner replay or cold ANN benchmark.

| Measurement | Existing two-bit | One-bit |
|---|---:|---:|
| Nominated truth retained | 636/636 | 633/636 |
| Logical union code+factors | 196,669,440 B | 101,314,560 B |
| Score+sort | 2.229396 s | 42.848772 s |

Queries51,53,57 each lost one nominated truth neighbor; the other61 did not regress. Four of640 truth neighbors were outside the recorded nominations for both arms. One-bit logical bytes were51.515% of control, scalar scoring19.220x slower. Encoding12.299541s; scientific full-process peak RSS488376KiB; explicit model842103232B under caller2GiB. Process wall61.31s; these timings have warm filesystem caches and do not establish cold latency, throughput, physical request savings, or lifecycle cost.

Decision: frozen one-bit/top256 MISSES preregistered per-query quality. Preserve its compression/quality Pareto point; do not advance this arm to a serving layout or claim generalization. Production routing/scoring/layout/defaults remain unchanged. Next causal falsifier: one globally larger candidate SOURCE shortlist (e.g.512), same frozen estimator/seed, development ordinals0..63 reported separately from fresh64..127 held-out validation, before any CPU/layout work. This is a prospective hypothesis, not a qualified feature or authorized gate relaxation. No query-specific policy, seed tuning, or dataset name belongs in library logic.

Source b5efa9cd8b30ad40c8cd033409884eae1f1d9d47; ELF51250d9ed73da9683a84ec75638fc08bc67dd1f8fea89111943cd87e95dd56e7. Authority8aaebf97bf4cf57469e5fde6321ad95197ab6d268cef0702c8e8e1a063eb3614 derives unchanged nominations from closed goldce13a10a... . First8 baseline and one-bit top256 IDs/order/score bits exactly reproduce the prior internal scalar diagnostic; historical production baseline score oracle remains unavailable. Scientific reportfaeaff61cb84e49f018f704eecb3b6278a9a84ae9964e8d04f469bfb5e998a10.

Verification: generic finite authority cap8MiB, existing96x metadata admission unchanged;13 native focused tests; exact locked workspace Clippy correctness+suspicious; REAL override-unset workspace testbuild; release and selfcheck all PASS. Actual-input admission, separate CLI/selfcheck/k257 INVALID canary, and scientific run all exit0 on ONE original unit. Original1MiB metadata refusal and Git missing-inherited-blob failure are preserved as setup/metadata outcomes, not algorithm negatives. Source frozen02:11:58; no cutoff miss in this repair. Original first8 slice's15s edit-limit miss remains historical.

This commit adds the generic diagnostic example, regression tests and reproducible decision; it does not integrate a one-bit production backend. Compiled Rust/Cargo/math blobs are identical to the qualified candidate. README separately corrects a producer-reference description: canonical preparation checks ID/hash correspondence and copies records; it does not rebuild the plane.

Root research reconciliation rejects assuming identical ordered SOURCE traces in a future serving arm: unit maxima affect completion/page ordering (two_bit_generation.rs1940-2080). Actual end-to-end tests must rerun the planner and measure final quality, bytes/GETs, memory, cold tails and total cost. No S3 Vectors/Turbopuffer win is established.

Evidence: s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261020/source-onebit-wide64-a0001/study-closure.tar.gz . Native source bundles, full reviews/research, original failures, qualification and terminal reports are retained. Same-day first8 parent archive remains immutable under source-onebit-falsifier-a0001/study-closure.tar.gz.

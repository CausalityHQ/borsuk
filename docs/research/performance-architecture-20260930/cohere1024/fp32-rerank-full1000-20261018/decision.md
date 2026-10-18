# Full1000 authenticated FP32 rerank serving

Decision: retain the quality Pareto candidate; current efficiency is worse. No production-performance or vendor-win claim.

| Arm | Recall@10 | p90 ms | p95 ms | p99 ms | Serial QPS | Native CPU s |
|---|---:|---:|---:|---:|---:|---:|
| A1 | 97.46% | 177.217 | 232.520 | 347.486 | 7.4668 | 103.344 |
| B1 | 98.03% | 341.808 | 412.744 | 618.856 | 4.7842 | 104.111 |
| B2 | 98.03% | 288.441 | 349.540 | 503.995 | 5.0876 | 103.615 |
| A2 | 97.46% | 238.382 | 329.943 | 442.638 | 6.7947 | 101.196 |

Both controls9746/10000; both candidates9803/10000. Independent raw frozen truth recount, every reference ID/physical ordinal/score bit, and all preceding plans/router/SOURCE/SQ8 charges pass. The canonical reference covers the already fetched population, not global exact ANN search. This is the development cohort, not held-out generalization.

B1/A1 query-wall ratio1.5607, serial-QPS ratio0.6407, p95 ratio1.7751. B2/A2 ratios1.3355,0.7488,1.0594. Control p95 drift232.520->329.943ms confounds exact tail-effect attribution; neither comparison establishes a speed gain. Candidate native CPU barely changes; canonical-stage medians51.023/39.232ms suggest investigating I/O/scheduling, without claiming causality from medians.

Each1000-query control uses97797 SOURCE GET/24255289344B plus30014 SQ8 GET/9974177024B. Candidate adds16000 canonical GET/65664000B. These are logical charges, separate from native transport counters, not billed/wire counts. Fresh process/source-cache off; provider cache eviction is unproven. Percentiles are empirical nearest rank. Serial QPS=1000/sum(query wall); recorded-pass QPS uses the native process_wall counter (authentication/startup/reduction, not an external process-teardown clock).

Source7a98b3d43472237437cbc64713cffc6044f7aaf3; prior evidence50316ebe3f9c687a5bb20716a408643e72f67c1a. Native ELF72ea8d0088fb85953918e060104cebf2c7d434f1a1bb72ccec7a28c9b0ef0be9; reader1ef63aff75891db6e2ce1f3d34e634be041d9df58203a0ef63ff095995779e4e. Source qualification reused; actual admission and separate canary rerun for both attempts. Library2GiB/host8GiB/CPU0-7/Q1/swap0, original40min guest shutdown unchanged.

Attempta0001 preserves valid A1 and B1 exit124 at enforced270s; incomplete B1 was not used as scientific metrics or quality rejection. Attempta0002 prospectively allows420s per cell, retains1260s measurement cap and fixed shutdown, reruns fresh full ABBA and preflight. The predecessor label typo is corrected by an append-only metadata correction; original authority is retained.

Next: choose one falsifiable efficiency intervention while preserving the exact quality result. Contextless read-only specialist researchad89cca192264466 is pending; simulated expert/stakeholder perspectives are hypotheses, not authority or consensus. No new implementation or paid experiment is authorized by its response alone. Reconcile against source and cheapest valid native check.

Archive: s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261018/fp32-serving-full1000-closure-a0001/evidence.tar.gz ; terminal.json published last. Per-cell complete/sealed archives were checksum-HEAD verified before the next cell. Both attempts, protocol, failures and root recount are retained.

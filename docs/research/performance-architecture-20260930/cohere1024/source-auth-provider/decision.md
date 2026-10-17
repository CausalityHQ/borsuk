# SOURCE SHA256 provider: valid screen, no adoption

The SOURCE-only AWS-LC candidate69a2281 was correct on affected tests and the real canary, but the predeclared fixed32-query ABBA screen did not demonstrate a useful quality/performance/resource gain. Keep the existing sha2 implementation. Do not run full1000 or change production defaults for this candidate.

All four native cells and all four authenticated completed readers exited0. Returned IDs, score bits, physical ordinals, plans, traces and ALL logical read/byte charges match exactly. Each cell scored309/320 hits (96.5625%) on the selected32; this is not the full1000 recall. The unchanged incumbent's previously measured full1000 recall remains97.83%.

Candidate/control pooled query CPU ratio1.0136983198848009 (+1.37%); wall1.012255194609727 (+1.23%). CPU forward1.0181732823453633/reverse1.0092250627325554; wall forward1.0021614273109676/reverse1.0224954660881607. Candidate did not improve either order's CPU or wall. p95 forward205.044690->201.423909ms, reverse193.928684->197.863170ms: mixed. These are small-panel observations, not significance or general regression claims.

Identical per-cell SOURCE3474GET/914411520B and SQ81004GET/290875648B, zero failedGET. PeakRSS control74,907,648/74,440,704B, candidate75,522,048/73,347,072B. No demonstrated memory or quality advantage. No vendor, billed cost, concurrency or cold-cache guarantee.

This disposition uses no arbitrary minimum gain: no useful gain was established. Keep the unintegrated source and all evidence for research. No SOURCE-auth provider production code is adopted.

Correctness:38 affected tests, workspace Clippy correctness+suspicious and full workspace test compilation passed. Root release passed; actual-input admission plus separate2-query canary both native/strictreaders passed with exact semantic+charge parity. Both independent reviews found no correctness/authentication/portability/memory blocker, and warned speedup remained unproven. sha2 already uses SHA-NI; this was not missing hardware acceleration.

Source-only feature switch uses aws-lc-rs1.18.1 only !sq8; all checks remain and SQ8 retains sha2. Historical0228/7256 backend was NOT SQ8-only: it also affected SOURCE open/local-validation through the shared verifier. That corrected scope does not change the old arm's VALID REJECT or its original20% gate.

Preserved setup failures: worker missing cc, intended placeholder oracle failure, corrected zero-test filter; root releasea0001 nonexistent --features s3 command error, prelaunch missing-rg observation. None is algorithm rejection. Releasea0002 and runtime gates are separate successful attempts. Only terminal measurement cells were inspected.

Next: independent contextless source-grounded plan ab9a07dc3b7c46dd examines remaining measured CPU paths and proposes one falsifiable intervention; no new algorithm selected yet. Formats remain unreleased, but authentication must not be weakened. Host remains actively used for this bounded planning task until13:00UTC deadline.

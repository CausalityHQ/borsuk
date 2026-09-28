# Bounded planner trace: verified prerequisite

Delivered code changes the hidden offline diagnostic API to return the existing ranked candidate pages, initial seed page, actual two-bit primary page, each phase's unique evaluated unit IDs, and work exhaustion. It moves existing vectors without rescoring. Normal planning/search, routing/defaults and persistent formats are unchanged. The demo preserves normal output and trace page arrays, adding the metadata; trace mode preserves at least the original400000-byte codec allowance.

Retained record/IDs are conservatively charged before codec preparation using authenticated rows,159 candidate pages,128 seed evaluations and1272 walk evaluations. Graph/planner work buffers remain under the separate existing1MiB per-query admission allowance. The caller owns returned trace payload after query slot release; allocator/runtime/transport overhead is not measured RSS. One-page generations have no walk. This is preparation for a topology mechanism experiment, not a quality or serving result.

## Closed AWS evidence

All cells used profile causality in eu-central-1 on bounded Spot with4 Cargo jobs,1500-second test and1800-second worker caps,80GiB encrypted disposable EBS, immutable source/attempt reservations, terminal-only observation and automatic termination. No local Rust builds/tests/queries or Spark work ran. Red cells used Graviton c7g.2xlarge/AZc; final green used the established x86 c7i.2xlarge/AZa whole-workspace platform.

| Cell | Closed result | Decision |
| --- | --- | --- |
| red/a0001 | Demo compiler E0308; no tests ran; terminal exit92 | Invalid TDD gate. Correct only the default demo record and retain failure. |
| red/a0002 | Named generation fixture failed exactly once with the required unimplemented trace panic; Cargo101, expected-red worker0 | Intended red observed before implementation. |
| green/a0001 | Focused generation fixture1 pass; full workspace/all-targets2683 passes,0 failures,26 existing ignored across146 targets | Trace parity/admission prerequisite passes. |

The extended fixture checks identical diagnostic/normal plans, bounded unique in-range nonempty phase IDs, seed membership, actual primary selection, non-exhaustion on the small graph, and two scratch rejection paths. Existing graph tests separately exercise exhaustion. This does not establish corpus-level control parity or a large-graph quality effect.

Each terminal and all four artifacts were independently SHA/length checked. Before implementation, all390 Rust/Cargo files matched red/a0002's frozen stub archive. After green, all390 matched the final archive, with no changed source files. Source and terminal SHA values are in each cell's verification.json. The failed first archive differs from the corrected red checkpoint only in the demo caller.

| Instance | Observed wall seconds | Estimated compute USD | State |
| --- | ---: | ---: | --- |
| i-07ff3917aa43ee9e9 | 217 | 0.0108 | terminated |
| i-044367b330bcb0f3d | 257 | 0.0129 | terminated |
| i-0d8d9518641d2872d | 896 | 0.0530 | terminated |

Costs are estimates from observed actual-AZ Spot quotes and controller elapsed time; they exclude EBS/S3 and are not invoice or product lifecycle cost measurements. Sourcebase is e4438da7; final dirty-source archive1480ec3a8100485cbdf2598074bbd6e5641b87e1d68aa0cd962446d2f57b6123; terminald3484117592025be187de44cbb2f648a9d10ac495a6f0d106bec62e667aa079f.

Narrow Opus d1b5ae694cc74a9c found no code blockers; scope and independently verified accounting/harness limitations are recorded in review-reconciliation.md. The architecture dual-review cooldown and conditional mechanism decision remain intact. No paid job or consultation remains active for this slice.

## Next and remaining gates

Continue next-gate.md: immutable graph/root adapter, current-v3 control identity and exact plan/score parity, alternating paired dev64 replay and complete resource/spend preregistration before launch. Do not launch quality from this correctness gate alone.

Existing ReLAION consumed method-validation256–999 still fails the0.5pp exhaustive-SQ8 deficit gate:98.8508064516% returned recall@100 versus99.5685483871% exhaustive, a0.717742pp deficit. CoHere validation was not run after that failure. No quality query ran in these cells, and no new cold HTTP percentile, QPS, physical S3 request/RSS or total-dollar result exists. Fresh confirmation, post-maintenance quality and rewrite cost,1M/10M/100M scale and matched wins against BOTH S3 Vectors and Turbopuffer remain unqualified. The full native product goal stays active; no operator decision is required for preparation.

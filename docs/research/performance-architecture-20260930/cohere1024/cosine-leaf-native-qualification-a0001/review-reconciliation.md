# Metric ranking review reconciliation

Reviewed candidate: 4883bb0bd81c9cc268a1e0c45c5473c07a520d48. Dual group: 92f05671e5fc4a53. Research actual provider fallback Sol; engineering Astra. Full separate answers retained in metric-review-results.md.

Both reviewers found no production implementation correctness blocker and confirmed unchanged L2 serving semantics. They independently identified that the original zero-prototype authenticated fixture could not distinguish public metric dispatch. Repaired in private child bf7b1b7bcb148f2ecfe58c856ff172bca3b82b82: root-authenticated nonzero unequal prototypes, literal public L2/cosine order, explicit first16 successful nomination. Same four test names retained. All15 affected tests executed on exact repaired source, terminal0, source before=after, cgroup drained; no mock evidence substituted.

Research norm-convention finding: documented that this routing cosine differs from VectorMetric::distance near zero; zero prototypes remain neutral, nonzero finite queries use f64 norm/dot. This is intentional routing behavior, not a shared scorer change.

Both threshold findings: next-gate.md separates SOURCE-scored closure coverage from returned recall. Historical coverage314/320 versus returned313/320. Paired net gain requires315 coverage if paired control reproduces314; returned314 is only a subsequent consumed-panel mechanism screen. Both arm selections must be sealed before truth, with preprocessing/source/config pins; gained/lost slots separate from fetched bridge pages. Unchanged SQ8 limits explicitly32GET/16773120B.

No second dual review started for these small demonstrated repairs. Native affected pass is evidence for the fixture; release, workspace/all-target Clippy and actual complete workspace test compilation remain required before integration. New policy has no production caller, schema/default/training/layout change. No current recall, cold latency, QPS, cost, or vendor win inferred from the fixture or qualification.

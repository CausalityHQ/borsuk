**Retain d014838 as a small, workload-specific CPU improvement**, based on the supplied evidence. Preserve historical verdicts; this result supports retaining the quality gain without imposing a new minimum speedup.

| Metric | Observed candidate change |
|---|---:|
| Total CPU, mean across runs | **3.30% lower** |
| Planning time, mean across runs | **7.40% lower** |
| Wall time, mean across runs | 0.15% lower; paired directions disagree |
| Mean of run-level p95 values | 1.38% lower |
| Retrieval | Identical semantic digest; **0.9783 recall@10** throughout |

CPU reductions were 3.76% and 2.84% in the adjacent pairs. That is encouraging descriptive evidence, without establishing statistical significance, a throughput gain, or a competitor win. Averaging run-level p95 values does not produce a pooled p95.

Relevant simulated perspectives—not expert testimony:

- **Systems:** preserving lane accumulation order and retaining scalar tails makes the mechanism plausible. Objection: serial execution does not establish concurrency behavior; variable RSS does not demonstrate memory savings. Keep **+1,200 bytes per active query** as a conservative resource model, not a measured stack claim.
- **Statistics:** ABBA helps balance linear time drift. Objection: two runs per arm on one host cannot characterize variability; 1,000 queries are not 1,000 independent treatment repetitions.
- **Retrieval:** matching semantic digests and hit counts strongly support equivalence on these requests. Objection: they do not establish equivalence across dimensions, distributions, or boundary cases.

**One next plan:** retain this candidate, then inspect the planning call sites and stage attribution before choosing another change. Planning still consumes about 31.5 seconds per candidate cell. Establish the full32 frequency and separate accumulation cost from allocation, copying, and surrounding traversal. Optimize only the largest demonstrated avoidable cost. The supplied totals do not justify another SIMD rewrite or identify planning as the overall bottleneck.

**Bounded falsifier, proposed only:** before further optimization, run one frozen reverse-order **BAAB** block with the same inputs, host configuration, and limits. Require the identical semantic digest and hits in every cell; any mismatch falsifies equivalence. Predeclare that nonpositive aggregate CPU savings in this block defeats the claim of a repeatable CPU improvement pending investigation. Report wall time, p95, and RSS independently; do not turn them into retroactive gates. A positive result remains descriptive.

I did not inspect source or independently verify receipts, and ran no builds or experiments. Main risk: the measured benefit may not persist under concurrent or broader workloads.
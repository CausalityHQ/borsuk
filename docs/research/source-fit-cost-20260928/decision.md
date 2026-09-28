# Closed source fitter cost gate

2026-09-28. Frozen source base bc5c87ad3fa8636fc15ec303c466632fcf521f92, archive9075006cab1b455a4b491069ee4f9aa3d4187f34876da5098d74a411843b2de8. One AWS Causality c7g.2xlarge Spot worker i-0e5340bab72fc12ef, eu-central-1a, independently verified terminated. Native controller session6029 closed exit0; terminal complete exit0. This means successful collection of all four outcomes, including the declared flat timeout; it does not mean all fitters passed.

## Verified measurements

ReLAION source-only, D768, no query split and no query/truth access. Four CPU affinity, 4GiB process address space, 1GiB fitter admission. Registered V85 first100k and V36 1M source objects, exact Parquet lengths and hashes. Same CLI and normalized inputs within each scale; source/layout hashes differ across scales. No equality of the two corpus prefixes is claimed.

| Source | Existing fitter | Wall seconds | Peak RSS KiB | Outcome |
|---|---|---:|---:|---|
| ReLAION100k | flat control |7.23 |83,420 |Complete; historical raw/normalized/order SHAs reproduced |
| ReLAION100k | hierarchical |7.75 |41,248 |Complete; valid permutation and150 extents |
| ReLAION1M | flat control |120.03 |793,796 |Timeout exit124 at120s; no completed order |
| ReLAION1M | hierarchical |131.67 |388,428 |Complete; valid permutation and1470 extents |

All successful hierarchical extents are contiguous and1..1024rows. Independent closed-artifact verification rechecked all28 artifact hashes/lengths, all three complete permutations and both extent inventories. Source archive SHA/18,538,408bytes verified;374 archived Rust/Cargo/harness/config files matched the current tree. Verification is in verification.json. Terminal SHA354c2f217540d696319c65e9ccd4b703a2821d6f3cf248b0527aaa177c7191a5.

Whole source probe333.45s/maxRSS793,796KiB; compile319.93s/maxRSS4,395,104KiB is separate and outside the fitter4GiB cap. Cargo/rustc1.98.0, NumPy2.3.3, PyArrow24.0.0, AWS Graviton3. Worker observed767s, compute **estimated $0.0436** from observed Spot quote0.2048/hour, excludingEBS/S3 and not an invoice. No physical request/byte accounting or total lifecycle dollar result.

## Decisions and limits

Flat1M fails this fixed120s envelope. Do not extend or rerun it. Its full completion time is censored; hierarchical131.67s is not proven faster than flat, whose observed lower bound is120s. Observed RSS at timeout is not flat's final completed peak. Hierarchical passes mechanics/source-feasibility at1M only. No returned recall, serving latency/QPS/RSS, generation swap, update maintenance or vendor comparison was measured.

The source arithmetic at100M admits59.512GiB hierarchical payload (assumed24B RankedRow), not measured RSS; graph assignment has no fixed work guarantee and skewed trainer depth is not proven logarithmic. Its1M user CPU133.17s versus wall131.67s supplies no32-core speedup evidence. No100M extrapolated duration or cost is promoted to a measurement. Full generation source/build/compaction remains unqualified, including308GB canonical source and~842.8GB workspace admission.

The hierarchical layout's existing mean/minimum-summary query routes remain KILL; see prior-layout-evidence.md. Successful fitting does not repair their quality or license a new extent router. Existing compaction preserves surviving order and appends updates without semantic refitting; post-compaction quality/rewrite amplification remain open. Latest library assurance remains the unchanged e6c2e97 AWS full gate2683passed/0failed/26existingignored, not a new full-suite run.

## Ordered next gates

1. Reconcile the default dual architecture critique after its earliest18:16:42UTC eligibility, using next-review-question.md and this terminal evidence; check durable jobs and never duplicate. The narrow harness GO does not replace this gate.
2. Decide whether the topology-only paired consumed-development100k falsifier is useful despite the remaining layout/lifecycle gates, or stop before quality. No candidate is selected by this cost result.
3. A surviving query mechanism requires a separate scalable-layout quality gate, authenticated fresh cohort, then frozen1M coldHTTP,10M/100M and matched BOTH-vendor recall/latency/QPS per total lifecycle dollar. Existing1000-query panels are consumed; no confirmation cohort is sealed.

No active paid BORSUK job remains, no operator decision is needed now, and the full product goal stays active.

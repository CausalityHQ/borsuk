**Choose A: retain unchanged SQ8 scoring and address physical fetch locality. The evidence does not yet justify a specific replacement layout. Do not implement B or continue the rejected residual arm.**

Reviewed `743b3edcb7a52e852105e7038fb4265596d03d7b`. I independently authenticated all 33 terminal bodies, recounted all 128 rankings, and matched the eight native source files to the qualified revision. No files edited, tests run, or experiments launched.

1. **P1 — The residual mechanism validly fails its frozen ranking gate.** The failure occurs after authentication, training and encoding, when ranking the fixed 4,096-row cohort against unchanged SQ8:

   | Panel | Intersection total | Mean overlap | Fourth-smallest |
   |---|---:|---:|---:|
   | ReLAION | 6287/6400 | 98.234375% | 96 |
   | CoHere | 6163/6400 | 96.296875% | 93 |
   | Required | ≥6336/6400 | ≥99% | ≥98 |

   Native exit, resource closure and same-instance termination agree. This rejects the specified estimator/cohort mechanism; it does **not** measure query/GT recall, prove all four-bit methods impossible, or establish serving performance. [Closed decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/source-probe/native-diagnostic/a0001/decision.md)

2. **P1 — B lacks the required causal evidence.** The implemented score is `original_norm − 2·ordered_f32_dot(q, p+r̂)`. Original norm bytes are preserved; the predictor is reconstructed without normalization. Against the decoded-coordinate target, ideal error is `−2q·(r̂−r)`. Radial error can affect that dot product, but the closed results do not separate radial, directional and numerical contributions. Native SQ8 also includes the query constant and uses different accumulation order, so bitwise equivalence is not promised. A reconstructed-vector cosine scorer would be a distinct estimator, not a demonstrated correction. The earlier passing decoded-SQ8 cosine control supplies no causal attribution for this residual representation. [Scorer](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/pq_residual_four_bit.rs:242), [frozen semantics](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/prospective-method.md:15)

3. **P1 — A must preserve useful incidental coverage while eliminating expensive gaps.** My authenticated recount of the earlier sealed results found:

   | Closed SQ8 quantity | ReLAION | CoHere |
   |---|---:|---:|
   | Same-population plans fitting 16 MiB | 50/64 | 32/64 |
   | Maximum same-population bytes | 32,747,520 | 27,081,600 |
   | Maximum bytes in nominee-containing 16-row groups | 8,224,320 | 8,748,480 |
   | Returned top-100 appearances outside those groups | 3 | 52 |

   The 52 CoHere appearances span 27 queries; these are returned-result appearances, **not** independently classified truth hits. Fetching every old row remains too large under any permutation. Fetching nominees alone has the established `.95609/87` coverage ceiling. Thus locality must reduce gap bytes while retaining enough useful surrounding rows. The frozen graph-affinity permutation already failed—35/64 and 16/64 fits—and supplies no successful replacement. [Sealed result roster](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/aws-terminal.json), [actual packing decision](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-diagnostic/a0002/decision.md)

4. **P1 — Release gates remain unresolved.** The 19 recorded gates establish affected correctness, compilation and Clippy; full workspace tests were compiled, not all executed. Payload-range counts exclude startup. Product acceptance must charge metadata, retries and recovery against the declared **total** GET boundary. Exit zero with `status: REJECT` must remain a completed rejection in automation. [Qualification](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/implementation-gates/a0002/decision.md), [range-accounting declaration](/home/rb/worktrees/borsuk-prod-ready-v9/docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/native-diagnostic/a0001/screen/report.json)

**Required before choosing A’s implementation:** finish a closed-data, per-query group table separating nominee groups, bridge-only groups, SQ8 returned winners and their already-scored truth contributions. Compute mandatory-group bytes, bridge bytes, and exact minimum 32-range cover; include the groups containing incidental winners and cross-query co-selection. This is diagnostic evidence, not permission to train a layout on consumed queries or truth.

The cheapest subsequent native falsifier is a **metadata-only replay of one independently frozen, source-only layout** against all 128 retained nominations. Verify mappings and nominee preservation; compare its cover arithmetic against an independent interval-DP oracle, with exhaustive tiny cases. Charge every bridged row and reject any envelope violation. Reuse the prior bounded packing envelope—CPU1, 256 MiB, no swap, 300 seconds—only after aggregate admission. Survival then requires a separate unchanged-SQ8 paired recall gate on the actual new fetched populations: mean ≥98%, fourth-smallest ≥95. Preserve generation/hash authentication and seal results before truth reduction.

For D768, justified arithmetic is:

- SQ8 payload: `780T`; 16 MiB permits 21,509 rows, or 1,344 complete 16-row groups.
- At 100M, payload alone is **78 GB**, before replication or additional mappings.
- Extrapolating current admission terms gives approximately **59.003 GB per resident generation**; the graph allowance itself is 51.2 GB.
- Current SQ8 query allowance is `4N + 3B + 8 MiB + 256·min(N, floor(B/780)) + 4D`: approximately **464.23 MB/query** at 100M and B=16 MiB. Charge distinct pinned generations, concurrent opening transients, deltas, maintenance and runtime additionally. [Resource model](/home/rb/worktrees/borsuk-prod-ready-v9/crates/borsuk/src/fine_sq8_groups.rs:245)

Those are projections beyond the current 100k guard, not measured 100M feasibility or a vendor win. Root retains layout selection and launch authority.

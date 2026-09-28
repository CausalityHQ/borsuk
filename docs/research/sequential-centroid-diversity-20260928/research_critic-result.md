# Verdict: STOP the paired 100k topology falsifier as drafted

Run one offline headroom check first. It costs almost nothing and decides whether the graph topology is the layer causing the misses at all. I edited no repository files, ran nothing and launched nothing. I did add one memory note under `~/.claude/.../memory`.

## Factual and design blockers (these apply to this experiment, not to later qualification)

**1. At 100k the test runs in a near-exhaustive regime, so it cannot isolate the graph's navigation.** From `crates/borsuk/src/two_bit_generation.rs:377-416`:
- 100k rows gives 391 pages of 256 rows and 3,125 units of 32 rows.
- The seed search does 128 evaluations and the walk does 1,272, so one query evaluates 1,400 of 3,125 units (about 45%).
- The candidate count is `min(pages, 159)`, so 159 of 391 pages (about 41%) are then scored by the two-bit plane.
- `search_pages_seeded_impl` (`unit_centroid_graph.rs:649-760`) is an unbounded best-first flood that stops only at the evaluation cap. It then ranks pages by their minimum evaluated unit distance.

When a flood reaches 45% of the graph, link topology mostly changes the order in which units are reached, not whether they are reached. At 100M the same work covers about 0.045% of 3.1M units, which is where diversity pruning actually matters (this is the HNSW heuristic argument from Malkov & Yashunin, and V249 found the same for row graphs). So the result doesn't transfer in either direction:
- a null result does not kill diversity at scale;
- a pass says nothing about 100M navigation.

**2. The dev64 gates barely test anything.** The control already passes the deficit and mean gates on dev64:
- ReLAION: returned 6,346 vs flat 6,369, a 0.359pp deficit.
- CoHere: 99.09375% vs 99.234375%, a 0.14pp deficit.

The only gates that can fail are "+7 fetched GT hits" and "no returned regression". The ceiling for that gain is small: 28 misses on ReLAION and 15 on CoHere, so CoHere must recover about 47% of its misses with no losses. Even a pass would not touch the validation KILL, which needs at least 162 net returned hits on 744 queries. That panel is consumed, so rerunning it could only be exploratory. The draft admits this, but then a pass leads to no decision.

**3. The treatment changes more than it claims.** `build_with_diversity` passes the diverse builder to every HNSW layer (`unit_centroid_graph.rs:203-208`). That means the seed search `graph.search(..., 1, 128)` can land on a different seed page. The seed page is also forced into the plan as the primary page for the budget planner (`two_bit_generation.rs:397-405, 440`). It is still one mechanism, but the draft must trace seed/primary-page changes per query, or the attribution is incomplete.

**4. The control's layout cannot be the 100M layout (arithmetic).** The flat fitter needs 90 + 30 quadrillion multiply-accumulates, about 1.2e17 (`scale-preflight.json`).
- A 16-core AVX-512 c7i peaks at roughly 1.6e12 FMA/s. That is about 20 hours at 100% of theoretical peak, against a build target of 12 hours on 32 threads.
- At 1M it was cut off at 120s.

So the flat layout is not merely "unqualified"; it is infeasible under the declared build envelope. The only scalable fitter (hierarchical) has its unit-summary routes KILLed at 91.8–93.1% fetched on CoHere. A topology win would therefore attach to a layout that cannot ship.

**5. The V282 reference should not be able to block the run.** The draft makes a missing V282 identity a preregistration blocker. V282 is a different architecture used only as a historical reference. It should be disclosed, not allowed to block a causal comparison.

The control-parity design, the graph/root-only adapter and the kill-on-first-failure rule are sound. There are no correctness defects in them.

## Lifecycle and scale (later qualification, not blockers for a cheap check)

- The graph itself is cheap to maintain: it is an O(units) rebuild over centroids. Topology is not what makes the lifecycle fail.
- The unresolved lifecycle risks are elsewhere, and this experiment leaves them untouched:
  - compaction appends updates with no semantic assignment, so layout quality decays;
  - about 842.8 GB of workspace admission and a 308 GB canonical rewrite;
  - maintenance is same-host and quiescent only.
- The 34.6 GB component ceiling fits under the 48 GiB target on paper. RSS is unmeasured.
- The two-vendor comparison is untouched. None of this blocks a cheap check, but none of it is advanced by one either.

## The one gate: an offline perfect-navigation headroom check

Use the already-reconstructed current-v3 control generation.

**Panels:** dev0–63 on both corpora, plus ReLAION val256–999 labelled as a diagnostic on an already-consumed panel. Nothing is selected from it.

**Per query:**
1. Build an oracle candidate set: the same seed page plus the top 158 pages ranked by exact minimum unit-centroid distance over all 3,125 units. V146's flat scorer is the authenticated oracle for this.
2. Run the oracle set through the unchanged two-bit ranking, budget planner and SQ8 scoring, then record fetched and returned GT hits.
3. Classify every control miss into one of three buckets:
   - **(U) unreached:** no unit on the GT page was evaluated;
   - **(R) reached but outranked:** the page was evaluated but ranked beyond the 158 slots;
   - **(O) oracle miss:** the page is not in the oracle top 159 either.
4. Record the fraction of units evaluated and the `work_exhausted` flag.

**Decision rule, fixed in advance:**
- If oracle fetched gain is below 7 hits on either dev64 corpus, or oracle validation returned gain is below 162, STOP the topology arm permanently. Even a perfect graph could not pass the gates. The responsible layer is then the unit-centroid min-distance page score, or how candidates convert into pages, not the links.
- Otherwise GO with the fixes below. Bucket U vs R also shows which part of the search a better graph could affect.

**Cost:** this reuses existing primitives (`search_pages_seeded_with_scores`, `UnitCentroidPages`) plus a small harness. It needs one small AWS Spot run of CPU-seconds, about $0.05, with no new query consumption.

## If the oracle shows headroom: fixes before one bounded Spot launch

1. Preregister the oracle headroom as the effect ceiling, and report the candidate's gain as a fraction of it.
2. Trace per query: seed/primary page changes, units evaluated, distinct pages reached, exhausted flag, and hits gained and lost separately (not only net).
3. State plainly that the 98% mean, 95 p05 and 0.5pp gates don't bind on dev64, and that a pass does not address the validation KILL.
4. Run both graphs in the same binary and process with alternating order. Record build time, graph bytes and `graph_resident_bytes` against the cap for both.
5. Demote V282 to a disclosed reference only.
6. Label the result as a 100k-regime mechanism observation with no scale transfer. It should be followed by a navigation test at a non-degenerate fraction (1M or more), not by promotion.

## Prioritized next steps

1. Run the headroom check above. It is the only step that should happen next.
2. Then either:
   - it shows no headroom: close topology and record the page score as the responsible layer; or
   - it shows headroom: run the corrected ReLAION-then-CoHere falsifier once.
3. Separately, and more important for the product: the build blocker is the flat layout's infeasibility at 100M, not the graph. The untested pairing is the hierarchical layout with the flat route's 159-candidate two-bit re-rank held fixed, measured under the same oracle. That isolates the layout as the only change. The earlier hierarchical KILLs used direct extent nomination, not this route, so this is not reviving a killed arm. It needs its own design review because hierarchical extents are variable-size, 1 to 1,024 rows, and don't map directly onto 256-row pages.

**Final: STOP.** Run the offline perfect-navigation headroom check first. GO on the topology falsifier only if that check shows at least 7 recoverable fetched hits per dev64 corpus and at least 162 returned hits on validation.

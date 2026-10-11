## research_critic

What this change does: `4883bb0` adds an explicit metric ranking over all authenticated leaves. It preserves production L2 selection. **I found no implementation blocker in the scoped diff; green qualification and real coverage evidence remain outstanding.**

1. **The metric arithmetic and literal oracles are sound.**
   In candidate `semantic_unit_router.rs:1271`, cosine uses f64 norms and dot products, clamps similarity, rejects zero queries, and gives zero prototypes distance 1. For admitted finite f32 inputs and dimensions ≤1024, even squared subnormal values remain representable in f64, while maximum-value accumulations stay far below overflow. No rescaling framework is needed.

   The synthetic L2 distances are `0.765625` for leaf 0, `0.25` for leaves 1–48, and `4` for leaf 49. Cosine instead puts leaf 0 first. Thus `[1..48]` and `[0..47]` are valid independent policy oracles. ID tie-breaking applies to equal computed scores; floating-point arithmetic does not promise exact ties for every mathematically identical direction.

2. **The old L2 path does not drift.**
   The subtraction, f64 accumulation, sorting, first-eight boundary, `1.15` multiplier, and profile limits remain unchanged. Production `two_bit_generation.rs:1662` still calls `select_leaves`; the new method has no production caller. I found no added aliases, compatibility readers, schema changes, or training changes.

   The full-ranking API correctly leaves truncation to its caller. `nominate_selected` validates count, IDs, duplicates, and closure bounds; it does not establish that the caller chose the highest-ranked leaves. The experiment must explicitly take the first 48.

3. **Should fix: document the routing-specific cosine convention.**
   The existing `VectorMetric::distance` treats zero or sufficiently small norms as maximum distance 2 (`metric.rs:824`). This helper intentionally treats exact-zero prototypes as neutral and accepts arbitrarily small nonzero norms. For example, it can rank a zero mean ahead of a negatively aligned mean.

   The current documentation states the behavior correctly. Add one sentence saying it differs from `VectorMetric::distance`, so a later “deduplication” does not silently change the experiment.

4. **Should fix: the authenticated API test cannot detect ignored metric dispatch.**
   Its fixture contains only zero prototypes (`semantic_unit_router.rs:1492`), so L2 and cosine both return ID order. It would pass if the public method always selected L2. The private helper tests distinguish the policies correctly.

   Smallest repair: add one nondegenerate authenticated fixture where the public API produces different L2 and cosine orders. No additional test framework is needed.

5. **The memory statement is defensible as payload, not RSS.**
   On this 64-bit host, scored tuples use `16N` bytes and a separate ID output uses `8N`; `24N` is a conservative allocation-payload bound. Admission limits Scale1m to at most 977 leaves, giving 23,448 bytes. Allocator overhead, sorting stack, resident router data, and concurrent calls are outside that statement. Clarifying “temporary plus output payload” would improve it.

The first-principles limitation is substantive: for unit query \(q\), prototype length \(r\), and directional similarity \(s\), L2 scores \(1+r^2-2rs\). Cosine removes \(r\), but mean length may encode concentration, cancellation, and raw-vector magnitudes. Promoting a weak aligned mean can help; normalizing a weak, noisy direction can also displace a useful leaf. The counterexample proves a policy distinction, not the cause of the six misses.

Cosine ordering is equivalent to Euclidean ordering over normalized nonzero prototypes. An angular `acos` transform adds no ordering benefit. Inner-product ranking would retain mean magnitude and test a different hypothesis; mixing these alternatives would weaken this single causal screen.

The [planned gate](/data/target/borsuk-routing-coverage-next.pending/next-gate.md:9) appropriately requires net coverage gain across all 320 slots, seals selections before truth access, preserves caps, and labels this consumed panel diagnostic. Before launch, make three details explicit in the frozen protocol:

- Seal both control and candidate selections, with the exact query preprocessing and source/configuration identities.
- Define paired coverage as **SOURCE-scored closure**, reporting gained and lost slots separately from fetched bridge pages.
- Separate the coverage threshold from returned recall. Six coverage misses plus one covered ranking miss imply historical coverage of 314/320 and returned recall of 313/320; a net coverage survivor would need at least 315 covered slots if the paired control reproduces that baseline. Freeze SQ8’s numeric limits explicitly: 32 GETs and 16,773,120 bytes.

**Verdict:** suitable for root-owned green qualification, with the small documentation/test repairs above. No basis yet for production promotion or recall, latency, QPS, or vendor claims.

The red receipt corroborates successful compilation followed by the expected assertion failure, exit 101, drained cgroup, and zero OOM events. I ran no qualification or data experiment, accessed no query/truth/vector payloads, and changed no files. External prior-art literature was outside the permitted scope.

## engineering_critic

What this change does: `4883bb0` adds explicit full-leaf metric ranking while preserving production L2 selection. I found no implementation correctness or security blocker in the reviewed diff.

1. **Clarify the coverage threshold before launching the probe.** [next-gate.md:11](/data/target/borsuk-routing-coverage-next.pending/next-gate.md:11) says `>=314/320`, while line 9 requires positive net source coverage. With six source-coverage misses, the baseline is **314/320**; another 314 is zero gain and must reject. Smallest repair: explicitly require **source coverage ≥315/320**, and label **returned recall ≥314/320** separately if that is the intended later screen. The existing net-gain requirement is correct; the numeric shorthand is ambiguous.

2. **The public API test cannot detect ignored metric selection.** [semantic_unit_router.green.rs:1492](/data/target/borsuk-routing-coverage-next.pending/semantic_unit_router.green.rs:1492) builds entirely zero prototypes, so cosine and L2 both produce ID order. Hardcoding L2 in the public wrapper would pass every new test: the distinguishing oracle exercises the private helper. Smallest repair: add one authenticated fixture with unequal, nonzero prototypes where the public cosine and L2 orders differ, then exercise explicit truncation and successful `nominate_selected`.

The remaining checks look sound:

- **Math and stability:** cosine uses f64 norms/dot products, handles zero prototypes as documented, rejects zero cosine queries, and clamps roundoff. Finite f32 extremes at admitted dimensions fit comfortably in f64.
- **L2 preservation:** accumulation, distance checks, sorting, and profile cutoffs are unchanged. No production caller uses the new helper.
- **Oracles:** the fixed-48 expectations are valid: L2 selects `1..48`; cosine selects `0..47`. The literal axis-aligned extreme-value expectations are also valid.
- **Authentication and bounds:** authenticated leaf IDs equal their positions. Nomination still validates counts, IDs, duplicates, and closure. The 24-byte-per-leaf figure is a conservative ranking-buffer payload bound, **not total memory or RSS**.
- **Scope:** no schema, training, compatibility alias, or default-policy expansion. Sealing selections before truth and measuring all 320 slots makes the planned gate appropriate as a consumed-panel diagnostic only.

**Verdict:** proceed with exact-source green qualification; resolve the threshold wording before coverage execution and strengthen the public API test.

Not checked: execution, compilation, Clippy, or runtime receipts, as requested. The supplied green source matches the candidate Git blob by SHA-256; the dirty worktree was untouched.

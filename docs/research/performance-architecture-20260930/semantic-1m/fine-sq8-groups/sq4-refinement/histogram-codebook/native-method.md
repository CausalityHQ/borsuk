# Histogram SQ4: one native scoring intervention

Status: prospective, unimplemented and unqualified. Root decision after closed uniform SQ4 a0002 and read-only consultation bd62649f807d4f69. No fresh recall, cold latency, QPS, scale or vendor claim.

## Causal evidence and choice

FIRST100k D768 cosine k100 consumed64 per dataset: identical-population SQ8 recall ReLAION .99515625 / CoHere .9903125; uniform SQ4 .93078125 / .89046875. All 128 range/byte envelopes passed. Keep original uniform REJECT immutable. Try one dataset-wide per-dimension nonuniform 16-center book. Rotation plus residual correction changes more arithmetic and field semantics; it remains a separately specified fallback, not another simultaneous arm.

## Native algorithm

Stream every authenticated original SQ8 row, validating exact EOF, group hashes, whole SHA, IDs and finite original norm, into checked u32 histograms H[D][256]. Neither queries, nominated subsets nor truth may influence training. Train datasets sequentially; pass two reauthenticates the original source before encoding.

For each dimension, solve weighted contiguous 1D clustering over occupied unsigned bins with K=min(16,occupied bins), f64 prefix costs and full dynamic programming; deterministic equal-cost split picks smallest predecessor. W/M/V use widened checked count arithmetic and f64 moment arithmetic. The occupied-bin count per axis equals admitted N. Empty or inconsistent histograms refuse. Fewer than 16 bins preserve every occupied bin and pad deterministically. Store bin-coordinate weighted means as finite ordered f32 centers. This is exhaustive DP search under computed f64 costs, not exact real arithmetic. Negative cancellation and numerical tolerance must have explicit bounded handling verified by independent direct-deviation enumeration.

Map all 256 unsigned bins to their closest stored center, smaller index on equal distance. Explicitly report decoded f32 reconstruction SSE against uniform17; do not claim mathematical optimality after rounding or infer recall from MSE. Fix and record any numerical allowance before native corpus execution.

Packed rows stay i64 ID + recomputed f32 squared norm + ceil(D/2) nibble bytes (396B at D768); lower nibble first, unused odd nibble zero. Reconstruct x[j]=low[j]+center[j][code]*step[j] in exactly that operation order. Recompute norm sequentially in f32.

Prepare query using existing cosine_vector. Preserve original sequential arithmetic:
shift += q[j]*low[j]; qnorm += q[j]*q[j]; weight[j]=q[j]*step[j]; shift -= qnorm/2.
inner += center[j][nibble]*weight[j]; score=norm-2*(inner+shift).
Rank score.total_cmp then ID. No reassociation/FMA, norm division, source normalization or copied old norm. Uniform centers17*i must reproduce old uniform scorer bitwise. Arbitrary centers cannot expand into u8 SQ8 rows.

## Library and authenticated layout

Implement reusable bounded Rust Codebook/trainer/packed scorer in existing fine_sq8_groups.rs; minimal existing CLI addition in bin/hierarchical_semantic_cells.rs. No dependency/controller framework or unrelated refactor. Reuse the existing paired SQ4 resource/authentication/closure machinery. Old uniform entry retains its semantics; new strict schema/codec identity distinguishes this experiment.

Binary little-endian center object <=64KiB including header; root/manifest binds source records, coefficients, histogram/trainer/tie policy, codebook, packed payload/group hashes and compiled/config identities. Authentication and allocation admission precede book/body reads. Bad dimensions/count/order/finite state/padding/truncation/growth/hash/source binding fail closed. Book remains immutable for generation; updates map original SQ8 reconstructions through the same book. Retraining requires new generation/reencoding; never fit from SQ4 reconstructions. This increment does not claim existing update/GC integration.

## Resources

D768 histogram786432B, encoding maps196608B, one retained book49152B, reusable interval table524288B plus exact charged DP/backpointers/prefix/stream capacities. Checked admission before reads/allocation; tick guard during DP and both passes, charge cumulative operations/bytes. Discard histogram/maps/scratch after encoding, retain only admitted book per pinned generation. Query has direct packed scoring and bounded query preparation, no D+12 per-row expansion. Report actual capacities, startup bytes/reads, build time, score time and kernel peak separately.

Keep <=32 payload ranges and <=16777216 verified payload bytes/query. Book startup is separately counted; no claim of <=32 total cold requests with an extra book read. Future serving format may cofetch book with required metadata but is outside this increment. At 100M, packed row bodies alone39600000000B/generation; unchanged graph/router and pinned generations remain additional costs, with no feasibility claim.

## Native correctness before science

1. Weighted DP versus independently enumerated contiguous partitions/direct weighted deviations: holes, skew, ties, <=16 occupied bins, overflow/cancellation cases.
2. Exhaustive256 mapping; independent scalar nibble decode/norm/distance arithmetic; uniform17 bitwise parity, nonunit queries, nonzero low/non-dyadic step, zero/signedzero, odd D, short tail, ID ties/near-rank cases.
3. Authenticated codebook serialization/reopen and corrupt source/coefficient/book/payload/EOF; cap rejection before body reads and before coexistence.
4. Actual tiny builder/paired diagnostic, BOTH books/payloads sealed before requests, all128 plans/full results sealed before GT. Missing/tampered truth, late source growth, output cap and sync failures retain correct INVALID/partial accounting, no overwrite.
5. Exact source remote affected lib/bin tests, release binary, workspace Clippy correctness+suspicious, real shim-unset test compilation jobs1. No local Cargo. Source-only candidate is UNVERIFIED until these gates0.

## One later consumed-panel falsifier

Root freezes new native/config identities after qualification, authentic cheap fixture and infrastructure canary, then ONE bounded original native job. Reuse retained original inputs and frozen nominees/covers; evaluate learned candidate plus unchanged original256 SQ8 and exactly matched population SQ8. No truth training; reauthenticate source, books, payloads, all128 sealed outputs/prefix before truth.

Per dataset gate: >=6272 hits/6400, fourth-smallest hits>=95, all nominees retained, <=32 ranges and16MiB/query, authenticated supervisor original exit0/resources/drain/cleanup/durability. Report every128 result even after a quality failure; malformed late evidence yields INVALID, not completed REJECT. Record matched top100 replacements and rank-boundary score gaps pretruth. Sufficient replacement bounds are diagnostics, never reject by themselves.

Config/setup/compiler/authentication/supervision failures are INVALID, fix then rerun exact repaired source under a new immutable attempt. Valid quality/envelope failures are REJECT. A survival permits fresh1M object-native serving qualification; it does not establish production completion.

## Research sources and limits

Consult bd62649f807d4f69 requested Fable then Opus; both authentication failed, final answer GPT-6.1-Sol. Full answer and provider audit retained beside this spec.
Primary mechanism references: https://arxiv.org/abs/1701.07204 (exact 1D clustering DP); https://arxiv.org/abs/2504.19874 (rotation, scalar quantization and residual correction). Their workloads/dimensions are mechanism-relevant but not matched BORSUK results. MSE optimality does not establish ANN recall.


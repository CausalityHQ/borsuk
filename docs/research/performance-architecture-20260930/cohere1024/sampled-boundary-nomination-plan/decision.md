# Sampled row boundary nomination: implementation contract

Status: IMPLEMENTATION_AUTHORIZED, NATIVE_UNVERIFIED. Production adoption and paid measurement are not qualified by this decision.

Base: `3b70a8e3b863cca220ce1c15df5ac363f9e0aec3`. Plan `04b2d7cf56634a36`; independent review group `de63b45acc5340a3`, research `e72c26b670284d48` (configured Sol fallback after Opus usage limit), engineering `3ce26f1a08d94f05` (Astra). Both reject the original plan unchanged and support one corrected prototype.

## One causal intervention

Retain the existing physical layout, L2 ranking, scoring, normalization and SOURCE/SQ8 cover rules. Replace only nomination with 40 original L2 seed leaves plus eight leaves nominated by a source-derived adjacency prior. No parameter sweep, new centroid hierarchy, row copies, graph reachability repairs or cold claim.

Construct one sampled-row graph, without requests/truth open: physical sample `min(32*u+16, rows-1)` for each physical unit, reconstructed from authenticated SQ8 low/step and normalized by the existing library policy. Map unit `u` through physical membership, never logical ID arithmetic. Use the sequential nondiverse graph helper with m16/m0=32/ef64. This is a pruned approximate insertion graph; it is not an exact kNN graph. Historical discovery failures do not isolate diversity as their sole cause, and switching diversity would change this prior.

Each retained directed base arc contributes once to its source/target leaf count; use `tower.last()`, omit intra-leaf arcs and all upper-layer/artificial edges. Keep top16 destinations by count, leaf-ID ties. For zero-based seed ranks 0..39, accumulate checked votes `(40-rank)*count`. Exclude the 40 seeds, choose descending votes then original L2 rank then leaf ID, deduplicate, and fill shortages in original L2 order. Empty adjacency falls back to original L2. At least48 leaves produce exactly48 distinct selections. Declare midpoint bias, leaf-size/popularity bias and approximate discovery; do not repair these by introducing another arm.

## Authentication and ownership

Full SQ8 length/SHA/EOF, logical-ID bijection under this experiment's ordinal contract, physical-order SHA, calibration/geometry and file identity must pass before adjacency publication or any requests/truth open. Reject nonfinite decoded coordinates and zero normalization. SQ8 records include logical ID, stored squared norm and codes.

Bind a separately versioned sidecar to generation, root, membership, SQ8, physical order, dimensions, normalization, sample rule and algorithm parameters. Include its digest in the paired selection seal. Filesystem/manifest/ordinal benchmark restrictions stay in the example; the generic library operates on admitted geometry, samples and unit membership. The sidecar is experiment evidence, not serving publication authority.

## Scientific endpoint remains unchanged

SOURCE-scored **page closure** is the coverage metric, not direct owning-unit nomination and not fetched gap bridges. Original control must reproduce returned313/320 and SOURCE coverage314/320. Candidate survival requires coverage>=315/320 and gained>lost with paired accounting. The proposed zero-loss requirement is rejected as unsupported. Preserve the consumed32 scope and all scientific fetch/scoring limits. Survival licenses a returned-recall falsifier, not production adoption or a cold/vendor win. q537 remains a separate unexplained covered ranking loss.

## Implementation and admission

Own only `crates/borsuk/src/semantic_unit_router.rs` (generic construction/selection/tests) and `crates/borsuk/examples/compare_native_replay.rs` (source-bound streaming, distinct candidate entrypoint and existing seal/control/cover orchestration). Preserve the old cosine mode and immutable receipts. Keep the HNSW helper private. Reuse orchestration rather than duplicating a reporting framework.

The proposed 256MiB construction assertion is withdrawn. Implement a checked phase allocation model before allocating: samples, row headers, actual/bounded tower capacities, insertion/visited/heaps/pruning scratch, bounded flat directed edge-pair sort/aggregation, metadata, validation, serialization and query scratch. Never allocate a leaf-squared matrix. Drop sample vectors and row graph before paired replay; use max(construction phase, replay phase plus sidecar) with explicit lifetimes. Serialized sidecar size is not total resident memory or simultaneous-generation memory. RSS and cgroup peak are measured separately; the model is not a measurement.

Provisional remote construction envelope: CPU1, memory512MiB, swap0, 900s; no widening after outcomes. Admit the model before source construction. Staging allowance must include every exact input/output, initially 2GiB for this nomination-only screen; a later SOURCE replay requires a separate larger envelope. Remote compilation: jobs2, memory16GiB, disk64GiB, bounded execution. Use causality Spot EC2 only. No local Cargo/native/data parsing/mocks.

Required native fixtures: partial units; D1/D1024 and D1025 refusal; repeatable equal-distance graph/ties; directed base-versus-upper counting; isolated/empty cross-leaf fallback; exactly48 distinct selections; checked vote/admission overflow; nonidentity IDs/order; duplicate/corrupt/truncated/grown SQ8; wrong generation/calibration/membership; zero/nonfinite reconstruction; adjacency seal durability before requests/truth; paired seal before truth; identity rechecks and failure cleanup. Run affected tests, locked release, workspace/all-target correctness+suspicious Clippy and real shim-unset test-build on the exact source before handoff.

Then real-input source-only construction admission and a separate disposable smoke are required before freezing and launching the one scientific screen. Construction-limit failures under verified enforcement are resource failures; setup/identity/protocol defects are INVALID. Preserve failures and interruptions, exact instance/source/configuration receipts, terminal sync and immediate termination/volume cleanup. The sampled SQ8 result qualifies neither full-FP32 kNN nor Fable's cut-edge partition/layout. Build/rebuild lifecycle cost remains unmeasured.

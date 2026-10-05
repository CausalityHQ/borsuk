# Fixed SQ4 refinement falsifier

Prospective, not measured. Root source before amendment: adad658df36c1983db6f3f2add1d4efa992dcfcd. Research consultation 03891c062afa4323 completed exit 0; full recommendation in research-result.md.

## One intervention

Preserve original physical row order, group16, graph, PQ coefficients/codes, nominees and original SQ8 low/step coefficients. Encode each code c as nearest integer c/17 (integer (c+8)/17), nibble 0..15; decode as 17*n. No query/truth fitting. Record LE ID8, reconstructed squared norm f32, ceil(D/2) bytes; unused high nibble for odd D must be zero. Recompute norm using unchanged native decode/arithmetic semantics, never copy old norm. Bind original authenticated payload/router identities separately from new payload SHA and codec marker.

Use existing deterministic smallest-gap cover_pages with candidate row bytes 396 at D768, page_rows16, max_gets32. Assert every original nominee remains and the original 256-range row set is contained. No packing permutation, new routing, replication, second fetch wave or parameter sweep.

Native arithmetic inspection: sq8_source.rs computes decoded=low[d]+f32(code)*step[d], then accumulates norm+=decoded*decoded sequentially in f32. The fine-index wrapper passes cosine_vector(original query) once to exact_sq8_nominee.rs; preserve this same existing preprocessing in all three scoring arms. For that prepared q the kernel computes shift=sum(q*low)-sum(q*q)/2, weights=q*step, inner=sum(code*weights), and score=norm-2*(inner+shift). Preserve this operation order; no f64 substitution, reassociation or fused arithmetic in this arm. A nonzero original vector can reconstruct to zero after SQ4 quantization: retain and score that exact representation with norm0 rather than dropping it or borrowing the SQ8 norm. The scorer accepts finite zero norms. Independently test this case and the error from copying the old norm.

## Paired quality gate

Sequential ReLAION and CoHere FIRST100k/D768/cosine/k100, same consumed64 requests each. Authenticate original payload/source/requests and sealed nomination authority. Seal both transcoded payloads before reading queries; freeze all128 candidate plans and complete ranked outputs before any truth open. Evaluate candidate SQ4 and SQ8 reference on IDENTICAL fetched physical row sets, retaining incidental rows. The reference reads/memory are separately accounted and may exceed serving envelope; never label it an eligible32GET serving result. Retain original256-range returned baseline separately.

Survive only if BOTH datasets returned mean R@100>=0.98 and sorted p05 index3 hits>=95, all128 complete/authenticated, all nominees retained, every candidate<=32 ranges and <=16777216 verified payload bytes. Record nominee containment/fetched coverage/returned recall and SQ4-vs-identical-population-SQ8 loss separately. Completed quality/resource-envelope violation REJECT; setup/authentication/durability/observed resource failure INVALID. Fix environment/config errors and rerun unchanged science, preserving failed attempts. Supervisor original exit/resource/drain/cleanup required; standalone report cannot qualify.

## Gates and boundaries

Implement Rust library diagnostic and existing CLI only. Tiny native fixture must independently verify all256 code mappings/nibble order/reconstructed norms/nonunit queries/ties/oddD/tail/exact covers/corruption/caps and real complete pipeline seal-before-truth. Secure regular descriptors NOFOLLOW/NONBLOCK, checked allocations before body reads, streaming original authentication, bounded per-range expansion charged in coexistence budget. Do not read whole original payload into a Vec. Reserve durable terminal closure/no overwrite; sync failure INVALID. Compile affected target, release binary, workspace Clippy correctness+suspicious and actual shim-unset jobs1 workspace test-build on exact source remotely before integration. No local Cargo.

Proposed experiment ceiling CPU1/1GiB/noSwap/600s, data sequential, no graph/PQ rebuild; ROOT must validate actual memory/scratch models and freeze final config before launch. These are prospective ceilings, not measured usage.

## Limits of evidence

Closed original plans hypothetically fit: ReLAION max16625664B/41984rows, CoHere13749120B/34720rows at396B/row. No actual SQ4 recall or latency measurement exists. Latest graph-affinity packing validREJECT R35/64,C16/64 byte fits; historical environmentINVALID preserved. SQ4 does not solve current graph resident RAM (~59.003GB at100M projection), builder/lifecycle/write throughput, cold tails or matched vendor comparison. No production promotion or100M feasibility claim.

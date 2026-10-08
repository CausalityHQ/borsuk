# Four-row scorer: review reconciliation

Reviewed candidate: `6d6e679fcd4738a036ab8bfe716b86e54a916748`, parent `c49a2e6d2a035bfaf42358fbf7fb60abf11e4222`. Dual critique `56b427bbd7ed4bc1` completed; both reviews were read in full. This is source inspection, not native qualification or a speed result.

The engineering blocker is confirmed: the tiny fixture used application IDs with the ordinal-only builder. Separate test-only repair `68eeca47fd4230b10b113cc257ef62e2a8f80c8b` uses the existing explicit identity source permutation, preserving IDs and all assertions. Original commit and contract remain immutable evidence. Additional test-only witnesses for each winning lane/tail row, mixed signed zeros, and late error positions are requested from the same worker; final source pin remains pending.

Both critics found no additional production arithmetic or authentication defect. Their source review does not prove compilation, exact emitted arithmetic, dispatch execution, stack admission, or performance.

## Gate decisions

- Retain the preregistered packed independent-row SIMD gate. The research critic proposes accepting scalar independent chains instead; that would change this arm's stated compiler-SIMD hypothesis. If only scalar chains are emitted, record an unmet code-generation gate, do not run timing or call it a measured performance rejection. Any later scalar-ILP arm needs a distinct prospective method.
- Inspect the exact retained timing ELF and the actual production serving call path. Portable compiler flags are mandatory. Require per-row serial order, no horizontal reduction/reassociation/FMA, and a cumulative incremental stack bound of 512 bytes. A Rust type-size assertion is insufficient. Compare equivalent production serving frames with the exact native-equivalent parent build; do not substitute the test harness's closure frame for the serving frame.
- The primitive's hexadecimal codegen environment value is only a seal. The root admission and independent verifier must authenticate the PASS assembly receipt, exact ELF/source hashes, and admission before any timing cell. A well-formed arbitrary hash does not authorize timing.
- Prove the actual AVX2-host serving dispatch and scalar fallback during native qualification. Do not introduce a counter into the timed hot kernel just to observe it. The direct kernel tests, dispatch boundary, actual serving call sites and execution evidence must collectively cover both routes.
- Require exact CPU1 quota/cpuset in the root resource validator, even though the test accepts a more restrictive quota. Preserve CPU30s, wall120s, memory256MiB, swap0 and pids128.
- Keep optional bounds-hoisting, ISA-dispatch removal, and cross-instance validated-record hardening out of this causal slice. Current production callers bind validation and scoring to one prepared query; no concrete cross-instance caller was identified.

## Remaining qualification

Freeze the final repaired source and qualified test roster. Run bounded remote debug/release scorer and planner tests, affected integration tests and exact release executable retention. Inspect assembly and production stack before the separate primitive. A primitive winner alone may proceed to workspace Clippy, unshimmed workspace test compilation, portability, integration, real-input admission, separate canary and cold ABBA. No production integration, native correctness, speed, cold-tail or vendor win is claimed here.

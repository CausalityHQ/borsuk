# Four-row explicit-vector repair: prospective qualification

Candidate 3138bfb0502c986d187e4098718f8b3945fd39c9 changes only the accumulator representation in score_four against 896d0423. No production integration or timing claim.

Both independent critics in group f9b98d02852a459f recommend GO to exact-source remote qualification, without a source repair. Their full reports accompany this decision. Root independently checked the committed delta, owned hashes, existing scalar-control Add implementation, pinned wide archive, and unchanged twelve mandatory names.

The previous a0002 remains correctness-pass but CODEGEN_GATE_UNMET_NO_TIMING. Its scalar ELF and historical result are immutable.

Prospective a0003 compiles release first and stops if the exact kernel lacks packed additions or contains FMA/horizontal arithmetic. This permissive stop screen is not an assembly qualification: root must inspect the actual accumulation loop, lane order, no reassociation, unchanged finalizers, and production caller/kernel stack high water including realignment before timing. Scalar finalizer adds and extraction shuffles are legal. Compare matched-profile candidate and retained 896 production caller frames, then account against the unchanged 512-byte incremental scratch allowance; unresolved indirect frames keep timing on HOLD.

Hash the retained release ELF before release tests and after all stages. Scalar-control uses a separate target directory. Seven serial stages preserve debug/release codec and planner tests and existing integration regressions, adding the scalar-control codec stage. Actual native results, exact ELF audit, Clippy correctness/suspicious, real unshimmed test compilation, portability, and real-input admission/canary/cold comparison remain outstanding. Compile limits stay CPU2/8GiB/swap0/pids512/jobs1/7200s, machine9000s, compute $1.50 plus ancillary $0.15. Primitive separately remains CPU1/256MiB/swap0/pids128 and its original quality-independent timing thresholds.

Do not interpret packed instructions as a speedup. The unchanged primitive compares four rows against single-row scoring, not against the prior four scalar chains. Only a later paired cold measurement can establish a product latency or throughput gain.

Source bundle has 415 native files, 2454 support files; independent streaming archive verification passed. The first bundle attempt failed solely because systemd defaulted to /home rather than the repository; the second recorded attempt fixed WorkingDirectory with source and limits unchanged. Four synthetic screen cases passed (packed positive, scalar-only refusal, horizontal refusal, FMA refusal); these are mocks, not native evidence. All assets remain non-launchable pending exact source admission, bootstrap canary, resource/cost and infrastructure checks.

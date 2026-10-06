# Shared SQ8 runtime probe

## Evidence and scope
Closed native preflight a0002: 100k Cohere D1024 cosine k10,1000 disjoint queries,97.23% mean recall,p90 80.403322ms,p95 82.411070ms. SQ8 stage mean44.977513ms includes fetch/authenticate/scoring; this is not a CPU profile and does not prove the scalar kernel dominates. Native source and receipts are frozen at60f4b8f2.

## One bounded intervention
Probe the generic shared exact_sq8_nominee::score_nominees kernel. SIMD across independent rows may execute eight rows concurrently while retaining each lane's sequential f32 coordinate accumulation. No horizontal reduction,FMA,reassociated sums,query normalization change,quantizer change,format/routing/fetch-plan change. Preserve all IDs/ordinals/errors and exact score bits. Generic positive dimensions,scalar fallback and short/tail rows. Reuse std::arch; no dependencies.

## Cheap native gates before scale
Author affected library tests and one standalone Rust example comparing exact candidate scores against an independent scalar oracle (odd/tail D,nonunit coefficients,negative/signed zero/near ties,invalid norms/IDs/query/roster). Benchmark generic D128/768/1024 and row counts including1/7/8/9/257/16192; fixed deterministic inputs and matched one CPU. First actual remote affected tests and release microbenchmark; retain both arms/work/checksums/resources/timing. A synthetic microbenchmark is a kernel measurement,not end-to-end or recall evidence. Reject the optimization if unsupported,bit parity fails or runtime regresses. Pass release affected tests,workspace Clippy correctness/suspicious and real unshimmed workspace test compilation before integration.

Then replay the SAME retained real cohort,queries,GT and generation with the old and candidate binaries,identical one-CPU limits. Require identical returnedIDs/scorebits and all1000 recall results before reporting end-to-end gain. Reuse authenticated retained objects; no Parquet preparation/retraining or new dataset. No10M paid benchmark until generic scale admission is implemented and qualified.

Root owns remote execution,cost/freeze,verification,integration and push. No local Cargo/native/data/GT; child source-only checks CPU1/256MiB/noSwap.

## Source inspection amendment
The main borsuk crate forbids unsafe code; std::arch runtime intrinsics cannot live in the owned scorer without an extra unsafe boundary. Probe the simpler safe eight-row independent-accumulator implementation first. LLVM may vectorize it, but no AVX2 backend claim is made without generated-code/native evidence. Preserve the existing safety policy, generic dimensions and exact score bits; accept only measured benefit. autoexamples=false requires a minimal Cargo.toml example registration; this third owned file is authorized solely for that registration.

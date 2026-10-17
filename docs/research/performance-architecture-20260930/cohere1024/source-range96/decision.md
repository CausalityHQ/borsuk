# SOURCE request budget: 128 versus 96
2026-10-17. Same qualified Rust executable a3678661701d97541c66632bd4b7bb73d5fa0fcd0d392266c523b3b60d9de415; native code d014838fbce1d433b90092f0d20051887422b3ef, documentation revision7139d4ae4ec1c2013826a07e61597e8ed57f9b2e. Only source_get_budget changes128→96. No library/default change.

## Decision
Keep128 as the latency reference. Retain96 as a measured lower-request/higher-byte operating point, with no latency or total-cost win claim. This is not a rejection of a format or routing idea. Quality-first Pareto research continues; no arbitrary minimum gain threshold was imposed.

## Frozen full1000 ABBA
Cohere Wikipedia multilingual-v3,1M,D1024,cosine,k10, identical corpus/query/truth/generation pins. Same m6i.4xlarge Spot i-0bbe0101f1825806e, serial queries, download64,cacheoff; SOURCE64MiB,SQ832GET/32MiB,query-memory2GiB; operationalCPU4/16GiB/noSwap/pids1024. Repeated same1000queries twice per arm, not2000 independent queries.
| Cell | Budget | CPU s | Wall s | p90 ms | p95 ms | p99 ms | Serial QPS |
|---|---:|---:|---:|---:|---:|---:|---:|
| A1 |128|113.888885|167.360676|213.454159|232.169799|279.902869|5.975119|
| B1 |96|115.685572|166.160633|206.683288|234.459927|371.610234|6.018273|
| B2 |96|116.307986|165.004360|206.354041|230.612835|323.526681|6.060446|
| A2 |128|114.930091|163.274940|204.775480|224.039424|275.933581|6.124639|

B/A pooled totals: CPU1.0138737704,wall1.0016010919,SOURCE-stage1.0189430182. Forward wall0.9928296008/reverse1.0105920778; CPU increases in both orders. Candidate p95/p99 worse in both pairs. Descriptive one-host block, not significance or generalization. Candidate RSS98.23–101.91MB versus control91.25–93.13MB; no steady-resident memory claim.

Each cell recall9783/10000=97.83%, no underfill. Ordered IDs, scorebits, physical rows, traces, SQ8 plan and SQ8 charges match exactly; canonical stream SHA570344bc429a4b65d51f1f8bba453cb0f97c415563a89eb4828a4d488c7a13ca. Clocks/transport and intentionally changed SOURCE charges are excluded; aggregate sum is excluded only after checking exact equality to router+SOURCE+SQ8 components.

Per1000queries SOURCE109872GET/28967583744B→92634GET/31383171072B:15.69% fewer requests,8.34% more bytes. SQ830041GET/10722616576B unchanged, zero failed logical GETs. Logical charges are not billed/wire measurements. Fewer GETs did not establish a latency win; added transfer/authentication work and larger response tails can offset request savings.

## Admission, smoke, provenance
Actual-input admission and separate real-backend2query canary passed for both arms, all native/readers0. Canary exercised a real merge: ordinal0SOURCE125→96GET,30547968→33251328B; ordinal99990GET unchanged. Native32query ABBA and full1000ABBA each4 native/4 strict readers0; final results rehashed against strict readers before analysis. Source audit3fbd4d6d8e034bc0 verified SOURCE cover256rows, original authentication units32rows, smallest-gap merging and completion-driven refill—not discrete waves.

A corrected counterfactual cost reconstruction matched every baseline GET/byte total per query across1000; predicts96 maximum50012160B, none over64MiB. Original inefficient processing was cancelled and wrong32-row cover model retained as non-authoritative failures; neither is ANN performance evidence. Initial canary comparison stopped on the derived aggregate sum; component equality verified before comparison normalization corrected, without rerunning native work.
Full invocation8484ef0e1ced4ba2923908e68c1182ad; original wait/analysisSSM56fd6ed0-f402-43d4-b7c9-09ff5f0844f7 Success0. SummarySHA22aa5e93adb80582cf959435684843297508e19341c09ed3babd6fb549462b7d. Native correctness/release/workspace Clippy/testbuild qualification reused from exact unchanged d014 code; no compilation on Devbox.

## Short diagnostic and next step
After the frozen block ended, unchanged128 executable was sampled at199Hz with per-task switch events. Exact traced2query canary and32query panel passed native, strict reader and full semantic/charge parity. Valid diagnostic-a0002:643 userCPU samples,0 lost; SHA256 digest_blocks30.33%, SOURCE blockscore25.97%. Whole-process userspace sampling includes startup and cannot identify phase-specific stalls, unread sockets or causal gains. Traced wall time is not performance evidence.

Research8aca59e39f394452 proposed a larger trace. Root limited this to a short CPU/scheduler diagnostic; rejected arbitrary10ms/5ms acceptance thresholds and a further1000query study. Original diagnostic-a0001 declared64MiB but Bash file units enforced128MiB: functional canary only, not operational qualification. Preserved it; a0002 sets65536 and asserts kernel67108864-byte file bound before execution. Optional old readelf failed on a missing library; perf resolved symbols without rebuilding the ELF or changing sysctls.
Next bounded source task audits and, if supported, optimizes SOURCE SHA256 verification using existing locked crypto, preserving all hashes/errors/drain. Prior SQ8-only backend's original5.256%/20% VALID REJECT stays unchanged; it is not a SOURCE-path result on this workload. Any new code needs native correctness, Clippy/testbuild and separate measured validation. No source change or speed promise is made by this report.

## Evidence
S3 bucket borsuk-bench-453182569524-euc1, prefix research/semantic-router/20261017/source-range96-closure-a0001/.
evidence.tar.gz67677639B SHA22633be458f929a95aefe31bce78d0314f0d725367e3a88fbd3f8ea1d2ae7b64; receipts.json SHA2813d3828331db5024370aa30678e071773c5837af5f692edcda6b401a4c02d5. Archive contains immutable configs/headers, command/exits/resources, raw results/readers, broken and corrected processing, source audit, profiler raw data/reports and canary provenance. Query/reader ELF artifacts and source archive remain pinned in source-block32-closure-a0001.
No matched S3 Vectors/Turbopuffer win, concurrency qualification or full product readiness is established.

# Closed fixed128 CPU attribution — 2026-10-13

Independent verification: CLOSED_NATIVE_CPU_PROFILE_VERIFIED. Original command4b5d885a-ea8f-47ae-98d4-07ad13673b3d on Spot i-0c4a2961e9a7537bd. All6 native/manager stages exited0; inputs/config/binaries authenticated before/after, declared CPU0/memory/swap/pids enforced, no OOM/forced cleanup. Instance terminated, root volumevol-02b0a80b92c815914 absent, external timerinactive. Source/config freezeeadadae549423531c91c3e58191f6d0b28145028; qualified Rust ELFs unchanged from prior width64 campaign.

128 preregistered original ordinals0..127, width64, untraced diagnostic panel, one query at a time. Real query527 admission and separate query0 canary closed before the panel. Linux software cpu-clock99Hz, DWARF4096-byte callchains. Perf6.8.12 from authenticated Ubuntu linux-tools-6.8.0-147 version6.8.0-147.147. Zero lost samples; header rounds sample count to1K. Entire native process lifetime sampled, including startup, transport/authentication and output. Parent callchains end at unresolved0xffffffffffffffff; self instruction symbols resolve for the main hotspots. No per-query/phase CPU claim from the sampled percentages, no overhead bound.

| Self sampled CPU | Resolved function | Source connection |
|---:|---|---|
|27.91%|sha2 SHA256 x86 digest_blocks|Stack includes SQ8 PageAuthority::verify_payload|
|18.74%|PreparedTwoBit::score|Stack includes plan_two_bit_source_walks/plan_walks|
|5.26%|exact_sq8_nominee::score_nominees|Returned SQ8 scoring path|
|3.99%|kernel rep_movs_alternative|Receive/copy stack|
|3.65%|aws_lc AES-GCM AVX512 decryption|TLS decryption stack|

Native query-window CPU11,865,799,088ns and wall18,618,824,444ns across128. Planning wall2,502,720,288ns, SOURCE9,299,759,936ns, SQ86,618,107,407ns. Native process peak73,347,072B; sampled unit cgrouppeak88,633,344B. These instrumented subset timings are diagnostics, not a population percentile/QPS improvement. Panel recall1249/1280 differs in population from the full1000 campaign; do not apply the population hill-climbing floor to this subset or claim a recall regression from it.

Decision: inspect a bounded batch kernel for the two-bit LUT scoring loop. Preserve each row's byte-order f64 accumulation and exact score bits; use scalar/reference and architecture-path parity tests before a real source-bound falsifier. This is an optimization target, not an implemented/winning candidate. Do not remove authentication, change cache policy or assume TLS lacks SIMD: the profile already resolves AVX512 decryption. SHA optimization needs separate feature/implementation evidence. Marginal CPU shares cannot establish the source of the width64 population p99 regression.

Next gate: native exact-score/reference parity on generic dimensions/partial batches/invalid records, actual retained-input correctness/recall falsifier, affected Rust compilation/Clippy/workspace test-build, then paired cold width64 original/candidate comparison from frozen revisions with explicit changed source component. Preserve97.80% population anchor and97.60% prospective algorithm floor; score-only scheduling/numerical-equivalent work aims for zero semantic loss. No new dataset, warm cache, unqualified vendor or production readiness claim.

Raw query rows and perf.data are retained only in the SHA-authenticated opaque S3 archive; local extraction contains native completed summaries, completed perf reports and supervision metadata. Three preceding setup failures and their diagnoses/cleanup remain immutable. No live compute remains.

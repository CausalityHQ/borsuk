# Cold fetch scheduling candidate

Prospective library increment, based on qualified revision 689d93c4. No measured speedup or native qualification is claimed.

The measured 100k D1024 k10 cold workload has 97.23% recall, p95 219.460 ms and p99 245.018 ms. Source verification averages 66.438 ms; SQ8 fetch/rank averages 77.738 ms. This supports testing one scheduling axis: admitted per-stage parallelism 16 versus 32, with identical nomination, cover, scoring, GETs and bytes, cache off.

Remove the two semantic min(16) assignments and the router buffered min(16) in two_bit_generation.rs. Shared sq8_s3_range.rs already accepts explicit concurrency, admits all ranges before I/O, collects outcomes in input order, and drains every admitted request before returning. Preserve query slots, memory/GET/byte limits and default16 callers. Set the HTTP example's ordinary SQ8 default explicitly16; it currently requests32 and relies on the clamp. Audit all other explicit32 callers.

Generation memory admission charges the complete retained source/SQ8/router payloads, so avoid multiplying the whole byte allowance by concurrency. Transport/TLS/socket/chunk/future overhead still requires bounded runtime RSS qualification. Multiple active queries and pinned generations count separately.

Use one actual OneAttemptS3 loopback HTTP fixture with concurrently served, authenticated source and SQ8 bodies. Existing helper serves sockets serially. Prove at least17 and at most32 unfinished response bodies separately for source and SQ8, exact16/32 ID/score-bit/trace/request/charge parity, fixed-range GET/byte refusal before transport, and ordered errors plus complete drain after earlier-index delayed failure and later-index early failure. A Native100k router selects at most16 leaves: preserve that rule; source/SQ8 provide concurrency proof. Bound deadlines, bytes, handler count and cleanup/join.

Allow the benchmark runner to select16 or32 explicitly, record it in output identity, default16. No new controller, cache, dataset or scoring algorithm. Qualify exact source remotely: affected fixture, release, workspace Clippy correctness/suspicious and real env-unset test-build. Exact-input admission and disposable canary precede any paid paired cold measurement. Keep historical artifacts immutable. Choose defaults from measured recall/tails/RSS/cost, never assume higher parallelism wins.

Independent read-only plan consultation656e5902bf2a460c completed; root verified the clamps, shared fetcher and serial HTTP helper against qualified source. Active original SDK compiler exec25602/i-04607d2cf10e8e1bc must remain unchanged.

# Disposable CPU profiler canary

Status: preregistered, not launched at this commit. This is the next diagnostic after the verified cold membership result at b5ce2635.

The generic Rust membership change passed exact-source native gates and the cold ABBA screen: unchanged recall 97.23%, p95 down 22.9–24.7%, serial QPS up 34.1–34.6%. S3 still leads on p50 and serial QPS. No overall vendor or lifecycle-cost win is claimed.

Use Linux perf software `cpu-clock:u`, 99 Hz, DWARF 8192-byte call chains with the unchanged qualified binary. This disposable smoke hashes that binary 128 times and verifies the existing native config-SHA refusal (exit 2, zero queries/GETs/truth opens). It runs no ANN and makes no latency or quality claim. It establishes platform sampling/decoding only; Rust stack attribution remains to be tested by the later candidate-only profile.

The binary contains `.symtab`, `.eh_frame` and outlined ranking/authentication symbols. Prefer this platform tool before adding Rust timing APIs or another diagnostic schema. Official command reference: https://man7.org/linux/man-pages/man1/perf-record.1.html

The stage is CPU1/512MiB/no swap/pids256; record60s, report30s, decode30s, CLI20s, outer180s. The machine has a 1740-second shutdown deadline within the 1800-second envelope. Reserve at most $0.25 compute plus $0.15 ancillary, one Spot attempt. Stop/terminate/wait before collection. Environment errors are INVALID, not algorithm rejection.

Local checks are mocks: 12 real Bash-stage cases and 10 wrapper lifecycle cases under CPU1/256MiB/no swap/120s; actual historical CLI refusal and five mutations also checked. Rust, perf sampling, systemd instance gates and cloud calls were mocked locally. An initial mock assumed split `--mmap-pages 64`; actual argv uses `--mmap-pages=64`. Corrected the mock only and reran the failing layer; original failure receipt is preserved.

After the canary succeeds, freeze one unchanged-candidate CPU profile on the existing 1000 queries. Its instrumented latency is excluded from performance comparisons. CPU samples may guide ranking, hashing, sorting or allocation work; off-CPU time alone does not prove S3 network wait. The existing S3 reference will not be repeated, and warm caching remains deferred.

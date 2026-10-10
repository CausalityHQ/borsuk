# Scale1M PID128 supervisor repair: SOURCE ONLY

Rust/native library source and qualified ELF bytes are unchanged by this slice.
Only the baseline ELF pin changes from the historical 1M failed run to the
already-qualified bounded-pool binary. Source authority and exact Q32 seals
stay explicit; historical invalid runs are immutable.

The original five-phase native invocation/configuration tail remains byte-identical.
The observer is separate CPU0/256MiB/PID128; four build payloads CPU0-3/8GiB,
the query payload CPU0/512MiB, all no swap and PID128. Each original invocation
and kernel cgroup path is retained through close/drain before manager stop.

The canary uses real kernel supervision with Bash fixtures and ELF usage only;
it is not an ANN benchmark. Its source-bound excerpts include the exact traps,
phase manager/holder, resource checks, and observer initialization. Proposed
cases cover plain exits0/2/3/17, timeout, overflow and escaped-session cleanup.
Canary, collector, verifier and launch remain RUNTIME UNVERIFIED.

Before launch: review both critics; add direct collector refusal coverage;
freeze a finite disposable EC2 canary envelope/asset roster; execute once on
causality Spot; collect original statuses and terminate. Only after that,
real1M input admission and a new recorded native attempt may proceed.
No publication, vendor benchmark repeat, paid launch, or performance win is
claimed by this source checkpoint. Production main is unchanged.

# Bounded metadata ranges / paired cold first response, 2026-09-30

Preparation only: LAUNCH HOLD until the owned source slice, telemetry contract,
configuration, ARM qualification helper, bounded worker/controller and independent
verifier are complete and committed. No compute has been launched for this arm.
No new architecture, source fit, scorer, discovery graph or query caps.

## Causal change and frozen control

Control: authenticated ARM binary3565d27f from arm-sha-startup/a0001, independently
validated by cold-first-query/a0001 source2ea57d8f and immutable closed receipts.
FIRST1M D768 cosine ReLAION/CoHere, development0–63, k10. Current verified cold
p90=3670.383/3684.872ms and R10=99.375%/96.875%. This is the frozen system to
reuse as a fresh same-host control, not a claim that old timings are matched.

Candidate: fixed8MiB payload ranges, at most4 concurrent requests/buffers, serial
object order. Authenticate manifest root before child payloads; admit exact object
lengths and cumulative byte cap before payload. Retain exact decode/hash/schema,
owned scratch lifetime, cancellation, error cleanup and query physical caps.
No full object buffering, no detached work, no cache. HEAD/extra range requests
are an explicit tradeoff. Logical submitted metadata request counts and maximum
range payload budget are required; SDK physical retries/bytes remain unmeasured
unless instrumentation explicitly proves them. No universal32GET limit on startup;
32GET/16,773,120B query limits remain unchanged. Four8MiB buffers are32MiB,
additional to admitted decoded metadata and bounded transport overhead.

Shared staging serves both two-bit and original object-native generation paths;
focused affected tests must cover both callers, length/range/byte-cap/root rejection,
range completion and cleanup. Existing Cargo/dependencies and ARM SHA feature
remain unchanged. Native qualification must bind all Rust/Cargo identities and
allow only the reviewed staged-transfer slice relative to the closed control.
No repeat full cloud assurance solely for controller changes; affected native
qualification is required because staging code changes.

## Direct cold matched experiment

One Spot host; reuse the exact frozen control executable, build candidate once.
No standalone startup diagnostic campaign. Four ordered blocks:

1. control, development0–31;
2. candidate, development0–31;
3. candidate, development32–63;
4. control, development32–63.

Within each block ReLAION then CoHere, ordinals ascending. Each query/arm occurs
exactly once:64calls/arm/dataset,256fresh processes/namespaces and256first POSTs.
No health or preceding ANN probe. Authenticate/preload requests, exact GT10 prefix
and frozen ordered-ID/source/scorer/physical references before timing. Request
preencoded; interval starts before process spawn and ends at complete first HTTP
response bytes, including refused TCP connects. Same protocol as closed cold gate:
loopback plainHTTP/client CPUs4–5/native0–3/four Tokio threads; metadata fresh,
no application SQ8 cache, S3 cache uncontrolled, one POST/no HTTP retry. Abort on
failed authority/parity/call; preserve raw failed evidence; no automatic replacement.

Diagnostic GO requires all256calls/IDs/counters/authority valid, EACH arm/dataset
mean R10>=95%, and candidate coldp90 strictly lower than matched control on EACH
dataset. Report actual per-query paired differences and each arm p50/p90/p95/p99;
ABBA timing reduces but does not remove time/cache confounding. Also report
phase medians, metadata logical GET/HEAD and byte counts, query GET/bytes, scratch,
RSS/cgroup/zeroSwap/OOM. Fixed published-context gate is separate: EACH candidate
coldp90<444ms plus quality. Preserve FAIL, do not kill globally a correct quality
candidate for this contextual latency miss. No R100 measurement. Serial cold
calls/s is not offered8QPS/saturation; total EBS/S3 cost unmeasured unless accounted.
No matched vendor win from published-context comparison.

## Closure and remaining delivery

Freeze exact worker/resource/time/cost envelopes in executable configuration
before launch. Spot default; source/prereg/config authority, ACK-before-persistence
ownership, same-ID observation/terminal cleanup, collect only after termination,
independent archive/input/binary/256record/resource/termination verification.
No incomplete measurement inspection or duplicate cloud job. A scientific FAIL
ends this ranged-transfer arm and identifies transfer/decode/query bottleneck;
retain previous valid candidates and immutable gates. If it survives, next gate
is direct bounded cold offered-load/cost curve, then disjoint scale cohorts and
incremental lifecycle. BOTH-vendor matched comparison,10M/100M memory/scale,
generation swap/recovery and saturation/total cost remain open.

# Bounded compressed-source cache: native qualification

Status: preregistered, native execution pending. No performance or production-default claim.

## Exact source

The generic library cache is opt-in and generation-local. It retains authenticated
compressed source blocks and uses the existing fetcher for unchanged original
miss ranges. Complete source-cover admission remains mandatory on warm hits.

Candidate `9934588b5e8ad02a9274f3f264554425ca41cdc8` preserves production commit
`93d806c7e1d44b1a9c3b2232925fbac905e55289` and adds 56 test-only lines. The exact
OWN1 SHA256 is `bbf29735331b69897106b90a64833fa5e19dfb0e9a56a7e1d71e8c084934bf0a`.

Combined qualification snapshot `461ba972611347aa7c90bd941535f6ca43b0589b`
includes maintenance candidate `230e8639850002fc4645311f231e1ec5c90d0b16` and
preserves its five D769-to-D1025 refusal-fixture corrections. Combined generation
SHA256: `aea5ba63bf5abae7b7e4b5ffab37fa39aeb3ed0721bc2de124b40137b255b4f7`.
All 414 native files have identity
`cfc6fb6b48f2dc605054d8883dc82e122451aab7c6fabfc6672b71a67c32dd31`.
The explicit 2,449-file build-support archive is 13,187,402 bytes, SHA256
`84ab8e541a67a413e38a5ea77bb4af60b174bca66cc756930377705a2077c7f9`.

The combined snapshot is qualification-only. Main does not yet contain this cache.

## Review decision

Independent review group `1f4e679045d5495b` completed. Neither critic demonstrated
a production correctness defect. Engineering required an exact warmed full-range
byte oracle and a deterministic retained-hit/eviction witness. The separate repair
adds both without changing production code, caps or test names. Root read the full
repair and matched its file hash. Both full reports are preserved beside this file
as deterministic gzip files; `review-receipt.json` pins their original bytes.
Optional cache policies, zero-copy storage, single-flight and mutex sharding are
deferred until measurements justify them.

## Compiler and correctness gate

The exact eight serial commands and resources are in `native-envelope.json`.
The new cache oracle must pass once in both its debug and release owning stages.
All three required source-walk tests must pass once in debug; affected suites must
have nonzero successful counts. No ignored, failed or missing mandatory test is
accepted. Required release build, workspace correctness/suspicious Clippy, and the
actual unshimmed complete workspace test-build script must all exit zero.

Compiler envelope: jobs1, CPU2, 8 GiB RAM, swap0, pids512, native7200s,
machine9000s, c7i.2xlarge Spot in eu-central-1. Maximum reservation is $1.25
compute plus $0.15 ancillary. Source bytes must match before and after execution.
Record native and tee exits, stage commands/times, resource ceilings, swap/OOM,
process/cgroup drain and terminal artifacts. Terminate and wait for the original
instance before collection. An interrupted compiler cell is incomplete; preserve
its identity and evidence before any new attempt.

The existing compaction qualification `i-0ab2c3a35c4304516` / watcher83397 remains
protected. Its terminal evidence must be collected and the instance terminated
before this next compiler cell is launched. Setup/configuration failures are
INVALID: repair the demonstrated defect and preserve the original attempt.

## Subsequent performance falsifier

`performance-draft.json` remains non-launchable until exact native runner/reducer
qualification, real-input admission and a separate disposable canary pass.
Use only the frozen Cohere100k/D1024/cosine/k10 corpus and existing 1,000 requests.
Both arms start in a fresh process. Queries0..500 warm the candidate; disjoint
queries500..1000 form the primary measurement. Seal all1,000 outputs before truth.
Preserve per-query plan/trace/ID/score-bit parity and distinguish source GETs/bytes
from cache observations and SDK transport counters.

At100k, 32 MiB can hold the entire compressed source. This is a full-residency
ceiling experiment, not evidence for partial-cache locality at10M. Preserve the
cache-off RAM/latency point. A pass establishes neither a production default nor
a measured vendor win. Compiler peak RAM is not serving RAM.

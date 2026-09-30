# Bounded centroid conversion candidate

Status: implementation under qualification; no performance result.

The authenticated graph-decode a0004 candidate remains the control. Its
ReLAION FIRST1M D768 cosine k10 development queries 0–63 achieved 99.375%
mean recall@10 and 1242.809 ms cold p90; CoHere FIRST1M on its development
queries 0–63 achieved 96.875% and 1289.024 ms. These are consumed development
panels, not fresh holdouts or competitor measurements. CoHere p99 worsened
in that experiment and remains an open tail risk.

The sole proposed production change converts little-endian FP16 centroid
payloads through the locked `half` crate's bulk slice conversion, using a
2 KiB scratch array. The final FP32 array, format, geometry checks, finite
checks, norm summation, hashes and scoring remain unchanged. The previous
graph duplicate-check improvement belongs to both arms. No extra payload
copy proportional to the complete centroid plane is introduced.

The rationale is the measured 48,000,032-byte centroid object and aggregate
candidate decode p50 of 268.399 ms (ReLAION) / 268.539 ms (CoHere). Individual
centroid conversion time has not been measured. Bulk conversion may fail to
improve end-to-end latency; no speedup is assumed.

Qualification requires exhaustive finite FP16 bit parity, rejection of every
nonfinite encoding, conversion-block tails, existing centroid scoring tests,
actual workspace Clippy, all-targets test compilation and one source-bound
workspace suite. Local x86 results must be labelled separately from fresh
ARM focused tests and paired release builds.

Before any paid launch, freeze a new source/control manifest, include the
changed centroid file in compiled snapshots, authenticate local assurance,
and independently validate the controller/runtime protocol. Reuse the
reviewed ABBA cold mechanism and immutable inputs. Preserve all-success,
ordered reference parity, recall@10 >=95%, resource/read caps, and lower
decode p50 and end-to-end cold p90 on BOTH datasets as the development GO
gate. Report p95/p99, including regressions. The 444 ms published context
and current-candidate offered QPS remain separate, unmet measurements.

No launch is authorized by incomplete compiler evidence. A failed gate ends
this arm and leaves a0004 intact. A development GO still requires a fresh
representative panel, offered-load/cost measurements and larger-scale work.

## Closed a0001 decision: FAIL, 2026-09-30

Independent verifier exited zero, authenticating all 96 terminal artifacts and
256 closed records. Original controller exited zero; owned Spot
`i-0df0f3e9b0b626649` terminated before collection. Source `4c2986f5`.
Both FIRST1M D768 cosine k10 panels are consumed development queries 0–63,
64 calls per arm/dataset. All calls succeeded; ordered reference parity and
quality gates passed. Numbers below are verified matched internal measurements.

| Dataset | Control R10 | Candidate R10 | Control cold p90/p95/p99 ms | Candidate cold p90/p95/p99 ms | Decode p50 control → candidate ms |
|---|---:|---:|---|---|---|
| ReLAION | 99.375% | 99.375% | 1286.733 / 1348.139 / 1549.338 | 1226.492 / 1288.464 / 1343.877 | 267.940 → 252.597 |
| CoHere | 96.875% | 96.875% | 1266.279 / 1288.982 / 1534.798 | 1279.715 / 1359.559 / 1463.871 | 267.899 → 252.455 |

Actual quality delta is zero percentage points on both panels. Decode gate
passed, but BOTH-dataset cold-p90 gate failed: ReLAION -60.241 ms, CoHere
+13.436 ms. Preserve frozen FAIL; no retrospective tolerance or rerun to seek
acceptance. Bulk decoding is a correct incremental mechanism, not a qualified
end-to-end winner. Keep graph-decode a0004 as the last accepted arm.

Remaining gap: these cold p90 values exceed the 444 ms published comparison
context; this is not a matched vendor measurement. Current offered/saturation
QPS and total lifecycle cost are unknown. Serial campaign completion rate
0.825854 calls/s is not offered throughput. No new R100, unopened-panel,
10M/100M or maintenance/recovery measurement.

Next decisive test is the source-only semantic-router geometry and returned-
quality falsifier described in the architecture reconciliation, preserving
exact FP16 geometry and prior failed-arm lessons. A global metadata decode
optimization cannot eliminate the authenticated query-fetch critical path.

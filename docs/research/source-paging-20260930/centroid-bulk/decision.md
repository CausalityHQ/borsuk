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

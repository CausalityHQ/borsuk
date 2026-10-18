# Q1/Q2 closed-loop concurrency: measured Pareto tradeoff

Cohere1M D1024 cosine k10, fixed1000, fixed40, Offload2, same generation/client/runtime/binary; fresh process per cell. App cache off; backend cache unknown. Tokio current_thread cooperative ordered batches, not rolling scheduling or peak service capacity.

Decision: retain Q2 as an opt-in throughput option; retain Q1 for lower latency. No default promotion. All1000 canonical result IDs, score bits, plans, traces, charges and recall rows match in every cell. Recall is97.37% (9737/10000).

|Cell|Slots|Cohort QPS|p90 ms|p95 ms|p99 ms|Cohort CPU s|Max RSS MiB|
|---|---:|---:|---:|---:|---:|---:|---:|
|A1|1|6.7439|180.358|202.134|256.190|114.430|90.45|
|B1|2|8.6322|318.841|342.686|392.211|106.749|130.00|
|B2|2|8.7677|319.499|343.627|401.267|102.132|133.79|
|A2|1|7.1125|169.576|185.391|218.066|101.098|84.35|

Temporal B1/A1 and B2/A2: throughput +28.00%/+23.27%; p95 +69.53%/+85.35%. CPU ratios0.9329/1.0102 do not show a consistent CPU improvement. Q2 latency overlap is established in both cells by sum query wall exceeding cohort elapsed. Cohort QPS is1000/cohort elapsed, never reciprocal mean per-query latency.

Each cell:127811 successful query GETs and34,229,466,368 consumed query payload bytes. Native transport deltas independently match logical totals, with zero stream/transport failures. Startup adds18 HTTP attempts and5,264,793 bytes; whole-process attempts127829 and bytes34,234,731,161. These are native HTTP boundary counters, not wire/billed bytes or an IMDS-total claim. No request/byte reduction.

SOURCE interval wall increases1.67x/1.73x, versus SQ8 about1.10x/1.11x; this localizes the next investigation but does not isolate worker queueing from cooperative scheduling. Stage intervals overlap and cannot be added as cohort time or treated as isolated CPU.

Next smallest causal falsifier: inspect the shared SOURCE worker queue and current_thread scheduler against this closed evidence; propose one scheduling/worker-allocation change, then hold slots/input/binary accounting fixed in a paired check. Do not rerun unchanged Q2 or lower recall to hide latency.

Qualified source89fb834, documentation delivery03f4f50; runner ELFa03c60d63969c93d580ac0b471de2f61910c51f3660938687bc830473d59ed92. All affected/source-auth/release/workspace Clippy/real shim-unset testbuild gates passed. Preserved a0001–a0003 failures are environment/compiler/test fixtures, not scientific negatives.

Source-bound real-input admission and separate disposable real-S3 smoke passed before measurement. The smoke exercised serial panel validation/SDK/cleanup; Q2 overlap/cancellation/model boundary were separately qualified with native local-store fixtures. No claim that smoke established real-S3Q2 capacity.

Original run SSM947230ce-cb0d-489a-911b-18ae145302e1; invocation9c538ceafb19444ba41078d69eecaa43. Four immutable cell checkpoints verified before collector closure. Total measurement cohort time excludes startup/truth/reader/archive.

No statistical significance, universal cold, production default, total lifecycle-dollar or competitor-win claim. Existing S3 Vectors raw baseline has96.17% recall/p95206ms; timer/retry/precision/cache/protocol differ, so this is context rather than matched parity. Turbopuffer matched credentials/results remain unavailable.

External closure: s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261018/concurrent-query-abba-closure-a0001/. terminal.json is written LAST, after independent termination of i-0a71976260f8bd031, preserved EBS availability and timer/BG cleanup. Consult that receipt for final infrastructure closure; do not infer it from this pre-termination source commit.

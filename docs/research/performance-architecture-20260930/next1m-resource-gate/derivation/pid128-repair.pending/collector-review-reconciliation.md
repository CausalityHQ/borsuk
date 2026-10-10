# Collector smoke review reconciliation

Original review group: fe86c3258f2a4710, frozen input 0fe4f9b5e0b685d8bc5557558e7777de4791135f. Research 35ac730d0b644b62 and engineering 115996cc5e2445ef completed; original full reports retained alongside this record. No duplicate review or runtime execution.

Both reviewers' launch-ownership and write-race findings are fixed: preserve launch return status, capture invocation identity even after unsuccessful response, refuse missing ownership, wait for original terminal manager state and complete exit record before mutation. Replacement launch is treated independently and never retried.

Live-child and deadline fixtures now authenticate their actual preconditions and intended collector progress. The deadline and elapsed time are recorded, with an 18-second bound for the four-second deadline plus finite cleanup allowance and timestamp granularity. Mutation cases retain manager identity and precise changed evidence; replacement must remain alive under its new identity.

Collector cleanup is proved BEFORE harness cleanup for both live cases: failure-cleanup.exit must be zero, MainPID zero and the actual original cgroup drained or removed. Replacement deliberately refuses stale-identity cleanup. For already-empty metadata refusal cases, cleanup exit 0 or 1 is retained and independently checked against actual empty/absent cgroup; their retained empty unit is stopped by the harness. This narrower claim is explicit in result.json. It does not qualify collector failure cleanup for already-empty retained units, and does not alter the production collector to force a pass. A live cleanup failure cannot be rescued into success by the harness.

Failed-case canonical evidence and independent harness cleanup observations are retained in the advertised output directory. Successful output has a complete manifest and filesystem sync; remote acceptance still requires original process status and authenticated output seal.

Static-only latest source check: run-p847276-i713738577.service, invocation f6f5b843a49a4e2098de3b5d76f854d3, bash syntax and ShellCheck exit zero, unit exit zero, 370 ms, peak 49.6 MiB, CPU1/256MiB/noSwap/120s. No local runtime/native/data/mock execution. Internal 240 seconds is admission only; adapter enforces external 300 seconds and parent runtime 310 seconds. Actual causality EC2 staging and subsequent exact-source 1M native gate remain UNRUN.

# Discovery loss isolated; candidate remains KILL

ReLAION first100k, D768, cosine k100, previously consumed validation queries256–319 (64). Diagnostic only; no new quality or latency qualification.

| Stage | Mean GT100 coverage/recall (%) |
|---|---:|
| All159 discovered pages |99.203125|
| Nominal top84 pages |99.203125|
| Actual84 selected pages |99.203125|
| Fetched ranges |99.203125|
| Returned SQ8 |98.750000|
| Paired flat SQ8 |99.531250|

All four page-stage hit counts are equal on every query, not merely in the mean. Seven queries have discovery coverage below100. Exact original range/byte parity holds for64/64 plans. The first observed missing-page layer is discovery; budget admission and GET bridging add no missing GT hits on this sample. This does not prove that every full-panel deficit has the same cause. SQ8 ranking is also lossy, with the unchanged paired flat control above.

Decision: retain the full-panel native fitter KILL. Do not increase GETs, rerun a full panel, promote to1M, or infer vendor performance. Next material candidate must address source-layout/discovery locality and flat fitting cost; check it cheaply before a fresh frozen quality gate. Required design critique remains held by its native cooldown.

Diagnostic API shares normal admission, charges the159-ID trace to scratch, and leaves ordinary serving allocation/configuration unchanged. Initial demo trace failed its existing400000-byte scratch cap before a query plan; preserve that exit1. Diagnostic mode now adds1272 bytes, solely for the trace, to retain the original codec allowance. Original failed file was empty. Narrow test red exit101, green exit0; both original release builds exit0; final diagnostic exit0. No paid job or local heavy build started. Local memory PSI was0, swap charged8192 bytes; no stale owned build required stopping. Builds ran on existing Spark.

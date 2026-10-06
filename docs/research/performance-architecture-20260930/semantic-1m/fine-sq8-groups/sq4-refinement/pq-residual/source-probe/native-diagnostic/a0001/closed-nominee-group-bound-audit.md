# Closed nominee-group lower-bound audit

Authenticated all128 corrected-four-bit query-result bodies and the terminal-bound native report. No query body, ground-truth body, corpus, ANN or new experiment was opened/run. This audits already-emitted SQ8 winners and per-query GT hit counts.

For each query, retain every16-row group containing an original nominee. Every old SQ8 top100 winner in those groups remains eligible. With unchanged SQ8 arithmetic and score/ID tie ordering, removing other rows cannot push those retained old winners below rank100. Subtracting ALL outside-group winner appearances from the old GT hit count therefore gives a conservative hit lower bound, without classifying which removed winners are true hits or assigning hits to replacement rows. This is a bound, not measured group-only recall.

| Consumed FIRST100k D768 k100/64queries | ReLAION | CoHere |
|---|---:|---:|
| Max mandatory group bytes | 8,224,320 | 8,748,480 |
| Exact current-order min32-range bytes max | 32,747,520 | 27,081,600 |
| Fits32/16MiB | 50/64 | 32/64 |
| Outside-group SQ8 winner appearances | 3 | 52 |
| Group-only mean recall lower bound | 99.46875% | 98.21875% |
| Group-only p05 hit lower bound | 99 | 94 |

Current-order min32 cost equals mandatory group rows plus all but the31 largest inter-group gaps. Per-query table records each charge. Arithmetic reproduces the frozen cover evidence.

Implication: unchanged SQ8 precision has a plausible byte budget for mandatory groups, but physical scatter violates range/byte caps. CoHere p05 bound remains below95; no group-only quality promotion follows. A new source-only layout must improve physical co-selection locality and preserve enough useful surrounding rows, then pass an independent paired recall gate. Do not train that layout on this consumed-query audit or GT. This audit does not choose a winning layout or remove the100M router-memory/lifecycle blockers.

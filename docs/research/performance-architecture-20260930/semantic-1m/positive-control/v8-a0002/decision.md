# Local positive control PASS

ReLAION FIRST100k D768 cosine, consumed development queries 0–63: all 64 completed. Returned recall@10 is 628/640 (98.125%), recall@100 is 6217/6400 (97.140625%). Root independently recomputed both counts from returned IDs and unchanged truth. Recall@10 equals the frozen historical reference; this is an internal path positive control, not fresh-panel validation.

The run took 9.13 s with 51,872 KiB maximum process RSS. These are local diagnostic process measurements, not cold HTTP latency, saturation QPS, total object-cache RSS or billed cost. Logical SOURCE/SQ8/leaf read counters and stage coverage are in verification.json; no physical S3 requests were measured. Original v8-a0001 adapter failure remains immutable.

Proceed with the exact preregistered fresh 1M panel construction, then actual object-native build/quality and cold HTTP. No design or quality threshold changed to obtain this PASS.

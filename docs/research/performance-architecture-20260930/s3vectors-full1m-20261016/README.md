# S3 Vectors: closed single-client 1M Cohere baseline

Frozen source `ab9243bf7174d087061f80dd7852e0afe8ffecec`, original-f32 corpus, D1024 cosine, k10, 1000 queries, eu-central-1, ENHANCED index, concurrency1 and one SDK attempt. Publication enumerated and bit-exactly read back all 1,000,000 rows. All seven phase/pipeline/runner exits were zero.

| Metric | Result |
|---|---:|
| Recall@10 | 95.71% (9571/10000) |
| p50 | 128.846090 ms |
| p90 | 199.659548 ms |
| p95 | 211.891269 ms |
| p99 | 260.565673 ms |
| First query | 357.083422 ms |
| Serial reciprocal QPS | 7.402313 |
| Whole-pass QPS | 7.095096 |

The independent verifier authenticated corpus/query/truth hashes, all 1000 query hashes, response body hashes and raw distance bits, complete stream/seal identity and EOF, and recomputed quality and nearest-rank latency. Truth was opened after complete response validation. The producer emits header +1000 rows + completion trailer. The original verifier's 1001-line assumption failed; its source and failure were preserved, and the separate corrected verifier passed. Fifteen malformed framing cases were rejected. Neither the native experiment nor collector was rerun.

One hour of inactivity was enforced (observed 3,626,944 ms); this does not prove backend eviction or guaranteed cold service. The first query includes lazy credential resolution. Client reuse was enabled. Query timing includes request construction/signing/transport/deserialization/validation and excludes input staging and receipt writes.

MemoryMax512MiB was configured; observed cgroup-charged peak537,382,912B is not RSS. Terminal memory/pids event counters were unavailable. Response cap4MiB applies after SDK buffering; no transport-allocation bound or concurrent-capacity qualification is claimed. Measured cost is unavailable.

The earlier separate BORSUK Get64 result had recall97.80%, p95186.85ms and serial QPS6.53. These are different runs with unresolved preprocessing/protocol equivalence, not a controlled head-to-head. No matched S3 Vectors or Turbopuffer win is established.

Native cleanup and the original independent GetVectorBucket NotFoundException/exit254 prove service bucket absence. Original compute unit has no process; the EC2 host is used only for active separate library qualification/source work, under its unchanged shutdown guards. Persistent EBS is preserved.

Evidence was uploaded create-only and read back byte-for-byte. Terminal was uploaded last. See evidence-reference.json and decision.json. Next: qualify capsule lifecycle and immutable publication; run a frozen BORSUK control/candidate on the authenticated original corpus/query/truth pipeline before a matched comparison.

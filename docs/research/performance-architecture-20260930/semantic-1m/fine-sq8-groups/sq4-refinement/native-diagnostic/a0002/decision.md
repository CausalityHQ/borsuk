# Fixed SQ4: valid consumed-panel rejection

Exact source-qualified native command completed exit 0; original controller exit 0. Same instance `i-023fe623d50cfd7a4` terminated and waited. All 152 terminal bodies authenticated; all 128 queries and payload identities sealed before truth.

Both datasets: FIRST100k, D768, cosine, k100, consumed64 split. Targets: mean recall@100 >=98%, p05 hits >=95, <=32 local range reads and <=16 MiB verified payload/query.

| Dataset | Original 256-range SQ8 recall / p05 hits | Same fetched population SQ8 recall / p05 hits | Fixed SQ4 recall / p05 hits | SQ4 mean / max bytes | Mean / max local ranges |
|---|---|---|---|---|---|
| ReLAION | 99.484375% / 99 | 99.515625% / 99 | 93.078125% / 88 | 6,271,749 / 16,625,664 | 31.9375 / 32 |
| CoHere | 98.515625% / 95 | 99.03125% / 97 | 89.046875% / 84 | 8,488,854 / 13,749,120 | 32 / 32 |

All 128 byte/read envelopes fit. Quantization loses 6.4375 and 9.984375 percentage points against SQ8 on exactly identical fetched IDs. Fetched truth coverage is 99.984375% / 99.625%; source nomination coverage is 99.796875% / 95.609375%. CoHere relies on additional fetched rows to recover nomination misses.

The whole native transcode + paired scoring + offline quality command took 9.841322753 s and peaked at 148,426,752 bytes, with zero swap/OOM and complete drain/cleanup. This is not per-query cold latency or QPS. No physical S3 query, fresh1M, 100M, lifecycle or matched vendor win is established.

Decision: preserve this codec as rejected research evidence; do not promote it to serving. The earlier a0001 sealed-request-path failure was INVALID before scoring and was corrected without changing query bytes or the native method. It is distinct from this valid quality rejection.

Next native intervention must reduce matched scoring loss while preserving bytes and coverage. A focused research consultation `bd62649f807d4f69` is assessing a truth-free, nonuniform 16-center scalar codebook versus rotation/RaBitQ alternatives at equal total bytes. Lower reconstruction MSE alone will not establish recall. No new arm is frozen by this note.

Large generated payloads and the identical already-qualified binary remain pinned in `artifact-transports.json` to original immutable S3 keys and retained external raw files; remaining closed artifacts are embedded raw or gzip. The parent verifier authenticated the full original body set.

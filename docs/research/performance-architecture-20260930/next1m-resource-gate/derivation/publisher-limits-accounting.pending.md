# Ordinary publication admission proposal

Qualified source at ea45653c, semantic Scale1m, N1M/D1024, x86_64. The mandatory ten-field publisher limits are explicit in publisher-limits.pending.json. No retained-generation path or maintenance swap is involved. Fresh prefix generation1/base_epoch0, no old head.

Memory512MiB, one active query, parallelism16, SQ8 bytes16,773,120/GET32, source bytes64MiB/GET128 preserve the baseline checker limits. Baseline caller-owned pinned allocations are computed by the native checker at execution; publisher already_pinned_bytes=0 does not assert zero checker/runtime RSS.

Supersedes the unexecuted c0862c80 draft's parallelism1 assumption. Exact check_cohere_native_baseline.rs default_fetch_parallelism returns16 and its runtime validator accepts only16|32. No source, binary, algorithm, recall threshold or memory/GET/byte cap changed; this repairs prospective configuration before launch.

Codec scratch=(256*256+1024)*8=532480. At N1M semantic trace IDs=512+4096+48+3072+8=7736, times8=61888. TwoBitPlanTrace has six Vec fields plus usize, source-derived x86_64 size152; graph alternative5662 IDs*8+4*64-byte discovery records=45552 is smaller. Total native query scratch proposal=532480+61888+152=594520 bytes. This is source-derived; the actual native baseline/admission must confirm compatibility, not a new compilation or measured RSS claim.

Publication and serving have distinct allocation admissions. Root/runtime/cgroup512MiB-vs8GiB distinctions and all resource refusal evidence remain unchanged. Final gate config remains pending wrapper and canary.

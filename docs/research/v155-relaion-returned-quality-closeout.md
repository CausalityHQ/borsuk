# V155 ReLAION-1M returned-quality closeout

**Decision: pass the preregistered development gate.** This licenses a fresh,
paired qualification gate for the generic cached sparse route. It does not
freeze production defaults or establish live-S3 latency, held-out quality,
10M/100M scaling, or an empirical recall guarantee. The cohort is
ReLAION-1M **validation-1000, already used**.

## Frozen attempt and authentication

- Source: `10850f917c4583f05d8c33afec459f4f76b194bd`, pushed to
  `origin/main` before execution. Source archive SHA-256:
  `de67cc65077f58cbbff1328cbcd668f950b45f3b4c1f03410bdee664ebe52683`.
- One Causality Spot c7i.12xlarge, instance `i-090e24fb09a65ec98`,
  attempt `a0001`, elapsed 177 seconds; terminal status `complete`,
  no interruption; instance terminated. Terminal SHA-256:
  `784097f577f11bd49468473e43b1ba06642bf107ecde06a8b0b0ce09b0cd9cdb`.
- Terminal:
  `s3://borsuk-bench-453182569524-euc1/research/v155-relaion-returned-quality/10850f917c4583f05d8c33afec459f4f76b194bd/runs/v155-20260924T150712Z/a0001/terminal.json`.
  The launcher authenticated all 27 terminal artifacts, checked the immutable
  pre-GT replay seal, and independently recounted 1,000 rows. A separate
  postterminal check authenticated the replay, evidence, summary, exported
  IDs, seal, and frozen GT parquet by SHA-256, reloaded the parquet directly,
  and reproduced the same verdict.

## Paired results

The flat arm reproduces closed V142 exactly: 99,607 exact-source and 99,255
SQ8 GT100 hits. The sparse arm is the frozen V154 cached sparse page plan.
Both use the same V116 source-only 512 nominees, Rust bounded-range SQ8
top-512, exact source union rerank, 16 MiB/query and 32 GET/query caps.

| ReLAION-1M validation-1000, 100 GT/query | Flat β=4 | Cached sparse | Unit |
| --- | ---: | ---: | --- |
| Exact-source returned Recall@100 | 99.607 | 99.567 | % |
| Exact-source GT hits | 99,607 | 99,567 | /100,000 |
| SQ8-only returned Recall@100 | 99.255 | 99.222 | % |
| Exact-source p05 | 98 | 98 | hits/query |
| Exact-source queries below 90 | 2 | 2 | queries |
| Physical fetched-GT coverage | 99.695 | 99.647 | % |
| Planned range bytes, total | 11,801,736,960 | 11,134,007,040 | bytes/1,000 queries |
| Planned GETs, total | 23,581 | 22,126 | GETs/1,000 queries |
| Mean source union | 676.279 | 679.088 | IDs/query |

Sparse versus flat: 20 query wins, 931 ties, 49 losses in exact-source hits.
Sparse loses 40/100,000 hits while saving 667,729,920 planned bytes (5.66%)
and 1,455 planned GETs (6.17%). The preregistered floor was at least 99,500
hits and within 100 of flat; sparse clears both. The sparse SQ8-only p05 is
97 versus flat 98, a tail signal that the exact-source union masks. Keep it
visible in the next gate.

V154's paired **offline planner CPU**, on this same used split and frozen
architecture, was flat p95 6.489573 ms and cached sparse p95 0.759366 ms
(8.55×). V155 independently measured the Rust SQ8 range-scoring and exact
source-rerank phases, not complete serving latency: SQ8-scoring p95 was
14.309/14.176 ms and source-rerank p95 2.273/2.163 ms, flat/sparse. Phase
times are diagnostic and must not be added to claim live end-to-end latency.

The V155 remote replay's peak RSS was 8,383,580 KiB; its cgroup peak was
10,878,943,232 bytes. That replay loads a million-vector source parquet to
audit exact IDs and is **not** production query memory. The Rust scorer peak
RSS was 1,528,100/1,528,120 KiB flat/sparse. These measurements do not prove
a count-independent memory bound or a recall-adaptive resource policy.

## Next gate

Freeze one dataset-independent recall/resource rule and the exact production
index/serving revision before opening a fresh paired D96 and D768 quality
cohort. Preregister the query selection, GT construction, source archive,
baseline, transport cap, returned Recall@100 floor and tail floor. Preserve
the V36 sealed holdout until the final architecture qualification protocol is
fixed. A positive fresh-quality result then proceeds to matched live-S3
p50/p95/p99, cold/warm and cost gates; only winners advance to 10M and 100M.
For Lean, prove planner invariants and conditional resource bounds against
explicit input assumptions; measure recall, latency and resident memory on
each scale because the proof cannot establish their empirical values.

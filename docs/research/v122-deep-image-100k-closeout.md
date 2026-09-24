# V122 D96 100k development screen closeout

V122's one immutable Causality Spot attempt completed with exit 0 on
2026-09-24. Source commit `afe07cb5a9ba8518263375595f589639fdf3f4f1`,
attempt `s3://borsuk-bench-453182569524-euc1/research/v122-deep-image-100k/afe07cb5a9ba8518263375595f589639fdf3f4f1/runs/v122-20260924T011355Z/a0001`,
terminal SHA-256
`5475dfdb8608b80c2dbdc3550721d9f5d106fb79c613cbc83cb40a7a29a4a89c`.
Spot `i-0bffec86c5ba0f78b` was a `c7i.4xlarge` in `eu-central-1c` and
terminated after the 246-second run. The original launcher exited 0.

The 100,000 D96 source rows were sampled without replacement from V119's
9,990,000-row authenticated corpus using seed 122001. The build used V120's
source-only layout method and balanced PQ64. Only deep-image publication test
ordinals 9000 through 9999 were used for this development screen, and exact
cosine GT100 was recomputed within the sealed subset. The published full-corpus
GT was not used. The V121 untouched first 1,000-query split remains unread.

| Metric, 1,000 queries | Candidate | Paired V109-style capped control |
| --- | ---: | ---: |
| Returned Recall@100 | 99.106% (99,106/100,000 GT hits) | 98.203% (98,203/100,000) |
| p05 returned hits per query | 98 | 94 |
| Queries below 90 hits | 0 | 12 |
| Physical coverage of GT hits | 99,942 | 98,827 |
| Maximum planned GETs per query | 1 | 32 |
| Maximum planned bytes per query | 10,800,000 | 3,760,128 |

The candidate won 287 paired queries, tied 713, and lost none. All planned
queries stayed within 32 GETs and 16,777,216 bytes. The preregistered 100k
gate passed. These are offline SQ8 returned-score results, not live S3 query
latency or throughput. The entire 100k SQ8 body is 10,800,000 bytes, so the
candidate fetched all rows in one GET. This gate tests the D96 quantization
and returned-score path but gives no evidence of selective routing at 10M.

Independent closeout downloaded and checked the length and SHA-256 of all 32
terminal-listed S3 artifacts. It recounted the 1,000 evidence rows against
the sealed `truth.npy`, confirmed query ordinal and GT identity, all per-arm
hits, p05, sub-90, paired outcomes, GETs, bytes and physical bounds, and
confirmed unique 100k subset and layout IDs. The terminal's summary SHA-256
is `80342a3184b22d38a9e0cdad1da8be832f2740ca89b79d9aa47d07c89d7aacef`.

The subset selection peaked at 8,017,452 KiB process RSS while reading the
9.99M-row Parquet source; layout fit took 13.23 s at 909,244 KiB, router fit
2:05.37 at 581,144 KiB, and offline evaluation 48.58 s at 198,936 KiB.
These are worker process measurements, not serving cgroup RAM bounds.

Decision: allow V121's untouched first-1,000 deep-image quality and flat-router
runtime gate after validating this sealed V122 result and termination. V121
still needs the matched ReLAION-1M rebuild described in the V120/V121 method
audit before any exact-method cross-corpus promotion. No dataset-specific
setting or vector-count quality switch is selected from this screen.

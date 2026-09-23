# V114 exact local SQ8: paired ReLAION-1M development closeout

**Decision: promote to a separate live S3 RAM/file gate.** The frozen V114
exact local scorer preserved 99.234% returned Recall@100 on all 1,000
ReLAION-1M development queries under a 32-GET, 16,777,216-byte one-wave
physical plan. This is an offline returned-result gate with authenticated
local SQ8 page bytes; it is not a live S3 latency or throughput result.

## Frozen authority and terminal

- Source commit: `87881c71e048d557c5c1285ffa0ac1d8c381f764`.
- Source archive: `s3://borsuk-bench-453182569524-euc1/research/v114-1m-paired/87881c71e048d557c5c1285ffa0ac1d8c381f764/source/source.tar.gz`, 11,504,263 bytes, SHA-256 `dc4b98b92f935a5a65c529c26ea42d2c08606c465a4c52fc5da658be46d8ba05`.
- Attempt prefix: `s3://borsuk-bench-453182569524-euc1/research/v114-1m-paired/87881c71e048d557c5c1285ffa0ac1d8c381f764/runs/v114-1m-dev-20260923T222600Z/a0001`.
- Terminal: `complete`, exit 0, 711 seconds; terminal SHA-256
  `4da6827429b2b4bf56234c64adf99ff719a6adbed23eb06c23862317d56d22c1`.
  All 40 terminal-listed artifacts were independently streamed from S3 and
  matched byte counts and SHA-256 values, totaling 61,503,475 bytes.
- Causality Spot `c7i.12xlarge` instance `i-03d77abafb7f18237` in
  `eu-central-1c` is confirmed **terminated**. No On-Demand exception.
- The frozen V36 source/query/GT, V63 layout and V70 SQ8 object passed their
  pinned size/SHA-256 checks. The V77 manifest regenerated to 92,669,264
  bytes and historical SHA-256
  `131a4cd80dfee8ef4d0486b2043349e01b6c230d702f8d17ad1224e3fbc6a874`.
  The source-derived mirror was sealed before query and GT download.

## Paired returned-quality decision

Each arm used the same 1,024-region PQ64 top-512 roster and the same frozen
SQ8 object. The V109 capped baseline used historical ranked-page admission;
the V114 exact oracle used source-derived SQ8 scoring and 513/1 weighted
physical intervals. Rust RAM and file placements produced identical score
bits, primary sets, page votes and ranges to the Python oracle. The final
returned IDs for each planned SQ8 page range were scored offline using the
historical V109 arithmetic. Thus "production" here means the Rust-selected
page plan with an offline Python returned scorer; live S3 return latency is
still unmeasured.

| ReLAION-1M split | Arm | Returned GT100 hits | Recall@100 | p05 hits/query | Queries below 90 hits |
|---|---|---:|---:|---:|---:|
| Development first 200 | Same-run V109 capped baseline | 19,739 / 20,000 | 98.695% | 95 | 4 |
| Development first 200 | V114 exact local route | 19,822 / 20,000 | 99.110% | 97 | 0 |
| Development all 1,000 | Same-run V109 capped baseline | 98,803 / 100,000 | 98.803% | 95 | 19 |
| Development all 1,000 | V114 exact local route | 99,234 / 100,000 | 99.234% | 98 | 1 |

The full paired gain is **431 GT hits, or 0.431 percentage point**. Baseline
counts reproduce the frozen V109 200/1,000-query controls exactly. The V114
aggregate happens to equal historical V112's 99,234 hits, but V112 used a
different code revision and offline route; it is context, not this cell's
paired control. The same-run exact oracle and Rust-selected route both
returned 99,234 hits. Independent validation matched all 200 prefix queries
and all 1,000 full queries, including source and query/GT bindings, nominee
rosters, returned IDs and per-query hits.

The production plan used at most **32 GETs/query** and **16,773,120
bytes/query**, 4,096 bytes below the 16,777,216-byte cap. There were zero
physical cap violations. The preregistered all-1,000 gate passed: at least
99,000 hits, p05 at least 90 and both the paired V109 p05 and exact-oracle
p05 minus one, no more sub-90 queries than V109, and exact score/route parity.

## Resource observations and limits

The terminal's `/usr/bin/time -v` records show V77 manifest export 253.179
seconds / 9,849,648 KiB peak RSS; source-only mirror sealing 6.24 seconds /
8,450,116 KiB; 1,000-query preparation 126.21 seconds / 908,228 KiB; Rust
RAM-plus-file scoring 9.52 seconds / 780,816 KiB; offline reduction 23.45
seconds / 1,122,576 KiB; independent validation 147.45 seconds / 8,552,788
KiB. These are **phase process** times and RSS peaks, not per-query serving
latency, steady memory per placement, 100M resource predictions, or prices.
The Rust score phase includes opening and authenticating both placements.

## Next gates

1. Freeze a live S3 gate on this exact source revision and the same
   ReLAION-1M development cohort. Pair authenticated local RAM and NVMe file
   placements, identical page plans and cold/warm runs at matched concurrency.
   Use one real `eu-central-1` NVMe Spot host and one S3 range wave, then
   measure p50/p95/p99, QPS, charged steady/peak RAM, NVMe use and IOPS,
   S3 GETs/bytes, hydration, and two-generation rollover. Stop and redesign
   if either placement misses preregistered latency or resource gates.
2. Apply the frozen method to untouched ReLAION-1M validation and an
   independently built deep-image-96-angular corpus. Use corpus-only fitting
   and the same acceptance rule. No development GT-driven threshold or
   special dataset branch may enter the method.
3. Qualify a measured memory/recall/latency frontier over `(N,D,R,C,G,L)`
   at 10M and then 100M. Larger RAM at 100M is allowed when the requested
   recall and measured latency require it. There is no vector-count knee.
   Lean's conditional score, vote and byte proofs remain proof obligations;
   workload recall, tail latency and charged memory require measurements.

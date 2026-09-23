# V115 ReLAION-1M source-only router parity closeout

Decision: **pass** the preregistered ReLAION-1M development-1000 source
router/nominee-set gate. This is offline parity evidence, not a live S3
latency, throughput, returned-recall, or cross-corpus measurement.

The frozen source commit was `8140fd86defff60ff35ef33be7596f2bda34f879`.
Its immutable archive was
`s3://borsuk-bench-453182569524-euc1/research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/source/source.tar.gz`,
11,522,717 bytes, SHA-256
`ba2127add8430b336625eb49cdebe4a06ecb2c7c504da439e9db355c8aa8c852`.
The one Causality Spot `c7i.12xlarge` attempt was `i-017a453bf7b7a7051`
in `eu-central-1c`; EC2 launched it at 2026-09-23 23:07:11 UTC, and it
terminated after the terminal. The attempt prefix is
`s3://borsuk-bench-453182569524-euc1/research/v115-source-router-parity/8140fd86defff60ff35ef33be7596f2bda34f879/runs/v115-router-20260923T235000Z/a0001`.
The `23:50Z` substring is an inaccurate label chosen before launch; EC2
launch time and the S3 terminal are the authoritative chronology.

The terminal says `complete`, exit 0, 518 elapsed seconds, terminal SHA-256
`2fb4b98354776560984e2974ab05bfe04d038771d0fb912f7f3b75d8874b2eb0`.
All 21 terminal-listed artifacts were independently streamed from S3 and
matched their recorded byte lengths and SHA-256 digests: 95,915,849 bytes in
total. The pinned source, layout, development query, GT, frozen V114 requests
and regenerated V77 manifest hashes all passed. Queries and GT were fetched
only after the source-only router artifact was sealed.

The five router planes (`summaries`, `books`, `codes`, `low`, `step`) are bitwise
identical to the frozen V77 source-derived fields. The sealed router manifest
SHA-256 is `d558a77443d6a1a50b9b3d01e821f134b1cc0992aa8bcb7ef3dc9ed2941221fe`.
The frozen V114 requests SHA-256 is
`fbb93b017df9ec6196286f082e4b5b55b48d00606ad2fc1f74d7e75cc5b7e75e`.
Rust nomination matched the V114 nominee **set** for all 1,000 development
queries: zero set mismatches. Ordered rosters differed on 48 queries (99
positions; at most four positions per query). For changed rows, the reference
Python PQ-score gap was at most `1.1920929e-7` and 35 changed positions tied
at float32 precision. Different accumulation order in Python/Rust is the
likely cause; the measured fact is set equality. Exact SQ8 scoring and page
vote aggregation depend on the set, and the exact primary is sorted by score
and ID. The live implementation must still verify returned IDs and hits.

Resource phase observations on that build instance were: source router
218.75 seconds wall and 12,683,428 KiB peak process RSS; V77 manifest export
222.80 seconds and 9,850,932 KiB; 1,000-query Rust nomination 19.81 seconds
and 93,536 KiB; parity validator 0.69 seconds and 333,544 KiB. These are
single-process phase observations, not serving p95 latency, concurrency QPS,
steady cgroup memory, or 100M scale measurements.

The relevant paired historical quality control remains the V114
ReLAION-1M **development-1000** exact-local result at its own frozen commit:
99,234/100,000 GT100 hits (99.234% Recall@100) versus the same-run V109
capped baseline of 98,803/100,000 (98.803%). V115 did not remeasure GT hits;
its parity result supports carrying the nomination set into the next live
experiment, where returned IDs and quality must be checked again. The
100k development gate was previously passed, but untouched ReLAION
validation, deep-image-96-angular, and 10M/100M remain open.

Next: integrate the Rust returned-range scorer and page verifier into a
persistent Rust runtime with an authenticated S3 range transport. Add bounded
parallel local file reads and cgroup/page-cache accounting. Then run one
preregistered paired RAM/NVMe live S3 development campaign with the absolute
latency/QPS feasibility screen and equal returned IDs, before cross-corpus
or scale promotion. Memory must be selected from the measured
`(N,D,R,C,G,L)` frontier; no dataset-name branch or vector-count knee is
authorized.

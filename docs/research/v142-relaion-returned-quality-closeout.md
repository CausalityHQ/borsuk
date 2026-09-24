# V142 ReLAION-1M returned-quality replay closeout

**Decision:** V140's unchanged β=4 selective-page policy passes the
preregistered second-corpus development quality screen. It is a candidate
for production Rust integration, not a frozen default or a live-S3
serving result. The cohort is **ReLAION-1M validation-1000, already used**,
with 1,000 Recall@100 queries and 100,000 GT positions. Its V116 layout
and fitter history differ from the deep-image D96 development cohort.

## Execution and authentication

The successful Causality Spot `c7i.12xlarge` attempt was `a0003`, from
source commit `da63de3292905fe9246ee50c078345592e7c9083` and complete
source archive SHA-256
`e015d844fca66f945c37cc03adbfdbe6c48cbaa96969526ccaee46cc666071bf`.
Its immutable prefix is
`s3://borsuk-bench-453182569524-euc1/research/v142-relaion-returned/da63de3292905fe9246ee50c078345592e7c9083/runs/v142-20260924T100356Z/a0003`.
Terminal SHA-256
`8de76254115170d6269322c3b59223f398bf6a56e67f60ef2fcd84493b83d739`
reports `complete`, exit 0, 138 seconds, instance
`i-08c1bd9ff4a3be341`, independently observed **terminated**. The
launcher authenticated all 17 terminal-listed artifacts against their
recorded sizes and SHA-256 values. Summary, scored top-512, replay and
evidence SHA-256 values are respectively
`534d244697a289a51ac1f212fb2557faa4538912b2d32ff673418e1d4014a019`,
`a8288b8e050631c564bdecb87aecb506932e61435a340168f3a4cd86ba53ee0a`,
`d5bc985d1574b27d0c18b5b63e7b6046b6da02e25c4f4eae40fd904544016614`,
and `d009727edfac62a29d4fc53729ba13ebccaeadf382ddcbe07ddd6b4c0c447109`.

Independent post-terminal recount checked all 1,000 scored, replay and
evidence rows against the authenticated V36 truth parquet and V116 sealed
top-100 IDs. It reproduced every source and SQ8 returned hit count,
same-run broad/control Rust SQ8 set parity, planned bytes and GETs,
union sizes, p05/sub-90 summaries, and paired wins/ties/losses. The
worker also validated that every SQ8 physical source ID agrees with the
V63 layout and V36 source ID mapping before scoring source vectors. The
worker downloaded truth **after** all three arms' returned IDs were
sealed. Physical GT coverage was computed in the worker from the
authenticated SQ8 mapping and GT; the independent recount checked its
per-query and aggregate consistency with returned SQ8 hits.

Attempts `a0001` and `a0002` are preserved as non-measurements. `a0001`
stopped at query 470 before GT download because NumPy float32 reduction
changed one broad top-100 set relative to V116 Rust. Its failed terminal
SHA-256 is
`a401f5f7d8890517249191e9a19a8137a975b0a6e1e7fc05dd2e5a647970e1c2`;
the replay diagnostic SHA-256
`8987ebfac3ea1303bd6f30b21969d7d2c9ceb797341038981e2477cb39494df4`
was checked after terminal. `a0002` stopped at compile before input
download because the V114 research binary imported an unused source-tier
module; failed terminal SHA-256 is
`48c6d0074908ce7aae60571fd7efa79f8c2fa618be4b823fa5ed7924fc8598d3`.
Its compile diagnostic identified the missing `native_source_id_map` and
`native_source_tier` imports, and the unused module was removed. Both
instances were observed terminated. The β=4 plan, cohort, controls,
thresholds and exact-source scorer did not change across attempts.

## Paired result

All recalls below are measured GT hits out of 100,000 positions on the
used ReLAION validation-1000 split. Bytes are planned SQ8 range bytes,
not actual S3 response bytes.

| Metric | β=4 selective pages | V116 broad candidate | V116 fixed capped control |
| --- | ---: | ---: | ---: |
| Exact-source Recall@100 | **99.607%** (99,607 hits) | 99.563% (99,563) | 99.432% (99,432) |
| SQ8-only Recall@100 | 99.255% (99,255) | 99.208% (99,208) | 98.618% (98,618) |
| Fetched SQ8 physical GT coverage | 99,695 | 99,646 | 99,004 |
| Exact-source p05 hits/query | 98/100 | 98/100 | 97/100 |
| Exact-source queries below 90 hits | 2 | 2 | 2 |
| Mean planned SQ8 bytes/query | 11,801,736.96 | 14,179,027.2 | 10,700,951.04 |
| p95 planned SQ8 bytes/query | 16,773,120 | 16,773,120 | 16,773,120 |
| Mean planned GETs/query | 23.581 | 18.343 | 22.494 |
| Mean source-union size/query | 676.279 | 669.831 | 669.247 |
| Offline Rust SQ8 scorer p95 ms/query | 11.422 | 11.412 | 11.396 |
| Offline Python source scorer p95 ms/query | 2.233 | 2.071 | 1.968 |

β=4 won/tied/lost **60/898/42** queries against the broad candidate
and **109/868/23** against the capped control by exact-source hit count.
Its aggregate exact-source gain over broad is 44 GT positions while
planning 2,377,290.24 fewer SQ8 bytes/query on average (16.77% less).
All arms stayed within 32 GETs and 16,777,216 planned bytes/query.
β=4 clears the unchanged gates of at least 99,500 exact-source hits,
p05 at least 98, at most two sub-90 queries, and no worse aggregate
quality than the same-run capped control. The V116 broad and capped
source/SQ8 totals reproduce V126 exactly.

The Rust scoring stage took 27.52 seconds for all 1,000 queries and
peaked at 1,528,124 KiB RSS. Python source-union replay took 27.28
seconds and peaked at 8,336,908 KiB RSS; its evaluation cgroup peaked
at 10,044,547,072 bytes including cache. These are offline batch
resource measurements on the Spot worker. Per-query scorer times exclude
route construction, network requests, source fetches, S3 retries,
startup and concurrency. They cannot establish a live-serving latency,
throughput or charged-memory claim.

The fetched SQ8 physical coverage is only a bound on SQ8-only returns.
The router nominee union can return GT positions outside fetched ranges:
the capped control demonstrates this with 99,432 exact-source hits versus
99,004 physically covered GT positions.

## Next gate

Implement one generic β=4 page ranking and returned-range planner in the
production Rust path, preserving authenticated generation and source-ID
mapping. Qualify the same frozen algorithm on matched-layout D96 and
D768 with a fresh held-out split, then measure complete live-S3 p50/p95/p99,
GET attempts and response bytes, concurrent throughput, startup hydrate
cost and charged memory including pinned generations. Select resource
budgets from requested recall, vector count, dimension, compression and
measured cost under a smooth generic policy, without a vector-count knee
or dataset-specific branch. Existing Lean bounds establish conditional
planner and payload properties; they do not prove empirical recall,
hardware latency or cloud service rate without validated assumptions and
measurements.

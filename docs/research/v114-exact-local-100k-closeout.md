# V114 exact local SQ8: ReLAION-100k correctness closeout

Status: **100k correctness gate passed; promote to paired 1M development and
live S3 gate.** This is a source-frozen, query-blind score and route
equivalence result. It is not a returned-recall, latency or 100M-scale
measurement.

## Frozen run and provenance

- Dataset: ReLAION-100k, **development** split, 1,000 queries; source
  SHA-256 `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`,
  query SHA-256 `4834cf63a50971b7d605c00f91b5142f67b049e91ea2c62c220271b50bffa6ac`.
- V113 completed, source-only PQ64/SQ8 artifact seal SHA-256
  `803b00d9366bc8feb0e58e4900e27b1973c5d9578c33a0d6007fe4c28722c7b2`.
  V114 created a 78,000,000-byte exact SQ8 local object and 609,376-byte
  SHA-256 block sidecar from that artifact before downloading queries.
- Frozen implementation commit `3443d7674432e281e6709bbb5ecee30f96b54a1e`;
  archive SHA-256 `e3acaee12267d25fb40ae6d43df57f0fb0e7e4269a3690468fb185e0aad1e1d2`.
- Causality Spot `c7i.8xlarge`, `eu-central-1c`, instance
  `i-0ff5022058902ae27`, terminal `complete`, exit 0, elapsed 192 s.
  The launcher waited for the instance to terminate; EC2 returned
  `terminated`.
- Immutable terminal prefix:
  `s3://borsuk-bench-453182569524-euc1/research/v114-exact-local/3443d7674432e281e6709bbb5ecee30f96b54a1e/runs/v114-100k-20260923T215331Z/a0001`.
  Terminal SHA-256 `b0123edaf3895bc893c952cc01bb6ccde120533be9f7e53a1ea1800aebc1054c`.
  Independently streamed all 23 terminal-listed artifacts (125,708,594
  bytes) from S3 and checked each byte count and SHA-256. The reduction
  SHA-256 is `fbf9ec4307f6b6ddeb7af7f142d2c7cba30048e8908c5e8fe8b8b12a91834263`.

The earlier attempt at commit `584f456e` failed during installation after
16 s because its redundant `curl` package request conflicted with the
AMI's installed `curl-minimal`. Its terminal was uploaded and the instance
`i-0e4c6ea2388d800c0` was terminated. It produced no score result.

## Verified decision evidence

The source-only PQ64 model nominated 512 rows for each frozen query. The
Python reference and both explicit Rust placements scored the same exact
SQ8 rows and selected 100 primary rows under `(score, ID)`. The physical
route gave each primary nominee 513 votes and every other nominee one,
then used the preregistered 32-GET / 16,777,216-byte interval budget.

| ReLAION-100k development metric | V114 verified result | Meaning |
| --- | ---: | --- |
| Python versus Rust RAM and file score, primary and route equality | 1,000 / 1,000 queries | All 512 nominee score bits, both top-100 lists, page votes, ranges, bytes and route score agreed per query. |
| Mirror/source row equivalence | 100,000 / 100,000 rows | Separate validator reread all IDs, norm bits, codes, block digests and artifact identities. |
| Maximum planned object GETs | 32 GETs/query | Zero cap violations. |
| Maximum planned object bytes | 16,773,120 bytes/query | 4,096 bytes below the preregistered cap; zero cap violations. |
| V114 versus completed V113 exact-SQ8 oracle primary **sets** | 1,000 / 1,000 queries | Same frozen corpus, queries and artifact; 986 / 1,000 ordered lists were identical because floating-point accumulation changes some within-set order. No primary membership changed. |

Historical negative comparator: on the same ReLAION-100k development cohort,
V113's 16-byte resident representation had **74.037/100 mean** exact-SQ8
primary overlap and **62/100 p05**; its scalar control had **72.538/100
mean** and **58/100 p05**. V114 tests exact local SQ8 placement and
equivalence; these overlap numbers are not returned Recall@100 and are not
a paired V114 serving or timing comparison.

The completed stages took 0.31 s to write the mirror, 54.85 s to prepare
query rosters/reference routes, 30.64 s for the serial Rust score/route
process, and 53.80 s for independent validation. Reported peak RSS for
those processes was 125,796, 205,372, 94,528 and 265,136 KiB,
respectively. These are **process observations for this 100k offline
attempt**, not serving memory or per-query latency; the corpus fits local
page cache and no S3 data GET was issued by a query.

## Decision and next gates

The exact local score tier clears the cheapest correctness gate. Freeze
one 1M source revision, corpus-derived PQ64 roster and physical layout,
then run paired ReLAION-1M **development** arms in one campaign:
V109-style page admission, a same-run exact SQ8 oracle through the new
physical planner, and production RAM/file local mirrors. Compare actual
returned Recall@100 and p05, per-query GT hits, primary/page/interval
equivalence, GET and byte caps. V112's historical ReLAION-1M development
99.234% returned Recall@100 is a context check, not a substitute for the
paired oracle. Gate against the preregistered 99.0% mean floor, paired
tail rule and zero cap violations.

Only after that gate passes, run cold and warmed live S3 query arms on
an EC2 type with real local NVMe at matched concurrency, measuring p50,
p95, p99, QPS, RAM, SSD occupancy, hydration and two-generation rollover.
Then test untouched ReLAION-1M validation and deep-image-96-angular with
the same method and quality rules before 10M/100M qualification. Memory
is a measured function of dataset size, dimension, requested recall,
concurrency and generations; no vector-count knee or 100M memory ceiling
is inferred from this 100k result. Conditional Lean proofs remain
conditional on score and route assumptions and do not establish empirical
recall or latency.

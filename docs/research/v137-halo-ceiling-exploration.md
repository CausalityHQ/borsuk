# V137 post-hoc physical halo diagnostic

**Status: exploratory coverage ceiling, not a selected serving method.**
V132's actual S3 range schedule lost its paired latency screen because the
candidate read 9.428 MB/query on average. This cheap diagnostic asks whether
a source-only neighborhood around the fixed capped control ranges could
recover enough physical GT coverage with fewer bytes. It does **not** time
S3, rerank returned vectors, or qualify another embedding family.

The inputs are V122's already-used **deep-image-96-angular random 100k train
subset**, publication-test ordinals 9000–9999. We read the complete V122
attempt's authenticated `evidence.jsonl` (6,183,526 bytes, SHA-256
`deac3e5e9d15a54753a1543bed338e31b23daaf3f234f64afabde5e79bcc6ae6`)
and `built/layout.npy` (800,128 bytes, SHA-256
`8b23fb6d2f76704394733f5540f25e36db961386b568495b7bfd73fc52d60aea`)
from the immutable a0001 prefix described in
`v122-deep-image-100k-closeout.md`. The standalone standard-library
recount is `scripts/v137_halo_ceiling.py`; it verifies both hashes, layout
permutation, all 1,000 query identities, and exact agreement of the
zero-halo per-query bytes, GETs and GT coverage with sealed V122 records.

For each already-selected control byte range, extend both ends by `h`
physical 256-row pages, clamp to the object, then merge overlapping or
adjacent ranges. This selection uses control ranges and page geometry only;
GT IDs enter **afterward** to count which exact neighbors lie within the
resulting ranges. No reported `h` was preregistered as a default.

| Halo pages each side | GT100 positions covered / 100,000 | Mean planned MB/query | Mean merged GETs/query | Max bytes/query | Cap violations |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0, sealed capped control | 98,827 | 1.536 | 24.674 | 3,760,128 | 0 |
| 1 | 99,365 | 2.829 | 20.067 | 5,491,584 | 0 |
| 2 | 99,527 | 3.863 | 15.955 | 7,178,112 | 0 |
| 4 | 99,720 | 5.354 | 11.007 | 9,400,320 | 0 |
| 8 | 99,891 | 7.187 | 6.464 | 10,578,816 | 0 |
| 16 | 99,965 | 8.877 | 2.668 | 10,800,000 | 0 |

Here MB uses decimal bytes/1,000,000. The maximum merged GET count was 32
at h=0 and h=1, then at most 29 at h=2; every inspected plan stayed below
32 GETs and 16,777,216 bytes. The h=2 row is an **observed physical
coverage ceiling on a reused split**. It is not returned Recall@100 and
must not be presented as a fresh 99.5% quality win. The full V135 candidate
already measured 99,942 exact-source hits with mean 9.428 MB of SQ8 reads;
the h=2 diagnostic uses about 41% as many planned bytes but has no S3 timing.

The result motivates a cross-corpus, source-only selective range design,
not a hardcoded two-page rule. At D768 and 1M, each 256-row page is about
200 KiB and a 32-range halo can consume the 16-MiB cap quickly; a page count
selected from this D96 cohort would be dataset and geometry tuning. The
next decisive offline screen must use one budget-aware policy on this used
deep-image cohort and the already-used ReLAION-1M development cohort,
recording per-query GETs, bytes, exact-source returned quality, p05 and
sub-90 tails. Only a cross-corpus winner warrants a new paired live-S3
attempt and fresh 1M validation.

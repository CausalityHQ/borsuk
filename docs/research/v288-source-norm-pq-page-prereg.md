# V288 source-norm PQ64 page-ranking check

Status: frozen before reading development query outcomes. V287's authenticated
PQ64 code plane, globally scanned on the V283 CoHere first100k D768 cosine
k100 physical layout, fetched 98.59375 GT100 mean/p05 94 with Euclidean
ADC and 98.390625/p05 93 with reconstructed-norm cosine. The V285 exact-row
page ordering fetched 100/100. V284/V286 already tested eight source means
per page; V23 tested 32 prototypes on a different 10M D96 corpus. Neither
will be rerun here.

**Single change.** Keep the same 64-byte PQ codes, books, pages, 84-page/32-GET
greedy schedule, and 16,777,216-byte data ceiling. Score each row by the dot
product of its PQ reconstruction with the query divided by the row's *source*
L2 norm stored as binary16, rather than the reconstruction norm. Source norms
are calculated from the authenticated raw source without queries or truth.
The extra resident code plane is 2 bytes per row: 200 KiB at 100k, projected
200 MB at 100M for one generation before allocation and rollover. This is a
fidelity check, not a sublinear product route or measured 100M memory.

Pin source, generation archive/root, layout, development requests and GT100
to the V283/V287 hashes. Globally scan codes on **only** CoHere development
ordinals0–63, rank each page by its maximum row score with stable page-ID
ties, and use the unchanged scheduler. KILL if fetched GT100 mean <98.9,
p05 <96, any plan exceeds 32 GET/16 MiB, process RSS exceeds 2 GiB, or the
single local run exceeds two minutes. This necessary fetched bound allows
roughly one SQ8 ranking loss before the V282 returned 98%/p05 95 gate. A pass
permits one bounded Rust/sublinear route and returned-SQ8 check on the same
development split, then the unchanged two-dataset validation gate. It cannot
promote 1M, service latency, or a vendor claim.

Historical stored-source-norm **two-bit final scoring** lost quality on a
different ReLAION roster. That result is a warning, not this PQ64 page-order
measurement. No training, cloud job, full panel, or parameter sweep.

## Completed local decision (2026-09-27 UTC)

**KILL source-norm PQ64 page ranking.** The sole frozen run from `c6304279`
completed exit 0 in 3.88 seconds with 368,496 KiB peak process RSS. Its
[result](v288-source-norm-pq-page-result.json) has SHA-256
`b4f6f165bfb5492a2dbd007a26439ede4dc59802f8b58da837ab16ca45dfa2fa`.

| CoHere first100k D768 cosine k100, development0–63 | Fetched GT100 mean | p05 hits | Max GET | Max planned bytes |
| --- | ---: | ---: | ---: | ---: |
| V287 PQ64 reconstructed-norm cosine control | 98.390625 | 93 | 32 | 16,773,120 |
| V288 PQ64 with source binary16 norm | 95.343750 | 87 | 32 | 16,773,120 |
| Frozen necessary gate | >=98.900000 | >=96 | <=32 | <=16,777,216 |

The source-norm substitution loses 3.046875 fetched GT hits/query against the
same-code V287 cosine control. Source norms do not correct this PQ64 page
ordering error. No sublinear router, SQ8 returned replay, validation panel or
paid run is promoted. These are fetched-page coverage numbers, not returned
recall or HTTP latency. The next design needs more row geometry or a different
physical-page representation, with a new source-only necessary check before
any cloud work. The BORSUK EC2 active-instance query returned `[]` at closeout.

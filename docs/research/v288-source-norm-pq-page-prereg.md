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

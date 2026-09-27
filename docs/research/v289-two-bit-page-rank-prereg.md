# V289 rotated two-bit resident page-ranking falsifier

Status: frozen before reading CoHere development query outcomes. The V283
semantic-cell layout makes an exact-row page-ordering oracle cover 100 GT100
rows/query within 84 pages, 32 GET and 16,777,216 SQ8 bytes. V287 global
PQ64 misses its necessary fetched-coverage gate at 98.59375 mean/p05 94;
V288's stored source norm worsens it. V284/V286 eight unit means/page also
failed. The historical ReLAION-100k 200-byte rotated two-bit code lost only
97/100,000 GT100 positions in *page nomination* against paired exact rows,
but failed as a sole final scorer and its separate 1M page-local code wave
missed the byte cap. Those distinct failures are not rerun or treated as a
product pass here.

**One representation change.** Use the existing source-only 200-byte rotated
two-bit record rule (`_fit_records`, rotation seed 20260923), in V283's fixed
physical order. The source mean and sign/Hadamard transform are trained only
from authenticated CoHere first100k D768 raw rows. Score every reconstructed
row by cosine with each query, then each page by its maximum row score.
Keep V284's stable page ordering and greedy 84-page/32-GET/16-MiB SQ8 page
schedule. All records remain resident in this **global-score upper screen**;
an eventual product route must be sublinear. The 200-byte plane projects to
20 GB at 100M for one generation, 40 GB for two pinned generations, before
page summaries, query memory, allocator and service reserve. This is an
unmeasured RAM projection, not a chosen product cap.

Use only CoHere development ordinals0–63 and the V283 source/layout and
V285 request/truth hashes; no validation reuse. KILL if fetched mean GT100
<98.9, p05 <96, any plan >32 GET or >16,777,216 bytes, process RSS >2 GiB,
or one local run >300 seconds. The necessary bound reserves roughly one SQ8
ranking loss before V282's returned 98%/p05 95 gate. A pass permits **one**
sublinear Rust route design and same-query returned-SQ8 test, then the
unchanged paired ReLAION+CoHere 100k validation gate. A fail forbids a code
plane build, cloud run or 1M claim. Report build and query wall/RSS as offline
diagnostics only, never HTTP latency/QPS. No parameter sweep or new paid job.

## Completed development decision (2026-09-27 UTC)

**GO to one bounded, sublinear route screen; no serving or scale promotion.**
The preregistered `b4f22f59` source-only run completed exit 0 in 7.75 s with
538,780 KiB peak process RSS. Its [result](v289-two-bit-page-rank-result.json)
SHA-256 is
`24af97e0da0b27a6bb33371b57a969b899c4b062a025bd6374c84196bf94a9e6`.

| CoHere first100k D768 cosine k100, development0–63 | Fetched GT100 mean | p05 hits | Max GET | Max planned bytes |
| --- | ---: | ---: | ---: | ---: |
| V287 resident PQ64 global page score | 98.390625 | 93 | 32 | 16,773,120 |
| V289 resident rotated two-bit global page score | **99.953125** | **100** | 32 | 16,773,120 |
| Frozen necessary gate | >=98.900000 | >=96 | <=32 | <=16,777,216 |

The score is query-to-page nomination with SQ8 data bytes *planned*, not
returned recall or an actual S3 GET. An independent eight-row arithmetic
check compared the closed-form score against explicit rotated-vector cosine;
maximum absolute error was `5.55e-16`. The code plane is 200 bytes/row and
all 100,000 records are scored for every query here. Neither the 7.75 s
whole-process diagnostic time nor its RSS predicts service latency or 100M
memory. The next gate must bound row-code scoring work sharply while keeping
coverage, then test returned SQ8 on development only. No cloud job yet.

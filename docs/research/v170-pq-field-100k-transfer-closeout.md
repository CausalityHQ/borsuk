# V170 direct PQ field: 100k transfer closeout

## Decision

**Advance the direct PQ64 score field to a frozen 1M paired screen.** The
preregistered V170 gate passed on the **reused** ReLAION-100k D768
development-1000 cohort. The direct field and a PQ-free neighbor-rank arm
used the same 32-row candidate universe, exact-primary rule, interval DP,
and each query's V163 GET/byte ceiling. V163 is the strongest returned
quality baseline on this layout. The result does not select a production
default or establish a fresh holdout, live S3 latency, charged serving RAM,
or 100M feasibility.

| ReLAION-100k D768 development-1000, used | V163 smooth layout | 32-row neighbor rank | 32-row direct PQ |
| --- | ---: | ---: | ---: |
| Returned GT100 hits / 100,000 | 99,322 | 99,338 | **99,357** |
| Returned Recall@100 | 99.322% | 99.338% | **99.357%** |
| p05 returned GT100 hits/query | 98 | 98 | **98** |
| GT100 IDs in fetched SQ8 ranges / 100,000 | 99,704 | 99,723 | **99,747** |
| Planned SQ8 bytes / 1,000 queries | 15,657,408,000 | **14,779,090,560** | 14,785,704,960 |
| Planned GETs / 1,000 queries | 15,275 | **13,888** | **13,888** |

Direct PQ gained **35** returned GT100 hits over V163 while using
871,703,040 fewer planned bytes (5.57%) and 1,387 fewer planned GETs
(9.08%). It gained **19** hits over the same-unit neighbor-rank arm;
paired wins/ties/losses were 22/972/6. The preregistered 10,000-resample
PCG64(170) 95% interval for the paired total gain was **8–32 hits**.
The direct arm used 6,614,400 more bytes than neighbor rank (0.045% of
neighbor's total), with identical GET counts on every query. On the 905
queries where both arms used exactly the same bytes and GETs, direct PQ
gained 16 hits (18 wins, 882 ties, 5 losses). That 905-query breakdown is
post hoc and is **not** a replacement for the preregistered whole-cohort
gate. The resource difference means the aggregate 19-hit gain is a
same-ceiling result rather than a strict equal-byte result; the 1M screen
must report a resource frontier and equal-resource paired points.

The direct arm cleared each declared threshold: at least 99,347 hits,
at least 10 hits over neighbor rank, positive bootstrap lower endpoint,
p05 at least 98 and no worse than neighbor rank, mandatory primary coverage,
and no per-query resource excess over V163. It returned 99,357 hits,
19 more than neighbor rank, bootstrap lower endpoint 8, and p05 98.
The plan is a fixed resource-envelope experiment. It does not yet implement
the V169 caller recall target or minimum measured-cost policy.

## Immutable execution and verification

One Causality Spot `c7i.8xlarge` instance `i-09b45535c65be602c`
ran source commit `85c73dde4964779336ad856f7170ffbaa020711d` from
`s3://borsuk-bench-453182569524-euc1/research/v170-pq-field-100k/85c73dde4964779336ad856f7170ffbaa020711d/runs/a0001/`.
Its source archive SHA-256 is
`557cd94b9c04bece54a8d23c807a520e64a3b0cad84105d9b5149109aba7f20f`.
The complete terminal SHA-256 is
`9d20e60e8b6ae115b51347e4360a613df0ff105070cdcc9b3eb07ed6734a81b1`;
the checked summary SHA-256 is
`34a4df2d6614ef9c876a244002e98df721890d934dc2b4c0da4e114167432390`.
The worker uploaded the GT-blind plan and seal to distinct S3 keys **before**
downloading truth. The plan seal SHA-256 is
`d364520f598e9aeb8b4a794e6fe1c7cc29e5d37ae4d9688062b8191b18387848`.
The checker replayed all 1,000 plans and returns, compared raw and summary
hashes, and reported `pass` / `advance-to-1m`. The controller streamed and
rehashed every terminal-listed artifact plus the earlier GT-blind objects,
then confirmed the EC2 instance `terminated`. An independent postterminal
read rehashed the terminal, summary, checker and raw rows and recounted the
three paired hit comparisons and resource excess. The raw SHA-256 is
`d8081a9feaec74d750393b675819985941a15f409890d788e00ad9b38e3b3894`.

The offline plan phase took 22.07 wall seconds for 1,000 queries and
peaked at 188,420 KiB process RSS; the two-arm SQ8 reduction took 16.32
seconds and peaked at 291,244 KiB; the full replay took 37.42 seconds and
peaked at 292,096 KiB. These are worker process measurements, **not**
serving latency or charged per-generation memory. No live S3 query GETs
were timed; the bytes and GETs above are planned quantities.

## Next gate and limits

Freeze V170's direct field, neighbor-rank control, candidate geometry,
primary rule and 32-row physical admission for a paired ReLAION-1M D768
validation-1000 **used** transfer screen on the authenticated V164 order.
Report SQ8-only returned recall, physical GT coverage, planned bytes/GETs,
per-query resource frontiers, and equal-resource direct-versus-control
points. Add the same exact-source union rerank as V155 before comparing
exact-source Recall@100 against the strongest measured 1M baseline:
V155 cached sparse 99,567/100,000, p05 98,
11,134,007,040 planned bytes and 22,126 GETs. V164's same-layout context
is 99,553 exact-source hits, 99,199 SQ8-only hits,
14,568,253,440 bytes and 16,776 GETs. The 1M gate must decide whether
the 100k ranking gain survives a much less saturated corpus without
trading unacceptable bytes or GETs. A failure calls for a causal change
to utility calibration or physical scheduling, not a dataset-specific
coefficient.

Only after a successful 1M and fresh real-query holdout should the
production format, adaptive recall/resource contract, live-S3
latency/cost, and 10M/100M build/query/memory gates advance. The V170
planner's mandatory-coverage and byte/GET properties were checked by
executable recount; a Lean refinement proof remains conditional work.

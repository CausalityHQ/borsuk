# OPQ8 proofs

`PhysicalIntervalBudget.lean` proves exact SQ8 page-to-byte conversion for
the V63/V70 one-object layout used by V110/V111: 336 units of 49,920 bytes
fit under 16 MiB, and 337 do not. Given authenticated page and GET counts,
it proves the 32-GET/16-MiB arithmetic gate. It does not yet prove the
NumPy interval DP's refinement to those counts; the exact DP recurrence is
tested against exhaustive small layouts. The proof does not establish
query-time recall, live S3 latency or 100M memory peaks.

Run `lean Opq8Planner.lean` from this directory with the pinned Lean
toolchain. The file imports only `Std` and contains no admitted theorems.

The model covers the 100k one-GET-per-group planner. It proves group
selection order, uniqueness under a unique input ranking, exact byte
accounting and the 32-read/16-MiB caps. It also proves one pairwise
fixed-point score-order lemma, truth-owner counting lemmas conditional on
the owners already being certified as selected, and 8-byte code-plane
arithmetic. It assumes valid group lengths and an authenticated ranking.
The file also proves the budget bound for an abstract 1M planner that
recounts merged GETs from sealed physical predecessors, including a
bridging example where three groups need one GET. It does not prove that
the Python incremental merge counter equals this recount, or that the
predecessor map was constructed correctly. Python refinement and
numerical certificates for actual queries remain separate work.

The 100M arithmetic theorem adds a conditional memory result: if two
complete route-code planes, a float64 score array and a float64 indexed
lookup array coexist, their `32N` bytes exceed the 3-GiB campaign cap
with its 64-MiB allowance at 100M rows. It excludes allocator and
metadata overhead; it is not a latency theorem.

For a full OPQ8 route scan at 100M rows, a checked theorem counts exactly
800,000,000 modeled table lookups. Another theorem says that if a certified
upper hardware rate cannot complete those lookups within a target time,
the full-scan implementation misses that target. This is a conditional
scalability and latency *lower* bound; it needs a defensible hardware rate
and does not apply to a route that visits fewer rows.

For a future data-range implementation, a separate conditional theorem
combines a 32-GET/16-MiB plan with certified per-GET, per-byte and local
compute upper bounds into a sequential latency ceiling. A region-work
theorem similarly converts a certified visited-row cap into at most eight
table lookups per visited row. These results provide arithmetic implications;
the bounds and implementation correspondence must be supplied and checked
for any production claim.

The same file now gives a conditional **two-wave** sequential latency
ceiling for at most 32 code GETs/16 MiB and 32 data GETs/16 MiB. Its
per-request, per-byte and local-work time bounds are premises to be
measured for the deployed reader; the theorem does not predict S3 tail
latency or parallel-wave scheduling from source code alone.

It also gives a conditional **three-wave** ceiling for separate sign,
magnitude and data GET waves, each capped at 32 requests and 16 MiB.
The third wave can increase actual latency; only measured service-time
bounds can make the theorem a useful numerical SLO certificate.
For matching sign and magnitude page covers issued concurrently, a
separate theorem bounds latency by the slower code wave plus the data
wave, conditional on a joint-completion service premise. It does not
establish that the S3 client or network actually achieves that overlap.

`SourceRangeFidelity.lean` adds paired finite-cohort hit accounting:
authenticated source/compressed hit pairs and bounded lost hits imply
the 100k and 1M aggregate fidelity floors. It separately proves the
number of compressed-score queries below 90 GT100 cannot exceed the
number of source-score queries at risk after charging their certified
lost hits. A certificate of at most 49 at-risk queries therefore proves
the 1M sub-90 gate. This still needs authenticated per-query hit/loss data.
The file also proves the
96-byte sign/PQ record arithmetic, a conditional 16-MiB **code** payload
row bound, 94 sign or 96 PQ table lookups per fetched row, and 9.6 billion
PQ lookups for a full 100M-row scan. It proves page-minimum score order
when every row's score error is bounded and two true page minima differ
by more than twice that bound. Actual hit pairs, score-error bounds,
Python refinement, S3 service times and unseen-query recall are explicit
external premises. In particular the checked lookup count is a work
bound, not a latency or scalability claim.

The threshold-admission theorem also proves exact equality of the
admitted page list and its truth-hit count when every competing page
has a certified absolute score-error bound and its true score lies
outside that error band around a fixed admission threshold. This is a
conditional finite-query recall certificate: the threshold, page scores,
truth-owner counts and error bounds must be authenticated, and a concrete
quota/range planner must be proved to implement the threshold model.
It cannot certify the failed sign96 or PQ96 development runs merely from
their aggregate error statistics.

For the historical 200-byte rotated two-bit format, Lean proves a
20,000,000,000-byte code plane at 100M rows, 76,800,000,000 coordinate
decode/score operations for a full scan, and at most 83,886 row records
in a 16-MiB code payload with group framing. These are exact arithmetic
for the modeled format; streaming memory, observed latency, throughput,
and recall depend on the implementation and data.
It also proves at most 327 complete 256-row pages of 200-byte records fit
one 16-MiB wave. Two independent 16-byte-per-row resident route-code
generations at 100M rows plus a 64-MiB process allowance exceed 3 GiB,
before summaries, mappings or query buffers. That arithmetic requires
code sharing or a narrower representation for a two-generation design
under this cap; it does not establish that either is implementable.

For a proposed progressive split, Lean proves that every two-bit symbol
rejoins exactly from its sign and magnitude bits, and that a 104-byte
sign/scale/norm record plus a 96-byte magnitude record equals the old
200-byte record. Under separate 16-MiB payload caps, the sign wave can
contain at most 161,319 rows and the magnitude wave at most 174,762.
These statements establish neither coverage of the sealed 1M group
plan at the new 104-byte width nor fidelity of rows fetched from only
the first plane. Both need a frozen source-only gate before promotion.
If both planes fetch the same pages, Lean additionally proves the
96-byte magnitude payload cannot exceed the 104-byte sign payload;
the sign wave's 16-MiB payload cap therefore suffices for the mirrored
magnitude payload. The GET-count relationship assumes matching page
intervals and requires a reader/planner refinement certificate.

For the proposed page-local code object, a further theorem proves that
the exact payload of the pages in a physical code cover is 200 times
their total row count. A 16-MiB code-wave cap therefore permits at most
83,886 rows, including rows on bridged pages. The sealed page map and
Python range planner must supply the actual cover and establish refinement
before this bound describes a served query. This implies at most
64,424,448 coordinate decode/score operations for the modeled code wave;
it is a work count rather than a wall-clock bound.

The code-cover ceiling theorem proves that a scorer restricted to fetched
pages cannot hit more truth positions than those pages contain. If an
authenticated Boolean certificate establishes the observed 98,468 GT100
code-cover hits and that every final hit came from the cover, no scorer or
reranker on that same cover can clear the preregistered 98,651 screening
floor. This rules out rescoring the rejected cover; it does not rule out
a different layout, page choice, or two-wave schedule.
For the later terminal-closed progressive cover, a separate theorem says
that its authenticated 98,986 GT100 contained positions would imply the
98,151 final aggregate floor if at most 835 paired positions are lost
between cover and final result. The terminal-closed paired scorer cell
subsequently contained 98,728 GT100 truth owners in final data pages, so
its 258-position
cover-to-final gap satisfies that premise; GT10 and the lower tail were
checked separately by the cell validator. This is a finite-cohort
certificate and does not establish actual returned recall or unseen-query
recall. The sealed final data objects are SQ8 pages and require an
authenticated read and rerank gate.

The sealed progressive code/data plans give per-query lower wave sizes
of 16,773,536 sign bytes, 15,483,264 magnitude bytes and 16,756,872
SQ8 data bytes on the development cohort. `mirrored_plan_byte_floor`
proves their sum is at least 49,013,672 bytes. A second theorem proves
that a target latency is impossible if a **certified maximum aggregate
transfer rate** cannot carry that many bytes by the target time. The
plan minima and rate cap remain external assumptions; this is a
necessary bandwidth condition, not measured or sufficient latency.
The checked-in `docs/research/progressive-transfer-floor-certificate.json`
is generated by `scripts/certify_native_progressive_transfer_floor.py` from
the two complete terminal-bound plan files. It authenticates their bytes and
the 1,000-query roster and recomputes the numerical premise. Physical range
replay remains in the independent remote closeout; the certificate is not an
unseen-query or observed-throughput guarantee.
AWS publishes nominal c7i.8xlarge and c7i.12xlarge network bandwidth
of 12.5 and 18.75 Gbit/s. If all sealed-plan response bytes traverse
one such instance interface, the checked integer arithmetic caps the
mirrored schedule at respectively 31 and 47 ideal completed queries
per second per instance. The proof does not turn those published caps
into observed S3 throughput, nor apply to a different byte schedule.

A second finite-query theorem bounds recall loss for a **fixed score
threshold** by the authenticated truth count of pages whose true scores
fall in the error band immediately below that threshold. It needs a
certified per-page score upper-error bound and the page scores/truth
counts. It does not yet transfer to the production greedy byte/GET
planner; that requires a checked planner refinement or its own admission
certificate. This conditional result can bound cohort recall with data
assumptions, while unseen-query recall remains an empirical question.
If a separately checked threshold model produced the closed 1M source-
score total of 98,920 GT100 hits, a boundary-band certificate of at most
769 truth positions would imply the 98,151 GT100 floor in that model.
The existing greedy range plans have no such threshold refinement yet.

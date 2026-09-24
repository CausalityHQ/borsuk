# OPQ8 proofs

`BudgetedPageAdmission.lean` proves that primary-first page admission stays
within its GET and byte caps when every proposed coalesced cover is charged
before admission. It checks the current D96/D768 full-page byte arithmetic
and proves that a linear resident payload formula grows monotonically with
vector count and with a chosen higher-recall bytes-per-row budget, without a
vector-count switch. The 100M payload examples exclude headers, hierarchy,
allocator, page cache, scratch and concurrent generations beyond the stated
count. Rust cover refinement, useful page ranking, actual recall, charged RAM
and S3 latency remain separate obligations. Run
`lean BudgetedPageAdmission.lean` from this directory with the pinned
toolchain.

`AdaptiveRerank.lean` proves a conditional score-interval rule for a
quantized-first exact-source reranker. If a row's sound upper score bound is
below a threshold and `k` distinct candidate witnesses have sound lower
bounds at or above that threshold, the row is strictly outranked by those
`k` witnesses and cannot enter a correct exact top-`k` result. A production
certificate must establish interval soundness for the deployed FP16/CPU
kernel, authenticate the witness IDs and exact fallback scores, and refine
the actual deterministic top-`k` implementation to this model. The file also
proves the linear `N × (2D + metadata) × G` FP16 payload law: with 8 bytes of
metadata per row, two complete 100M generations are 40.0 GB at D96 or
308.8 GB at D768, before allocator, router, mappings, page cache, query
scratch and concurrent work. These are payload projections without a vector
count knee; they do not prove charged RAM or latency. Run
`lean AdaptiveRerank.lean` from this directory with the pinned toolchain.
The V124 finite-cohort corollary says that a scorer restricted to the sealed
ReLAION-1M validation-1000 nominee sets cannot reach 99,000 GT100 hits if
their independently authenticated capture is 96,849. The proof uses that
capture count as an external premise and does not apply to V116's expanded
remote candidate set, a changed router or unseen queries.
The same file checks exact payload and digest-table arithmetic for a
block-verified float32 source tier with an 8-byte source ID per row and a
64-byte header. At 100M rows and two complete generations the source
artifact totals 78,400,000,128 bytes at D96 or 616,000,000,128 bytes at
D768. With 64-KiB blocks, the modeled digest table is 38,281,280 bytes for
D96 or 300,781,312 bytes for D768. A 4-KiB block choice at D768 would require
4,812,500,032 digest bytes for two generations. Block size is an explicit
memory/I/O policy input; these checked payloads establish neither charged
memory nor query-time local-read latency.

`IntervalLatticeNormalization.lean` proves the arithmetic behind V121's D96
planner-state reduction: when full and final-page charges share a positive
divisor, dividing those charges and the budget preserves feasible charged
plans and physical bytes. It checks the normalized state product of
14,742,816 at the separately observed 368-page maximum. Rust implementation
refinement, query quality, planner CPU latency and arbitrary final-page
complexity remain separate obligations.

`PhysicalIntervalBudget.lean` proves exact SQ8 page-to-byte conversion for
the V63/V70 one-object layout used by V110/V111: 336 units of 49,920 bytes
fit under 16 MiB, and 337 do not. Given authenticated page and GET counts,
it proves the 32-GET/16-MiB arithmetic gate. It does not yet prove the
NumPy interval DP's refinement to those counts; the exact DP recurrence is
tested against exhaustive small layouts. The proof does not establish
query-time recall, live S3 latency or 100M memory peaks.
It also proves that V112's 513-to-1 vote encoding ranks any gain of one
precise nominee above at most 512 secondary votes.
The generic vote theorem replaces 513 with `secondaryBudget + 1`: for any
bounded secondary roster, one additional primary vote outweighs every
possible secondary vote. This is an arithmetic design rule for a future
larger shortlist, not evidence that such a shortlist meets recall or cost
targets. The current V114 implementation still uses the frozen 512-row
operating point; changing that point requires a new paired campaign.
The partial-final-page theorem proves that exact physical bytes remain under
the cap when a short tail is conservatively charged in whole planning units.
It assumes the tail's actual bytes fit those charged units; it does not prove
the Rust planner implements that premise or establish S3 latency.

`NomineePrimaryStability.lean` proves a conditional V113 certificate: if
each nominee has an authenticated score-error bound and the precise
top-100 boundary exceeds those bounds, approximate scoring selects the
same 100 rows and gives the same result under any deterministic page-vote
function. It also checks that a 16-byte plane across two complete 100M
generations occupies 3.2 billion payload bytes. Neither statement is a
measured recall, latency, or charged-memory result. Run
`lean NomineePrimaryStability.lean` from this directory to check it.
The fixed-route corollary proves equal returned GT hits when the margin
premise makes the entire primary roster identical and every other route
input is held fixed. The generic payload law is `16 × rows × generations`
bytes; it has no vector-count knee or memory cap. A charged-memory claim
still needs measured allocator, codebook, layout and concurrency overhead.

`ResidentExactnessLowerBound.lean` proves a generic distinction requirement
for an exact query-independent SQ8 scorer. The stored norm is observable at
the zero query, and basis-query scores reveal each code coordinate in the
checked unit-step model, so a
state that answers all of them exactly must encode distinct SQ8 code vectors
distinctly. It does not machine-check a bit-count lower bound and does not
formalize the reduction for general affine SQ8 steps. It does not apply to a
restricted query family or an approximate scorer. Run
`lean ResidentExactnessLowerBound.lean` from this directory to check it.
The same file checks the exact `(D + 12) × N × G` payload of the existing
SQ8 record format (8-byte ID, 4-byte norm, D code bytes): 156 billion bytes
for 100M 768-dimensional rows across two generations, or 21.6 billion
bytes at 96 dimensions. These are payload projections, not charged RAM,
SSD occupancy, or an affordability result.

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

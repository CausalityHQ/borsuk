# Single-wave code serving: falsifiable architecture candidate

Status: research hypothesis, not a qualified reader or frozen production format.
This is a material alternative to the stopped mirrored 104/96 code waves. It
must be rejected at the earliest decisive failed gate. The next action is an
offline sealed replay before building new code objects or launching reads.
Historical results keep their original layouts, splits and source revisions.

## Intended result and known limits

The production goal is object-storage-native ANN with at least 99% returned
Recall@100 on an untouched ReLAION-1M query split, at most 32 authenticated
GETs and 16,777,216 planned response bytes per query, a measured competitive latency
and throughput envelope, and at most 3 GiB resident memory at 100M including
generation rollover. Query-visible mutations and recovery are later product
gates. These are targets, not established properties.

The closed ReLAION-100k development actual-read code-wave cell scored its
selected rows with the 200-byte rotated two-bit record: its primary page
nomination contained 98.418% GT100 versus 98.515% for exact scores on the
same fetched rows. This 0.097 percentage-point **page-containment** gap
supports testing the scorer; it does not measure code-only returned recall.
The closed ReLAION-1M development mirrored cell contained 98,728 GT100 in
its final SQ8 data pages; the 193-position paired loss likewise concerns
page nomination, not final code-only ranking. Neither is transferable to a
different layout without a new test.

V63's ReLAION-1M corpus-only k-means-8192 centroid-chain layout puts every
development query's 100 ground-truth neighbors within an oracle choice of
64 256-row pages. That is an oracle containment result, not a route. V66's
compact page summaries and V72's resident row codes show useful but distinct
route components; neither establishes their joint recall under one 200-byte
wave. V85's capped PQ16 reader returned 93.505% Recall@100 on a different
layout and SQ8 payload. This is a serious risk, not a proof against PQ16 on
the new geometry. The stopped OPQ8 page-local 200-byte wave contained only 98,468 GT100
under 16 MiB on its *different* layout and route. A new test must identify
which layer changes that result.

## Candidate and alternatives

Use a new generation format with one page-ordered S3 payload per base/delta
role. Each physical row has the existing bit-exact 200-byte rotated two-bit
record, not separately fetched sign and magnitude planes. A range response
must have HTTP 206, the requested Content-Range and length, and matching
sealed page SHA-256 digests. One immutable generation authority must bind
format version, object identities, page geometry and digests, positional
source IDs, rotation seed, row-code books, liveness and tombstones. Reject
mixed or unsupported generations. The reader scores all live fetched rows with
the primary two-bit formula and returns the top 100 by score and stable ID.
It does not fetch SQ8 or exact vectors on the query path. Retain exact source
vectors separately for rebuild and paired research controls.

Lay out 256-row pages in a corpus-only k-means centroid-chain order. Route
with a compact hierarchy: page summaries narrow the search, then resident
row codes nominate pages. A graph or tree entry layer may reduce page-summary
work, but its *observed* recall and visited-node bound must be measured;
calling it logarithmic is only a hypothesis. The offline replay compares
flat and hierarchical routing on identical pages, and fixes PQ16, PQ24,
PQ32 and PQ64 as separately reported row-code controls. PQ64 is a quality
control that fails the 100M resident-memory target. A 128-bit rotated sign
code is only a later representation candidate if these controls expose a
width gap. The
nomination planner coalesces adjacent pages by exact encoded range geometry
and charges every bridged page against both caps. It must select pages without
access to truth or exact source scores.

For the first offline replay, predeclare two and eight PQ192 summaries per
page, flat versus 1,024-region coarse filtering, and row shortlists of 512,
2,048 and 4,096. These are diagnostic arms, not a grid to tune against the
burned development queries. Two PQ192 summaries per 256-row page cost
1.5 bytes/row compressed but 24 bytes/row if eagerly decoded to float32;
only a bounded compressed scoring path could satisfy the eventual resident
cap. The eight-summary arm is a quality control with greater memory cost.
Any navigable entry layer needs its own fixed construction and visit budget
before a scale claim.

The 16-MiB cap holds at most 83,886 complete 200-byte rows before framing;
at 256 rows/page it holds at most 327 full pages. A shortlist of 4,096 rows
can scatter far beyond that limit, so shortlist size alone is no read-cost
bound. The planner must seal the actual physical pages, merged GET ranges and
bytes before opening truth. Mean 4–6 MiB/query and any throughput derived
from it are hypotheses, not measured performance. At the nominal 18.75-Gbit/s
`c7i.12xlarge` network cap, a mean above 12,849,506 transferred bytes/query
would make 182.4 QPS impossible on that interface even with zero overhead.
This is conditional arithmetic against the strongest measured bounded
BORSUK reader, not a prediction of S3 throughput.

The fallback choices are bounded. If code-only ranking fails while paired
page containment passes, a wider record is a separate preregistered candidate
with a new byte worksheet. Exact reranking reintroduces a second wave and
is a different architecture. If routing fails while a feasible truth-aware
cover passes, change the hierarchy or page representation. If a proved
physical optimum fails, change the layout or read cap. Do not keep
the rejected OPQ8 route and merely remove its SQ8 data wave.

## Gates, in order

1. **100k code-only fidelity.** Reuse the closed ReLAION-100k development
   fetched-row authority and 200-byte records. Return the best 100 IDs by
   two-bit score, then by exact float32 score on precisely those rows. This
   offline scorer test needs no new GET or page layout and isolates returned
   ranking from page nomination. Predeclare primary-versus-exact mean loss
   at most 0.25 percentage point, compressed marginal p05 no more than one
   hit below exact, and report the p95 of per-query `exact_hits - code_hits`
   plus sub-90 counts. A failure stops sole-scorer work; a pass cannot
   establish 1M fidelity or route selectivity. The existing 100k actual-read
   page-containment evidence remains a separate baseline.
2. **1M offline physical and quality replay.** Use the same 1,000
   ReLAION-1M **development** queries and truth roster, frozen corpus-only
   k-means layout and old bit-exact records, permuted by authenticated
   source ordinal. First prove corpus IDs, row vectors, rotation seed and
   record bytes match the V63 layout source; if they do not, rebuild codes
   from authenticated source under a new committed revision. No new S3
   serving read is needed. Fix the page
   summary encoding/count, hierarchy breadth, row-code family, shortlist
   sizes and minimum-byte physical range admission *before* opening truth.
   Run flat and hierarchical PQ16 on identical pages; compare PQ24, PQ32
   and PQ64 under identical caps and layouts. PQ64 is a high-memory control,
   never a memory-qualified product selection. Report pre-budget shortlist
   page containment, post-budget containment, returned Recall@100 with
   paired exact and two-bit scores on the same fetched rows, p05, sub-90,
   planned GETs and bytes for all 1,000 queries, and an authenticated
   transfer-floor/mean-byte certificate. Exact-score arms isolate scorer
   loss; flat-versus-hierarchical arms isolate coarse-filter loss. Use a
   feasible truth-aware page cover only as a *lower bound* on the optimum;
   only a proved ceiling below target could kill the physical layout.
   Advance a specific route only if two-bit returned Recall@100 ≥99.0%,
   p05 ≥90, paired mean loss ≤0.25 point, all planned caps hold and the
   conditional NIC ceiling permits the preregistered throughput target.
   A PQ64 failure rejects this fixed routing/planner family, not every
   possible route on the layout. This reused development split may decide
   architecture; it never validates generalization.
3. **Scale and rollover before serving spend.** Replay 10M selectivity
   offline with frozen route and caps. V82 measured 98.430% at 10M on
   deep-image-96-angular versus 99.155% at 1M on ReLAION-768; that
   cross-dataset comparison cannot isolate a scale effect and cannot justify
   extrapolation. For 100M, account for page summaries in their
   *served* decoded form, row codes, positional ID/page maps, graph,
   liveness, allocators, query buffers at declared concurrency, delta
   overlay and the 64-MiB process allowance. One 16-byte code per 100M
   rows is 1,600,000,000 bytes; two copies are 3,200,000,000 bytes, which
   **with** 64 MiB exceed 3 GiB before metadata. Specify codebook lifetime,
   stable addressing, maximum delta size and a bound on pinned generations.
   Show safe shared codes or a narrower width by construction and measured
   peak. Count visited summaries and row codes as N grows; a full page scan
   is O(N) even if the row scan is local. Failure stops serving promotion.
4. **Native serving qualification.** First prove native/reference score and
   stable-ID agreement on development queries, including ties and live
   deltas. Then use a *new preregistered untouched query cohort* for real
   authenticated S3 reads, reserving the existing sealed holdout for a
   frozen product. The old validation split was opened by V75 and V85.
   Freeze numerical returned Recall@100 ≥99.0%, p05 ≥90, GET/byte caps,
   p50/p95/p99, sustained QPS, failure-rate and cost bars against the
   strongest same-workload BORSUK baseline *before* the run. Report split,
   sample count, hardware, client reuse, concurrency, successful and failed
   queries, attempted GETs, received bytes and retries. A range failure
   returns an explicit query error, never a partial top-100 result. Bound
   deadlines and retry attempts; injected timeout, truncation, digest,
   mapping and mixed-generation faults must fail closed. No retuning on
   this cohort. Passing serving permits frozen 10M/100M measurement and
   mutation/recovery product gates.

All new heavy cells use one immutable Spot attempt from an exact pushed
source archive. Seal query-only plans before reading truth. Publish a
terminal marker, authenticate its complete artifact roster and independently
replay the decision. Discard and restart interrupted measurement cells under
a new attempt ordinal, sync terminal repetitions to S3 and terminate compute
immediately after a terminal marker. Monitor incomplete work by terminal and
infrastructure state only.

## Formal boundary

`formal/SourceRangeFidelity.lean` already proves the 200-byte row arithmetic,
the 16-MiB row ceiling and conditional finite-cohort truth-hit implications.
`formal/Opq8Planner.lean` proves conditional request/byte and latency
implications for modeled planners. A new single-wave refinement should prove
that the implemented range planner's charged byte/GET totals equal the
authenticated physical plan and that returned IDs are a subset of fetched
live rows. Given authenticated per-query truth masks, this yields a finite
cohort recall ceiling and, with score-margin assumptions, a lower bound.
None of these theorems can establish a distribution's future recall, S3
latency, machine throughput or generation-memory peak without empirical
premises. Those remain explicit gates above.

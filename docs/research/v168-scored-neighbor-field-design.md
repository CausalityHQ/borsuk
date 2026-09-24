# V168 design: score candidate neighboring rows before admission

Status: **design candidate, not a frozen experiment or production default**.
This note chooses the next falsifier after V167; the exact cohort, code,
and gates must be frozen in a separate preregistration before any new
measurement cell.

## Intended outcome and evidence

The product needs one generic recall/resource policy that works across
corpora and scales without a vector-count memory knee. Higher requested
recall may require more resident code, source memory, GETs, or bytes;
the system must estimate and charge those resources rather than silently
lower quality. The policy must retain exact-primary rows, authenticated
generation semantics and deterministic physical budgets, then beat the
strongest matched BORSUK baseline on returned quality and practical
latency/cost before any production default is frozen.

V167's closed ReLAION-1M source-pseudoquery fit128 result is decisive
against stopping based only on 513/1 votes. Its `1/1` plan retained the
same vote score as V165 on all 128 queries yet lost eight useful SQ8
non-nominee rows, all in seven unvoted units adjacent to nominee-voted
units. GETs rose 63.95% while bytes fell 42.87%. V166's isotropic
unit-moment model previously assigned too little mass to rare useful
units. The missing input is row-level query evidence for units that did
not contain a top-512 nominee, plus an explicit GET/byte tradeoff.

## Alternatives considered

| Approach | Decision and reason |
| --- | --- |
| Retune V167 fraction or add a primary-only fixed halo | Reject. `1/1` already loses eight rows; four missed rows were adjacent only to secondary-voted units. A fixed halo has no measured cost and V138/V137 already warned against post-hoc halo adoption. |
| Flat center/radius containment | Reject as primary selector. V138 admitted a median 3,084 of 3,125 D96 units and nearly all pages. Its soundness proof does not establish selectivity. |
| Reuse V98's bounded hierarchy unchanged | Reject. Its containment was 98.518% R@100, but downstream exact page/shortlist selection fell to 83.302%; a new page admission rule would be required. |
| Exact far-neighbor lists for every row | Defer. Query signal could be strong, but all-row construction and update cost at 100M are unbounded by current evidence. |
| Direct candidate-row PQ score field | **Select for the cheapest representation falsifier.** The existing V115 code plane already holds 64-byte row codes. Scores can be computed for every row of nominee units and their immediate physical neighbors, including rows ranked outside the top 512. |

## Selected first stage

Keep the frozen source-only physical order, V115 PQ64 router and exact
SQ8 primary roster for a representation screen. After 512 nominees are
identified, enumerate their 32-row physical units plus adjacent units.
Read the resident PQ64 code of every row in that bounded candidate set
and compute its query ADC score using the same V115 table arithmetic.
The plan may then assign positive evidence to a unit even when it has
no nominated row. Do **not** fetch its remote SQ8 range to compute the
score; that would invalidate the resource claim.
The current exact-primary experimental route uses a local SQ8 mirror;
its resident or storage cost and latency must be admitted explicitly
before a production profile can use it.

The first screen should compare direct row-score order and a
source-calibrated exceedance estimate against actual SQ8 threshold
crossings on **fresh** source pseudoqueries. It must separate units
with nominees from adjacent-only units. Fitting can use the index's
source rows but neither external validation truth nor the V166/V167
source IDs. A new disjoint holdout then decides whether row scores
discriminate the rare adjacent positives. If the signal is absent or
fails transfer, stop this representation and change layout or router;
do not tune against the used V167 fit or burn its sealed holdout.

The candidate-row work has a deterministic upper bound after a 512-row
roster: at most `3 × 512` 32-row units, at most 49,152 row codes, and
at most `49,152 × 64 = 3,145,728` PQ table lookups per query. This is
independent of corpus size **conditional on obtaining the roster**.
The current flat V115 summary search itself is not bounded well enough
at 10M/100M; V121 measured 184.76 ms/query offline at D96 9.99M.
V150/V151 graph attempts exposed page-discovery and duplicate-work
risks, while V154/V155's cached sparse route passed a used 1M CPU and
returned-quality gate. A bounded router must be qualified separately
against that strongest route before scale promotion.

One complete resident PQ64 plane costs exactly `64N` code bytes, or
6.4 GB at 100M rows; two complete generations cost 12.8 GB before
graph, summary, allocator and concurrency overhead. These are payload
projections, **not measured charged RAM**. A per-row residual-energy
byte would add 0.1 GB per generation at 100M if its discrimination
benefit justifies it. The user-facing memory limit is a resource
choice coupled to recall and corpus size, not a hard 3-GiB or 100M
switch. Moving codes to NVMe is a separate measured latency/cost option,
not an assumed equivalent to resident RAM.

## Scheduling and qualification sequence

The score field is only a representation. A scheduler must retain every
exact-primary unit and choose contiguous ranges using **both** encoded
bytes and GET count. V167 proved that minimizing bytes before GETs can
inflate GETs even at equal vote score. A candidate scheduler should
form a Pareto frontier or minimize a measured per-GET plus per-byte
cost under a fixed source-trained quality target, with the 32-GET and
16-MiB hard caps checked separately. The per-GET weight must come from
matched storage measurements or an explicit caller resource profile;
it may not be tuned by dataset name or N.

The first external quality gate is paired ReLAION-100k D768
development-1000 against V163's 99.322% returned Recall@100, p05 98,
15,657,408,000 planned bytes and 15,275 GETs, with V160's stronger
15,562,734,720-byte/9,894-GET resource point shown. The 1M gate must
pair against **V155** on the same used validation-1000 cohort:
99.567% exact-source returned Recall@100, p05 98,
11,134,007,040 planned bytes and 22,126 GETs. A 100k pass does not
freeze a default; fresh held-out quality, live S3 latency/cost, charged
RAM and 10M/100M build/query gates remain mandatory.

A recursive 32-row-leaf layout and leaf-centroid beam route are possible
later architecture changes, not assumptions in this first gate. Fable's
suggested under-one-hour 100M tree-build duration is an **unverified
estimate**; the fanout, build work, page locality, primary capture and
tail latency would need their own gates. The previous V98/V150 failures
prevent treating hierarchy alone as a solution.

## Formal boundary

Lean can prove conditional bounds on `64N` code payload, candidate-row
lookup count, whole-unit bytes, GETs and primary retention once the
implementation is refined to authenticated counters. Given a certified
per-row score-error interval, it can also prove threshold inclusion or
exclusion for a row. Expected captured row count is additive under
valid per-row probabilities; independence is not needed for linearity
of expectation. Those probabilities, a bounded-work router's candidate
recall, real query distributions, p95 latency and actual charged memory
remain empirical obligations. A proof with data assumptions must state
and validate those assumptions; it cannot replace these gates.

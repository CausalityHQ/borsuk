# Selected 1M routing architecture and final-wave diagnostic

Status: routing decision selected from terminal-closed 100k actual reads and
1M source-only evidence. Source-vector scoring passed the 1M final-range
diagnostic. Sign96 failed its 100k paired row-score gate by 65 GT100 hits;
one standard PQ96 rescue is the next frozen screen. No compressed production
row scorer or end-to-end 1M read has passed. This is a research contract,
not a released index format.

## Sealed route

- Dataset authority: ReLAION source, frozen physical page membership, query
  cohort and truth identities in `scripts/native_one_million_selector_cell.py`.
- Source-trained OPQ8 model: the exact model authenticated in the 100k gate;
  1M source rows encode to one eight-byte code per physical row. A format
  marker, model digest, source digest, physical-order digest, membership
  digest and group manifest bind a generation. Incompatible generations
  fail closed.
- Score all physical row route codes with the fixed eight-table ADC
  expression. Rank the 910 adjacent eight-page groups by the mean of their
  four smallest row scores, then minimum score and physical group ordinal.
  Reject nonfinite scores.
- Admit ranked groups with the existing adjacent-range planner. Physical
  neighbors of the same object role merge to one GET; enforce 32 GETs and
  16,777,216 total encoded bytes per code wave, including group headers.
  Selection may skip a group that does not fit and continue.
- For every query, authenticate planned code-object ranges against a sealed
  manifest and decode rows in exact physical page order. Final data-range
  planning is established only for source-vector scores; production row-score
  quality and actual reads remain open gates.

At 1M, source-only OPQ8 containment was 98,985/100,000 GT100 positions
with p05 95, against 98,151 and p05 89 for the paired page-centroid route.
The OPQ8 group plan read no code object in that gate. Its measured 100k
actual-read win used a 200-byte two-bit scorer; it does not validate a
96-byte 1M row representation. Preserve both outcomes as separate claims.

## Deferred physical code object

The passed 1M planner budgeted a possible 96-byte-per-row PQ96 object.
Construction remains deferred until its compressed-score gate is frozen.
The proposed candidate is 96
eight-bit codes for 96 contiguous eight-dimensional subspaces of a
768-dimensional vector, with one source-trained 256-centroid book per
subspace. Train without development queries or truth. The group body is a
header with page count and per-page row counts followed by page-ordered
records, with byte offsets, lengths and SHA-256 digests in a sealed
manifest. Give this layout a new incompatible format marker. The exact
training seed, sample, iterations, preprocessing and scorer must be
preregistered before source construction; they are not frozen by this
route decision.

If that future gate passes, use the same row-code bytes and data-range
rule for OPQ8 and page-centroid arms. Only code-group selection changes.
The closed source-vector diagnostic is an upper-quality target for that
compressed scorer; it does not promote the failed 8-byte row scorer.

## Scale and proof boundary

`formal/Opq8Planner.lean` proves abstract count/byte limits, conditional
truth-owner certificate aggregation, `8N` route-plane bytes, and `8N`
table lookups for a full row scan. At 100M, two resident route generations
and the current full-array NumPy scorer require at least 3.2 GB under the
specified simultaneous-array assumptions, exceeding the 3-GiB campaign
cap with margin. Chunking addresses scratch memory but leaves 800 million
table lookups per query. A measured region or hierarchical route that
examines fewer rows, plus production implementation refinement, is
required before 100M qualification.
Network latency, finite-precision score margins, actual recall and serving
throughput require authenticated data premises and measurements. They are
not consequences of the current Lean file.

The formal route should use explicit, checkable premises. For a finite
query cohort, authenticate the source, query and exact top-k truth IDs,
row-to-page map, sealed range plans and executable-to-model refinement;
then a Lean certificate can prove every counted owner-page hit and a
cohort recall lower bound. An unseen-query recall claim additionally
needs a stated query distribution, an untouched independent sample and
a concentration argument; a development-cohort certificate alone does
not imply it. The existing latency theorem composes per-request and
local-compute upper bounds with the 32-GET/16-MiB budget; those service
bounds must be supplied for the deployment and cold/warm cache state.
The 8N full-scan lookup proof is a linear-work result, so sublinear
scaling needs a new certified search structure and a refinement proof.
`formal/SourceRangeFidelity.lean` adds a checked finite-cohort fidelity
budget: at most 769 GT100 and 28 GT10 source-score hit losses imply the
aggregate fixed gates from the closed 98,920/9,956 source baseline.
Per-query p05 and sub-90 certificates, authenticated mask generation,
and implementation refinement remain separate proof obligations.

## Final-wave decision after the 1M range gate

The terminal-closed truth-aware 32-page upper bound killed the fixed
final-wave contract: OPQ8 can cover at most 86,474/100,000 GT100 with
p05 61 under 32 pages. The original source-vector page-nomination Stage B
and PQ96 construction must not run under that cap. The page-byte budget
was not the blocker: the best 32 truth pages used at most 6,451,712 bytes.

The terminal-closed 1M gate replaced the page-count rule with at most 32
contiguous data-page GET ranges and 16,777,216 encoded bytes, keeping the
sealed group plans and original physical layout. The OPQ8 score-driven
candidate retained only 91,282/100,000 GT100 positions, below the paired
page-centroid control's 91,541 and the fixed 98,151 candidate floor. Its
p05 was 64. The query-only selector was killed; the full evidence and
immutable attempt identity are in
`docs/research/algorithm-first-page-layout-ledger.md`. The earlier
truth-aware interval witness reached 98,935 GT100, p05 94 and 19 sub-90
queries under the same caps; this remains an existence witness only.

The terminal-closed source-vector diagnostic held those plans, layout,
interval rule and caps fixed and reached 98,920/100,000 GT100, GT10
9,956/10,000, p05 94 and 19 sub-90 queries. The paired control reached
98,140 GT100. Independent closeout authenticated all 15 artifacts and
recounted every truth mask; the complete attempt is in the ledger.
Only 65 candidate truth positions inside selected groups missed final
ranges, versus 7,703 with OPQ8 row scores. This identifies row-score
fidelity as the dominant loss under this frozen planner. The source
scan itself is not a production candidate: it reads the entire source
corpus and measured no serving latency.

The next gate is a preregistered 96-byte row-score comparison. Its primary
candidate is a rotated sign record with 752 sign bits (94 bytes) and one
binary16 row scale (2 bytes); standard PQ96 is a predeclared rescue only
if sign96 fails its 100k screen.
The sign96 attempt has now failed its paired 100k gate: 99,496 GT100 hits
versus 99,861 for exact source scores, a loss of 365 against the fixed 300
limit. The terminal-closed and independently read-back result is recorded
in `docs/research/algorithm-first-page-layout-ledger.md`. Do not promote
sign96 to 1M or tune it on these development queries. The PQ96 rescue is
separately sealed and uses the same selected groups, layout, source control,
range rule and 100k thresholds.
The earlier suggestion to keep both a binary16 scale and a binary16 norm
alongside 752 signs would require **98 bytes** and cannot satisfy the
frozen 96-byte projection. The sign scorer's promise is a hypothesis from
the paired 100k two-bit result, not a measured 96-byte result. First reject
inadequate score fidelity on a cheap 100k source-only screen; then, only
for survivors, use the frozen 1M group plans, original page layout,
page-priority rule and 32-range/16-MiB data-wave rule for a paired source-only
score gate against this source-vector result and the failed OPQ8 score
baseline. Each arm derives its own priority and ranges from its own row
scores; only group selections and algorithms are shared. Do
not substitute the old 32-page cap. The sealed source plans preserve page
priority order without numeric page-score gaps, so a gap certificate
requires another exact-score pass and cannot be read from those plans.
A passing compressed scorer can proceed to actual authenticated code and
data reads on a fresh untouched query cohort, then measured serving
latency, throughput and resource checks. At 10M/100M, replace the linear
full route scan with a measured hierarchical or region route before scale
qualification.

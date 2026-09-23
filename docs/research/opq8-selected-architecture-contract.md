# Selected 1M routing architecture and next code gate

Status: routing decision selected from terminal-closed 100k actual reads and
1M source-only evidence. This is a research contract, not a released index
format or a claim that 1M final-page quality has passed.

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
  manifest, decode rows in exact physical page order, and nominate final
  data pages under the existing 32-page/16-MiB data-wave cap. This last
  stage is the next decision gate, not an established 1M outcome.

At 1M, source-only OPQ8 containment was 98,985/100,000 GT100 positions
with p05 95, against 98,151 and p05 89 for the paired page-centroid route.
The OPQ8 group plan read no code object in that gate. Its measured 100k
actual-read win used a 200-byte two-bit scorer; it does not validate a
96-byte 1M row representation. Preserve both outcomes as separate claims.

## Candidate physical code object

The next gate proposes one 96-byte-per-row PQ96 object because the passed
1M planner explicitly budgeted 96-byte records. The candidate is 96
eight-bit codes for 96 contiguous eight-dimensional subspaces of a
768-dimensional vector, with one source-trained 256-centroid book per
subspace. Train without development queries or truth. The group body is a
header with page count and per-page row counts followed by page-ordered
records, with byte offsets, lengths and SHA-256 digests in a sealed
manifest. Give this layout a new incompatible format marker. The exact
training seed, sample, iterations, preprocessing and scorer must be
preregistered before source construction; they are not frozen by this
route decision.

Use the same PQ96 bytes, row scorer, page nomination and data-wave rule
for OPQ8 and page-centroid arms. Only the code-group selection changes.
Also score source vectors at float64 precision within each arm's selected
rows as a diagnostic to separate code representation loss from group
containment and page-cap loss. The diagnostic cannot promote a failing
production scorer. Its finite precision is recorded; it is not an
exact-real arithmetic oracle.

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

## Exact next decisions

First replay the sealed 1M plans against physical truth-owner pages and
the 32-page/16-MiB data cap. Calculate a truth-aware 32-page upper bound
and a source-vector page-nomination diagnostic for each route. If even
the upper bound or source-vector nomination misses the fixed page-quality
gate, revise the page budget or nomination architecture before training
a new code format.

If that cheap gate passes, build and seal one real 1M PQ96 code object, replay the two sealed 1M
group plans over authenticated S3 ranges, and measure paired final-page
GT10/GT100, p05, sub-90 tails, actual GET/byte counts, code-score loss,
data-page caps and phase RSS/swap. Record per-request and per-query timing
with a disclosed scheduling policy, separately from sequential campaign
wall time. The attempt runs on Causality Spot with create-only source and
terminal artifacts; interrupted cells restart from the same frozen source
revision, and compute terminates on terminal closure. Advance the route
only if the preregistered paired final-page quality and resource gate
passes. A later production serving latency/throughput gate is required.

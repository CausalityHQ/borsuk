# One-million page-representative selector screen

## Decision

The terminal-closed eight-page single-centroid selector at source
`6c81003e879eff4e741c9fc4855e47a7562d51b2` reached only 86.981%
mean GT100, 59% p05 GT100 and 89.34% GT10 containment on the frozen 1,000
ReLAION-1M development queries. It selected 32 groups and projected at most
8,340,352 code bytes. Its source and independent replay were authenticated;
the result kills only the single-centroid routing signal. The next cheapest
question is whether preserving the individual page geometry inside each
eight-page group repairs the miss before building a 200-MB two-bit code plane.

## Fixed source-only representation and selector

Use exactly the seven immutable ReLAION-1M inputs and their encoded
length/SHA-256 identities from
`docs/superpowers/specs/2026-09-23-rotated-two-bit-one-million-bridge-design.md`.
Parse the V85 base and delta generation's 7,278 pages in physical order and
retain the same consecutive, role-separated eight-page grouping, including
each role's short last group. Every live source ID must occur in exactly one
V85 page and exactly once in the source parquet; reject extra, missing,
duplicate, non-live or reordered page rows. Source vectors are streamed in
batches no larger than 4,096; no full 1M float32 matrix is resident.

Train one 768D page centroid from the exact float32 source vectors belonging
to each page, using float64 sums in source ordinal order and rounding once to
little-endian float16. Store all 7,278 page centroids (11,179,008 bytes) and
their ordered page-to-group map in a new incompatible source-only seal. The
single-centroid group artifact is not read. Query/truth objects become
available only after the new seal and arrays have been uploaded with
create-only S3 writes and read back byte for byte.

For each of the same 1,000 frozen development queries, score all page
centroids by squared L2 in float64. A group's score is the minimum score of
its member pages. Rank 910 groups by `(minimum score, role, group ordinal)`
and take exactly 32. Do not weight or average page scores, change group width,
reduce centroid dimensions, tune precision, or add a second routing arm on
these queries. The selected groups' projected code bytes are the same fixed
`sum(200 * live rows + 4 + 4 * page count)` formula used by the first screen.
Record ordered groups, GT10/GT100 membership and projected bytes for every
query. An independent validator reparses source/page membership, recreates
every centroid byte and query plan without calling the producer scorer, and
compares canonical evidence and aggregate metrics.

## Capability, resource and decision gate

Run on one Causality `c7i.8xlarge` Spot instance with a fresh immutable prefix.
Its construct phase has no query/truth files or network access; evaluation
runs as `nobody` in a network namespace after source-only sealing. Preserve a
terminal on failure or interruption, use a new attempt prefix if restart is
needed, and terminate the instance at terminal. Construction, evaluation and
independent validation each require peak RSS plus a 64-MiB wrapper allowance
below 3 GiB and zero swaps. The selector screen issues no code or final-page
Range GETs, so measured S3 serving latency is outside this decision.

The fixed advance thresholds are selected-group mean GT100 ≥97.5%, p05
GT100 ≥90%, GT10 ≥96%, at most 32 projected code GETs and at most
16,777,216 projected code bytes on every query. Any identity, independent
replay or resource mismatch is a provenance/implementation failure. A
quality or projected-budget miss is `page-representative-selector-killed`;
stop before building the code plane. A pass is
`page-representative-selector-feasible` and authorizes the full 1M two-bit
code-group and adjacent final-range serving cell, subject to its original
separate gates. Neither outcome freezes a production format or validates the
two-generation 100M resident-memory budget. A 768D float16 page table scales
linearly and may need a later compression gate even if this selector passes.

Read only terminal/infrastructure health while the campaign is incomplete.
After closure, collect controller exit and instance termination, read back all
terminal-listed artifacts by encoded length and SHA-256, independently sum
all 1,000 samples and record the decision in the research ledger. Treat prior
100k and single-centroid artifacts as immutable historical evidence.

# One-million geometric code-group order screen

## Decision and authority

The terminal-closed PQ80 projection did not move the 1M p05 tail relative to
PQ96: 51 of 1,000 queries remain below 90 GT100 hits even though 30 more
median groups fit. This identifies a failed score/order/planner combination,
not a unique cause. A separate truth-ranked diagnostic on the same physical
groups reached 100% p05 with 32 GETs and 16 MiB; it is only a capacity
diagnostic and cannot select a production route.

Test one material change to physical adjacency: reorder whole existing
eight-page groups by source-trained geometric proximity. Keep each group's
rows, pages, size and base/delta role unchanged. Keep the page-centroid query
score, stable logical-group ties, 96-byte projected rows, and 32-GET /
16,777,216-byte greedy planner unchanged. The new order is a proposed code
object layout, not an existing S3 object or a PQ96 quality result.

## Fixed construction

Use the exact seven frozen source/development object identities in
`scripts/native_one_million_selector_cell.py`. Rebuild and authenticate the
prior page-centroid artifact before query/truth download; require centroid
SHA-256 `757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
membership `55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and page order `bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`.

For two groups of the same role, define distance as the minimum squared L2
distance between their existing float16 page centroids, promoted to float64
for arithmetic. Separately for base and delta, start from original ordinal
zero and repeatedly append the nearest unvisited whole group. Break exact
distance ties by original ordinal. No alternative start, reversal, neighbor
count, or parameter sweep. Seal the two permutations, prior source seal hash,
distance rule and resulting physical group lengths before exposing queries.
There is no cross-role adjacency or read interval. The permutation must be a
bijection within each role.

## Evaluation and independent replay

For each query, rank logical groups with the existing minimum page score and
stable `(score, role, original ordinal)` ties. Translate that complete rank
to the sealed physical order, then run the unchanged one-pass adjacent-range
planner on projected 96-byte group lengths. Map selected physical groups back
to logical IDs to count GT10/GT100 containment. Record the full ordered plan,
physical intervals, GETs, bytes, and per-query hits. Also reproduce the
closed PQ96 original-layout baseline from the same inputs: 98.151% mean
GT100, 89% p05, 99.28% GT10 and 51 queries below 90.

An independent validator reconstructs the two permutations from the sealed
source centroids without invoking the producer constructor, recomputes the
logical ranking and physical admissions, and verifies all 1,000 rows,
aggregate metrics and baseline. Source construction and permutation sealing
must finish before query/truth access. Evaluator is unprivileged and without
network. Artifacts are create-only with readback; terminal and infrastructure
health alone are observed while incomplete. Every phase has process-tree
RSS plus 64 MiB below 3 GiB and zero swap. Use one Causality Spot attempt;
terminate immediately at terminal, and discard/restart an interrupted cell
under a new attempt prefix.

## Stop gate

Advance this layout only if mean GT100 >=97.5%, p05 GT100 >=90%, GT10 >=96%,
and paired means do not regress below the exact PQ96 baseline (98.151% GT100,
99.28% GT10). At most 49 queries may have fewer than 90 GT100 hits. All
queries must stay within 32 GETs and 16,777,216 projected bytes. Any valid
miss kills this one permutation; do not tune it on this cohort. A pass
authorizes a preregistered paired **100k actual PQ96 code-quality/read/
resource** gate against the successful 200-byte rotated two-bit control,
then fresh holdout and 1M code-plane qualification. Neither arm is product
or serving evidence until real row codes and S3 reads are tested.

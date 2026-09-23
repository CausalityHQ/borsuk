# One-million adjacent code-range selector screen

## Decision and frozen evidence

The authenticated ReLAION-1M page-representative screen at source
`7ac8fc842c5cfd5fd6701bdd3bd1f3d04f6dc17c` reached 93.213% mean
GT100, 71% p05 GT100 and 96.26% GT10 containment using 32 eight-page
groups. Its worst projected code wave was 9,108,752 bytes, leaving 7.67 MiB
of the 16-MiB limit unused while consuming all 32 GETs. The page-centroid
signal is still fixed and source-only. This screen asks whether contiguous
code-group Range GETs can spend the remaining bytes on additional ranked
groups without increasing the GET count or changing representation.

## Source-only plane and fixed planner

Use the same seven authenticated V98/V85 ReLAION-1M input identities, page
order, source-trained 7,278 float16 page centroids, 910 role-separated
eight-page groups and 200-byte projected row records defined in
`2026-09-23-one-million-page-representative-selector-design.md`. Rebuild the
source-only page-selector seal with the new frozen source revision; its
centroid, membership and page-order SHA-256 values must equal
`757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
`55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and `bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`
respectively before any query/truth download. This binds
the new decision to the same routing representation while avoiding a
posthoc score change.

For each of the same 1,000 development queries, score every page centroid
by squared L2 in float64, give each group its minimum page score and rank
all groups by `(score, role, group ordinal)`, exactly as before. Starting
with no groups, scan that complete fixed order once. A candidate group is
admitted if the union of selected groups after adding it has at most 32
maximal contiguous same-role intervals and the sum of their exact group
payload lengths is at most 16,777,216 bytes. Otherwise skip that group and
continue the scan; a later group can be admitted if it touches an existing
interval or bridges two intervals. A group's payload length is exactly
`4 + 4 * member page count + 200 * live rows`. There are no gaps, padding,
cross-role merges, rescoring, quotas, lookahead, replacement, repeated
passes or tuned stopping parameters. Each maximal interval corresponds to
one projected S3 Range GET in a future contiguous code object.

Record the full ordered admitted-group list, final disjoint interval list,
projected GETs/bytes and GT10/GT100 containment for every query. An
independent reducer must rebuild the source-only artifacts and recompute
the page scores, greedy admission, intervals, group membership and every
sample without using the producer planner. It must reject any difference
in selected groups, intervals, bytes, hits or aggregate metrics.

## Resource and stop gate

Run one immutable Causality Spot attempt. Construction sees only the five
source-side objects; source seal upload/readback precedes query/truth
availability. Evaluation runs unprivileged without network access. Worker
and controller use the existing interruption terminal recovery, artifact
readback, 3-GiB per-phase RSS gate with a 64-MiB allowance and zero-swap
check. An interrupted cell is noneligible and may restart only under a new
attempt prefix. Do not inspect incomplete measurement artifacts.

The fixed quality gate remains mean GT100 ≥97.5%, p05 GT100 ≥90% and GT10
≥96% on all 1,000 development queries, with ≤32 projected Range GETs and
≤16,777,216 projected bytes on every query. A valid miss is
`adjacent-code-range-selector-killed`; do not build the 1M two-bit code
plane. A valid pass is `adjacent-code-range-selector-feasible` and only
authorizes the separately preregistered full 1M code-read and final-page
serving cell. This projection is not an actual S3 read, latency result,
production format or two-generation 100M resident-memory proof. Compare
against the prior page-selector screen as historical diagnostic evidence;
publication quality requires a fresh frozen holdout after architecture
selection.

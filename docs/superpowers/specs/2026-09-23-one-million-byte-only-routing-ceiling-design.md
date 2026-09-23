# One-million score-ranked byte-only routing ceiling

## Diagnostic question

The completed adjacent-range selector at source
`1626e78507dbc6531e9e9ff3b51ec79459f26564` passed mean GT100
(97.541%) and GT10 (98.96%) but missed p05 GT100 (86% versus 90%). It
selected 68–89 groups under 32 contiguous code-range GETs and used almost
the entire 16-MiB projected byte budget. Before changing code-group physical
order, test whether the unchanged page-centroid ranking and 200-byte row
format could pass p05 if every group were individually addressable. This
is a diagnostic ceiling for **score-order-preserving** layout changes, not
an operational candidate or a truth-aware oracle.

## Frozen method

Rebuild and authenticate the same source-only page-centroid, membership
and physical-page artifacts from the seven frozen ReLAION-1M identities.
Before downloading query/truth, require the previous completed screen's
centroid, membership and page-order SHA-256 values:
`757fbe7c6b2112ea5904a5bba0e26fb4ac929cc39ca1fc470b12dee35b61a704`,
`55e36613b293ffc08c11b9da8c2f2293cf89e43310551f316f2c5b57a84e0d13`,
and `bb8ebb3642de174a338621f08d4a739b265d91914eb25ebe41831ffed0f33b4b`.
Use the same 910 eight-page group payload sizes and rank all groups for each
of the same frozen 1,000 development queries by minimum page-centroid
squared L2, with `(score, role, group ordinal)` ties.

Scan the complete ranked order once. Admit a group if its exact
`4 + 4 * member pages + 200 * live rows` projected payload fits the
remaining 16,777,216-byte code budget; otherwise skip and continue. Ignore
GET count and adjacency in this diagnostic. No replacement, lookahead,
training on queries, score change, group-width change or tuned byte cap is
allowed. Record ordered groups, hypothetical singleton GET count, exact
projected bytes, GT10/GT100 containment and aggregate nearest-rank p05.
An independent reducer must rebuild the source-only artifacts and replay
every score, byte admission and sample without calling the producer planner.

## Decision and limits

Run one immutable Causality Spot attempt with source/query phase separation,
unprivileged networkless evaluation, a terminal on worker or controller
failure, immediate instance termination, all-artifact readback, 3-GiB
per-phase RSS plus 64-MiB allowance and zero swap. Read only terminal and
infrastructure health while incomplete.

If this byte-only score-order schedule misses mean GT100 ≥97.5%, p05 GT100
≥90% or GT10 ≥96%, classify `score-ranked-byte-ceiling-fails`: no physical
layout can make this exact score-ordered byte-only group set pass while
keeping the same row width and cap. A pass is
`score-ranked-byte-ceiling-feasible`: it merely permits a separate
source-only locality-layout design. The diagnostic may require far more
than 32 GETs and cannot authorize a code-plane build or serving claim.
Fresh frozen holdout, actual S3 reads, native final-page serving and the
two-generation 100M memory gate remain required for production.

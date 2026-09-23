# V112 precise nominee evidence ceiling

Status: preregistered offline diagnostic, 2026-09-23. No result exists yet.
This uses the already analyzed ReLAION-1M **development** first 200 as a
development filter, not a fresh quality claim. A later passing candidate
must be tested on the remaining development queries and untouched
validation queries before promotion.

## One decision

V110 proved that the present V63 whole-page layout can physically contain
all 20,000 GT100 positions in the first 200 queries under 32 GETs and
16,777,216 SQ8 bytes. V109's capped query route contained 19,826 and
returned 19,739; V111's exact optimizer using top-512 nominee **counts**
contained 19,791 and returned 19,707. All 19,921 GT positions on the
PQ64 top-512 nominated pages can fit the caps with truth-aware page
selection. The missing query-time information is therefore page value.

Test whether **precise SQ8 scores for just the 512 nominated rows**
provide enough page value to clear the current quality gate. This is a
ceiling for any proposed resident reranker whose row order aims to
reproduce those SQ8 scores. Obtaining these scores from S3 in a serving
query would add an I/O wave, so the diagnostic is not a production reader.
It does not rule out a representation trained for source-float ranking
if the SQ8-like ceiling fails.

## Frozen paired cell

- Pushed source archive and one Causality Spot attempt, with input SHA-256
  identities copied from V109/V111: ReLAION-1M source, development
  queries and GT100, V63 physical order, V70 SQ8 object and regenerated
  V77 manifest. Spot interruption discards the attempt; terminal
  artifacts are synced before instance termination.
- Reproduce V109's two same-query controls exactly: uncapped V77
  19,832/20,000 returned hits and V109 greedy capped 19,739/20,000,
  with their GET/byte figures. A mismatch is a harness failure.
- For each query, retain V77's 1,024-region page route and PQ64 top-512
  row nomination. Look up only those nominated rows in the authenticated
  local SQ8 object and calculate the identical SQ8 final score. Sort by
  score and stable ID. Give each nominated row in this SQ8 top 100 a
  primary page vote; give each of the other 412 nominated rows a
  secondary vote. Optimize intervals lexicographically: maximize primary
  votes, then secondary votes, under 32 GETs/16 MiB. This can be encoded
  as `513 * primary_count + secondary_count` per page because total
  secondary votes are at most 512. The physical planner and final SQ8
  scorer are otherwise V111's unchanged algorithms. No GT100 value
  enters nomination, voting, planning or scoring.
- The reducer independently verifies top-512 nomination, SQ8 nominee
  scores, weighted optimum and witness, GET/byte bounds, returned-ID
  membership and GT100 hits. Report **physical containment** separately
  from **returned Recall@100**, plus p05, sub-90, GETs, bytes and offline
  resource usage. The diagnostic uses local authenticated SQ8 bytes;
  planned GETs are not live S3 latency.

## Decision rule

Stop after 200 if the candidate has fewer than **19,800 returned hits**,
p05 below 90, any query above 32 GETs or 16 MiB, or a control mismatch.
A passing first 200 permits replay of the remaining 800 development
queries to assess stability, but does **not** open validation yet: a
deployable resident reranker must first reproduce the useful page-choice
signal. If this precise SQ8-rerank ceiling fails, stop the SQ8-like
resident-rerank line and change nomination, page geometry or data-read
architecture. Do not tune the 100-row vote threshold or secondary
weight on this burned prefix.

Memory for a subsequent deployable code is a measured frontier
`M(N,R,C,G)` over collection size, recall target, concurrency and
pinned generations. A 16-byte or 32-byte extra row plane would add
`16*N*G` or `32*N*G` bytes before reserves; these are arithmetic
projections, not measured 100M peaks or a fixed 3-GiB release gate.

Lean can prove the lexicographic vote encoding and the conditional
interval byte/GET feasibility. A cohort recall bound would additionally
need authenticated premises for the top-100 estimator's GT overlap and
the SQ8 final-rank displacement. Neither unseen-query recall nor S3
latency follows from source code alone.

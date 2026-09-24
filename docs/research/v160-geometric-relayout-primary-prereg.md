# V160 geometric relayout with frozen exact-primary routing

## Decision and evidence boundary

Test whether the V158 exact-primary 100k loss is repaired by a query-blind
geometric physical order under the same 32 GET and 16,777,216-byte charged
SQ8 read envelope. V159's 77.302% fetched GT100 result used a source-file-order
V114 mirror. The separate V114/V155 1M successes used a V63 clustered layout;
they are not controls for this paired 100k cell. This is a used ReLAION-100k
D768 development-1000 **architecture screen**, never a publication or
cross-dataset guarantee.

## Frozen inputs and construction

- Authenticate the closed V114 SQ8 mirror, manifest, ordered V114 PQ64
  nominees and exact-SQ8 primary rosters, V158 terminal, frozen queries and
  GT100 using their recorded lengths and SHA-256 values. Refuse a missing
  terminal or a mismatched source identity. The source Parquet SHA-256 is
  `a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d`.
- Reuse the closed, query-blind balanced-two-means-480k membership with SHA-256
  `f72b80f1341bd64599e51a69e627f0b9a2280be4f5235a6c5995fef8eacd866c`
  (762,442 bytes), under the terminal-closed native geometric layout attempt
  `f7a09ea5df975fb6c2e36a53ed56f53492b334fd`/`a0001`. Authenticate its
  membership seal and source-ID mapping. The historical oracle and failed
  centroid/microcluster routers are prior evidence, not extra arms.
- Sort rows by `(membership.page_ordinal, membership.in_page_ordinal)` and
  re-cut that one permutation into fixed SQ8 pages. The page size is the
  largest power of two, divisible by 32, that fits 32 separate pages under
  the byte cap: `2^floor(log2(floor(B/(G*(D+12)))))`; at B=16,777,216,
  G=32, D=768 it is **512 rows**. This rule depends on geometry and transport
  budget, never dataset name, GT or a vector-count knee. Preserve each SQ8
  row's 780 bytes (stable ID, norm and codes) exactly. Bind the permutation,
  old and new SQ8 digests, quantizer, membership and method to a versioned
  construction seal before the evaluator can read GT.

## One paired measurement

Map each frozen nominee and exact-primary source ordinal through the sealed
permutation. The source-order control is V158's terminal-closed exact arm.
The new arm uses the unchanged 513/1 primary/secondary page votes, the same
weighted contiguous-interval optimization with 512-row page and 32-row
unit geometry, at most 32 GETs and 16,777,216 encoded SQ8 bytes, then the
same SQ8 scorer and top-100 ID tie rule. The candidate changes both physical
row order and page width; this gate judges their combined layout policy and
cannot attribute a gain to either component alone. Count all range
bytes and GETs, including any required authentication or retry in a later
live-S3 gate. This screen is offline planned and fetched-byte scoring; it
does not measure live S3 latency.

Plan all 1,000 queries GT-blind, write and hash a canonical plan file and
seal it with `gt_opened=false`; only then read truth. Independently replay
every plan, returned ID, GT hit and charged byte/GET count from authenticated
inputs. Publish per-query records and p05/median/p95, mean Recall@10 and
Recall@100, exact-primary distinct-page counts, physical GT coverage, and
paired wins/ties/losses. Do not optimize construction or page width using
these GT outcomes.

## Stop and next decision

Advance the relayout as a candidate only if mean returned Recall@100 is at
least 97.5%, p05 at least 90%, mean Recall@10 at least 96%, all requests
obey both caps, and the independent replay passes. The V158 exact arm
at 77.302% is the direct paired control; the historical geometric-router
94.349% returned result is a different method and only contextual evidence.
The GT-blind distinct-primary-page distribution diagnoses whether the
relayout concentrated the selected rows; it is not a separate pass criterion.
A quality failure after locality improves isolates the vote/interval
admission method for redesign. No result here changes the
row-level PQ-first ceiling of 69.066%; do not rerun PQ-first as a quality
candidate. A 100k pass only permits a separate frozen 1M transfer gate,
then fresh multi-corpus quality, live-S3 latency/cost, and two-generation
100M memory/build qualification before a production default or claim.

Run one Causality Spot attempt from a clean pushed source commit. Record
instance identity, immutable reservation/source/input hashes, terminal and
all output hashes. Sync a terminal marker even on failure, discard an
interrupted cell and restart under a new attempt, and terminate compute at
terminal. No local full suite or build is authorized while devbox swap
pressure persists.
